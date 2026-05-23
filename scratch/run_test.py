import os
import subprocess
import time
import urllib.request
import urllib.error
import json
import sys
import threading

def get_container_ip(container_name):
    cmd = ["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container_name]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    ip = res.stdout.strip()
    if not ip:
        raise ValueError(f"Could not get IP for container {container_name}")
    return ip

def send_request(url):
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.read().decode(), resp.status
    except urllib.error.HTTPError as e:
        return e.read().decode(), e.code
    except urllib.error.URLError as e:
        return str(e.reason), 500

def get_active_requests(worker_ip):
    url = "http://localhost:9901/clusters?format=json"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            for cluster in data.get("cluster_statuses", []):
                if cluster.get("name") == "arbitrage_spot_cluster":
                    for host in cluster.get("host_statuses", []):
                        address = host.get("address", {}).get("socket_address", {}).get("address")
                        if address == worker_ip:
                            for stat in host.get("stats", []):
                                if stat.get("name") == "rq_active":
                                    val_str = stat.get("value")
                                    return int(val_str) if val_str else 0
    except Exception as e:
        print(f"Error querying active requests: {e}")
    return 0

def main():
    print("=== Phase 1: Local Routing Core Load Test ===")

    # 1. Start the Go Control Plane in the background
    print("Starting Go Control Plane...")
    cp_process = subprocess.Popen(
        ["./control-plane-bin"],
        cwd="/home/suzaykid/Projects/conscious-cloud/control-plane",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Give control plane time to start and bind ports
    time.sleep(3)
    
    if cp_process.poll() is not None:
        stdout, stderr = cp_process.communicate()
        print(f"Control plane failed to start. Stdout:\n{stdout}\nStderr:\n{stderr}")
        sys.exit(1)
    
    print("Control Plane started successfully.")

    try:
        # 2. Build and run the docker-compose stack
        print("Starting Docker Compose stack (Envoy + Worker A + Worker B)...")
        subprocess.run(["docker", "compose", "down", "-v"], check=True)
        subprocess.run(["docker", "compose", "up", "--build", "-d"], check=True)
        
        # Wait for Envoy to boot
        print("Waiting for Envoy and Workers to boot...")
        time.sleep(10)

        # 3. Retrieve container IPs
        worker_a_ip = get_container_ip("conscious-cloud-worker-a-1")
        worker_b_ip = get_container_ip("conscious-cloud-worker-b-1")
        print(f"Worker A IP: {worker_a_ip}")
        print(f"Worker B IP: {worker_b_ip}")

        # 4. Seed Worker A as healthy endpoint in the control plane
        # Wait, the seed snapshot initializes with 10.0.0.1, we must immediately override it with Worker A IP
        print("Registering Worker A (On-Demand) as primary endpoint...")
        url = f"http://localhost:8082/update-eds?ips={worker_a_ip}&weights=100&healths=healthy"
        body, status = send_request(url)
        print(f"Control Plane Response: {body} (Status: {status})")
        if status != 200:
            raise RuntimeError("Failed to seed Worker A")

        # Verify Envoy can route to Worker A
        print("Verifying Envoy routing to Worker A...")
        success = False
        for _ in range(10):
            body, status = send_request("http://localhost:10000/")
            if status == 200:
                print(f"Envoy response: {body}")
                success = True
                break
            time.sleep(1)
        if not success:
            raise RuntimeError("Envoy failed to route to Worker A within timeout")

        print("Warming up Envoy connection pool to Worker A...")
        # Send requests concurrently to warm up connection pool
        def warm_worker():
            for _ in range(10):
                send_request("http://localhost:10000/")
                time.sleep(0.05)
        
        threads = [threading.Thread(target=warm_worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        print("Connection pool warmed up.")

        # 5. Start the load test in a separate thread
        print("Starting background load test (1,000 req/sec total via hey)...")
        hey_output = []
        def run_hey():
            # Concurrency = 20, QPS per worker = 50 -> 1000 QPS. Duration = 25 seconds
            cmd = ["/home/suzaykid/go/bin/hey", "-z", "25s", "-q", "50", "-c", "20", "-h2", "http://localhost:10000/"]
            res = subprocess.run(cmd, capture_output=True, text=True)
            hey_output.append(res.stdout)

        hey_thread = threading.Thread(target=run_hey)
        hey_thread.start()

        # Let the load test run on Worker A for a few seconds
        time.sleep(5)

        # 6. Execute IP Swap Sequence:
        # Step A: Add Worker B (Spot) as UNHEALTHY. Envoy active health checks will probe it.
        print("\n[Swap Step 1] Provisioning Worker B. Registering as UNHEALTHY to initiate health checking...")
        url = f"http://localhost:8082/update-eds?ips={worker_a_ip},{worker_b_ip}&weights=100,100&healths=healthy,unhealthy"
        body, status = send_request(url)
        print(f"Control Plane: {body}")

        # Step B: Wait for Worker B to pass health checks (needs 2 consecutive HTTP 200s, interval is 5s)
        print("Waiting for Worker B to pass active health checks in Envoy...")
        start_time = time.time()
        timeout = 20
        hc_passed = False
        while time.time() - start_time < timeout:
            try:
                req = urllib.request.Request("http://localhost:9901/clusters")
                with urllib.request.urlopen(req) as response:
                    lines = response.read().decode().splitlines()
                    for line in lines:
                        if f"{worker_b_ip}:8080" in line:
                            parts = line.split("::")
                            if len(parts) >= 4:
                                flags = parts[-1]
                                if "failed_active_hc" not in flags:
                                    hc_passed = True
                                    break
            except Exception as e:
                print(f"Error querying Envoy health flags: {e}")
            if hc_passed:
                break
            time.sleep(0.5)
        
        if hc_passed:
            print("Worker B passed active health checks!")
        else:
            print("Timeout waiting for Worker B active health checks to pass!")
            sys.exit(1)

        # Step C: Drain Worker A (On-Demand) by setting its health status to DRAINING
        print("\n[Swap Step 2] Setting Worker A health status to DRAINING. Connection draining initiated...")
        url = f"http://localhost:8082/update-eds?ips={worker_a_ip},{worker_b_ip}&weights=100,100&healths=draining,healthy"
        body, status = send_request(url)
        print(f"Control Plane: {body}")

        # Step D: Wait for connections on Worker A to drain gracefully
        print("Draining Worker A...")
        start_time = time.time()
        timeout = 15
        drained = False
        while time.time() - start_time < timeout:
            active_reqs = get_active_requests(worker_a_ip)
            print(f"Active requests on Worker A: {active_reqs}")
            if active_reqs == 0:
                drained = True
                break
            time.sleep(0.5)
        
        if drained:
            print("Worker A fully drained (0 active requests).")
        else:
            print("Timeout waiting for Worker A to drain!")

        # Step E: Evict Worker A completely
        print("\n[Swap Step 3] Evicting Worker A from endpoint list...")
        url = f"http://localhost:8082/update-eds?ips={worker_b_ip}&weights=100&healths=healthy"
        body, status = send_request(url)
        print(f"Control Plane: {body}")

        # 7. Wait for load test to finish and analyze results
        print("\nWaiting for load test to complete...")
        hey_thread.join()
        
        output = hey_output[0]
        print("\n=== Load Test Summary ===")
        print(output)

        # Parse success rate
        # We look for:
        # Status code distribution:
        #   [200]	25000 responses
        # If there are any non-200 responses, the test fails.
        if "Status code distribution" in output:
            lines = output.split("\n")
            non_200 = False
            found_200 = False
            for line in lines:
                if "[" in line and "]" in line:
                    if "[200]" in line:
                        found_200 = True
                    else:
                        non_200 = True
                        print(f"WARNING: Non-200 status code observed: {line.strip()}")
            
            if found_200 and not non_200:
                print("\nSUCCESS: 100% of requests returned HTTP 200! Zero connection drops achieved.")
            else:
                print("\nFAILURE: Some requests failed or returned non-200 status codes.")
                sys.exit(1)
        else:
            print("\nWARNING: Could not parse status code distribution from hey output.")
            sys.exit(1)

    finally:
        print("\n=== Envoy Logs ===")
        subprocess.run(["docker", "compose", "logs", "envoy"], cwd="/home/suzaykid/Projects/conscious-cloud")
        print("\n=== Worker A Logs ===")
        subprocess.run(["docker", "compose", "logs", "worker-a"], cwd="/home/suzaykid/Projects/conscious-cloud")
        print("\n=== Worker B Logs ===")
        subprocess.run(["docker", "compose", "logs", "worker-b"], cwd="/home/suzaykid/Projects/conscious-cloud")
        
        print("\nCleaning up resources...")
        # Terminate Go Control Plane
        cp_process.terminate()
        cp_process.wait()
        print("Go Control Plane stopped.")
        
        # Stop docker compose
        subprocess.run(["docker", "compose", "down", "-v"], cwd="/home/suzaykid/Projects/conscious-cloud", check=True)
        print("Docker containers stopped.")

if __name__ == "__main__":
    main()
