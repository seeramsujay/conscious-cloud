import argparse
import logging
import random
import time
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("orchestrator")

CONTROL_PLANE_URL = "http://localhost:8081/update-eds"

MOCK_IPS = [
    "10.0.1.10",
    "10.0.1.11",
    "10.0.1.12",
    "10.0.1.13",
    "10.0.1.14",
]


def push_endpoint(ip: str) -> bool:
    url = f"{CONTROL_PLANE_URL}?ip={ip}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = resp.read().decode()
            logger.info("Control plane responded: %s", body)
            return True
    except urllib.error.HTTPError as e:
        logger.error("HTTP error pushing %s: %s", ip, e.code)
    except urllib.error.URLError as e:
        logger.error("Connection error: %s", e.reason)
    return False


def simulate_arbitrage_cycle():
    on_demand_ip = "10.0.0.1"
    logger.info("Starting arbitrage cycle. On-Demand: %s", on_demand_ip)

    spot_ip = random.choice(MOCK_IPS)
    logger.info("Provisioning Spot instance: %s", spot_ip)

    if push_endpoint(spot_ip):
        logger.info(
            "Spot instance %s added. Envoy draining On-Demand via weight=0.", spot_ip
        )

    time.sleep(30)

    logger.info("Terminating Spot instance: %s", spot_ip)
    if push_endpoint(on_demand_ip):
        logger.info("Failed back to On-Demand instance: %s", on_demand_ip)


def main():
    parser = argparse.ArgumentParser(description="Conscious Cloud AI Orchestrator")
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run continuous arbitrage cycles",
    )
    parser.add_argument(
        "--ip",
        type=str,
        help="Push a single endpoint IP to the control plane",
    )
    args = parser.parse_args()

    if args.ip:
        push_endpoint(args.ip)
        return

    if args.daemon:
        logger.info("Running in daemon mode. Ctrl+C to stop.")
        try:
            while True:
                simulate_arbitrage_cycle()
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Orchestrator stopped.")
    else:
        simulate_arbitrage_cycle()


if __name__ == "__main__":
    main()
