# **Exhaustive Architectural Specification: Autonomous Intra-Cloud Compute Arbitrage Engine**

The transition from static, monolithic infrastructure to hyper-dynamic, spot-arbitrage architectures introduces unprecedented operational complexity into the L4/L7 routing fabric. For high-compute, stateless microservices operating within Amazon Web Services (AWS), capturing the 70% to 91% cost savings offered by Spot Instances necessitates an infrastructure capable of sub-millisecond traffic shifting.1 Standard ingress controllers and naive service discovery mechanisms are fundamentally ill-equipped to handle the volatility of AWS spot markets, where compute capacity is reclaimed with minimal warning. This report provides an uncompromised, production-grade architectural specification for engineering an autonomous compute arbitrage engine using Envoy Proxy and a bespoke Artificial Intelligence (AI) orchestrator. The architecture relies entirely on an Aggregated Discovery Service (ADS) implementing Delta xDS streaming, rigorous socket-level connection draining algorithms, and entirely event-driven control loops to eliminate race conditions during infrastructure mutations.

## **SECTION 1: xDS Control Plane and Envoy Data Plane Architecture**

The foundational pillar of the compute arbitrage engine is the strict decoupling of the data plane (Envoy) from the configuration state. To achieve dynamic traffic shifting without initiating localized proxy restarts—which would drop connections and introduce severe latency spikes—a specialized Control Plane must be engineered utilizing the envoyproxy/go-control-plane library. Due to the highly ephemeral nature of AWS Spot instances, the Control Plane is required to propagate Internet Protocol (IP) address mutations to the Envoy fleets within single-digit milliseconds.2

### **1.1 Architectural Blueprint: Go Control Plane and AI Orchestrator Lifecycle**

The Control Plane functions as the synchronization nexus between the AI Orchestrator and the Envoy proxy fleet. The AI Orchestrator dictates optimal compute placement based on real-time ingestion of AWS spot pricing history feeds, predictive capacity models, and current workload metrics. The envoyproxy/go-control-plane library furnishes the generalized gRPC server primitives required to serve the Universal Data Plane API, acting as the translation layer between the Orchestrator's business logic and Envoy's strongly-typed Protocol Buffers.2  
The lifecycle mapping is executed through a sequence of strict, latency-optimized state transitions. This interaction lifecycle ensures that the Envoy data plane is continually synchronized with the physical reality of the AWS Virtual Private Cloud (VPC) network.

1. **Arbitrage Decision Epoch:** The AI Orchestrator evaluates the real-time AWS pricing matrix and calculates that transitioning a subset of high-compute stateless rendering workloads from an On-Demand target group (e.g., c6i.4xlarge) to a newly available Spot target group (e.g., c6i.8xlarge) yields a statistically significant positive arbitrage margin.  
2. **Compute Provisioning and Registration:** The AI Orchestrator interfaces with the AWS EC2 API to provision the necessary instances within the target Availability Zone. Upon successful boot, operating system initialization, and network binding, the instances register their IP addresses to a centralized, high-throughput memory store (e.g., a Redis cluster) utilized for cluster state management.  
3. **Control Plane Mutation Trigger:** The AI Orchestrator triggers an internal gRPC or RESTful endpoint exposed by the custom Go Control Plane, passing the updated endpoint topology.  
4. **Cache Invalidation and Snapshot Generation:** The Go Control Plane invokes the SnapshotCache interface.3 For maximum efficiency across independent resource types, the architecture utilizes a MuxCache. The MuxCache combinator mixes a SimpleCache (a snapshot-based cache maintaining a consistent view of configurations) for relatively static Listener Discovery Service (LDS) and Cluster Discovery Service (CDS) configurations, with a LinearCache for highly volatile Endpoint Discovery Service (EDS) updates.3 The LinearCache is optimized for a single type URL collection, maintaining a version vector and comparing request versions against the latest resource versions to respond only with updated elements.3 The server generates a new cache.Snapshot containing an incremented version\_info string.4  
5. **gRPC Propagation:** The Go Control Plane executes SetSnapshot(context.Background(), nodeID, snapshot), mutating the in-memory cache.4 The embedded xDS server detects the cache mutation and immediately pushes the configuration delta over the established HTTP/2 gRPC stream to the corresponding Envoy nodes.2

### **1.2 State Machine Transition Flow for Endpoint Mutability**

When an AWS Spot instance is added to or removed from the routing topology, the state machine transitions through a rigorous sequence to ensure continuous routing stability. A naive addition of an IP address directly into the EDS payload results in immediate HTTP 503 Service Unavailable errors if the underlying application process (e.g., a Dockerized Go binary or Python WSGI server) is not yet fully bound to the target socket or has not populated its local caches.2  
The addition and removal of endpoints must therefore follow a synchronized state machine coordinated between the AI Orchestrator, the Control Plane, and Envoy's internal health-checking subsystems.

| Endpoint State | Triggering Mechanism | Envoy Behavior and Routing Status |
| :---- | :---- | :---- |
| STATE\_UNALLOCATED | AI Orchestrator requests instance via AWS EC2 API. | The instance does not exist in the Control Plane. Envoy is entirely unaware. |
| STATE\_PROVISIONED | AWS assigns an Elastic Network Interface (ENI) and IP. | The AI agent injects the IP into the EDS payload. The Control Plane pushes the update to Envoy. |
| STATE\_EDS\_PROPAGATED | Envoy receives the Delta xDS payload. | The endpoint is inserted into the cluster load assignment. Active routing is intentionally suppressed. |
| STATE\_HEALTH\_CHECKING | Envoy's active health checker initializes probing. | The endpoint is flagged internally with /failed\_active\_hc until the configured healthy\_threshold is met.7 Traffic remains at ![][image1]. |
| STATE\_HEALTHY\_ACTIVE | Active health checks succeed consecutively. | The endpoint transitions to healthy. The configured load balancing weight is applied, and active traffic routing commences. |
| STATE\_TERMINATION\_SIGNALED | AWS issues the 120-second Spot Interruption Notice. | The AI Orchestrator pushes an EDS update setting the endpoint's weight to ![][image1]. |
| STATE\_WEIGHT\_DRAINING | Envoy receives the ![][image2] EDS update. | Existing sticky sessions and active connections persist, but no new connections are routed to the endpoint.9 |
| STATE\_CONNECTION\_SHEDDING | Envoy connection manager reaches duration limits. | Envoy initiates HTTP/2 GOAWAY emission and HTTP/1.1 Connection: close injections to force downstream reconnections.11 |
| STATE\_EDS\_REMOVED | drain\_timeout expires; all connections zeroed. | The AI Orchestrator completely purges the IP from the EDS snapshot. Envoy evicts the endpoint from memory. |

### **1.3 Production Configuration Schemas**

The following schemas define the exact configuration payloads required for the Cluster Discovery Service (CDS) and the Endpoint Discovery Service (EDS), optimized specifically for the high-throughput arbitrage workload.  
**Cluster Discovery Service (CDS) Schema:** The CDS dictates the upstream properties. It relies strictly on EDS for service discovery, bypassing logical or strict DNS resolutions, which eliminates blocking behavior in the forwarding path and ensures eventual consistency without DNS TTL propagation delays.13

YAML  
resources:  
  \- "@type": type.googleapis.com/envoy.config.cluster.v3.Cluster  
    name: arbitrage\_spot\_cluster  
    connect\_timeout: 0.25s  
    type: EDS  
    eds\_cluster\_config:  
      eds\_config:  
        ads: {}  
        resource\_api\_version: V3  
    lb\_policy: LEAST\_REQUEST  
    common\_lb\_config:  
      healthy\_panic\_threshold:  
        value: 10.0 \# Trigger panic routing only if \<10% nodes are healthy  
    outlier\_detection:  
      consecutive\_5xx: 3  
      interval: 10s  
      base\_ejection\_time: 30s  
      max\_ejection\_percent: 50  
    health\_checks:  
      \- timeout: 2s  
        interval: 5s  
        interval\_jitter: 1s  
        unhealthy\_threshold: 2  
        healthy\_threshold: 2  
        http\_health\_check:  
          path: "/healthz"

**Endpoint Discovery Service (EDS) Schema:**  
The ClusterLoadAssignment protobuf structure dictates the exact list of available IPs, partitioned into priorities or weighted segments. During a traffic shift, the AI Orchestrator modulates the load\_balancing\_weight parameter to smoothly drain or ramp up specific endpoints.

YAML  
resources:  
  \- "@type": type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment  
    cluster\_name: arbitrage\_spot\_cluster  
    endpoints:  
      \- locality:  
          region: us-east-1  
          zone: us-east-1a  
        lb\_endpoints:  
          \- endpoint:  
              address:  
                socket\_address:  
                  address: 10.0.1.45 \# Newly Provisioned Spot Instance IP  
                  port\_value: 8080  
            load\_balancing\_weight: 100  
            health\_status: UNHEALTHY \# Relies on active HC to transition to HEALTHY  
          \- endpoint:  
              address:  
                socket\_address:  
                  address: 10.0.1.99 \# On-Demand Instance IP marked for draining  
                  port\_value: 8080  
            load\_balancing\_weight: 0 \# New traffic suppressed, existing traffic maintained

### **1.4 gRPC Streaming Mechanics: Delta xDS vs. State of the World (SotW)**

In an environment characterized by frequent scaling events—where Spot instances are provisioned and destroyed dozens of times per hour—the legacy State of the World (SotW) xDS protocol becomes a systemic bottleneck. SotW mandates that the Control Plane transmits the entire array of endpoints upon every single configuration mutation.2 For example, if the cluster contains an array of \`\` and IP\_B is removed, the Control Plane must reserialize and transmit \[IP\_A, IP\_C\]. Items missing from the list are implicitly assumed deleted.2 For clusters exceeding hundreds or thousands of nodes, this introduces immense CPU spikes on the Control Plane, severe serialization overhead, and latency lag, potentially creating a desync window where traffic states fall out of alignment.2  
The arbitrage engine must utilize the **Incremental (Delta) xDS protocol (DELTA\_GRPC)**. Delta xDS transmits exclusively the state differential.17 Utilizing the DeltaDiscoveryRequest and DeltaDiscoveryResponse protobufs, the Control Plane explicitly issues a deletion directive rather than resending the entire world state.2

JSON  
{  
  "system\_version\_info": "v1045",  
  "resources":,  
  "removed\_resources": \["arbitrage\_spot\_cluster\_10.0.1.99"\]  
}

This protocol reduces the computational complexity of the Envoy data plane update from ![][image3] (where ![][image4] is the total number of endpoints in the cluster) to ![][image5] (where ![][image6] is the number of explicitly mutated endpoints). By configuring api\_type: DELTA\_GRPC in Envoy's envoy.yaml bootstrap file, the system minimizes the delta window during which the in-memory endpoint array diverges from physical AWS reality, enabling ultra-low latency, sub-millisecond updates without inducing control plane CPU throttling.2

## **SECTION 2: Low-Level Connection Draining and Traffic Shifting**

Cost arbitrage success relies on shifting massive computing workloads from On-Demand endpoints to Spot endpoints without dropping a single active client request. This mandates a profound mathematical and algorithmic understanding of Envoy's connection pools, upstream decoupling mechanisms, and graceful termination sequences.

### **2.1 The Mathematics of Traffic Shedding**

Envoy utilizes sophisticated load-balancing algorithms to manage traffic distribution across the endpoint pool. Assuming the LEAST\_REQUEST policy with configurable weights is utilized, the probability ![][image7] of routing a new incoming request to a specific endpoint ![][image8] out of a total set of ![][image4] healthy endpoints is defined by a weighted evaluation function. The algorithm utilizes an ![][image9] approach where it randomly samples two healthy hosts and selects the one with fewer active requests, mathematically skewed by the configured load\_balancing\_weight.7  
When the AI Orchestrator initiates a shift from an On-Demand instance to a newly provisioned Spot instance, the Control Plane manipulates the load\_balancing\_weight over a deterministic time curve ![][image10]. Initially, the On-Demand instance possesses ![][image11] and the uninitialized Spot instance possesses ![][image12]. Once the Spot instance passes active health checks, the Orchestrator executes a linear phase-out:  
![][image13]  
![][image14]  
As ![][image15] approaches ![][image1], Envoy's routing matrix halts all new TCP/HTTP stream initialization toward the On-Demand instance. However, assigning a weight of ![][image1] (or utilizing the explicit DRAINING health status) does not automatically sever established long-lived connections, WebSocket tunnels, or sticky sessions.9 The backend remains entirely accessible to clients with existing multiplexed streams, necessitating active connection shedding via connection timeouts.

### **2.2 Precise Configuration for Zero-Drop Connection Draining**

A complex interplay of protocol-specific configuration parameters governs Envoy's connection draining semantics. To achieve mathematically perfect zero-drop shedding, the upstream and downstream connection lifecycles—which Envoy explicitly uncouples—must be heavily tuned.12 The HTTP/1.1 specification dictates that an HTTP Proxy is an L7 proxy and should separate the L3/L4 connection lifecycle; headers like Connection: keep-alive from a downstream client do not directly dictate the upstream connection to the backend.12  
To force the draining of an endpoint whose weight has been set to ![][image1], Envoy utilizes several strict timers.

#### **drain\_timeout vs. parent\_shutdown\_time Mechanics**

It is critical to distinguish between connection draining (drain\_timeout) and process draining (parent\_shutdown\_time).

* **parent\_shutdown\_time (Process Draining):** This parameter is utilized during Envoy *hot restarts*. When a new Envoy process spins up, it binds via Unix Domain Sockets (UDS) to the old process, receives the listening sockets, and the parent is given the parent\_shutdown\_time to drain existing connections before the old PID exits.9  
* **drain\_timeout (Connection Draining):** This parameter dictates the time Envoy waits during the graceful termination of an active connection, totally independent of the Envoy process lifecycle. It is triggered when an active connection hits the idle\_timeout or the max\_connection\_duration.11 For our arbitrage engine, we rely purely on drain\_timeout triggered by max\_connection\_duration limiters, avoiding Envoy proxy restarts entirely.

| Parameter | Configuration Scope | Functionality |
| :---- | :---- | :---- |
| max\_connection\_duration | HTTP Connection Manager | The absolute maximum lifecycle of a connection. Upon reaching this duration, if there are active streams, the drain sequence kicks in, and the connection is force-closed after the drain period.12 |
| drain\_timeout | HTTP Connection Manager | The designated grace period between initiating connection shutdown procedures and forcefully terminating the socket. Used to prevent race conditions with in-flight requests.11 |
| delayed\_close\_timeout | HTTP Connection Manager | A local grace period following local close processing, allowing Envoy to wait for a TCP FIN/RST from the peer before closing the socket, mitigating truncation of final byte transmissions.12 |

#### **HTTP/1.1 Connection Draining Mechanics**

Because HTTP/1.1 does not support true stream multiplexing, connection uncoupling operates at the header level. When the connection reaches max\_connection\_duration, Envoy initiates the drain sequence. Envoy responds to the next downstream HTTP request by proactively injecting the Connection: close header into the HTTP response payload.12 This semantic instructs the downstream client to process the current payload and immediately sever the underlying TCP socket instead of keeping it alive. If the client fails to close the connection gracefully, Envoy relies on the delayed\_close\_timeout (configured to at least 1000ms) to mitigate socket write race conditions, preventing truncation of the response code.12

#### **HTTP/2 and gRPC Stream Draining Mechanics**

HTTP/2 multiplexing requires a deeply sophisticated two-phase termination dance to prevent active Remote Procedure Calls (RPCs) from failing. Envoy initiates the drain sequence by emitting a preliminary GOAWAY frame containing a maximum stream ID of ![][image16].11 This preliminary notification informs the client that a graceful shutdown is imminent, but crucially, it permits the client to finish establishing any new streams that were already in-flight due to the speed-of-light network propagation delay.11  
During the explicitly configured drain\_timeout window (e.g., 20 seconds), Envoy continues to service the active multiplexed streams.11 Once the drain\_timeout expires, Envoy fires the final, terminal GOAWAY frame containing the highest stream ID it successfully processed. Following this event, the socket is forcibly closed. This mechanism ensures that the downstream client application SDK (e.g., the gRPC C-core) seamlessly and transparently reconnects to the newly designated Spot instance endpoint, establishing a new connection pool without dropping the active requests.

### **2.3 Symbiosis with Downstream Load Balancers (AWS ALB/NLB)**

In an enterprise architecture, an AWS Application Load Balancer (ALB) or Network Load Balancer (NLB) typically sits sequentially in front of the Envoy ingress fleet. In this topology, Envoy's connection draining faces a critical race condition against AWS's native target deregistration\_delay.22  
When the Envoy fleet itself scales down, the underlying EC2 instance must be removed from the AWS Target Group. The ALB institutes a forced wait time (defaulting to 300 seconds) before fully severing routing to the deregistering target. However, if Envoy has already executed its drain\_timeout and closed its listener sockets while the ALB is still waiting out the deregistration\_delay, the ALB may mistakenly route a new connection to a closed Envoy port, yielding a 502 Bad Gateway or 504 Gateway Timeout error.  
To mitigate this architectural flaw, the parameters must be mathematically aligned:

1. **Configure Inverted Delays:** Configure the ALB deregistration\_delay to a value significantly *higher* than Envoy's max\_connection\_duration \+ drain\_timeout.  
2. **Rely on AWS Early Termination:** AWS Elastic Load Balancing evaluates in-flight state continuously: If a deregistering target has no active connections and no in-flight requests, the load balancer *immediately* completes the deregistration process without waiting for the full deregistration delay to elapse.22  
3. **Proactive Envoy Eviction:** Consequently, Envoy's rapid, proactive emission of HTTP/2 GOAWAY frames and HTTP/1.1 Connection: close headers forces the active connection count to ![][image1] swiftly. The AWS ALB observes the connection drop, recognizes the zero-state, and instantly and safely deregisters the target, maintaining a flawless, zero-downtime client experience.

## **SECTION 3: Deep Failure Mode and Race Condition Analysis**

Real-time cost arbitrage inside AWS implies an intensely volatile physical infrastructure. Instances are violently preempted, API rate limits are aggressive, and eventual consistency data models are severely tested. The architecture must anticipate, isolate, and natively mitigate the following engineering roadblocks.

### **3.1 The State Desync Blackhole**

The "State Desync Blackhole" is a critical race condition that occurs when the AI agent detects a newly assigned Spot instance IP and immediately updates the xDS server. Envoy receives the EDS delta and, trusting the Control Plane, immediately attempts to route traffic to the new IP. However, the application microservice container (e.g., a Kubernetes pod or a standalone Go binary) has not fully initialized its TCP sockets, downloaded its model weights, or populated its local caches. The incoming traffic drops into a black hole, resulting in cascades of 503 Service Unavailable or Connection refused errors.2  
**Mitigation: Multi-Tiered Active Probing Health Check Sequence**  
The control plane must act strictly as an *intent* distribution mechanism, while the data plane must act as the absolute *verification* engine. To eliminate the blackhole, the architecture deploys a multi-tiered verification sequence:

1. **Passive Outlier Detection:** Configured at the cluster level, outlier detection immediately ejects the node if a consecutive series of 5xx HTTP codes or connection timeouts are observed. The consecutive\_5xx threshold is set to 3\.7  
2. **Aggressive Active Health Checking:** Upon receiving a new EDS endpoint, Envoy defaults the endpoint's state to UNHEALTHY. The cluster configuration defines a strict http\_health\_check interval of 5s with an unhealthy\_threshold of 2 and a healthy\_threshold of 2\.8  
3. **Traffic Suppression:** Traffic is mathematically walled off from the endpoint until the application natively returns HTTP 200 OK on the /healthz endpoint. If an instance fails to bind entirely, Envoy flags it internally with /failed\_active\_hc and refuses to assign it a load balancing weight.7  
4. **Graceful Removal Flags:** If the AI agent removes the IP via EDS while active health checks are ongoing (e.g., the instance crashed during boot), Envoy gracefully applies the /pending\_dynamic\_removal flag, ensuring the internal state machine does not leak memory or perpetually attempt connections to a ghost socket.25

### **3.2 AWS API Throttling & Blindness**

The conventional approach to tracking Spot Instance lifecycle states involves aggressive polling of the ec2:DescribeInstances API or the AWS Spot Price History API. In environments managing hundreds or thousands of ephemeral nodes, this guarantees catastrophic RequestLimitExceeded API throttling. Once throttled, the AI orchestrator goes completely blind to the infrastructure state, unable to detect failures or route traffic away from dying nodes.  
**Mitigation: 100% Event-Driven Tracking via Amazon EventBridge**  
The polling architecture must be entirely eradicated. AWS EventBridge is a serverless event bus that detects real-time system changes at the Nitro hypervisor level, propagating them with microsecond latency without counting against account-level API rate limits.27  
The infrastructure defines a strict EventBridge Rule matching the exact JSON schema of an EC2 state change 28:

JSON  
{  
  "source": \["aws.ec2"\],  
  "detail-type":,  
  "detail": {  
    "state": \["running", "shutting-down", "stopped", "terminated"\]  
  }  
}

This rule triggers an AWS Lambda function that executes an atomic HSET or DEL operation against a globally replicated Amazon ElastiCache (Redis) cluster. The AI Orchestrator subscribes to Redis keyspace notifications via Pub/Sub. When a terminated event occurs, the AI instantly triggers a gRPC call to the Go Control Plane, updating the EDS snapshot.29 This decoupled pipeline ensures zero AWS API calls are utilized in the steady-state monitoring loops, rendering the architecture immune to throttling while maintaining a sub-second view of the cloud provider state.

### **3.3 The 2-Minute Spot Interruption Cliff**

AWS reclaims Spot Instances with precisely 120 seconds of warning. This notification is delivered via the Instance Metadata Service (IMDSv2) at http://169.254.169.254/latest/meta-data/spot/instance-action and simultaneously via EventBridge (EC2 Spot Instance Interruption Notice).27 If a stateless compute task (e.g., deep learning inference, heavy ETL batch processing, or 3D rendering) demands more than 120 seconds to complete, the interruption will violently sever the process, requiring the orchestrator to restart the workload from zero, wasting the compute cycle entirely.  
**Mitigation: Decoupled Queues and NVMe Checkpointing (The HUGI Pattern)**  
To survive the 2-minute cliff, the architecture mandates the implementation of the HUGI (Hurry Up and Get Idle) pattern, combined with message queue decoupling and state checkpointing.33

1. **Event Trapping and Isolation:** An EventBridge rule traps the specific interruption notice schema 27:  
   JSON  
   {  
     "source": \["aws.ec2"\],  
     "detail-type":  
   }

   This instantly alerts the AI Orchestrator via the Redis Pub/Sub pipeline to isolate the node by pushing an EDS update setting the Envoy weight to ![][image1]. The node is drained of incoming synchronous API traffic.29  
2. **Message Queue Decoupling:** Heavy microservices do not receive complex workloads synchronously via the Envoy HTTP path. Instead, Envoy routes a lightweight API trigger to the worker, which subsequently polls a high-throughput message queue (Amazon SQS or RabbitMQ).1  
3. **Visibility Timeout Mechanics:** When the worker pulls a message, the queue applies a strict visibility timeout. If the Spot instance is terminated before the worker explicitly acknowledges message completion, the visibility timeout expires, and the message automatically reappears in the queue to be ingested by a healthy, newly provisioned Spot instance.  
4. **Application-Level Checkpointing:** For tasks taking longer than the 120-second warning, the application runtime intercepts the SIGTERM signal passed down by the termination notice.1 The application rapidly dumps its in-memory task state (e.g., intermediate matrix tensors, processed string offsets) directly to local NVMe storage. An asynchronous sidecar daemon flushes the NVMe buffer to a distributed durable store (Amazon S3 or Redis) before the instance is ultimately terminated by the hypervisor.1 When the replacement instance picks up the visible SQS message, it retrieves the checkpoint from S3, resuming computation exactly where the preempted instance left off, ensuring zero lost compute time.

## **SECTION 4: Production Code & Reference Implementations**

The theoretical frameworks discussed above must be grounded in precise code and configuration. The following components represent the uncompromised, low-level configuration files and Go code required to orchestrate the infrastructure.

### **4.1 Go Control Plane Implementation**

The following highly optimized Go snippet demonstrates the xDS server mapping the gRPC interfaces, utilizing the V3 SnapshotCache. This server opens a local REST endpoint allowing the external AI Orchestrator to inject new Spot IP addresses, triggering an atomic version increment and subsequent Delta xDS cache invalidation.3  
By importing the github.com/envoyproxy/go-control-plane modules, the architecture provides a standard API implementation capable of pushing configurations to Ephemeral Envoys at scale.3

Go  
package main

import (  
	"context"  
	"fmt"  
	"log"  
	"net"  
	"net/http"  
	"sync/atomic"

	"google.golang.org/grpc"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"  
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"  
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"  
	clusterservice "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"  
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"  
	endpointservice "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"  
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"  
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"  
	"github.com/envoyproxy/go-control-plane/pkg/server/v3"  
)

const (  
	grpcPort \= 18000  
	nodeID   \= "arbitrage-fleet-node"  
)

var version int64

func main() {  
	ctx, cancel := context.WithCancel(context.Background())  
	defer cancel()

	// Initialize the Cache. In production, a MuxCache is preferred to separate   
	// static CDS from volatile EDS, but a SimpleCache demonstrates the core mechanic.  
	snapshotCache := cache.NewSnapshotCache(true, cache.IDHash{}, nil)

	// Setup the gRPC server with max concurrent stream configuration  
	grpcServer := grpc.NewServer(grpc.MaxConcurrentStreams(1000))  
	xdsServer := server.NewServer(ctx, snapshotCache, nil)

	// Register Discovery Services for ADS and Delta xDS  
	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(grpcServer, xdsServer)  
	endpointservice.RegisterEndpointDiscoveryServiceServer(grpcServer, xdsServer)  
	clusterservice.RegisterClusterDiscoveryServiceServer(grpcServer, xdsServer)

	// Local HTTP Endpoint for AI Orchestrator to inject new Spot IPs  
	http.HandleFunc("/update-eds", func(w http.ResponseWriter, r \*http.Request) {  
		ip := r.URL.Query().Get("ip")  
		if ip \== "" {  
			http.Error(w, "Missing 'ip' parameter", http.StatusBadRequest)  
			return  
		}

		v := atomic.AddInt64(\&version, 1\)  
		versionStr := fmt.Sprintf("v%d", v)

		// Generate the new EDS Payload  
		edsAssignment := \&endpoint.ClusterLoadAssignment{  
			ClusterName: "arbitrage\_spot\_cluster",  
			Endpoints:\*endpoint.LocalityLbEndpoints{{  
				LbEndpoints:\*endpoint.LbEndpoint{{  
					HostIdentifier: \&endpoint.LbEndpoint\_Endpoint{  
						Endpoint: \&endpoint.Endpoint{  
							Address: \&core.Address{  
								Address: \&core.Address\_SocketAddress{  
									SocketAddress: \&core.SocketAddress{  
										Address:       ip,  
										PortSpecifier: \&core.SocketAddress\_PortValue{PortValue: 8080},  
									},  
								},  
							},  
						},  
					},  
					LoadBalancingWeight: \&core.UInt32Value{Value: 100},  
				}},  
			}},  
		}

		// Create atomic snapshot of the new topology  
		snap, err := cache.NewSnapshot(versionStr, maptypes.Resource{  
			cache.EndpointResponse: {edsAssignment},  
		})  
		if err\!= nil {  
			log.Printf("Snapshot generation failed: %v", err)  
			return  
		}

		// Apply Snapshot, triggering Delta xDS propagation to connected Envoy nodes  
		if err := snapshotCache.SetSnapshot(ctx, nodeID, snap); err\!= nil {  
			log.Printf("Failed to set snapshot: %v", err)  
			return  
		}  
		w.WriteHeader(http.StatusOK)  
		w.Write(byte(fmt.Sprintf("EDS Cache Mutated to version %s. Delta xDS dispatched.", versionStr)))  
	})

	// Start auxiliary REST API for Orchestrator integration  
	go func() {  
		log.Println("AI Orchestrator API listening on port 8081")  
		if err := http.ListenAndServe(":8081", nil); err\!= nil {  
			log.Fatalf("HTTP server failed: %v", err)  
		}  
	}()

	// Start xDS gRPC Server  
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))  
	if err\!= nil {  
		log.Fatalf("Failed to listen: %v", err)  
	}  
	log.Printf("xDS Control Plane active on port %d", grpcPort)  
	if err := grpcServer.Serve(lis); err\!= nil {  
		log.Fatalf("Failed to serve: %v", err)  
	}  
}

### **4.2 Envoy Proxy Boot Configuration (envoy.yaml)**

This configuration binds the Envoy Proxy strictly to the Aggregated Discovery Service (ADS) utilizing Delta xDS. It leverages tls\_context for mutual TLS (mTLS) between the proxy and the control plane, ensuring cryptographic verification and preventing unauthorized injection of routing tables.17 The max\_connection\_duration and drain\_timeout parameters are explicitly configured to support the connection shedding logic detailed in Section 2\.

YAML  
node:  
  id: "arbitrage-fleet-node"  
  cluster: "stateless\_microservice\_tier"

dynamic\_resources:  
  ads\_config:  
    api\_type: DELTA\_GRPC  
    transport\_api\_version: V3  
    grpc\_services:  
      \- envoy\_grpc:  
          cluster\_name: xds\_cluster  
  cds\_config:  
    ads: {}  
    resource\_api\_version: V3  
  lds\_config:  
    ads: {}  
    resource\_api\_version: V3

static\_resources:  
  clusters:  
    \- name: xds\_cluster  
      type: STRICT\_DNS  
      connect\_timeout: 1s  
      http2\_protocol\_options: {}  
      upstream\_connection\_options:  
        tcp\_keepalive:  
          keepalive\_time: 300  
      load\_assignment:  
        cluster\_name: xds\_cluster  
        endpoints:  
          \- lb\_endpoints:  
              \- endpoint:  
                  address:  
                    socket\_address:  
                      address: control-plane.internal  
                      port\_value: 18000  
      transport\_socket:  
        name: envoy.transport\_sockets.tls  
        typed\_config:  
          "@type": type.googleapis.com/envoy.extensions.transport\_sockets.tls.v3.UpstreamTlsContext  
          common\_tls\_context:  
            tls\_certificates:  
              \- certificate\_chain:  
                  filename: "/etc/envoy/certs/client.crt"  
                private\_key:  
                  filename: "/etc/envoy/certs/client.key"  
            validation\_context:  
              trusted\_ca:  
                filename: "/etc/envoy/certs/ca.crt"

### **4.3 Architectural Validation and Literature Assessment**

The architectural paradigm of aggressive cloud compute arbitrage utilizing Spot Instances and dynamic proxies is rigorously validated across modern industry literature.

* **Cost Reductions & Framework Verification**: Analyses of high-compute Natural Language Processing (NLP) and machine learning workloads underscore that migrating from pay-as-you-go instances to all-spot configurations produces upwards of 65% to 71% structural cost savings.33 The literature explicitly cites checkpointing mechanisms, idempotent processing, and distributed coordination as the bedrock for enabling graceful handling of instance interruptions.  
* **HUGI & Checkpointing Strategies**: Modern distributed infrastructures leverage the HUGI (Hurry Up and Get Idle) framework, heavily relying on automated object storage (S3) or NVMe checkpointing to endure the volatility of Spot interruptions without losing computational state.1  
* **Industry Application at Scale**: Large-scale production deployments at entities like Pinterest (yielding $4.8 million in annual savings via multi-regional Spot routing) and Snap (utilizing 90% spot percentages for inference) demonstrate the absolute necessity of automated, low-latency target eviction handling.1  
* **L4/L7 Scaling Limits and Envoy Optimization**: Peer-reviewed research, such as the analysis of decentralized schedulers in data centers, confirms that granular load-balancing topologies—analogous to Envoy's weighted cluster assignments—reduce task scheduling bottlenecks inside dense computing environments.36 Furthermore, integrating Delta xDS mitigates the CPU overhead inherently generated by the older SotW algorithms in environments with frequent configuration updates, allowing service meshes to maintain stability during rampant endpoint explosion.2

## **Conclusion**

The realization of an autonomous, live, intra-cloud compute arbitrage engine requires transcending standard container orchestration. By fusing a predictive AI orchestrator with an aggressively tuned go-control-plane xDS server, the infrastructure guarantees millisecond-accurate topological mapping. Transitioning to DELTA\_GRPC eradicates protocol overhead, while profound manipulation of HTTP/2 GOAWAY signaling, HTTP/1.1 header injection, and socket-level TCP timeouts natively ensures zero-drop connection shedding. Fortified by an AWS EventBridge-driven state evaluation matrix and resilient asynchronous checkpointing, this architecture enables the uninterrupted execution of high-compute microservices on fundamentally volatile AWS Spot infrastructure, maximizing financial efficiency without sacrificing absolute system reliability.

#### **Works cited**

1. Spot Instances and Preemptible GPUs: Cutting AI Costs by 70% | Introl Blog, accessed May 23, 2026, [https://introl.com/blog/spot-instances-preemptible-gpus-ai-cost-savings](https://introl.com/blog/spot-instances-preemptible-gpus-ai-cost-savings)  
2. xDS Deep Dive: Dissecting the "Nervous System" of the Service Mesh \- DEV Community, accessed May 23, 2026, [https://dev.to/kanywst/xds-deep-dive-dissecting-the-nervous-system-of-the-service-mesh-3m5i](https://dev.to/kanywst/xds-deep-dive-dissecting-the-nervous-system-of-the-service-mesh-3m5i)  
3. envoyproxy/go-control-plane: Go implementation of data-plane-api \- GitHub, accessed May 23, 2026, [https://github.com/envoyproxy/go-control-plane](https://github.com/envoyproxy/go-control-plane)  
4. envoy-proxy-go \- Go Packages, accessed May 23, 2026, [https://pkg.go.dev/github.com/shivamsinghsre/envoy-proxy-go](https://pkg.go.dev/github.com/shivamsinghsre/envoy-proxy-go)  
5. go-control-plane/envoy/service/status/v3/csds.pb.go at main \- GitHub, accessed May 23, 2026, [https://github.com/envoyproxy/go-control-plane/blob/main/envoy/service/status/v3/csds.pb.go](https://github.com/envoyproxy/go-control-plane/blob/main/envoy/service/status/v3/csds.pb.go)  
6. Exploring gRPC Load Balancing: Gateway, Service Mesh, and xDS with Go | by phucch, accessed May 23, 2026, [https://phuc-ch.medium.com/exploring-grpc-load-balancing-gateway-service-mesh-and-xds-with-go-a527ab0e7ce8](https://phuc-ch.medium.com/exploring-grpc-load-balancing-gateway-service-mesh-and-xds-with-go-a527ab0e7ce8)  
7. Administration interface — envoy 1.39.0-dev-47d159 documentation, accessed May 23, 2026, [https://www.envoyproxy.io/docs/envoy/latest/operations/admin.html](https://www.envoyproxy.io/docs/envoy/latest/operations/admin.html)  
8. How to configure Envoy health checks for backend endpoints \- OneUptime, accessed May 23, 2026, [https://oneuptime.com/blog/post/2026-02-09-envoy-health-checks-backends/view](https://oneuptime.com/blog/post/2026-02-09-envoy-health-checks-backends/view)  
9. Unable to drain endpoints before removal · Issue \#7218 ... \- GitHub, accessed May 23, 2026, [https://github.com/envoyproxy/envoy/issues/7218](https://github.com/envoyproxy/envoy/issues/7218)  
10. How to View Envoy Endpoint Configuration with istioctl \- OneUptime, accessed May 23, 2026, [https://oneuptime.com/blog/post/2026-02-24-how-to-view-envoy-endpoint-configuration-with-istioctl/view](https://oneuptime.com/blog/post/2026-02-24-how-to-view-envoy-endpoint-configuration-with-istioctl/view)  
11. How do I configure timeouts? \- Envoy proxy, accessed May 23, 2026, [https://www.envoyproxy.io/docs/envoy/latest/faq/configuration/timeouts](https://www.envoyproxy.io/docs/envoy/latest/faq/configuration/timeouts)  
12. HTTP Connection Lifecycle Management — Envoy Insider, accessed May 23, 2026, [https://envoy-insider.mygraphql.com/en/latest/connection-life/connection-life.html](https://envoy-insider.mygraphql.com/en/latest/connection-life/connection-life.html)  
13. Service discovery \- Strict DNS \- Envoy proxy, accessed May 23, 2026, [https://www.envoyproxy.io/docs/envoy/latest/intro/arch\_overview/upstream/service\_discovery](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/service_discovery)  
14. envoy/config/cluster/v3/cluster.proto at d1b67883af754acae4666cd165f0424ed1ebb816 · envoyproxy/envoy \- Buf, accessed May 23, 2026, [https://buf.build/envoyproxy/envoy/file/d1b67883af754acae4666cd165f0424ed1ebb816:envoy/config/cluster/v3/cluster.proto](https://buf.build/envoyproxy/envoy/file/d1b67883af754acae4666cd165f0424ed1ebb816:envoy/config/cluster/v3/cluster.proto)  
15. Introduction to Envoy xDS and Configuration Distribution in Istio | Jimmy Song, accessed May 23, 2026, [https://jimmysong.io/blog/istio-delta-xds-for-envoy/](https://jimmysong.io/blog/istio-delta-xds-for-envoy/)  
16. EnvoyCon 2021 Delta xDS: Aditya Prerepa & John Howard \- Sched, accessed May 23, 2026, [https://static.sched.com/hosted\_files/envoyconna21/08/EnvoyCon%202021%20Delta%20xDS\_%20Aditya%20Prerepa%20%26%20John%20Howard%20%281%29.pdf](https://static.sched.com/hosted_files/envoyconna21/08/EnvoyCon%202021%20Delta%20xDS_%20Aditya%20Prerepa%20%26%20John%20Howard%20%281%29.pdf)  
17. Configuration sources (proto) — envoy 1.39.0-dev-10840c documentation, accessed May 23, 2026, [https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/core/v3/config\_source.proto](https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/core/v3/config_source.proto)  
18. HTTP 连接生命周期管理 \- Istio & Envoy 内幕, accessed May 23, 2026, [https://istio-insider.mygraphql.com/zh-cn/latest/ch2-envoy/connection-life/connection-life.html](https://istio-insider.mygraphql.com/zh-cn/latest/ch2-envoy/connection-life/connection-life.html)  
19. How to Configure Envoy Connection Pooling for Performance \- OneUptime, accessed May 23, 2026, [https://oneuptime.com/blog/post/2026-02-09-envoy-connection-pooling/view](https://oneuptime.com/blog/post/2026-02-09-envoy-connection-pooling/view)  
20. HTTP connection manager (proto) — envoy 1.39.0-dev-839f03 documentation, accessed May 23, 2026, [https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/filters/network/http\_connection\_manager/v3/http\_connection\_manager.proto](https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/filters/network/http_connection_manager/v3/http_connection_manager.proto)  
21. LDS/Enhancement: proactively close or gracefully drain idle, accessed May 23, 2026, [https://github.com/envoyproxy/envoy/issues/44116](https://github.com/envoyproxy/envoy/issues/44116)  
22. ALB Connection Draining is always reaching the "Deregistration Delay" \- Server Fault, accessed May 23, 2026, [https://serverfault.com/questions/919335/alb-connection-draining-is-always-reaching-the-deregistration-delay](https://serverfault.com/questions/919335/alb-connection-draining-is-always-reaching-the-deregistration-delay)  
23. Configure connection draining for your Classic Load Balancer \- AWS Documentation, accessed May 23, 2026, [https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/config-conn-drain.html](https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/config-conn-drain.html)  
24. All cluster endpoints are active despite health checks \- Google Groups, accessed May 23, 2026, [https://groups.google.com/g/envoy-users/c/7KTkaNvO4yg](https://groups.google.com/g/envoy-users/c/7KTkaNvO4yg)  
25. Question: EDS \+ active health check endpoint removal behavior · Issue \#11527 · envoyproxy/envoy \- GitHub, accessed May 23, 2026, [https://github.com/envoyproxy/envoy/issues/11527](https://github.com/envoyproxy/envoy/issues/11527)  
26. Backend health checks \- kgateway, accessed May 23, 2026, [https://kgateway.dev/docs/envoy/main/traffic-management/health-checks/backend/](https://kgateway.dev/docs/envoy/main/traffic-management/health-checks/backend/)  
27. Easy AWS: Architecting Revisited. New approach: advantages, pitfalls and… | by Antonella Blasetti | Apr, 2026, accessed May 23, 2026, [https://aws.plainenglish.io/easy-aws-architecting-revisited-86a67c4aedf3](https://aws.plainenglish.io/easy-aws-architecting-revisited-86a67c4aedf3)  
28. Amazon EventBridge: An In-Depth Look \- Dash0, accessed May 23, 2026, [https://www.dash0.com/knowledge/amazon-eventbridge](https://www.dash0.com/knowledge/amazon-eventbridge)  
29. How to Trigger Lambda Functions from EventBridge Rules \- OneUptime, accessed May 23, 2026, [https://oneuptime.com/blog/post/2026-02-12-trigger-lambda-eventbridge-rules/view](https://oneuptime.com/blog/post/2026-02-12-trigger-lambda-eventbridge-rules/view)  
30. Tutorial: Log the state of an Amazon EC2 instance using EventBridge, accessed May 23, 2026, [https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-log-ec2-instance-state.html](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-log-ec2-instance-state.html)  
31. Filtering event rules using customized JSON event patterns in AWS User Notifications, accessed May 23, 2026, [https://docs.aws.amazon.com/notifications/latest/userguide/common-usecases.html](https://docs.aws.amazon.com/notifications/latest/userguide/common-usecases.html)  
32. Event Driven Architecture- Create an EventBridge Rule to get notified on EC2 Instance state change… | by Sumbul Naqvi | Medium, accessed May 23, 2026, [https://medium.com/@sumbul.first/event-driven-architecture-create-an-eventbridge-rule-to-get-notified-on-ec2-instance-state-change-3947ede87e30](https://medium.com/@sumbul.first/event-driven-architecture-create-an-eventbridge-rule-to-get-notified-on-ec2-instance-state-change-3947ede87e30)  
33. Cost-Effective Scaling Strategies for NLP Workloads in Cloud Computing \- ResearchGate, accessed May 23, 2026, [https://www.researchgate.net/publication/403970792\_Cost-Effective\_Scaling\_Strategies\_for\_NLP\_Workloads\_in\_Cloud\_Computing](https://www.researchgate.net/publication/403970792_Cost-Effective_Scaling_Strategies_for_NLP_Workloads_in_Cloud_Computing)  
34. EventBridge \- AWS Studies, accessed May 23, 2026, [https://jbcodeforce.github.io/yarfba/serverless/eventbridge/](https://jbcodeforce.github.io/yarfba/serverless/eventbridge/)  
35. We Cut Our Cloud Bill by 71%: How We Optimized AI Infrastructure Without Sacrificing Performance | by Reliable Data Engineering | Medium, accessed May 23, 2026, [https://medium.com/@aminsiddique95/we-cut-our-cloud-bill-by-71-how-we-optimized-ai-infrastructure-without-sacrificing-performance-b9c16fbeb496](https://medium.com/@aminsiddique95/we-cut-our-cloud-bill-by-71-how-we-optimized-ai-infrastructure-without-sacrificing-performance-b9c16fbeb496)  
36. Dodoor: Efficient Randomized Decentralized Scheduling with Load Caching for Heterogeneous Tasks and Clusters \- arXiv, accessed May 23, 2026, [https://arxiv.org/html/2510.12889v1](https://arxiv.org/html/2510.12889v1)

[image1]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAoAAAAaCAYAAACO5M0mAAAAz0lEQVR4Xu2RMatBYRyHj09AyqBk5y4oM4uvIIusFguTsl+fQEwiq+5wPwCDycAmZbRciyys9z4nv79eymVVnnqG9zm/zumc43mvRQonOMMFNjBwtYA47rGscxhX2LosRAfXN62KRwxa8G+/w7EFkcdfLFqIKfQtiLT6p4WsQs+C+FAfWsgpdC2IhPqXhbzCw+G9RyfVRxaeHkYVBhaEvXXbjT/47QYoeOdhyY3+n9m4Aep4wpAb/Y9+wIrOEdxi87JwyOAU57jE2tXVN//xBxjyMMcov/oxAAAAAElFTkSuQmCC>

[image2]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADUAAAAXCAYAAACrggdNAAABUUlEQVR4Xu2VPSiHURSHr8hCZPA12Qw2JYtikJFQJmVB+ZjIohjNBkmMrMSAyaSEMpgMzFaElAXP6Rx1Oxn+f4qr7lNPvff8euvc7nvPG0Imk8n8IiVY6ot/hTRzjA/4jo94iU1YgWf4bNk9nltdGMMny15w3erJMB+0uWkfwFrQrM8H0INHWOWDFBgP2viiD2A3aDbi6nLKB9jg6skwELTxZVfvwg3LZlw2gaOulhSdQRvfjGplOIlDli1FWSPuBT2tQpETl/taqHP62vdpCdr4flQbxhrstiweBFvYHK2TpC5o46e2rsdBe261bMfWvbhgz0kjn9obXtta7svnpyXjXTYlo78SD7HcsuSR/9AdtmNbVJeNyKaucAU7oqwYtvGiCGf1tZ9xE/S0pnwAr+aqD1JH7pOcVq0P4Nas9kHqyMiVEf4VJ9jvi5lMJvMv+QBztFLEl4bHgwAAAABJRU5ErkJggg==>

[image3]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADMAAAAaCAYAAAAaAmTUAAACyElEQVR4Xu2XW6hNURiFf/f7nVxK8oKSKIlIwhMlJeUF5VJKeBAK8YISkTxIUnIpiTzILUTIg+JFRIlzQrxIcil3xjj/WvY848yzJ521lNpfjdpr/GvONa//nNusRo3/ji7QBWigBlrAJOgI1EoDZXMWWqhmAeyENqtZJkugK2oWRHuoznyW/pq+0EboOnQZugHdgTaZV6zQewbN0kBGd+gR9BL6Cb2FejZ6w2wv9Mk8/hV60Dhs66FL4iVZDr2CVkMdA38wdA+6C/UIfDLHvExr8ZU10FPzBrNxykjoIdRLA6Af9AMarYEY3GD7oQ/QOInlTDRvyB7xj0GnxItxERoPfTGfJZ3ludAW8UI4u2vVjLHVvKGLNRDQFvoG1Yv/2NIfYcNvZ7+ZnfitRZVwA/ugyeKFHIbOqamMhb5D9616CuSyYyM+Bl5X8+mfHXgxpkK7s9+jzOvhvgi/xyXcLnhWuI/r1VROmFe+SgNCvsyeBN7QzJsSeDG2QTOCZy45lpuZPQ+BzlTCUVZA79UM4cgwu7Di4RJTtpu/x72VMybzUhvzpvmhmjPNvNzV7HkZtLISjjLfvIzutd90Mn+h6kugM/TafPMOC3x2ItUZZqfYGcR0z7Jc5qctPZh5Z8Is24R35uu+gwYCeAqzonXiM2WnlhmzFNe7Ms+87EnztJ+Cy+yzmspR80qnayCDVxR2dpcGzGeMZaslgAPQBDVBG6ucOwclFoMD8lxNZZB5lmAe54bO6W9+Mr+x6neuOms+NQ+AXkDdNJDBfcLOcJZSMDWnkkQDfcxHnh26ZX6VuWbeSL16KIes6aHZ27wuLgs2lgMS6zBnlgcov5+C9W1Qs2g4qmxQ6jrTEvLrzAgNFA1vBlxqzV00i4B3uTyNl85S8xt2GeR/AXho/zOOQwvULAAeCzvULBsewOet+L/NPDrK3I81avwpvwBStJEnLJFq6wAAAABJRU5ErkJggg==>

[image4]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABMAAAAaCAYAAABVX2cEAAABDElEQVR4XmNgGAXDFzgD8S0gfg/E/4H4IKo0GJwF4n8MEPlvQDwbVRoTbAHiewwQDZZociCQDcTLgJgJXQIdsALxGSCOYIAYthZVGgymMEB8QRDYAPFkIGYG4vtA/BeIVVBUQCxjRxPDChqB2A/KzmWAuG4aQppBCoi3I/HxggNAzAtlcwHxGwZIQItAxeKBuAjKxgv4GCCGIYMmBojr6qD85UCsi5DGDfyBuB5NTBSIvwPxKyDmBuLLqNK4ASiWrNEFgWA6A8R1c4B4EZocTnAOiFnQBRkgsQmKVZCBMWhyWIEdEJ9GF0QCoPQGMkwCXQIZuAHxAwZEFnkCxPbICqDAnAGSlUbBKBhQAADIFjDhxd8YOAAAAABJRU5ErkJggg==>

[image5]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADYAAAAaCAYAAAD8K6+QAAADD0lEQVR4Xu2YWahOURiGX0PmDCHzXLgQSspw4cKQMqYkyZFZikQk4sqFseRC7lwQKcqFKZSQOzIklIyJG3OUmfc9316dtb+z9/5d/P8p+p966/zvt/Zea6/9rW+tfYAqVf5LWlNnqW4+UCbaUxepDj5QaU5RNd4sM1Oo81QTH6gUi2Gz2RAcpTZ582/oRG2mLlMXqCvUdWoL1SxqF5D3nJrmAwlNqRvUK+o39Yvql2qRZjD1Gtb2BXWLahPFR1NvnFeSlbABrKVaRH4v6g5sgO0iX8yCXdPY+Z6lsEFqwBNdLKB7rKE+Uo9dLOYutdqbWTSiDlCfqJEuFhgDG9Re5x+mjjsviyPUCtg9lrtYYB41E9Zmv4vFaKynvZnFNtjNFvlAhFLqB/XU+Q+p9c7zaOJuU11g/exIh2sZQE2ltsLazEiHUyykPqBEERlB/YS9Xg0gD6WmOvwcecpzrZmiQYihsDcrlGb+DavfkFpa09+ptnXheoyFjaWv81McgzUqlbMhFR9FnoqAvHGRl8U6akHyt9bZzSgmZlNdYXvhV+pqOlyPIbB+9VIy0Uy9hzUa5GKe7bB2yu/A8MQbFnlZnKN6JH+fgKVRoCdsXYnJsPspHYvQNWo3yQcCLWENpKxSHmgFK8HfqIGRrwcq9WDNYdtFYCfsms7J7zhTdicxlfQiwoNpInJRzmudaAB57ILdaIPztQ2USsUJ1L7o9zLYNaNgW0XvKKYULVkUUJeKhRNwCNZovA8k6JikB9/jA7A3qWuLiocmZXr0W/2ESZob+R1h/ZyMvDxC8VAlzaU7rIQ/QPpEoNKsmX6L4jPgE+SXe23mKjb9I68PbFA6gsVVWA8pf1Xk5RHKfVEVr0WzpTeih7sGO05dgg1Yp+oiDqJ++dZ6vUd9gQ1WW4TWj9Dp4hnq1qpOJDqSaf2qrdbyfdj6z0MFTIfhijKHeonSR6pyoj1Xp5iKohOJ0jHvEFxuVDDeoXgDLxtLYF8CDYE+WzZ6s5Kow/neLDP60FQNUJY0GFrsZ1D5fw3oe7FKlX+BP+kRoQY3JHWnAAAAAElFTkSuQmCC>

[image6]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABYAAAAaCAYAAACzdqxAAAABPklEQVR4Xu2TPyhGURjGHyLZPkoZZZHhYxCjzWAxCIPhkzIiOyYGi2xKKRPKJlnNCCnZiJLRICWLeF7POd1z7ndvd9f91W8473Pue8+fe4GSkiIG6D19pz/0KY7rmITmfdMHehjH9ezTW+iBllTm6aSbUOO9VJbLHV2GHupJZZ5FugLNmUplmfTSIyTbHI3jP8ZoNz2DdtUex9nYSuag87bG83GMDjpNW+kXvYjjfE5oF61AjbeiVC9qpCNQvhbH2TTTm2D8Ro+DsTXzZ74BNR5O4nxs0nYwvoQ+P8N2UAuyK/oBLaaQdToejA/oJ22gC7TJ1dugSzt140LOoZV57Pxsu7O0P6jby62+FNRy6aOP0MV4ZqAGq0HN2HH1aqoeMUSfoa3ZZLuwCZcN0mskf98ufXXzzBfomy8p+Zf8AnQVQMXFAyHjAAAAAElFTkSuQmCC>

[image7]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAaCAYAAAC+aNwHAAAA00lEQVR4Xu2SPwuBURTGnyQDm8nCrpTFQtltBoNR+Ra+hEkpxeATvKPJSlb5t7MoshBFPHW83Pf04h0N91e/5Tznnnu7HcDyfxTolB7onW7pgs7piq5pi8bdA59wIAPSqp6lJzpWdQ8huqcbHTxZQoZndOCSgzT0dUBi9EKvNKGyFw3IgJoOSB2SdXRgMoQ0JY1amFbojnZpxMg8ROkZ8lEDwxFt0+K71Z8S5PaeDoLShAyo6iAoM3pDgEXxIwW5faKDX+Qh6+qu7xHykrLZZLF84wGIty178gkRTgAAAABJRU5ErkJggg==>

[image8]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAcAAAAaCAYAAAB7GkaWAAAAfUlEQVR4XmNgGMrAHojfAHEgugQIeAPxSSBWR5cgD7gB8XYgvgnEnsgS0kC8BYiZgfgsEK9DlswGYhsgVgDif0CcjywJA61A/AOIhdAlWID4ORAvQZcAgWAg/g/EtkCsBMQtyJL9QPwEyp4NxNpIcgwmDBBvbADiEGSJkQAA9EAS9Xxtj/4AAAAASUVORK5CYII=>

[image9]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACsAAAAaCAYAAAAue6XIAAACT0lEQVR4Xu2XS6hNYRiGP3cHuXTU6Yg6UhgQJYPDwOCUlKRMjCiHgQwMFInMDJRIRmYGlJShWy6JkgkTGShEBs7EPXK/vK9v//n227/X/p3OMtpPPZP3+/fa317rv6xt1qHDiDMZXoK9WqhgOrwGZ2ihbs7DzRoG+jRosBZegWO0UBdbze+QMg4uhEfhW6lFzsB9GpYwE+6HN+FVeAvehQfg+DAuwew5XCf5YvgK3jGvf2guN9FvPnaKFqrYAYfgLjgx5HPgfXgPTgs52WD+mdGSRy5bdbPkAdypYY5R8IT5BZdLLbEC/oLHJD8Nz0mmlDTL77+gYY6D5o0MaiEwFn6HzyR/BHdLppQ0uwW+szYLbRn8Yf4YeIdbwWnBH/QxZJxjP+H6kOUoaXal+fX7JG/irPmgdvMlTYMnIZvbyFaFLEdJs4vMr8Wbl4V3klsKBy2QmnLIfBznVmJpI1sSshxsNj6RHLPNr7VaC4ku8wE0ty0lJsGX8CucH3I2WdrsJw2F1OwaLUTem8+7CVoIHDa/0B7JuaWVToPPGgppGnDPbckp80EDWmjAI5Q/5ogWzO84P1uywL5oKKQFNk8LkVnm29FD8wWT6IHH4WurPvOfWvut6zr8Zn78tiJtXVU70h+6ze8cG75tftTeMG+Cb0ZVnLT8ocBj+zF8YX/XBRczs9y85MLlC02tbDRvqOq4LYH7/HYNRxqebJwK+iLzL3BRvYFTtVAH28zf0IYLXxH3algn/MJNGhbAl2+uEz6h/wYPmIs2vL81XIwdOrTjN3TxfIaISbdBAAAAAElFTkSuQmCC>

[image10]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAC8AAAAaCAYAAAAnkAWyAAACU0lEQVR4Xu2WW6gNURzGv+N+CYmUiINQQnlxKdeSIiGRSyQPUsqLopRSPLg8eCEpCkke1EkpKRQiSW5FUS5RilxKkmv4vr41x+q/H/bWkT1q/+rXzHxrptas+a+1BmjQoEGDf8kS+pN+pA/pPfo+Za/pffqIfqFf6RQ/Vg7O0u20c5adhzs/MssG02+0X5bVlb70asi60E/0ecjF3RjUk3V0echmwqN+KOR6qXMhqytDaaeQ7YQ7r7mQ04EOD1npuEl/wCX1X9EH7vit2FCFafQBbYkNCZXcMzoiNpDx8HMatHzRWBGuq7IILpndsaEGjtH1MUw00fnpGLlCV9GtWaavrqX7jzp/AO78rNhQA1qdRsWwCr3g/aN7yBfSCyGrymP6mXaNDRmb4X1hPzzhhY4aqW3whNfeoQkuVtITdGO6LlgA7yev6GE6KeUH6VN6PZ3XhFYSjfrFkOeMgzsmjtIZ6Xw1XPMaSfGCDqO94U6vhcsqoqX6eAzJbTo9hpH+8I136Eu48/qM+iVQ3tx6pxlLv9MbcJ0WHMHvetcO/IF2hJfhbvQSnZvac/T19CVzesKbpJ77q4ymk+HS0EsWI/2Ejknna+hJ2gMunWZ4YLTiFPcXXEblS82m19L5gLyhLUykb2g7eFSLX4WBKS9WkjN0KVz/Wi020b10MSon9Ds6KGRb6B745fMVqE1oD1Cd74B/HaamXCOlvEDLrNrnpOt59DTd0HqH0Uu/DZmYAA/ALlR+qdKgcjkVw7IzBB5xzZtloa30aE/Qr8A+2j60NSg9vwBzYG460O9TxgAAAABJRU5ErkJggg==>

[image11]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAF0AAAAaCAYAAADVLFAXAAADuklEQVR4Xu2YaahNURTHl7HM8zy8SKaSDEkpT4lEZIiS+IIyfCJCUciQD1Lm6YM581AoJR4lYyglU15E5nkoMv3/b+1977a65777PM99T+dX/zpnrX3OvWedvdde64jExMTExMT8H1SFGlqjoz60A7oMXYE2QDV+GyFSAZoPXYPOQcehtuGAkqac6J8oC9SFhkM3oBnGR/gcl6DNknyundDJcBBYDl2HqrvzSdATqEFiRAS86VnoLfQTei96oxyoGnQB+uh8b6CLzk4mQB+c77PobCjtMHgPRQPI/50q6KNEfU0DW3tn6+fOm0NfodGJERpLBn1xYEvLHNGbTrUOsE7UN8Q6RP8EH6CmdZRyekp00PdBr4yNs/07tNadM068vlNihJIH3TS2SCaK3mSedYBDor5xxs43ewxqbOxlgXRBvwflWyN4B513x0w9vJ4ZIeQI9AOqYuwpGSZ6kxXGngttdL5pxsccNt7Yygrpgv4Jum2N4IVoaiKcbLy+SdJdAFcJ7a2NPSW9RQdvC2wVocnQSOcLcxV/7LDobC8JuBldFd1fMlUfXpgh6YLOmXrLGsEz0X2NnBa93q7yPc7e2dhT0lF08NHANgaqA/V1vnCj3C7/uDz6y0QFnZOI9sKCnid/IeisVznY56xGomUV6eJ8B9z5YGiuOy4q/UVzJquIbOKDPtM6JDq9PIceueOo9LLX2dsYe0qYSris7rhz5mufOnJEb8TSkjUpm4DKzvcncLNh2somPuizrEM04A+sUXQjZQlNuOp5faukuwA+G+0ZbaSES+c11APqHtgZaN6IpdAqqFfgKyrlRXuCDtZhYE5ng8JuMFPlFlyZGT7os60D7BbtP0IqiY5f7845KXneLTFCYWeaKjVFcld0tk+xDvDFaY11BLSANkFLoP2S/EO1oNXQItEHeurs2cQHnf2JxTdHzQJbV2djeiRsnL6J7nsevpiXos+fMcznnO2p2tjHTgxgKuqJ5mo/gwdCp9zxGdEKiCwTDXy28dXaAusQXY3+MwCPmXpZYJwIB4l+BuB3l9runKuGHSmLj4xhExSVa7lshlpjwFLRTcQzQnR/GCTa3fn9gTmPSzNbLITui362YNDZZTJ/c58K4STaJclylCuVH8hC2KXyfvyGww9jfDE2x5co3GDCF7YS2ipa2/NlEgaeL4Crwc+OmGLA+tR/JmBuz4daii453+UOEK1120FjnS2mGLAuPSg6s7dIsnHid2mmHeby6aJNBSsgu1RjYmJiYmJiYgrnF3d66hfeh6zvAAAAAElFTkSuQmCC>

[image12]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAFQAAAAaCAYAAAApOXvdAAADN0lEQVR4Xu2YWYiNYRjH//bskTVlruxulCjbKIRkjayRNWuSFIWZG1kuSMiaC1IokSxluSG7EKK4cCU3su8J/7/nPXO+ec6cmTHTmWPOfL/6d77v/7znnN73e9/nfd4PiImJiYmJyRyTqTvUVeoGNaJ4OHPUoup4s5ozhvpMdQ73vaiPVH5RixLQQFyh3lG/qQ/UAyqPakzdpD6F2FvqVvDFPNgfKPaF2hv8XOEJUvt0lLrmvBJZAxuYJT5AdsNiemKeYdQlqpkPVHN6wPq8zPmFwW/r/BTmwxqu8wFyEhab6XzN7rNUO+fnAjNgfZ7l/BXBH+78FMbDGm5zvvLFvhDTj0VZSM11Xq6wCtbnqc7XCpY/x/kpDII1PBTx6lKLqEkhtiESa0+dgs3SbNOaugfL/eXVYH2xFApgfZ7ifI2H/OXOT6E7rOGZiDedakENCbFogj6M5O6XixSikgPaBtZQtZZQ0p0QrlUuKHYi3I+m1obrTKI81sCbVUS6Jb84+KpwSkXL+xf1LNwrPyaWcx7sR1ReNaHOUfVDLFO0gtWA2RpQDaT6PNv5iU2pXAW+6sw3VB+qd8TXIOpHVJftoPpHYplCq+OyN9OgHHqbuvsPyv/7zfR0hfV5pfO1j8gvV2XzHDZLNa0934N2+UCERrABV+m1EzaLNbv3U8dgte5G2MZXL3xnIHUg+GrTMrR/ATtE6DpbaAL5/z9NXXdeWpQ/NUv1xD0vg5r7QAQth9Xh+hGsbQGsIvhBdQmxi9Q42ExXOz0IsYVaH67vo+ydONPoIPOe6hnu+8H6MaCoRRmogNcuVhI6bmkQSmMp9Y26QPUNnh6OTlPR49pjahrsAUZXg2a3al6dur4iOdDZRLu8SjKtFs1MVTxVQkOqEzWROg57Q5NAsy5Rw2qz+Ul1g6UXfSZ4CNsERiJZbXRIhmsWWuqa4UJl1pFI7DySx1bl1z2wCuIV1TH4Y2GDqGpDJdlWqiksZdRIlF8OwmaiNpnEzNLAvYa9YNHGswnJDWkU7O2Ncud22AAKpQuVZptRes6ukeitzVNvxlScBSi+/GMqgV5x6eSlzWaoi8VUgNr4P95GxVRX/gDpw7gdSkSzGgAAAABJRU5ErkJggg==>

[image13]: <data:image/png;base64,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>

[image14]: <data:image/png;base64,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>

[image15]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAADoAAAAaCAYAAADmF08eAAADOUlEQVR4Xu2YWahNYRiGX7PMMl9wbswpGS+MSSQiQ4YyXaBMN5QyXxkylDIkpAzJkDlcSKaSISIXCDlKigtDSIYM79v3r/bqs9Y++2xnl6P91Ft7fe/ea+///7//+7+1gSJFilQgE6g1PphAX2ofVcUblQH9+LtUbW+ksIFa6YOFRLNazQfLSS3qCdXPG1moSZXCJigr+oFXqffUL+oDdY8qoepSN6hPwXtH3QxxMZP6GLzP1I4Qz5c51B0fjDGI6u+DZAl13gfT0Jv1g+d5g2yHeaO8QYZQF6gG3siD+7DBJqFseYvkyWxG/aS6eiOJWbDBrPAGOQHzprm4suEs1dLF86Et7Du6eCOg1JQ/yRuBR9QiH0xiDOxGm1x8ILUzeAucN5ua4WL5Mhm2Dap6I7ActmpNvRHYC5v0MhkAG4zKdUR1WCqND97qmNeKOomKK+3rYdXWcwy2Wt9gE6HXqimeZdRzH0yiM2wwZ2IxzXJjanDw4vtjP9U+dv23aEUu+WCgDvWF2uiNGPNhE1EmzWGDuR6uW1Bjw+tuwdPsipGwVMqHodRT6oCLKztUC5LQZ/T9I7wRYwrsPTpusqI01R54HK61/6K0LIHdRClTjzqHHG6YhVP4s7pqkGkDXUd9p+p7I0Y00JwaDZ2TKuG9qZ6xuAanmzygtiCHwzkLKjY6szu5+G6kp+5tZDItDaXuVx9MQ12JVnWuN2A3kbZ5I0ZrahesTz1K9QjxhtRWahV1iHoV4nHSipE++wOZQqijTPXBo2L0wgfT0KxpVXUAe14G6YuTaALbe9FKDacuhtdXYJVbKA01WM90WFfmj5d2sGyaSNWgDiL5rFUxO+2DaWiP+L0TcY0a7YMx1lJHYtfjYPtdBeQNMvtd+1P739MG6Q2DOjOttu7fy3kROnaW+mAhUE8cn6TNsFlWykVFRoPVoLXqjUIszkOkT3Q2ohawozcKwWFkWkTt1VLYKi1GptsaRr2mOlBTQyyOHhKyNfVpqE+PtknBUa96HLaCe5BpJtSyKeW0NxdSl2GVW02AR3vwGcpX1aPHtD7e+NfpTt1Cjuch7MFbFbtSokKW618pOmp8pS5S5H/hN14Tp7mIgdvKAAAAAElFTkSuQmCC>

[image16]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAD0AAAAZCAYAAACCXybJAAAB4klEQVR4Xu2XSygFURjHP2yw8CxWrGxQZCMLMUShpFgoZedRslGEUiIrYqeUssLegiQlpewsPLLwipCFUkJJHv+vc0733NNwr0dmps6vft0533fu7f6bM2dmiCwWkyQ4BSfgOiwLb1MK7DFqgWcYzsnjbngDY+V4Gi7BSzkODPVwFz7Iz3YYo/Vb4Iw87oBPFArNOOSP0Ikwwyy6UQV3YCaJZcxn7h2O6ZM0lkkscx2HvA2dBhvhHuw1eq5sw3JtHAdP4BvM0ur5JJY4z0/X6oxD3oVegBck9ho+WRFDc8BXeAqTtfosiR/o1GqKNngGU7WaQ96FVpRQlKH5urwjMTlHq0/KmtqRu2CFPM6VvWY5Zhx4pY29IOrQTBGsNmprJH5A1Y9gnzyuhS8wT44ZB15rYy/4VmiTbBKhDii0Q5fCeRI7+Aask3VmCK7CZ9nXe//Jr0IvwntYYDZ8zo9Dt8JHEss1aKjQ6jKMikJ4CyvNxh/Dd4lNuBWlxeJrEVGh+83GZ/DN/RDWaDWHwndov6NCD5gNN/hevQKbjPoIbDBqfkaFHjQbboyTuI73pbxrH5PYjfXbkt/hNz8OzSfrSxJITHSTH0PjQ1N9yyiJJ0p+CeL/zU+Y5yRWr8VisVgsQeUD8WJvW+obeHsAAAAASUVORK5CYII=>