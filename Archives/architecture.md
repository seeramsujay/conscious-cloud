# Technical Specification & Constraints

*Refer to the full research report for mathematical proofs and low-level details.*

## 1. xDS Control Plane
* **Library:** `github.com/envoyproxy/go-control-plane`
* **Protocol:** `DELTA_GRPC` (Incremental xDS). State of the World (SotW) is strictly forbidden due to serialization overhead.
* **Cache Type:** `MuxCache` (mixing `SimpleCache` for CDS/LDS and `LinearCache` for EDS).

## 2. Envoy Connection Shedding
To achieve zero-drop routing, Envoy must be configured to uncouple downstream and upstream connections:
* Do not use `parent_shutdown_time` (No hot restarts).
* Rely entirely on `max_connection_duration` and `drain_timeout` inside the HTTP Connection Manager.
* The AWS ALB `deregistration_delay` must be explicitly set higher than Envoy's `drain_timeout`.

## 3. The Blackhole Mitigation
Endpoints added via the Orchestrator must default to `UNHEALTHY`.
* Configure Envoy active health checks (`/healthz`).
* Traffic is suppressed mathematically (`weight: 0` effectively) until the application native socket returns `HTTP 200 OK` consecutively.

## 4. The 120-Second AWS Termination Hook
* **Event source:** `aws.ec2` detail-type `EC2 Spot Instance Interruption Notice`.
* **Action:** Route EventBridge notice to Lambda -> Update Redis -> Trigger Control Plane -> Set endpoint weight to `0` in Envoy.
* **Worker limitation:** Any synchronous HTTP request handled directly by Envoy must resolve in < 120 seconds. Heavier workloads must be shunted to SQS queues by the ingress API.
