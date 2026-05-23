# 90-Day Sprint Roadmap

## Phase 1: The Routing Core (Days 1 - 20)
**Goal:** Prove we can programmatically shift traffic between two local Docker containers without dropping a connection.
* [ ] Scaffold the Go Control Plane using `envoyproxy/go-control-plane`.
* [ ] Configure Envoy `envoy.yaml` for `DELTA_GRPC` ADS.
* [ ] Write a dummy stateless HTTP worker (echo server with a configurable processing delay).
* [ ] Execute a local load test (e.g., using `hey` or `vegeta`). Send 1,000 req/sec while triggering an EDS IP swap. Ensure 0% error rate.

## Phase 2: AWS Infrastructure & State Tracking (Days 21 - 45)
**Goal:** Bridge the local routing core to physical AWS infrastructure without API polling.
* [ ] Write Terraform to deploy a private VPC, ALB, and Redis cluster.
* [ ] Configure AWS EventBridge rules to trap EC2 state changes (`running`, `terminated`).
* [ ] Write the AWS Lambda function to update Redis based on EventBridge triggers.
* [ ] Connect the Go Control Plane to subscribe to Redis Keyspace notifications.

## Phase 3: The AI Orchestrator (Days 46 - 70)
**Goal:** Automate the financial decision-making and infrastructure provisioning.
* [ ] Build the Orchestrator service to ingest AWS Spot Price histories.
* [ ] Implement the math logic: Calculate intra-AZ arbitrage margins.
* [ ] Give the Orchestrator IAM permissions to issue `RunInstances` and `TerminateInstances` commands.
* [ ] Integrate Orchestrator directly into the Go Control Plane API to issue topology updates.

## Phase 4: Hardening & The 2-Minute Cliff (Days 71 - 90)
**Goal:** Handle AWS Spot terminations gracefully for long-running tasks.
* [ ] Implement an SQS queue architecture for heavy tasks.
* [ ] Modify the dummy worker to simulate a 3-minute task with NVMe checkpointing.
* [ ] Inject a forced Spot Interruption via AWS CLI.
* [ ] Verify that Envoy drains the API connection safely, the worker checkpoints state, and the newly spun-up Spot instance resumes the SQS message from the exact checkpoint.
