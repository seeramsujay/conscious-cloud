# The Arbitrage Thesis: Live Cloud Compute Arbitrage Engine

## The Problem
Cloud compute is a top-3 operational expense for enterprise tech. AWS Spot instances offer 70-90% cost savings compared to On-Demand instances, but are fundamentally volatile. AWS reclaims Spot capacity with only a 120-second warning. Standard load balancers and monolithic architectures cannot migrate traffic fast enough to survive this volatility, resulting in dropped requests, corrupted data, and systemic downtime.

## The Solution
An autonomous, AI-orchestrated reverse proxy that live-migrates stateless high-compute workloads (e.g., rendering, ML batch inference) between AWS On-Demand and Spot instances. It executes this transition within the same Availability Zone to avoid data egress fees, maintaining zero dropped connections during the migration.

## The Moat (Why this wins)
* **Not a thin AI wrapper:** This is a hardcore, low-level distributed systems engineering product. It is inherently difficult to clone.
* **Direct ROI:** It is a financial painkiller. If the software cuts a client's AWS compute bill by 40% with zero downtime, it is an immediate B2B purchase.
* **Invisible AI:** The customer does not interface with the AI. The AI acts purely as a backend orchestrator, predicting spot pricing and executing topology mutations.

## Core Mechanics
1. **Predict & Provision:** The AI Orchestrator identifies an arbitrage opportunity and provisions an AWS Spot instance.
2. **Delta xDS Routing:** A Go-based Control Plane updates an Envoy Proxy fleet via sub-millisecond Delta gRPC streams.
3. **Zero-Drop Draining:** Envoy flawlessly drains the expensive On-Demand instance using HTTP/2 `GOAWAY` and HTTP/1.1 `Connection: close` semantics.
4. **HUGI Checkpointing:** For tasks taking >120 seconds, the system uses message queues and NVMe-to-S3 checkpointing to survive AWS termination notices.
