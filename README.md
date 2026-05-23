# Autonomous Intra-Cloud Compute Arbitrage Engine

An AI-driven, zero-drop L4/L7 traffic orchestrator designed to dynamically route stateless microservices across AWS On-Demand and Spot instances using Envoy Proxy and Delta xDS.

## System Architecture Overview

* **Data Plane:** Envoy Proxy (v1.30+)
* **Control Plane:** Custom Go server utilizing `envoyproxy/go-control-plane`
* **Orchestrator:** AI agent (Python/Go) evaluating AWS Spot pricing and triggering mutations.
* **State Management:** Amazon EventBridge -> AWS Lambda -> Redis (ElastiCache)
* **Workload Decoupling:** Amazon SQS / RabbitMQ

## Core Features
* **Zero-Drop Connection Draining:** Mathematically tuned `drain_timeout` and active connection shedding.
* **Sub-Millisecond Topology Updates:** Implementation of the DELTA_GRPC xDS protocol.
* **Race Condition Immunity:** Multi-tiered active health checking preventing blackhole routing.
* **API Throttling Bypass:** 100% event-driven AWS state tracking via EventBridge (Zero polling).
* **2-Minute Cliff Survival:** Support for the HUGI (Hurry Up and Get Idle) pattern and NVMe checkpointing.

## Repository Structure
```text
.
├── control-plane/       # Go-based xDS Server implementation
├── data-plane/          # Envoy configurations (envoy.yaml, TLS certs)
├── orchestrator/        # AI Agent logic and Spot market evaluation
├── infrastructure/      # Terraform modules (VPC, EC2, EventBridge, Redis)
├── workers/             # Dummy stateless workers for testing (Go/Python)
└── Archives/            # Docs: idea, roadmap, architecture specs
```

## Quick Start (Local Development)

1. Generate local mTLS certificates for Envoy-to-Control-Plane communication.
2. Build and run the Go Control Plane: `go run ./control-plane/main.go`
3. Launch the Envoy Data Plane via Docker: `docker run -c ./data-plane/envoy.yaml envoyproxy/envoy:v1.30-latest`
4. Use the Orchestrator API to inject dummy IPs and observe Envoy cluster rotation.
