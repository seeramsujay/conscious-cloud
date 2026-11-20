<p align="center">
  <img src="assets/logo.svg" alt="Conscious Cloud Logo" width="220" />
</p>

<h1 align="center">Conscious Cloud</h1>

<p align="center">
  <strong>Autonomous Intra-Cloud Compute Arbitrage Engine &amp; Zero-Drop Envoy Proxy xDS Orchestrator.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Data_Plane-Envoy_Proxy_v1.30+-A032E8.svg?style=flat-square&logo=envoy" alt="Envoy Proxy">
  <img src="https://img.shields.io/badge/Control_Plane-Go_(go--control--plane)-00ADD8.svg?style=flat-square&logo=go" alt="Go Control Plane">
  <img src="https://img.shields.io/badge/xDS_Protocol-DELTA__GRPC-4285F4.svg?style=flat-square" alt="Delta xDS">
  <img src="https://img.shields.io/badge/Cloud-AWS_Spot_%26_EventBridge-FF9900.svg?style=flat-square&logo=amazonwebservices" alt="AWS">
  <a href="future.md"><img src="https://img.shields.io/badge/roadmap-future.md-blueviolet.svg?style=flat-square" alt="Roadmap"></a>
</p>

---

## ⚡ Overview

**Conscious Cloud** is an AI-driven, zero-drop L4/L7 traffic orchestrator designed to dynamically route stateless microservice workloads across AWS On-Demand and Spot instances using Envoy Proxy and Delta xDS.

By anticipating spot termination warnings and streaming incremental topology mutations down to Envoy via gRPC, Conscious Cloud slashes cloud infrastructure costs by up to 70% while guaranteeing zero dropped connections.

---

## 🏗️ System Architecture

```mermaid
graph TD
    Client[End-User L4/L7 Traffic] --> Envoy[Envoy Proxy Data Plane v1.30+]
    
    subgraph Compute Fleet
        Envoy -->|Primary Dynamic Traffic| SpotNodes[AWS Spot Fleet 70% Savings]
        Envoy -->|Graceful Fallback| OnDemandNodes[AWS On-Demand Baseline]
    end

    EventBridge[AWS EventBridge: 2-Min Spot Warning] --> Lambda[AWS Lambda State Mutator]
    Lambda --> Redis[(ElastiCache Redis)]
    
    Redis --> GoControlPlane[Go xDS Control Plane Server]
    GoControlPlane -->|Sub-ms DELTA_GRPC| Envoy
    
    Orchestrator[AI Spot Arbitrage Agent] -->|Spot Market Bidding| GoControlPlane
```

### 🧩 Component Breakdown
* **Data Plane:** Envoy Proxy (v1.30+)
* **Control Plane:** Custom Go server utilizing `envoyproxy/go-control-plane`
* **Orchestrator:** AI agent (Python/Go) evaluating AWS Spot pricing and triggering mutations
* **State Management:** Amazon EventBridge -> AWS Lambda -> Redis (ElastiCache)
* **Workload Decoupling:** Amazon SQS / RabbitMQ

---

## 🚀 Core Features

* **Zero-Drop Connection Draining:** Mathematically tuned `drain_timeout` and active connection shedding prevent HTTP 502/504 errors during instance terminations.
* **Sub-Millisecond Topology Updates:** Real-time cluster membership synchronization via the `DELTA_GRPC` xDS protocol without server reloads.
* **Race Condition Immunity:** Multi-tiered active health checking prevents blackhole routing to unready nodes.
* **API Throttling Bypass:** 100% event-driven AWS state tracking via EventBridge (Zero polling).
* **2-Minute Cliff Survival:** Native support for the Hurry-Up-and-Get-Idle (HUGI) pattern and NVMe checkpointing.

---

## 📁 Repository Structure

```plaintext
/conscious-cloud
├── control-plane/       # Go-based xDS Server implementation
├── data-plane/          # Envoy configurations (envoy.yaml, TLS certs)
├── orchestrator/        # AI Agent logic and Spot market evaluation
├── infrastructure/      # Terraform modules (VPC, EC2, EventBridge, Redis)
├── workers/             # Dummy stateless workers for testing (Go/Python)
└── Archives/            # Historical docs: ideas, specs, architecture
```

---

## 🛠️ Quick Start (Local Development)

### 1. Build and Run the Go Control Plane
```bash
cd control-plane
go run ./main.go
```

### 2. Launch the Envoy Data Plane
```bash
docker run --rm -v $(pwd)/data-plane/envoy.yaml:/etc/envoy/envoy.yaml \
  -p 10000:10000 envoyproxy/envoy:v1.30-latest
```

### 3. Trigger Arbitrage Test
Use the Orchestrator API to inject dummy node IPs and observe Envoy dynamic cluster rotation in real-time.

---

## 🔮 Engineering Roadmap

To explore planned features including carbon-intensity grid optimization, eBPF TCP socket migration, and multi-cloud spot federation across AWS/GCP, check out [future.md](future.md).

---

## 📄 License

This project is licensed under the Apache License 2.0.
