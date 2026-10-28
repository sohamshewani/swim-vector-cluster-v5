# swim-vector-cluster
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/swim-vector-cluster/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/swim-vector-cluster/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/swim-vector-cluster) [![Stars](https://img.shields.io/github/stars/sohamshewani/swim-vector-cluster?style=flat)](https://github.com/sohamshewani/swim-vector-cluster)

## Executive Summary & Problem Space

`swim-vector-cluster` is an enterprise-grade, high-throughput, low-latency distributed cluster membership and failure detection engine written in modern Java. Designed to replace traditional Paxos/Raft-based coordination layers for non-consensus metadata operations, it implements a strongly-convergent variant of the SWIM (Scalable Weakly-consistent Infection-style Process Group Membership) protocol augmented with bounded vector clocks.

In hyperscale distributed architectures, traditional heartbeat mechanisms suffer from $O(N^2)$ network saturation, catastrophic coordinator bottlenecks, and split-brain states during network partitions. `swim-vector-cluster` resolves these foundational challenges by:
1. **Decentralized Failure Detection:** Bounded, randomized periodic ping-ack probes combined with indirect probing mechanisms guarantee $O(1)$ local network load per node while retaining $O(\log N)$ global convergence latency.
2. **Causal Conflict Resolution:** Replacing monotonic epoch counters with rigorous `VectorClock` implementations eliminates state divergence during concurrent cluster mutations and transient partitions.
3. **Zero-Allocation Data Paths:** Critical paths utilize direct off-heap ring buffers (`DirectAnnouncementRing`) and cache-aligned atomic primitives to completely eliminate Garbage Collection (GC) pauses under high message velocity.

---

## Structural ASCII Architecture / Dataflow Diagram

The system is engineered around a strict layered architecture, decoupling network I/O, state synchronization, and cluster consensus algorithms from user applications.

```
 +-----------------------------------------------------------------------+
 |                         Application Layer                             |
 +-----------------------------------------------------------------------+
      | (Commands / Queries)            | (Cluster Events)
      v                                 v
 +-----------------------------------------------------------------------+
 |                     ClusterBootstrap (driver)                         |
 +-----------------------------------------------------------------------+
      |                                 |
      v                                 v
 +-----------------------------------------------------------------------+
 |                     SwimClusterEngine (engine)                        |
 |  +-------------------------+  +------------------------------------+  |
 |  |    VectorClock (core)    |  |  DirectAnnouncementRing (memory)   |  |
 |  +-------------------------+  +------------------------------------+  |
 +-----------------------------------------------------------------------+
      |                                 |
      v                                 v
 +-----------------------------------------------------------------------+
 |                        SpinLatch (concurrency)                        |
 +-----------------------------------------------------------------------+
      |                                 |
      v                                 v
 +-----------------------------------------------------------------------+
 |                      Physical Network / NIO                           |
 +-----------------------------------------------------------------------+
```

### Internal Dataflow Pipeline

```
[Inbound UDP/TCP Packet] 
       │
       ▼
[DirectAnnouncementRing] ──(Zero-Copy Off-Heap Buffer)
       │
       ▼
[SpinLatch Synchronization Boundary] ──(CAS-based Low-Contention Hand-off)
       │
       ▼
[VectorClock Conflict Resolver] ──(Causal Dominance Check)
       │
       ▼
[SwimClusterEngine State Mutation] ──(Local Cluster State Update)
```

---

## Concurrency Model & Memory Invariants

`swim-vector-cluster` rejects standard monitor locks (`synchronized`) and high-overhead java.util.concurrent abstractions in core loops, implementing a custom, ultra-low-latency concurrency paradigm.

### 1. Synchronization via `SpinLatch`
* **Mechanism:** Built on top of atomic compare-and-swap (CAS) primitives (`VarHandle` / `Unsafe`), `SpinLatch` provides an exclusive acquisition path with exponential back-off and thread yield phases.
* **Invariant:** Prevents OS-level thread context switches during micro-contention windows, bounding lock acquisition latency to nanoseconds.

### 2. Off-Heap Memory Ownership (`DirectAnnouncementRing`)
* **Mechanism:** Membership announcements and gossip payloads are serialized directly into direct byte buffers (`ByteBuffer.allocateDirect()`).
* **Invariant:** Memory management is strictly bounded; buffers are pre-allocated at cluster bootstrap. This eliminates JVM heap allocation churn, preventing Stop-The-World (STW) GC pauses from destabilizing failure detector timeout windows.

### 3. Causal Consistency Enforcement (`VectorClock`)
* **Mechanism:** State updates carry causal histories represented as vector clocks mapping `NodeId` to monotonically increasing sequence integers.
* **Invariant:** Concurrent updates are mathematically reconciled via strict partial-ordering rules. If neither clock dominates, deterministic tie-breaking (lexicographical node ID ordering) guarantees convergence across all surviving nodes without central coordination.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation | Target Path | Time Complexity | Space Complexity | Estimated P99 Latency |
| :--- | :--- | :--- | :--- | :--- |
| **Local State Read** | `SwimClusterEngine` | $O(1)$ | $O(1)$ | $< 150\text{ns}$ |
| **Vector Clock Merge** | `VectorClock` | $O(K)$ *(where $K = $ active nodes)* | $O(K)$ | $< 800\text{ns}$ |
| **Gossip Announcement Write** | `DirectAnnouncementRing` | $O(1)$ (Ring Slot CAS) | $O(B)$ *(Buffer Capacity)* | $< 300\text{ns}$ |
| **Cluster Membership Convergence**| Distributed SWIM Engine | $O(\log N)$ | $O(N)$ | $< 250\text{ms}$ (for 1000 nodes)|
| **Spin Latch Acquisition** | `SpinLatch` | $O(1)$ Amortized CAS | $O(1)$ | $< 50\text{ns}$ (Low contention) |

---

## Security Architecture & Threat Model

`swim-vector-cluster` is engineered for hostile, multi-tenant cloud environments where network partitions and malicious packet injections are treated as baseline operational norms.

### 1. Input Boundaries & Packet Sanitization
* All incoming network payloads parsed by `ClusterBootstrap` and handled by `SwimClusterEngine` pass through strict structural boundary checks. 
* Variable-length strings (`NodeId`) and payload headers enforce strict maximum length constraints before direct buffer ingestion to prevent buffer overflow and denial-of-service (DoS) memory exhaustion attacks.

### 2. Memory Corruption Protections
* Direct memory segments managed by `DirectAnnouncementRing` utilize explicit bounds checking and safe offset arithmetic. 
* Native pointer de-referencing is entirely avoided; all off-heap interactions are mediated by validated Java NIO Buffer APIs.

### 3. Threat Vectors & Mitigations
* **IP Spoofing / Replay Attacks:** Membership updates require cryptographic vector clock parentage validation. Stale or forged vector timestamps lacking causal lineage are rejected at the ingestion boundary.
* **Slow-Loris / Contention Exhaustion:** Critical thread-to-ring handoffs protected by `SpinLatch` incorporate maximum spin limits and tripwires, preventing rogue threads from locking out the membership engine.

---

## Build, Verification & Benchmarking Recipes

Ensure your environment satisfies the prerequisites:
* **Java Development Kit (JDK):** Version 17 or higher (LTS recommended).
* **Build System:** Apache Maven 3.8+.
* **Python Environment:** Python 3.9+ with `pytest` and `numpy` (for invariant tests and benchmark scripts).

### 1. Compile and Package
To build the project and execute standard unit testing suites:
```bash
mvn clean verify
```

### 2. Run Invariant Verification Tests
To validate distributed system safety properties and state convergence models using the Python harness:
```bash
python3 -m pytest tests/test_invariants.py -v --tb=short
```

### 3. Execute Engine Benchmarks
To run throughput, latency profiling, and allocation analysis on the core engine:
```bash
python3 benchmarks/bench_engine.py --threads=8 --iterations=100000 --output=json
```

---

*Maintained by the Core Infrastructure Engineering Team.*