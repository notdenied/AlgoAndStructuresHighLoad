# System Design: Smart Grid Architecture

This document outlines the architecture for a regional smart grid telemetry system handling 500,000 sensors and real-time command distribution.

## 1. Architectural Goals
- **High Write Throughput**: Accept metrics from 500k sensors every second (~500k RPS).
- **Fault Tolerance**: Handle network partitions without data loss.
- **Scalability**: Support increasing numbers of sensors and billing consumers.

## 2. CAP Analysis: Choosing AP (Availability + Partition Tolerance)

In a regional electrical grid, **Availability** is critical. If a district loses connectivity to the central Data Center (DC), the local sensors and backup generators must continue to operate and log data independently.

> [!IMPORTANT]
> We choose **AP** over CP. In the event of a network partition (P), our system prioritizes remaining **Available** (A) to accept telemetry, sacrificing immediate **Consistency** (C). Once the partition is resolved, the system achieves **Eventual Consistency** by syncing buffered data.

### Why AP?
1. **IoT Reliability**: Sensors shouldn't stop working just because the cloud is unreachable.
2. **Safety**: Real-time monitoring of local grids must continue during outages.

## 3. Storage Strategy: LSM-Tree

For the extreme write-heavy load of 500k RPS, an **LSM-Tree (Log-Structured Merge-Tree)** is superior to a B-Tree:
- **LSM-Tree**: Sequential writes to MemTable and then to SSTables. High throughput, low write amplification.
- **B-Tree**: Random writes to disk pages would create a bottleneck and lead to high IOPS/latency.

## 4. Resilience and Buffer Design (Edge Computing)

To handle the "P" in CAP, we implement **Local Brokers** at the substation level:
- **Local Buffer**: Each major substation has a local broker (e.g., NATS or a custom persistent log).
- **Buffered Sync**: When the connection to the Central DC is lost, sensors continue writing to the local buffer.
- **Reconciliation**: When connectivity is restored, the local broker drains its buffer to the Central DC, ensuring all data is eventually recorded.

## 5. Billing and Data Processing

Batch processing for billing (MapReduce) is performed on the Central DC cluster. Since billing is not real-time, Eventual Consistency is acceptable. The MapReduce jobs aggregate month-wide data from the LSM-Tree SSTables, which are naturally partitioned by time or sensor range.

---
*Created as part of the Smart Grid Engineering Track (High Load Systems).*
