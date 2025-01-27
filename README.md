# A Fault Tolerant Distributed File System

A distributed, fault-tolerant file system implementation in Java that supports both replication and erasure coding strategies for data redundancy. The system demonstrates key concepts in distributed systems including fault tolerance, data integrity, and distributed coordination.

## 🚀 Features

- **Distributed Architecture**: File chunks distributed across multiple servers
- **Dual Redundancy Strategies**: 
  - Replication (3x replication factor)
  - Reed-Solomon Erasure Coding (6+3 configuration)
- **Data Integrity**: SHA-1 checksum verification for 8KB slices
- **Automatic Failure Recovery**: Self-healing system with chunk redistribution
- **Heartbeat Monitoring**: Health checking and system state management
- **Efficient Data Transfer**: Pipeline-based chunk propagation


## 🏗️ System Architecture
```plaintext
Client --> Controller
Controller --> ChunkServer1
Controller --> ChunkServer2
Controller --> ChunkServerN
Client --> ChunkServer1
Client --> ChunkServer2
Client --> ChunkServerN
ChunkServer1 --> ChunkServer2
ChunkServer2 --> ChunkServerN
```

### Core Components

1. **Controller Node**
   - System coordination and metadata management
   - Chunk server health monitoring
   - Chunk allocation and rebalancing

2. **Chunk Servers**
   - Local chunk storage and management
   - Integrity checking and repair
   - Chunk replication/forwarding
   - Regular heartbeat reporting

3. **Client**
   - File splitting and reassembly
   - Chunk distribution coordination
   - Read/write operations management


## 📋 Usage

### Starting the System

1. Start the Controller:
```bash
java com.distributed.dfs.controller.Controller <port>
```

2. Start Chunk Servers:
```bash
java com.distributed.dfs.chunkserver.ChunkServer <controller-ip> <controller-port>
```

3. Start Client:
```bash
java com.distributed.dfs.client.Client <controller-ip> <controller-port>
```

### Basic Operations

**Upload a file:**
```bash
upload <local-path> <destination-path>
```

**Download a file:**
```bash
download <source-path> <local-path>
```

## 🔧 Technical Details

### Chunk Management
- Chunk Size: 64KB
- Integrity Check: SHA-1 checksums for 8KB slices
- Storage Path: `/tmp/chunk-server/<file-path>_chunk<number>`

### Heartbeat System
- Major Heartbeat: Every 2 minutes (full metadata sync)
- Minor Heartbeat: Every 15 seconds (incremental updates)

### Replication Strategy
- 3x replication for each chunk
- Pipeline-based chunk propagation
- Automatic corruption detection and repair

### Erasure Coding Configuration
- Data Shards (k): 6
- Parity Shards (m): 3
- Reed-Solomon encoding for optimal storage efficiency

## 🔨 Build

```bash
gradle build
```

## 📚 Dependencies

- Java 11 or higher
- Reed-Solomon library for erasure coding

## 🤝 Contributing

Feel free to submit issues and enhancement requests!

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.