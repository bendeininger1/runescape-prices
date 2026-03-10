# RuneScape Price Data Ingestion Tool

# Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)

# Overview <a name="overview"></a>

The **RuneScape Price Data Ingestion Tool** is desgined to collect, transform, and store data from the [OSRS RuneScape WIKI price API](https://oldschool.runescape.wiki/w/RuneScape:Real-time_Prices) for Old School RuneScape following a medallion architecture.

## Architecture
The architecture of the end-to-end data pipeline is designed to handle both batch and streaming data processing. Below is a high-level overview of the components and their interactions:

### High-Level Architecture
```mermaid
graph TB
    subgraph "Data Sources"
        BS[Batch Sources<br/>1 Hour Aggregated Price <br/> Data, object mapping]
        SS[Streaming Sources<br/>Latest/ 1 Minute Price Data ]
    end

    subgraph "Transformation Layer"
        AS[Apache Spark]
        SPS[Lakeflow Spark<br/>Declarative Pipelines]
    end

    subgraph "Load/ Storage Layer"
        ADLS[Azure Data Lake<br/>Storage Gen2]
    end

    BS --> AS
    SS --> SPS
    SPS --> ADLS
    AS --> ADLS
```

### Data Flow Diagram

<p align="center">
  <img src="docs/runescape.png" alt="Data Flow Diagram" width="100%"/>
</p>

### Batch Pipeline Flow

```mermaid
sequenceDiagram
    participant BS as Batch Source<br/>(JSON files)
    participant LF as Lakeflow jobs
    participant GE as Great Expectations
    participant MN as MinIO
    participant SP as Spark Batch
    participant PG as PostgreSQL
    participant MG as MongoDB
    participant PR as Prometheus

    LF->>BS: Trigger Batch Job
    BS->>LF: Extract Data
    AF->>GE: Validate Data Quality
    GE-->>AF: Validation Results
    AF->>MN: Upload Raw Data
    AF->>SP: Submit Spark Job
    SP->>MN: Read Raw Data
    SP->>SP: Transform & Enrich
    SP->>PG: Write Processed Data
    SP->>MG: Write NoSQL Data
    SP->>PR: Send Metrics
    AF->>PR: Job Status Metrics
```

### Streaming Pipeline Flow

```mermaid
sequenceDiagram
    participant KP as Kafka Producer
    participant KT as Kafka Topic
    participant SS as Spark Streaming
    participant AD as Anomaly Detection
    participant PG as PostgreSQL
    participant MN as MinIO
    participant GF as Grafana

    KP->>KT: Publish Events
    KT->>SS: Consume Stream
    SS->>AD: Process Events
    AD->>AD: Detect Anomalies
    AD->>PG: Store Results
    AD->>MN: Archive Data
    SS->>GF: Real-time Metrics
    GF->>GF: Update Dashboard
```


Disclaimer: This site is not affilated with RuneScape, Old School Runescape, Jagex Ltd, or the OSRS Wiki.
