# RuneScape Price Data Ingestion Tool

# Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Flow Diagram](#data-flow-diagram)
4. [Requirements](#requirements)
5. [Install](#install)
6. [Contributing](#contributing)

# Overview <a name="overview"></a>

The **RuneScape Price Data Ingestion Tool** is designed to collect, transform, and store data from the [OSRS RuneScape WIKI price API](https://oldschool.runescape.wiki/w/RuneScape:Real-time_Prices) for Old School RuneScape following a medallion architecture.

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

### Requirements
Runs on Databricks Runtime 17.3 LTS. powered by Apache Spark 4.0.0  
Datbricks Connect  
Python 3.12.10+  
Java 21 SDK  
Databricks CLI [Link](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/install)

### Install
TODO

### Contributing
TODO

Disclaimer: This site is not affilated with RuneScape, Old School Runescape, Jagex Ltd, or the OSRS Wiki.
