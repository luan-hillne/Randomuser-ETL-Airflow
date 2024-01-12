#  Realtime Data Streaming 
![image](https://i0.wp.com/i.morioh.com/200806/bf221e77.webp?resize=413%2C228&ssl=1)

## Introduction

This project leverages a robust tech stack comprising Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra, all containerized using Docker for seamless deployment and scalability

## Overview

- **Data Source**: Utilizes the 'randomuser.me' API to generate random user data for the pipeline.
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.

## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Docker

## Getting Started

1. Clone the repository:
    ```bash
    https://github.com/luan-hillne/Randomuser-ETL-Airflow.git
    ```

2. Navigate to the project directory:
    ```bash
    cd Randomuser-ETL-Airflow
    ```

3. Run Docker Compose to spin up the services:
    ```bash
    docker-compose up -d
    ```
