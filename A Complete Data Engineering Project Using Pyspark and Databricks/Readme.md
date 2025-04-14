# A Complete Data Engineering Project Using Pyspark and Databricks Lakehouse

## Use Article like for detailed info:-  https://datatoinfolabs.com/a-complete-data-engineering-project-using-pyspark-and-databricks/

**Project Overview:**

The project involves processing data from smartwatches sold by a retailer. Upon purchase, each device is registered, capturing details such as User ID, Device ID, MAC Address, and Registration Timestamp. This registration data is stored in a cloud-based database on Microsoft Azure.​

Additionally, a companion mobile application allows users to create and manage their profiles, generating Change Data Capture (CDC) records (new profiles, updates, deletions) that are sent to a Kafka topic. The smartwatch continuously streams pulse rate data (BPM) to another Kafka topic.​

The company partners with various healthcare and fitness centers equipped with device identification scanners. When a user enters or exits a facility, login and logout events are captured and sent to a Kafka topic. Furthermore, the smartwatch's workout session start and stop events are recorded in a separate Kafka topic.​

![lakehouse platform](https://github.com/user-attachments/assets/89492595-7171-414f-8f2d-5217f1326eb7)


**In total, the project handles five distinct datasets:​**

- Device registration data​

- User profile CDC data​

- BPM stream data​

- Login/logout events​

- Workout session events​

**Design Goals and Best Practices:**

***The project aims to:***

- Design a secure Lakehouse platform with development, testing, and production environments.​

- Decouple data ingestion from data processing.​

- Support both batch and streaming workflows.​

- Automate integration testing.​

- Implement an automated deployment pipeline for testing and production environments.​

**Storage Design:**

***The architecture employs the medallion (multi-hop) pattern, organizing data into three layers:​***

- Bronze Layer: Raw data ingestion from various sources.​

- Silver Layer: Cleansed and enriched data.​

- Gold Layer: Aggregated and refined data ready for consumption.​

**Decoupling Data Ingestion:**

To enhance scalability and maintainability, data ingestion is decoupled from processing. This approach allows independent scaling and management of ingestion and processing components.​

**Designing Bronze Layer:**

The Bronze layer handles raw data ingestion from various sources, including Kafka topics and cloud databases, ensuring that data is stored in its original form for future processing.​

**Designing Silver and Gold Layers:**

- Silver Layer: Processes raw data from the Bronze layer, performing cleansing, deduplication, and enrichment to produce refined datasets.​

- Gold Layer: Further aggregates and transforms data from the Silver layer to create business-level data products suitable for analytics and reporting.​

**Environment Setup:**

***The project involves setting up the following components:​***

- Azure Databricks Workspace: Creating a collaborative environment for data engineering tasks.​

- Storage Layer: Implementing storage solutions for the Bronze, Silver, and Gold layers.​

- Unity Catalog: Setting up a unified governance solution for data and AI assets.​

- Source Control: Utilizing Azure DevOps for version control and collaboration.​

**Coding and Testing:**

**The development process includes:​**

- Coding: Implementing data ingestion and processing logic using PySpark within Databricks notebooks.​

- Testing: Conducting unit and integration tests to ensure code reliability and data accuracy.​

**Data Loading and Processing:**

- Historical Data Loading: Ingesting existing data into the Bronze layer.​

- Ingestion into Bronze Layer: Capturing real-time data streams and storing them in the Bronze layer.​

- Processing Silver Layer: Transforming raw data into cleansed and enriched datasets.​

- Implementing Gold Layer: Aggregating data to produce final datasets for analysis and reporting.​

**Run Script and Integration Testing:**

A run script is created to automate the execution of data pipelines. Integration tests are prepared to validate the end-to-end data flow and processing logic.​

**CI/CD Pipeline Implementation:**

The project sets up a Continuous Integration/Continuous Deployment (CI/CD) pipeline using Azure DevOps to automate testing and deployment processes, ensuring efficient and reliable code releases.​

This comprehensive project demonstrates the implementation of a scalable and efficient data engineering solution using modern cloud and big data technologies, following best practices in data architecture and pipeline development.
