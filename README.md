# Spotify Data Pipeline

A production-ready data pipeline that automates the extraction, transformation, and loading (ETL) of Spotify track and artist data for analytics and reporting.

---

## Overview

This project connects to the Spotify Web API to extract rich metadata about tracks, albums, and artists.  
The data is processed and transformed into a clean, structured format before being stored in a target destination such as cloud storage or a relational database.

The pipeline is designed to be modular, scalable, and easily deployable in a serverless environment such as AWS Lambda.

---

## Features

- **Authentication**: Securely connects to the Spotify API using OAuth 2.0.
- **Data Extraction**: Pulls detailed information about tracks, artists, and playlists.
- **Data Transformation**: Cleans and reshapes raw API responses into analytics-friendly tables.
- **Data Loading**: Uploads processed data to a storage solution (e.g., AWS S3, AWS Athena).
- **Automation Ready**: Built for deployment on AWS Lambda with optional event-driven scheduling. Uses Cloudwatch for daily triggers to update CSV data.

---

## Technologies Used

- **Python**
- **Pandas** 
- **Requests** 
- **Spotify Web API**
- **AWS Lambda** 
- **AWS S3** 
- **AWS Cloudwatch**
- **AWS Glue**
- **AWS Crawler**
- **AWS Athena**

---

## Goals

- Create a reliable and efficient way to collect Spotify data for analysis.
- Enable easy scaling of the pipeline to support large datasets.
- Maintain clean, production-grade code ready for cloud deployment.
- Allow extensibility to support additional APIs or storage options in the future.

---

## Potential Extensions

- Integrate with a Snowflake OLAP database for long-term storage instead of Athena.
- Build a monitoring system to track pipeline health and failures.
- Develop a lightweight dashboard to visualize the extracted data.
- Implement Airflow or Step Functions for orchestration as the pipeline grows.

---

## License

This project is licensed under the [MIT License](LICENSE).

> Designed and maintained with care for high-quality data workflows.
