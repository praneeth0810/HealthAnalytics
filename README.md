# HealthAnalytics: Unveiling Patterns in U.S. Healthcare Data

## Project Overview
HealthAnalytics leverages advanced data management and analytics technologies to transform complex healthcare data into actionable insights. This platform aids pharmaceutical companies in making informed strategic decisions for the launch of new drugs by analyzing drug usage patterns and prescriber behaviors at both city and individual levels.

## Key Features
- **Scalable Data Ingestion**: Utilizes Apache Hadoop for robust data management, ensuring smooth handling of large datasets.
- **Efficient Data Processing**: Apache Spark performs high-speed data processing, enabling quick and efficient analysis.
- **Secure Cloud Data Storage**: Integrates AWS S3 to provide reliable and scalable storage solutions, ensuring data availability and security.
- **Advanced Machine Learning Analysis**: Applies sophisticated machine learning algorithms to detect critical patterns in geographic and prescriber data, which are crucial for planning targeted marketing and distribution strategies.

## Project Components
### Data Pipeline
A comprehensive data pipeline that includes:
- **Data Ingestion**: Manages the bulk data transfer from diverse sources.
- **Data Processing**: Cleans, normalizes, and transforms raw data into a structured format for analysis.
- **Data Persistence**: Stores processed data in PostgreSQL for further analysis.

### Machine Learning Analysis
Contains scripts and data for performing detailed analysis on:
- **City-Level Insights**: Analyze demographic and geographic data to understand drug demand distribution across urban and rural settings.
- **Prescriber-Level Insights**: Examines detailed prescription data from healthcare providers to identify key influencers and predict market behaviors.

## File Structure
- **src**: Contains the source code organized into modules for easy navigation and maintenance.
  - **bin**: Automation and Python scripts to execute the pipeline.
  - **lib**: Required libraries and dependencies.
  - **logs**: Logs are used to monitor the operations performed.
  - **output**: Extracted data files from the pipeline operations.
  - **util**: Configuration files, mainly for logging setups.
  - **staging**: Input files necessary for initiating the pipeline processes.
- **venv**: Houses a virtual environment for the project’s Python dependencies.
- **Machine Learning Analysis**: Scripts and datasets for analyzing city and prescriber data.

## Running the Project
This project is configured to run on GCP. Specific steps involve setting up environment variables, starting necessary services, and executing the main pipeline script, which coordinates all tasks from data ingestion to output generation.

## Output Evaluation
- **Log Analysis**: Review the `presc_run_pipeline.log` for a detailed account of the pipeline execution.
- **Data Validation**: Check output files and database entries to validate the processing and persistence stages.

## Contributing
We welcome contributions from the community. If you want to improve HealthAnalytics, please fork the repository and submit a pull request.
