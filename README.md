# BigData_And_Cloud_Computing

# COM7020 – Big Data and Cloud Computing Assignment

## Project Title
Cloud-Enabled Big Data Architecture and Smart Meter Analytics using PySpark

## Overview
This project presents a Proof of Concept (PoC) for MetroEnergy Solutions (MES), demonstrating how Apache Spark (PySpark) can be used to process and analyze smart electricity meter data in a cloud-enabled big data environment.

The project includes data preprocessing, exploratory data analysis, feature engineering, demand analysis, and a machine learning model for energy consumption prediction.

## Objectives
- Process smart meter data using PySpark
- Perform data cleaning and preprocessing
- Analyze electricity consumption patterns
- Identify peak demand periods
- Visualize energy usage trends
- Build a simple demand forecasting model
- Demonstrate a scalable big data processing workflow

## Technologies Used
- Python 3
- Apache Spark (PySpark)
- Pandas
- Matplotlib
- Google Colab / Jupyter Notebook

## Project Structure

```
COM7020/
│
├── data/
│   └── Smart meter data.csv
│
├── notebooks/
│   └── MES_PoC.ipynb
│
├── output/
│   ├── charts/
│   ├── parquet/
│   └── reports/
│
├── report/
│   └── COM7020_Report.docx
│
└── README.md
```

## Project Workflow
1. Load smart meter dataset
2. Clean and preprocess data
3. Perform exploratory data analysis (EDA)
4. Engineer time-based features
5. Analyze daily and monthly energy consumption
6. Detect peak demand periods
7. Train a PySpark Linear Regression model
8. Visualize results and export outputs

## Dataset
This project uses a smart electricity meter dataset for academic purposes. The dataset contains timestamped energy consumption and electrical measurements such as voltage, current, and frequency.

## Output
The project generates:
- Cleaned dataset
- Daily and monthly energy summaries
- Peak demand analysis
- Charts and visualizations
- Machine learning predictions
- Processed Parquet files

## Disclaimer
This project was developed for the COM7020 Big Data and Cloud Computing module as an academic assignment. It is intended for educational purposes only.

## Author
**Rajkumar Methaniya**

MSc Project – COM7020 Big Data and Cloud Computing
