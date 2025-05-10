# 🏏 Cricket Data Analysis

This project aims to perform a comprehensive analysis of cricket data by building a robust data pipeline using **PySpark** and **Spark SQL**, implementing the **Bronze–Silver–Gold** layer architecture. The refined datasets are then used to derive business insights and visualize batting performance using **Matplotlib** and **Seaborn**.

---

## 📁 Project Structure

cricket/
├── cricket csv data/
│   ├── country.csv
│   ├── Batting/
│   │   ├── ODI data.csv
│   │   ├── t20.csv
│   │   ├── test.csv
│   ├── Bowling/
│   │   ├── Bowling_ODI.csv
│   │   ├── Bowling_t20.csv
│   │   ├── Bowling_test.csv
│   ├── Fielding/
│   │   ├── Fielding_ODI.csv
│   │   ├── Fielding_t20.csv
│   │   ├── Fielding_test.csv
├── cricket_notebooks/
│   ├── bronze/
│   │   ├── cricket01_bronze.py
│   ├── silver/
│   │   ├── cricket01_silver.py
│   ├── gold/
│   │   ├── cricket01_gold.py
│   ├── business trans/
│   │   ├── busines_trans_test.sql
│   ├── metadata/
│   │   ├── cricket01.sql
│   │   ├── gold/
│   │   │   ├── cricket01_gold.sql



---

## 🔧 Technologies Used

- **Apache Spark** with **PySpark**
- **Spark SQL**
- **Delta Lake** (for layered architecture)
- **Matplotlib**, **Seaborn** for visualizations
- **SQL** for business logic

---

## 🏗️ Pipeline Overview

### 🔹 Bronze Layer
- Raw ingestion of cricket CSV data.
- Minimal transformations, stored in Delta format.

### 🔸 Silver Layer
- Cleansing, validation, and enrichment.
- Data normalization and joins across batting, bowling, and fielding.

### 🥇 Gold Layer
- Aggregated and business-focused tables.
- Includes metrics such as:
  - Batting averages
  - Strike rates
  - Total runs
  - Performance by format (ODI, T20, Test)

---

## 📊 Visualization

Using the refined **Gold layer**, the following visualizations are generated:
- Top batsmen by format
- Strike rate comparisons
- Run distribution charts
- Performance trends

Generated using:
- `matplotlib.pyplot`
- `seaborn`

---

## 🚀 How to Run

1. Install required packages:
   ```bash
   pip install pyspark matplotlib seaborn
2.  Load and run scripts from the cricket_notebooks/ directory in order:

    Bronze → Silver → Gold
3.  Execute the SQL and Python scripts in business trans/ and gold/ to generate analytics.

4.  Visualize results with matplotlib and seaborn.

✍️ Author
Rohit Bansode
Data Engineer | Cricket Analytics Enthusiast
