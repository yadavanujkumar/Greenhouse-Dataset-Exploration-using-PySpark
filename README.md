# Greenhouse Gas Emission Analysis - PySpark Data Engineering Project

A comprehensive data engineering project using Apache PySpark to analyze greenhouse gas (GHG) emission factors in supply chains across different industries.

## 📋 Project Overview

This project analyzes the EPA's Supply Chain GHG Emission Factors dataset using PySpark's distributed computing capabilities. It performs data loading, preprocessing, quality validation, and various analytical operations to understand emission patterns across industries and GHG types.

## 🎯 Features

- **Data Loading & Preprocessing**: Load and clean large CSV datasets using PySpark
- **Data Quality Validation**: Comprehensive data quality checks and reporting
- **Emission Analysis**: 
  - Top emitters identification
  - Sector-level aggregation
  - Margin impact analysis
  - High-emission industry detection
  - Emission distribution analysis
- **GHG Type Analysis**: Breakdown of emissions by individual greenhouse gases
- **Automated Reporting**: Generate CSV reports for all analyses
- **Scalable Architecture**: Built with PySpark for handling large datasets

## 📁 Project Structure

```
.
├── main.py                          # Main application entry point
├── config/
│   └── config.py                    # Configuration parameters
├── src/
│   ├── data_loader.py              # Data loading and preprocessing
│   ├── analysis.py                 # Analysis functions
│   └── utils.py                    # Utility functions
├── output/                         # Generated analysis results
│   ├── analysis/                   # CSV analysis reports
│   └── reports/                    # Summary reports
├── requirements.txt                # Python dependencies
├── SupplyChainGHGEmissionFactors_v1.3.0_NAICS_CO2e_USD2022.csv
└── SupplyChainGHGEmissionFactors_v1.3.0_NAICS_byGHG_USD2022.csv
```

## 📊 Datasets

The project uses two EPA datasets:

1. **CO2e Emissions Dataset**: Aggregated CO2 equivalent emissions by industry
   - ~1,000 records covering various NAICS industry codes
   - Emissions with and without margins

2. **Individual GHG Dataset**: Detailed emissions by specific GHG types
   - ~18,000 records with individual greenhouse gases
   - Includes CO2, Methane, Nitrous oxide, HFCs, PFCs, and more

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Apache Spark 3.5.0
- Java 8 or higher (required for Spark)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yadavanujkumar/Greenhouse-Dataset-Exploration-using-PySpark.git
cd Greenhouse-Dataset-Exploration-using-PySpark
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Application

Execute the main application:
```bash
python main.py
```

The application will:
1. Load and preprocess the datasets
2. Perform data quality validation
3. Run various emission analyses
4. Generate reports in the `output/analysis/` directory

## 📈 Analysis Outputs

The application generates the following analyses:

1. **Top Emitters**: Top 20 industries with highest emissions
2. **Sector Analysis**: Aggregated emissions by industry sector
3. **Margin Impact**: Comparison of emissions with and without margins
4. **High Emission Industries**: Industries exceeding emission thresholds
5. **Emission Distribution**: Distribution of industries across emission ranges
6. **GHG Type Analysis**: Breakdown by individual greenhouse gas types

All results are saved as CSV files in the `output/analysis/` directory.

## ⚙️ Configuration

Edit `config/config.py` to customize:
- Application name
- Data file paths
- Output directories
- Spark configuration (memory, executors)
- Analysis parameters (top N emitters, thresholds)

## 🔧 Technical Details

### Technologies Used
- **Apache PySpark 3.5.0**: Distributed data processing
- **Python 3.x**: Core programming language
- **Pandas**: Data manipulation utilities
- **NumPy**: Numerical computations

### Key PySpark Operations
- DataFrame operations and transformations
- Aggregations and grouping
- Window functions
- Data validation and quality checks
- CSV/Parquet I/O operations

## 📝 Example Output

```
============================================================
GREENHOUSE GAS EMISSION ANALYSIS - DATA ENGINEERING PROJECT
============================================================

STEP 1: DATA LOADING AND PREPROCESSING
Loading data from: SupplyChainGHGEmissionFactors_v1.3.0_NAICS_CO2e_USD2022.csv
Loaded 1,016 rows with 8 columns

STEP 3: EMISSION ANALYSIS
Top 20 Industries by Emissions:
+----------+--------------------------------+----------------+
|NAICS_Code|Industry                        |Total_Emissions |
+----------+--------------------------------+----------------+
|324110    |Petroleum Refineries            |12.456          |
|325110    |Petrochemical Manufacturing     |9.234           |
...

ANALYSIS SUMMARY
✓ Processed 1,016 CO2e emission records
✓ Generated 6 analysis reports
✓ All results saved to 'output/analysis' directory
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👤 Author

**Anuj Kumar Yadav**
- GitHub: [@yadavanujkumar](https://github.com/yadavanujkumar)

## 🙏 Acknowledgments

- EPA for providing the Supply Chain GHG Emission Factors dataset
- Apache Spark community for the excellent framework
- NAICS for industry classification standards