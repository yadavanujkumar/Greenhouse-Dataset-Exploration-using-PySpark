# Troubleshooting Guide

This guide helps resolve common issues you might encounter when running the Greenhouse Gas Emission Analysis project.

## Common Issues and Solutions

### 1. Java Not Found

**Error:**
```
java: command not found
```

**Solution:**
- Install Java 8 or higher
- On Ubuntu/Debian: `sudo apt-get install openjdk-11-jdk`
- On macOS: `brew install openjdk@11`
- On Windows: Download from [Oracle](https://www.oracle.com/java/technologies/downloads/) or [OpenJDK](https://adoptium.net/)

### 2. PySpark Import Error

**Error:**
```
ModuleNotFoundError: No module named 'pyspark'
```

**Solution:**
```bash
pip install -r requirements.txt
```

### 3. Memory Issues

**Error:**
```
OutOfMemoryError: Java heap space
```

**Solution:**
Edit `config/config.py` and reduce memory settings:
```python
spark_config = {
    'master': 'local[*]',
    'executor_memory': '1g',  # Reduced from 2g
    'driver_memory': '1g'     # Reduced from 2g
}
```

Or set environment variable:
```bash
export SPARK_DRIVER_MEMORY=1g
export SPARK_EXECUTOR_MEMORY=1g
python main.py
```

### 4. CSV File Not Found

**Error:**
```
FileNotFoundError: [Errno 2] No such file or directory: 'SupplyChainGHGEmissionFactors...'
```

**Solution:**
Ensure you're running the script from the project root directory:
```bash
cd /path/to/Greenhouse-Dataset-Exploration-using-PySpark
python main.py
```

### 5. Permission Denied on Output Directory

**Error:**
```
PermissionError: [Errno 13] Permission denied: 'output/analysis'
```

**Solution:**
Create the output directories with proper permissions:
```bash
mkdir -p output/analysis output/reports
chmod -R 755 output/
```

### 6. Slow Performance

**Symptom:** Application takes too long to run

**Solutions:**
1. Increase Spark parallelism in `src/data_loader.py`:
   ```python
   spark = SparkSession.builder \
       .config("spark.default.parallelism", "4") \
       ...
   ```

2. Reduce data size for testing by limiting rows in analysis functions

3. Use more CPU cores by adjusting master setting in `config/config.py`:
   ```python
   'master': 'local[4]',  # Use 4 cores instead of all (*)
   ```

### 7. Jupyter Notebook Issues

**Error:**
```
ModuleNotFoundError when running notebook cells
```

**Solution:**
Install Jupyter and ensure correct kernel:
```bash
pip install jupyter
jupyter notebook notebooks/exploration.ipynb
```

Make sure the notebook is using the correct Python environment where dependencies are installed.

### 8. Hadoop Library Warning

**Warning:**
```
WARN NativeCodeLoader: Unable to load native-hadoop library for your platform...
```

**Solution:**
This is a common warning and can be safely ignored. It doesn't affect functionality. If you want to suppress it:
```python
import warnings
warnings.filterwarnings('ignore')
```

### 9. Character Encoding Issues

**Error:**
```
UnicodeDecodeError: 'utf-8' codec can't decode byte...
```

**Solution:**
The CSV files should be UTF-8 encoded. If you encounter this issue:
```python
# In src/data_loader.py, modify the load_csv_data function:
df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True,
    encoding="ISO-8859-1"  # Try different encoding
)
```

### 10. Module Import Errors

**Error:**
```
ImportError: cannot import name 'create_spark_session' from 'src.data_loader'
```

**Solution:**
Make sure you're in the project root directory and Python can find the modules:
```bash
cd /path/to/Greenhouse-Dataset-Exploration-using-PySpark
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
python main.py
```

## Performance Optimization Tips

1. **Use Parquet for intermediate data:**
   ```python
   # Instead of CSV
   df.write.mode("overwrite").parquet("output/data.parquet")
   ```

2. **Cache frequently used DataFrames:**
   ```python
   co2e_df = preprocess_co2e_data(co2e_df_raw)
   co2e_df.cache()  # Cache in memory
   ```

3. **Partition large outputs:**
   ```python
   df.repartition(4).write.csv("output/analysis/results")
   ```

## Getting Help

If you continue to experience issues:

1. Check the logs in the console output
2. Enable DEBUG logging in `src/data_loader.py`:
   ```python
   spark.sparkContext.setLogLevel("DEBUG")
   ```
3. Review Spark UI at http://localhost:4040 when application is running
4. Open an issue on GitHub with:
   - Error message
   - Python version (`python --version`)
   - Java version (`java -version`)
   - Operating system
   - Steps to reproduce

## Additional Resources

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Spark Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)
- [EPA GHG Emission Factors](https://www.epa.gov/climateleadership/ghg-emission-factors-hub)
