"""
Utility functions for PySpark data operations
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, avg, count, desc
from typing import List


def validate_dataframe(df: DataFrame, required_columns: List[str]) -> bool:
    """
    Validate if DataFrame has all required columns
    
    Args:
        df: PySpark DataFrame
        required_columns: List of required column names
    
    Returns:
        bool: True if all columns exist
    """
    df_columns = df.columns
    missing_columns = [col for col in required_columns if col not in df_columns]
    
    if missing_columns:
        print(f"Missing columns: {missing_columns}")
        return False
    return True


def show_data_quality_report(df: DataFrame) -> None:
    """
    Display data quality metrics for a DataFrame
    
    Args:
        df: PySpark DataFrame
    """
    print("\n" + "="*80)
    print("DATA QUALITY REPORT")
    print("="*80)
    
    total_rows = df.count()
    print(f"\nTotal Rows: {total_rows:,}")
    print(f"Total Columns: {len(df.columns)}")
    
    print("\nColumn Details:")
    print("-" * 80)
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
        print(f"  {column:50s} | Nulls: {null_count:8,} ({null_percentage:5.2f}%)")
    
    print("="*80 + "\n")


def save_dataframe(df: DataFrame, path: str, mode: str = "overwrite", format: str = "csv") -> None:
    """
    Save DataFrame to file
    
    Args:
        df: PySpark DataFrame
        path: Output path
        mode: Write mode (overwrite, append, etc.)
        format: Output format (csv, parquet, json)
    """
    if format == "csv":
        df.coalesce(1).write.mode(mode).option("header", "true").csv(path)
    elif format == "parquet":
        df.write.mode(mode).parquet(path)
    elif format == "json":
        df.coalesce(1).write.mode(mode).json(path)
    else:
        raise ValueError(f"Unsupported format: {format}")
    
    print(f"Data saved to {path} in {format} format")


def print_statistics(df: DataFrame, numeric_columns: List[str]) -> None:
    """
    Print statistical summary for numeric columns
    
    Args:
        df: PySpark DataFrame
        numeric_columns: List of numeric column names
    """
    print("\n" + "="*80)
    print("STATISTICAL SUMMARY")
    print("="*80)
    
    for column in numeric_columns:
        print(f"\n{column}:")
        stats = df.select(column).summary()
        stats.show(truncate=False)
