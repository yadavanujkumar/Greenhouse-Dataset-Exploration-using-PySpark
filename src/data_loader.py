"""
Data loading and preprocessing module
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, trim
from pyspark.sql.types import DoubleType, IntegerType


def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and configure Spark session
    
    Args:
        app_name: Application name
    
    Returns:
        SparkSession: Configured Spark session
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Spark Session created: {app_name}")
    print(f"Spark Version: {spark.version}")
    
    return spark


def load_csv_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load CSV data into Spark DataFrame
    
    Args:
        spark: Spark session
        file_path: Path to CSV file
    
    Returns:
        DataFrame: Loaded data
    """
    print(f"\nLoading data from: {file_path}")
    
    df = spark.read.csv(
        file_path,
        header=True,
        inferSchema=True,
        encoding="UTF-8"
    )
    
    print(f"Loaded {df.count():,} rows with {len(df.columns)} columns")
    
    return df


def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Clean and standardize column names
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame: DataFrame with cleaned column names
    """
    for column in df.columns:
        new_name = column.strip().replace(" ", "_").replace("(", "").replace(")", "")
        df = df.withColumnRenamed(column, new_name)
    
    return df


def preprocess_co2e_data(df: DataFrame) -> DataFrame:
    """
    Preprocess CO2e emissions data
    
    Args:
        df: Raw CO2e DataFrame
    
    Returns:
        DataFrame: Preprocessed DataFrame
    """
    # Clean column names
    df = clean_column_names(df)
    
    # Convert NAICS code to integer
    if "2017_NAICS_Code" in df.columns:
        df = df.withColumn("NAICS_Code", col("2017_NAICS_Code").cast(IntegerType()))
    
    # Convert numeric columns to double
    numeric_columns = [
        "Supply_Chain_Emission_Factors_without_Margins",
        "Margins_of_Supply_Chain_Emission_Factors",
        "Supply_Chain_Emission_Factors_with_Margins"
    ]
    
    for column in numeric_columns:
        if column in df.columns:
            df = df.withColumn(column, col(column).cast(DoubleType()))
    
    # Remove rows with null emission values
    df = df.filter(
        col("Supply_Chain_Emission_Factors_with_Margins").isNotNull()
    )
    
    return df


def preprocess_by_ghg_data(df: DataFrame) -> DataFrame:
    """
    Preprocess individual GHG data
    
    Args:
        df: Raw GHG DataFrame
    
    Returns:
        DataFrame: Preprocessed DataFrame
    """
    # Clean column names
    df = clean_column_names(df)
    
    # Convert NAICS code to integer
    if "2017_NAICS_Code" in df.columns:
        df = df.withColumn("NAICS_Code", col("2017_NAICS_Code").cast(IntegerType()))
    
    # Convert numeric columns to double
    numeric_columns = [
        "Supply_Chain_Emission_Factors_without_Margins",
        "Margins_of_Supply_Chain_Emission_Factors",
        "Supply_Chain_Emission_Factors_with_Margins"
    ]
    
    for column in numeric_columns:
        if column in df.columns:
            df = df.withColumn(column, col(column).cast(DoubleType()))
    
    # Remove rows with null values
    df = df.filter(
        col("Supply_Chain_Emission_Factors_with_Margins").isNotNull()
    )
    
    return df
