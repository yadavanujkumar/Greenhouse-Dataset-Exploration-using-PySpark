"""
Data analysis and transformation functions
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, desc, asc,
    round as spark_round, when, lit
)
from pyspark.sql.window import Window


def analyze_top_emitters(df: DataFrame, n: int = 20) -> DataFrame:
    """
    Find top N industries with highest emissions
    
    Args:
        df: CO2e emissions DataFrame
        n: Number of top emitters to return
    
    Returns:
        DataFrame: Top emitters analysis
    """
    print(f"\nAnalyzing Top {n} Emitters...")
    
    # Use the correct column name
    emission_col = "Supply_Chain_Emission_Factors_with_Margins"
    
    result = df.select(
        "NAICS_Code",
        col("2017_NAICS_Title").alias("Industry"),
        col(emission_col).alias("Total_Emissions")
    ).orderBy(desc("Total_Emissions")).limit(n)
    
    return result


def analyze_by_sector(df: DataFrame) -> DataFrame:
    """
    Aggregate emissions by industry sector (first 2 digits of NAICS)
    
    Args:
        df: CO2e emissions DataFrame
    
    Returns:
        DataFrame: Sector-level analysis
    """
    print("\nAnalyzing Emissions by Sector...")
    
    emission_col = "Supply_Chain_Emission_Factors_with_Margins"
    
    # Extract sector code (first 2 digits)
    result = df.withColumn(
        "Sector_Code",
        (col("NAICS_Code") / 10000).cast("int")
    )
    
    # Aggregate by sector
    sector_analysis = result.groupBy("Sector_Code").agg(
        count("*").alias("Industry_Count"),
        avg(emission_col).alias("Avg_Emissions"),
        spark_sum(emission_col).alias("Total_Emissions"),
        spark_round(avg(emission_col), 3).alias("Avg_Emissions_Rounded")
    ).orderBy(desc("Total_Emissions"))
    
    return sector_analysis


def compare_margin_impact(df: DataFrame) -> DataFrame:
    """
    Compare emissions with and without margins
    
    Args:
        df: CO2e emissions DataFrame
    
    Returns:
        DataFrame: Margin impact analysis
    """
    print("\nAnalyzing Margin Impact...")
    
    result = df.select(
        "NAICS_Code",
        col("2017_NAICS_Title").alias("Industry"),
        col("Supply_Chain_Emission_Factors_without_Margins").alias("Without_Margins"),
        col("Margins_of_Supply_Chain_Emission_Factors").alias("Margins"),
        col("Supply_Chain_Emission_Factors_with_Margins").alias("With_Margins")
    ).withColumn(
        "Margin_Percentage",
        spark_round(
            (col("Margins") / col("Without_Margins")) * 100, 2
        )
    ).orderBy(desc("Margin_Percentage"))
    
    return result


def analyze_ghg_types(df: DataFrame) -> DataFrame:
    """
    Analyze emissions by GHG type
    
    Args:
        df: Individual GHG emissions DataFrame
    
    Returns:
        DataFrame: GHG type analysis
    """
    print("\nAnalyzing GHG Types...")
    
    emission_col = "Supply_Chain_Emission_Factors_with_Margins"
    
    ghg_summary = df.groupBy("GHG").agg(
        count("*").alias("Record_Count"),
        spark_sum(emission_col).alias("Total_Emissions"),
        avg(emission_col).alias("Avg_Emissions"),
        spark_round(avg(emission_col), 6).alias("Avg_Emissions_Rounded")
    ).orderBy(desc("Total_Emissions"))
    
    return ghg_summary


def find_high_emission_industries(df: DataFrame, threshold: float) -> DataFrame:
    """
    Find industries with emissions above threshold
    
    Args:
        df: CO2e emissions DataFrame
        threshold: Emission threshold value
    
    Returns:
        DataFrame: High emission industries
    """
    print(f"\nFinding Industries with Emissions > {threshold}...")
    
    emission_col = "Supply_Chain_Emission_Factors_with_Margins"
    
    result = df.filter(
        col(emission_col) > threshold
    ).select(
        "NAICS_Code",
        col("2017_NAICS_Title").alias("Industry"),
        col(emission_col).alias("Emissions"),
        "Unit"
    ).orderBy(desc("Emissions"))
    
    return result


def calculate_emission_statistics(df: DataFrame) -> dict:
    """
    Calculate overall emission statistics
    
    Args:
        df: CO2e emissions DataFrame
    
    Returns:
        dict: Statistical summary
    """
    print("\nCalculating Emission Statistics...")
    
    emission_col = "Supply_Chain_Emission_Factors_with_Margins"
    
    stats = df.select(
        spark_sum(emission_col).alias("total"),
        avg(emission_col).alias("mean"),
        count("*").alias("count")
    ).collect()[0]
    
    return {
        "total_emissions": stats["total"],
        "average_emissions": stats["mean"],
        "industry_count": stats["count"]
    }


def analyze_emission_distribution(df: DataFrame) -> DataFrame:
    """
    Analyze emission distribution across industries
    
    Args:
        df: CO2e emissions DataFrame
    
    Returns:
        DataFrame: Distribution analysis
    """
    print("\nAnalyzing Emission Distribution...")
    
    emission_col = "Supply_Chain_Emission_Factors_with_Margins"
    
    # Create emission ranges
    result = df.withColumn(
        "Emission_Range",
        when(col(emission_col) < 0.5, "Very Low (< 0.5)")
        .when((col(emission_col) >= 0.5) & (col(emission_col) < 1.0), "Low (0.5-1.0)")
        .when((col(emission_col) >= 1.0) & (col(emission_col) < 2.0), "Medium (1.0-2.0)")
        .when((col(emission_col) >= 2.0) & (col(emission_col) < 5.0), "High (2.0-5.0)")
        .otherwise("Very High (>= 5.0)")
    )
    
    distribution = result.groupBy("Emission_Range").agg(
        count("*").alias("Industry_Count")
    ).orderBy("Emission_Range")
    
    return distribution
