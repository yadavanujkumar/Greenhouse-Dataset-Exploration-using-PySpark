"""
Quick example demonstrating how to use the analysis functions
"""

from pyspark.sql import SparkSession
from src.data_loader import create_spark_session, load_csv_data, preprocess_co2e_data
from src.analysis import analyze_top_emitters, analyze_by_sector
from config.config import data_path, app_name

def run_quick_example():
    """Run a quick example analysis"""
    
    print("\n" + "="*80)
    print("QUICK EXAMPLE - Top 10 Emitters Analysis")
    print("="*80 + "\n")
    
    # Create Spark session
    spark = create_spark_session(app_name)
    
    try:
        # Load and preprocess data
        print("Loading data...")
        df = load_csv_data(spark, data_path['co2e'])
        df = preprocess_co2e_data(df)
        
        # Analyze top 10 emitters
        print("\nAnalyzing top 10 emitters...")
        top_10 = analyze_top_emitters(df, n=10)
        
        print("\nTop 10 Industries by Greenhouse Gas Emissions:")
        print("-" * 80)
        top_10.show(10, truncate=False)
        
        # Sector analysis
        print("\nSector-level emission summary:")
        print("-" * 80)
        sector_summary = analyze_by_sector(df)
        sector_summary.show(10, truncate=False)
        
        print("\n✓ Example completed successfully!\n")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_quick_example()
