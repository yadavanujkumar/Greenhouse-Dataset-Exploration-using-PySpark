"""
Main PySpark Application for Greenhouse Gas Emission Analysis

This application analyzes supply chain greenhouse gas emission factors data
using PySpark to perform data engineering operations including:
- Data loading and preprocessing
- Data quality validation
- Emission analysis by sector and industry
- GHG type analysis
- Report generation
"""

import sys
import os
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.data_loader import (
    create_spark_session,
    load_csv_data,
    preprocess_co2e_data,
    preprocess_by_ghg_data
)
from src.analysis import (
    analyze_top_emitters,
    analyze_by_sector,
    compare_margin_impact,
    analyze_ghg_types,
    find_high_emission_industries,
    calculate_emission_statistics,
    analyze_emission_distribution
)
from src.utils import (
    validate_dataframe,
    show_data_quality_report,
    save_dataframe
)
from config.config import app_name, data_path, output_path, analysis_params


def main():
    """Main execution function"""
    
    print("\n" + "="*100)
    print(f"{'GREENHOUSE GAS EMISSION ANALYSIS - DATA ENGINEERING PROJECT':^100}")
    print("="*100)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Create Spark session
    spark = create_spark_session(app_name)
    
    try:
        # ========================================
        # STEP 1: Load and Preprocess Data
        # ========================================
        print("\n" + "="*100)
        print("STEP 1: DATA LOADING AND PREPROCESSING")
        print("="*100)
        
        # Load CO2e data
        co2e_df_raw = load_csv_data(spark, data_path['co2e'])
        print("\nRaw CO2e Data Schema:")
        co2e_df_raw.printSchema()
        
        # Load individual GHG data
        ghg_df_raw = load_csv_data(spark, data_path['by_ghg'])
        print("\nRaw GHG Data Schema:")
        ghg_df_raw.printSchema()
        
        # Preprocess data
        co2e_df = preprocess_co2e_data(co2e_df_raw)
        ghg_df = preprocess_by_ghg_data(ghg_df_raw)
        
        print("\nPreprocessed CO2e Data Schema:")
        co2e_df.printSchema()
        
        # ========================================
        # STEP 2: Data Quality Checks
        # ========================================
        print("\n" + "="*100)
        print("STEP 2: DATA QUALITY VALIDATION")
        print("="*100)
        
        show_data_quality_report(co2e_df)
        
        # Sample data
        print("Sample CO2e Data (first 10 rows):")
        co2e_df.show(10, truncate=False)
        
        # ========================================
        # STEP 3: Emission Analysis
        # ========================================
        print("\n" + "="*100)
        print("STEP 3: EMISSION ANALYSIS")
        print("="*100)
        
        # 3.1: Top Emitters
        print("\n--- Top Emitters Analysis ---")
        top_emitters = analyze_top_emitters(co2e_df, analysis_params['top_n_emitters'])
        print(f"\nTop {analysis_params['top_n_emitters']} Industries by Emissions:")
        top_emitters.show(analysis_params['top_n_emitters'], truncate=False)
        
        # 3.2: Sector Analysis
        print("\n--- Sector Analysis ---")
        sector_analysis = analyze_by_sector(co2e_df)
        print("\nEmissions by Sector:")
        sector_analysis.show(20, truncate=False)
        
        # 3.3: Margin Impact
        print("\n--- Margin Impact Analysis ---")
        margin_impact = compare_margin_impact(co2e_df)
        print("\nTop 15 Industries by Margin Percentage:")
        margin_impact.show(15, truncate=False)
        
        # 3.4: High Emission Industries
        print("\n--- High Emission Industries ---")
        high_emitters = find_high_emission_industries(
            co2e_df, 
            analysis_params['emission_threshold']
        )
        print(f"\nIndustries with Emissions > {analysis_params['emission_threshold']}:")
        high_emitters.show(20, truncate=False)
        
        # 3.5: Emission Distribution
        print("\n--- Emission Distribution ---")
        distribution = analyze_emission_distribution(co2e_df)
        print("\nEmission Distribution Across Industries:")
        distribution.show(truncate=False)
        
        # 3.6: Overall Statistics
        print("\n--- Overall Statistics ---")
        stats = calculate_emission_statistics(co2e_df)
        print("\nOverall Emission Statistics:")
        print(f"  Total Industries Analyzed: {stats['industry_count']:,}")
        print(f"  Total Emissions: {stats['total_emissions']:.2f} kg CO2e/2022 USD")
        print(f"  Average Emissions: {stats['average_emissions']:.4f} kg CO2e/2022 USD")
        
        # ========================================
        # STEP 4: GHG Type Analysis
        # ========================================
        print("\n" + "="*100)
        print("STEP 4: INDIVIDUAL GHG TYPE ANALYSIS")
        print("="*100)
        
        ghg_analysis = analyze_ghg_types(ghg_df)
        print("\nEmissions by GHG Type:")
        ghg_analysis.show(25, truncate=False)
        
        # ========================================
        # STEP 5: Save Results
        # ========================================
        print("\n" + "="*100)
        print("STEP 5: SAVING RESULTS")
        print("="*100)
        
        # Create output directories
        os.makedirs(output_path['reports'], exist_ok=True)
        os.makedirs(output_path['analysis'], exist_ok=True)
        
        # Save analysis results
        print("\nSaving analysis results...")
        save_dataframe(top_emitters, f"{output_path['analysis']}/top_emitters", format="csv")
        save_dataframe(sector_analysis, f"{output_path['analysis']}/sector_analysis", format="csv")
        save_dataframe(margin_impact, f"{output_path['analysis']}/margin_impact", format="csv")
        save_dataframe(high_emitters, f"{output_path['analysis']}/high_emission_industries", format="csv")
        save_dataframe(distribution, f"{output_path['analysis']}/emission_distribution", format="csv")
        save_dataframe(ghg_analysis, f"{output_path['analysis']}/ghg_type_analysis", format="csv")
        
        # ========================================
        # STEP 6: Summary
        # ========================================
        print("\n" + "="*100)
        print("ANALYSIS SUMMARY")
        print("="*100)
        print(f"\n✓ Processed {co2e_df.count():,} CO2e emission records")
        print(f"✓ Processed {ghg_df.count():,} individual GHG records")
        print(f"✓ Analyzed {stats['industry_count']:,} industries")
        print(f"✓ Identified {high_emitters.count():,} high-emission industries")
        print(f"✓ Generated {6} analysis reports")
        print(f"\n✓ All results saved to '{output_path['analysis']}' directory")
        
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        # Stop Spark session
        spark.stop()
        print(f"\n{'='*100}")
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*100 + "\n")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
