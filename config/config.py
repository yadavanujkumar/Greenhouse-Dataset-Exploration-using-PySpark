# Application Configuration
app_name = "Greenhouse Gas Emission Analysis"

# Data Paths
data_path = {
    'co2e': 'SupplyChainGHGEmissionFactors_v1.3.0_NAICS_CO2e_USD2022.csv',
    'by_ghg': 'SupplyChainGHGEmissionFactors_v1.3.0_NAICS_byGHG_USD2022.csv'
}

# Output Paths
output_path = {
    'reports': 'output/reports',
    'transformed': 'output/transformed_data',
    'analysis': 'output/analysis'
}

# Spark Configuration
spark_config = {
    'master': 'local[*]',
    'executor_memory': '2g',
    'driver_memory': '2g'
}

# Analysis Parameters
analysis_params = {
    'top_n_emitters': 20,
    'emission_threshold': 1.0  # kg CO2e/2022 USD
}
