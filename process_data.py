# Import required libraries
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from math import sin, cos, sqrt, atan2, radians

# Define file paths
passenger_od_data_path = '/path/to/2021_Passenger_OD_Annual.csv'
initial_table_path = '/path/to/initial_table.csv'
cbsa_population_path = '/path/to/cbsa-est2022.csv'
shapefile_path = '/path/to/NextGen_OD_Zone_ESRI_11152022.shp'

# Read the CBSA population data
cbsa_population = pd.read_csv(cbsa_population_path)

# Read the shapefile using Geopandas
shapefile = gpd.read_file(shapefile_path)

# Filter out non-MSA zones and compute the centroids
shapefile_filtered = shapefile[shapefile['zone_id'].str[:5].str.isnumeric()]
shapefile_filtered['centroid'] = shapefile_filtered['geometry'].centroid

# Create the initial table with city ID, city name, population, and centroid coordinates
initial_table = pd.merge(
    shapefile_filtered, cbsa_population,
    left_on='zone_id',
    right_on='CBSA',
    how='inner'
)
initial_table['latitude'] = initial_table['centroid'].apply(lambda point: point.y)
initial_table['longitude'] = initial_table['centroid'].apply(lambda point: point.x)

# Define the Earth's radius in kilometers
R = 6371.0

# Function to calculate distance between two points based on latitude and longitude
def calculate_distance_revised(row):
    lat1, lon1 = row['latitude_x'], row['longitude_x']
    lat2, lon2 = row['latitude_y'], row['longitude_y']
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a)) 
    distance = R * c
    return distance

# Initialize an empty DataFrame to hold the Master OD Table
master_od_table = pd.DataFrame()

# Create a chunk iterator to read chunks from the Passenger OD data
chunk_size = 10000  # Define the chunk size
chunk_iter = pd.read_csv(passenger_od_data_path, chunksize=chunk_size)

# Initialize a list to hold information about any exceptions that occur
exceptions = []

# Process each chunk and append it to the Master OD Table
for i, chunk in enumerate(chunk_iter):
    try:
        chunk_filtered = chunk[chunk['origin_zone_id'].str[:5].str.isnumeric() & chunk['destination_zone_id'].str[:5].str.isnumeric()]
        chunk_filtered['origin_zone_id_short'] = chunk_filtered['origin_zone_id'].str[:5].astype(int)
        chunk_filtered['destination_zone_id_short'] = chunk_filtered['destination_zone_id'].str[:5].astype(int)
        
        merged_chunk = pd.merge(chunk_filtered, initial_table,
                                left_on='origin_zone_id_short',
                                right_on='city_ID',
                                how='inner')
        
        merged_chunk_2 = pd.merge(merged_chunk, initial_table,
                                  left_on='destination_zone_id_short',
                                  right_on='city_ID',
                                  how='inner')
        
        merged_chunk_2['distance_of_trip'] = merged_chunk_2.apply(calculate_distance_revised, axis=1)
        
        merged_chunk_2['total_passengers'] = merged_chunk_2['annual_total_trips']
        merged_chunk_2['passengers_by_air'] = merged_chunk_2['mode_air']
        merged_chunk_2['passengers_by_vehicle'] = merged_chunk_2['mode_vehicle']
        
        columns_needed = [
            'origin_zone_id', 'origin_zone_name', 'population_2022_x', 
            'destination_zone_id', 'destination_zone_name', 'population_2022_y',
            'distance_of_trip', 'total_passengers', 'passengers_by_air', 'passengers_by_vehicle'
        ]
        finalized_chunk = merged_chunk_2[columns_needed]
        
        master_od_table = pd.concat([master_od_table, finalized_chunk], ignore_index=True)
        
    except Exception as e:
        exceptions.append((i, str(e)))
