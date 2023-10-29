# Import required libraries
import pandas as pd
import geopandas as gpd
from shapely import wkt
from haversine import haversine, Unit

# Define paths to data files
passenger_od_data_path = '/mnt/data/2021_Passenger_OD_Annual.csv'
shapefile_path = '/mnt/data/NextGen_OD_Zone_ESRI_11152022/NextGen_OD_Zone_ESRI_11152022.shp'
cbsa_data_path = '/mnt/data/cbsa-est2022.csv'
chunk_size = 50000  # Size of chunks to read from the Passenger OD data

# Define function to calculate distance between two coordinates (lat1, lon1) and (lat2, lon2)
def calculate_distance_revised(row):
    coord1 = (row['centroid_latitude_x'], row['centroid_longitude_x'])
    coord2 = (row['centroid_latitude_y'], row['centroid_longitude_y'])
    distance = haversine(coord1, coord2, unit=Unit.MILES)
    return distance

# Read CBSA population data and rename columns
cbsa_data = pd.read_csv(cbsa_data_path)
cbsa_data = cbsa_data.rename(columns={'CBSA': 'city_ID', 'NAME': 'city_name', 'POPESTIMATE2022': 'population_2022'})

# Read shapefile data and convert the 'geometry' column to WKT format
shapefile_data = gpd.read_file(shapefile_path)
shapefile_data['geometry_wkt'] = shapefile_data['geometry'].apply(wkt.dumps)
shapefile_data['centroid'] = shapefile_data['geometry'].centroid
shapefile_data['centroid_wkt'] = shapefile_data['centroid'].apply(wkt.dumps)

# Create an initial table by merging the CBSA population data with the shapefile data
initial_table = pd.merge(cbsa_data, shapefile_data, left_on='city_ID', right_on='zone_id', how='inner')
initial_table['centroid_coordinates'] = initial_table['centroid_wkt'].apply(wkt.loads).apply(lambda x: (x.y, x.x))
initial_table['centroid_latitude'] = initial_table['centroid_coordinates'].apply(lambda x: x[0])
initial_table['centroid_longitude'] = initial_table['centroid_coordinates'].apply(lambda x: x[1])
initial_table_unique = initial_table.drop_duplicates(subset=['city_ID'])

# Initialize an empty DataFrame to hold the Master OD Table and a list for exceptions
master_od_table = pd.DataFrame()
exceptions = []

# Create a chunk iterator to read chunks from the Passenger OD data
chunk_iter = pd.read_csv(passenger_od_data_path, chunksize=chunk_size)

# Process each chunk and append it to the Master OD Table
for i, chunk in enumerate(chunk_iter):
    try:
        # Filter the chunk to include only rows with numeric 5-digit zone IDs (i.e., MSAs)
        chunk_filtered = chunk[chunk['origin_zone_id'].str[:5].str.isnumeric() & chunk['destination_zone_id'].str[:5].str.isnumeric()]
        
        # Skip this chunk if it's empty after filtering
        if chunk_filtered.empty:
            continue
        
        chunk_filtered['origin_zone_id_short'] = chunk_filtered['origin_zone_id'].str[:5].astype(int)
        chunk_filtered['destination_zone_id_short'] = chunk_filtered['destination_zone_id'].str[:5].astype(int)
        
        # Merge with the initial_table to get origin city information
        merged_chunk = pd.merge(chunk_filtered, initial_table_unique,
                                left_on='origin_zone_id_short',
                                right_on='city_ID',
                                how='inner')
        
        # Merge again to get destination city information
        merged_chunk_2 = pd.merge(merged_chunk, initial_table_unique,
                                  left_on='destination_zone_id_short',
                                  right_on='city_ID',
                                  how='inner')
        
        # Calculate the distance of the trip
        merged_chunk_2['distance_of_trip'] = merged_chunk_2.apply(calculate_distance_revised, axis=1)
        
        # Use the 'annual_total_trips', 'mode_air', and 'mode_vehicle' columns for total_passengers, passengers_by_air, and passengers_by_vehicle
        merged_chunk_2['total_passengers'] = merged_chunk_2['annual_total_trips']
        merged_chunk_2['passengers_by_air'] = merged_chunk_2['mode_air']
        merged_chunk_2['passengers_by_vehicle'] = merged_chunk_2['mode_vehicle']
        
        # Finalize the chunk by selecting only the columns needed
        columns_needed = [
            'origin_zone_id', 'origin_zone_name', 'population_2022_x', 
            'destination_zone_id', 'destination_zone_name', 'population_2022_y',
            'distance_of_trip', 'total_passengers',
                    'destination_zone_id', 'destination_zone_name', 'population_2022_y',
            'distance_of_trip', 'total_passengers', 'passengers_by_air', 'passengers_by_vehicle'
        ]
        finalized_chunk = merged_chunk_2[columns_needed]
        
        # Append this finalized chunk to the Master OD Table
        master_od_table = pd.concat([master_od_table, finalized_chunk], ignore_index=True)
        
    except Exception as e:
        # Record any exceptions
        exceptions.append((i, str(e)))
        
# Save the Master OD Table to a CSV file for download
master_od_table_path = '/mnt/data/Master_OD_Table_Final.csv'
master_od_table.to_csv(master_od_table_path, index=False)
