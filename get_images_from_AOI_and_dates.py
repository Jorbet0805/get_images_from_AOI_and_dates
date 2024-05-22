# %%
# COMMAND ----------

#%pip install geojson
#%pip install pyproj
#%pip install folium
#%pip install rasterio
# %%
# COMMAND ----------

import pyproj
import requests
from geojson import Polygon, Feature, FeatureCollection
import plotly.express as px
from datetime import datetime
import folium
import requests
from io import BytesIO
import numpy as np
import rasterio
from PIL import Image
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark.sql.types import *
import numpy as np
import pandas as pd
from contextlib import closing
import gc
import multiprocessing
from datetime import datetime
import os
import shutil
import random
from pyspark.sql import SparkSession
import pdb
import json
from itertools import product
import requests
import rasterio 
from rasterio.crs import CRS
import xarray as xr
import rioxarray as rio_xr


# Create SparkSession (assuming you don't have an active session)
spark = SparkSession.builder.getOrCreate()

# Read data (replace "path/to/your/data" with your actual path)


# Process or analyze the data using Spark DataFrame methods
# ...


# %%
import sys
from datetime import datetime, timezone

import tools

def check_date_format(date_str):
    """
    Checks if the input string is in the expected date format (%Y-%m-%dT%H:%M:%SZ).

    Args:
        date_str: The date string to validate.

    Returns:
        True if the format is valid, False otherwise.
    """
    #pdb.set_trace()
    try:
        datetime.fromisoformat(date_str).astimezone(timezone.utc)
        return True
    except ValueError:
        return False


# %%
def check_list_of_aois(list_aois):
    """
    Checks if the input list contains only comma-separated strings of integers.

    Args:
        list_aois: The list of AOIs to validate.

    Returns:
        True if the format is valid, False otherwise.
    """
   
    for aoi in list_aois:
        try:
            # Split the string and convert each part to integer
            int_list = [int(x) for x in aoi.split(',')]
        except ValueError:
            return False
    return True


def check_config_format(config_dict):
    """
    Checks if the provided dictionary has the expected keys and formats.

    Args:
        config_dict: The dictionary containing configuration data.

    Returns:
        True if the format is valid, False otherwise.
    """

    required_keys = {"start_date", "end_date", "list_AOIs", "src_epsg", "ouput_file_path"}
    if not required_keys.issubset(config_dict.keys()):
        print("Error: Missing required keys in config.json")
        return False

    # Check start_date and end_date formats
    #if not check_date_format(config_dict["start_date"]) or not check_date_format(config_dict["end_date"]):
    #    print("Error: Invalid date format in start_date or end_date (YYYY-MM-DDT HH:MM:SSZ)")
    #    return False

    # Check list_AOIs format
    if not check_list_of_aois(config_dict["list_AOIs"]):
        print("Error: list_AOIs must contain comma-separated strings of integers")
        return False

    # Check src_epsg format (optional, you can add validation for valid EPSG codes here)
    #if not config_dict["src_epsg"].startswith("EPSG:"):
    #    print("Warning: src_epsg should start with 'EPSG:'")

    return True
# %%
# COMMAND ----------

'''This function is used simply because the NASSGEO api required bounds in epsg:5070, and the sentinel-2 stac api from earth-search uses epsg:4326. Thus we convert from our epsg:5070 bounds to epsg:4326 crs.
'''
def get_xfrmd_bounds_of_geom(bounds=(426362, 1405686, 520508, 1432630), src_epsg='EPSG:5070'):
    # Define the EPSG codes
    src_proj = pyproj.CRS(src_epsg)
    dst_proj = pyproj.CRS("EPSG:4326")

    # Create the transformer
    transformer = pyproj.Transformer.from_proj(src_proj, dst_proj, always_xy=True)

    # Transform the bounds
    min_lon, min_lat = transformer.transform(bounds[0], bounds[1])
    max_lon, max_lat = transformer.transform(bounds[2], bounds[3])

    # Print the transformed bounds
    # print("Transformed bounds in EPSG:4326:", (min_lon, min_lat, max_lon, max_lat))
    return min_lon, min_lat, max_lon, max_lat

# MAGIC %md ## Setup query function using the Sentinel-2 STAC API
# %%
# COMMAND ----------

'''
This function searches for the available imagery in a given area and timframe. 
Pagination is used to return all results
'''

def query_stac_api(bounds=(426362, 1405686, 520508, 1432630), \
                   src_epsg="EPSG:4326", \
                   start_date="2023-01-01T00:00:00Z", \
                   end_date="2023-12-31T23:59:59Z", \
                   limit=100):

    if src_epsg=="EPSG:4326":
        min_lon, min_lat, max_lon, max_lat = bounds
    else:
        min_lon, min_lat, max_lon, max_lat = get_xfrmd_bounds_of_geom(bounds,src_epsg=src_epsg)

    polygon = Feature(geometry=Polygon([[(min_lon, min_lat),
                                         (max_lon, min_lat),
                                         (max_lon, max_lat),
                                         (min_lon, max_lat),
                                         (min_lon, min_lat)]]))

    all_results = []
    more_results = True
    page = 1

    while more_results:
        query = {
            "datetime": f"{start_date}/{end_date}",
            "intersects": polygon.geometry,
            "collections": ["sentinel-2-l2a"],
            "limit": limit,
            "page": page
        }

        stac_url = "https://earth-search.aws.element84.com/v1/search"
        response = requests.post(stac_url, json=query)

        if response.status_code != 200:
            print(response.content)
            break

        results = response.json()

        # paginate to get all results
        if results['features']:
            all_results.extend(results['features'])
            page += 1
        else:
            more_results = False

    return all_results

# %%
# COMMAND ----------
def list_folders_second_to_deepest_level(path, folders, current_depth, target_depth):
    if current_depth == target_depth:
        folders.append(path)
    else:
        subfolders = [os.path.join(path, f) for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))]
        for folder in subfolders:
            list_folders_second_to_deepest_level(folder, folders, current_depth + 1, target_depth)
    return folders
# %%
# COMMAND ----------

assets_list = ['scl', 'coastal', 'blue', 'green', 'red', 'rededge1', 'rededge2', 'rededge3', 'nir', 'nir08', 'nir09', 'swir16', 'swir22']
scl_exclude_list = [0, 1, 7, 8, 9, 11] # ignore certain scl layer values....
# SCL_color_mappings = {
#   0: # No Data (Missing data) - black  
#   1: # Saturated or defective pixel - red 
#   2: # Topographic casted shadows ("Dark features/Shadows" for data before 2022-01-25) - very dark grey
#   3: # Cloud shadows - dark brown
#   4: # Vegetation - green
#   5: # Not-vegetated - dark yellow
#   6: # Water (dark and bright) - blue
#   7: # Unclassified - dark grey
#   8: # Cloud medium probability - grey
#   9: # Cloud high probability - white
#   10: # Thin cirrus - very bright blue
#   11: # Snow or ice - very bright pink
# }

# COMMAND ----------

# MAGIC %md ## Retrive existing data (to avoid reprocessing)

# COMMAND ----------
# %%
# This function is only needed/used for restarting processing after stopping for some reason (start where code left off)
#s2_file_path = '../FileStore/s2_sampled/s2_dense_test.parquet'
def get_existing_data(file_path="../FileStore/s2_sampled/s2_sampled.parquet"):
    existing_s2_dates = {}

    try:
        for item in list_folders_second_to_deepest_level(file_path, [], 0, 4):
            parts = item.split('/')
            bbox = None
            year = None
            scene_date = None

            for part in parts:
                if part.startswith('bbox='):
                    bbox = part.split('=')[1]
                elif part.startswith('year='):
                    year = part.split('=')[1]
                elif part.startswith('tile='):
                    tile = part.split('=')[1]
                elif part.startswith('scene_date='):
                    scene_date = part.split('=')[1]

            if bbox and year and scene_date and tile:
                key = (bbox, year, tile)
                if key in existing_s2_dates:
                    existing_s2_dates[key].append(scene_date)
                else:
                    existing_s2_dates[key] = [scene_date]
        return existing_s2_dates
    except:
        return {}

#print(get_existing_data(s2_file_path))
# %%
# COMMAND ----------
import os
def get_directory_size(directory_path):
  """
  This function calculates the total size of a directory and its subdirectories.

  Args:
      directory_path (str): The path to the directory.

  Returns:
      float: The total size of the directory in bytes.
  """
  total_size = 0
  for root, _, files in os.walk(directory_path):
    for file in files:
      file_path = os.path.join(root, file)
      if os.path.isfile(file_path):
        total_size += os.path.getsize(file_path)
  return total_size

# %%
# COMMAND ----------

def unique_indices(scene_ids, one_tile=False):
    scene_ids_ids = [x['id'] for x in scene_ids]
    unique_dict = {}
    for index, scene_id in enumerate(scene_ids_ids):
        base_id = scene_id.rsplit('_', 2)[0]
        number = int(scene_id.split('_')[-2])
    
        if base_id not in unique_dict:
            unique_dict[base_id] = {'index': index, 'number': number}
        elif number > unique_dict[base_id]['number']:
            unique_dict[base_id] = {'index': index, 'number': number}
    
    unique_indices_to_use = [item['index'] for item in unique_dict.values()]
    
    scene_ids = [scene_ids[ii] for ii in unique_indices_to_use]
    if one_tile:
        scene_ids_ids = [x['id'] for x in scene_ids]
        tiles = list(set([element.split('_')[1] for element in scene_ids_ids]))
        # Choose one tile at random
        chosen_tile = random.choice(tiles)
        # Filter the input list to keep only the elements with the chosen tile
        scene_ids = [scene_ids[index] for index, element in enumerate(scene_ids_ids) if chosen_tile in element]
    
    return scene_ids

# %%
# COMMAND ----------

# MAGIC %md ## Engine/Loop to retrieve and sample Sentinel-2 data

# %%
# COMMAND ----------

# Fast and stable multiprocessing solution (use of spark UDFs is unstable for this type of work)
def download_and_save_geotiff(geotiff_url: str,ouput_file_path: str):
    with closing(requests.get(geotiff_url, stream=True)) as geotiff_response:
        with rasterio.open( BytesIO( geotiff_response.content ) ) as src:
            input_crs = pyproj.CRS("EPSG:4326")  # WGS84
            output_crs = src.crs
           
            filename = os.path.basename(geotiff_url)

            # Construct output filepath based on output_path and filename
            output_filepath = os.path.join(ouput_file_path, filename)

            # Save the layer data to the output filepath
            # (assuming layer is a rasterio.io.DatasetReader object)
            src.write(output_filepath, driver='GTiff')  # Assuming TIFF format

            # Optional: Close the dataset after saving
            src.close()
        return src
    


def download_and_open_geotiff_xarray(geotiff_url: str):
  """Downloads a satellite image from a GeoTIFF URL and opens it as an xarray DataArray.

  Args:
      geotiff_url (str): The URL of the GeoTIFF image.

  Returns:
      xarray.DataArray: The downloaded GeoTIFF image as an xarray DataArray.

  Raises:
      ValueError: If the downloaded data cannot be opened as an xarray DataArray.
  """

  with requests.get(geotiff_url, stream=True) as geotiff_response:
    if geotiff_response.status_code != 200:
      raise ValueError(f"Failed to download GeoTIFF: {geotiff_response.status_code}")
    
    # Ensure rioxarray is installed
    try:
      rio_xr.open_rasterio
    except AttributeError:
      raise ImportError("rioxarray library is required for GeoTIFF support in xarray. Please install it using 'pip install rioxarray'.")

    #pdb.set_trace()  
    try:
      # Open the downloaded data as an xarray DataArray using rioxarray
      da = rio_xr.open_rasterio(BytesIO(geotiff_response.content))
    except (KeyError, ValueError):
      # Fallback to rasterio.open if rioxarray fails
      with open(BytesIO(geotiff_response.content)) as src:
        da = xr.DataArray(src.read(1),
                          coords={'x': src.coords[0], 'y': src.coords[1]},
                          dims=('y', 'x'),
                          attrs={'crs': CRS.from_string(src.crs.wkt)})
    
    
    return da


# %%
def process_result(result, existing_s2_dates, assets_list, scl_exclude_list, ouput_file_path, bbox, year, lock):
    props = result['properties']
    tile = result['id'].split('_')[1] + '_' + result['id'].split('_')[-2]
    #print('props', props)
    #print('tile', tile)
    #print("Time: ", result['properties']['datetime'])
    #pdb.set_trace()
    #print(f"Buscar: ", bbox, year, tile )
    #print(f"Bbox: ", bbox )
    #print('existing_s2_dates:')
    #print(existing_s2_dates)
    scene_date = result['properties']['datetime'].split('T')[0]
    
    try:
        not_these_tile_dates = existing_s2_dates[(bbox, str(year), tile)]
        #pdb.set_trace()
        #not_these_tile_dates = existing_s2_dates[(bbox, year, tile)]
        #if year=='2020' or year=='2019':
            #print(year)
            #print("not_these_tile_dates",not_these_tile_dates) 
            #print(f'existing_s2,{(bbox, year, tile)}')
            #print("existing_s2 got")
            #return 0  
    except:
        not_these_tile_dates = []
        #pdb.set_trace()
        #print(year)
        #if str(year)=='2020' or str(year)=='2019':
            #print(f'existing_s2,{(bbox, year, tile)}')
            #print("existing_s2 did not gived")
            #return 0

    # Check against existing scene dates already written for each bbox & year combo, and skip those already done...
    if result['properties']['datetime'].split('T')[0] in not_these_tile_dates:
        print('Already Existing: ' + 'Of' + f'{year}' + result['id'] + ' at ' + datetime.now().strftime("%H:%M:%S"))
        #pdb.set_trace()
        return 0
    
    unvalid_percent_area = props['s2:thin_cirrus_percentage'] + props['s2:cloud_shadow_percentage'] + props['s2:dark_features_percentage']
        
    if unvalid_percent_area < 30:
        try:
            print(f"Started Worker ID: {os.getpid()}: " + result['id'] + ' at ' + datetime.now().strftime("%H:%M:%S"))
            try:
                spark.range(0, 1).count() #keepalive cluster spark command
            except Exception as e:
                pass
           
            #############################################################################################################
            ############ for loop through SCL layer then 12 band values ################################################
            #############################################################################################################    
            
            #Clean directory 
            output_temp_filepath = os.path.join(ouput_file_path,
                                               'temp',
                                               'bbox='+bbox,
                                               'year='+str(year),
                                               'tile='+tile,
                                               'scene_date='+scene_date)
            
            #pdb.set_trace()
            tools.remove_temp_dir(output_temp_filepath)
            for ass in assets_list:
                geotiff_url = result['assets'][ass]['href']
                #layer = download_and_save_geotiff(geotiff_url, ouput_file_path)
                layer = download_and_open_geotiff_xarray(geotiff_url)
                #pdb.set_trace()
                # Extract filename from URL (assuming URL ends with the filename)
                filename = os.path.basename(geotiff_url)


                # Construct output filepath based on output_path and filename
                output_temp_filepath = os.path.join(ouput_file_path,
                                               'temp',
                                               'bbox='+bbox,
                                               'year='+str(year),
                                               'tile='+tile,
                                               'scene_date='+scene_date,
                                               filename)
                # Create the directory structure if it doesn't exist
                os.makedirs(os.path.dirname(output_temp_filepath), exist_ok=True)
                
                # Save the layer data to the output filepath
                # (assuming layer is a rasterio.io.DatasetReader object)
                layer.rio.to_raster(raster_path=output_temp_filepath, driver="GTiff")  # Assuming TIFF format

                # Optional: Close the dataset after saving
                del layer
            
            source_path_temp = os.path.join(ouput_file_path,
                                            'temp',
                                            'bbox='+bbox,
                                            'year='+str(year),
                                            'tile='+tile,
                                            'scene_date='+scene_date
                                            )
            
            destination_path = os.path.join(ouput_file_path,
                                            'bbox='+bbox,
                                            'year='+str(year),
                                            'tile='+tile,
                                            'scene_date='+scene_date
                                            )
            
            
            zip_name = f'{str(year)}-'+ f'{tile}-' + f'{scene_date}'+ f'.zip'
            destination_zip = os.path.join(ouput_file_path,
                                            'bbox='+bbox,
                                            'year='+str(year),
                                            'tile='+tile,
                                            'scene_date='+scene_date,
                                            zip_name
                                            )
            
            
            
            tools.compress_and_remove_images(directory=source_path_temp)
            
            tools.copy_and_remove_temp_dir(source_path=source_path_temp, 
                                           destination_path=destination_path)
           
            #if ass == 'scl':
            #    df_train_subset_bbox_year = df_train_subset_bbox_year[~df_train_subset_bbox_year[ass].isin(scl_exclude_list)]
            #    df_train_subset_bbox_year.reset_index(drop=True, inplace=True) #without this there's big issues (subtle but important)
               
            print(f"Finished Worker ID: {os.getpid()}: " + result['id'] + ' at ' + datetime.now().strftime("%H:%M:%S"))
            return 0
        except Exception as e:
            print('Exception: ' + str(e) + f' {os.getpid()}: ' + result['id'])
            return 0
    else:
        print('Low Area Percent: ' + str(unvalid_percent_area) + '  ' + result['id'] + ' at ' + datetime.now().strftime("%H:%M:%S"))
        return 0

# %%
#bbox_list = ['426362, 1405686, 520508, 1432630', '390747, 1195097, 437820, 1284288']
#years = [2019, 2020, 2021]
#pdb.set_trace()
#.tar
# %%
if len(sys.argv) != 2:
        print("Usage: python get_images_from_AOI_and_dates.py config.json")
        sys.exit(1)
    
     
config_file = sys.argv[1]
try:
        with open(config_file) as f:
            config_dict = json.load(f)
except FileNotFoundError:
        print("Error: Config file not found:", config_file)
        sys.exit(1)

#pdb.set_trace()
if not check_config_format(config_dict):
        sys.exit(1)
    
    
# Place your image download logic here using the parsed configuration data (config_dict)
print("Config file parsed successfully!")
print("start_date:", config_dict["start_date"])
print("end_date:", config_dict["end_date"])
print("list_AOIs:", config_dict["list_AOIs"])
print("src_epsg:", config_dict["src_epsg"])
print("ouput_file_path: ", config_dict["ouput_file_path"] )

start_date = config_dict["start_date"]
end_date = config_dict["end_date"]
list_AOIs = config_dict["list_AOIs"]
src_epsg = config_dict["src_epsg"]
ouput_file_path = config_dict["ouput_file_path"]

dir_path = ouput_file_path  # Replace with your actual directory path
dir_size_gb = get_directory_size(dir_path) / (1024 ** 3)

print(f"Size of '{dir_path}' is {dir_size_gb:.2f} GB")
existing_s2_dates = get_existing_data(ouput_file_path)

bbox_list = list_AOIs
years_and_dates = tools.get_years_and_dates(start_date, end_date)

lock = multiprocessing.Lock()

#pdb.set_trace()
# Implement your image download logic here based on the parsed configuration
# for el in df_train_partition_values_list[:1]:
for bbox, year_data in product(bbox_list, years_and_dates):
   
        bbox_tuple = tuple([int(x) for x in bbox.split(', ')])
        #pdb.set_trace()
        results = query_stac_api(bounds=bbox_tuple, \
                    src_epsg=src_epsg, \
                    start_date=str(year_data['start_date']), \
                    end_date=str(year_data['end_date']) )
        results = unique_indices(results) #dedupe the results
        #pdb.set_trace()
        def process_results_in_parallel(result):
            return process_result(result, existing_s2_dates, assets_list, scl_exclude_list, ouput_file_path, bbox, year_data['year'], lock)
    
        #for result in results:
        #    process_result(result, existing_s2_dates, assets_list, scl_exclude_list, ouput_file_path, bbox, year_data['year'], lock)

        print('multiprocessing', multiprocessing.cpu_count()*0.7)
        #pdb.set_trace()
        with multiprocessing.Pool(processes=int(multiprocessing.cpu_count()*0.7), maxtasksperchild=1) as pool:
        #with multiprocessing.Pool(processes=1, maxtasksperchild=1) as pool:
            pool.map(process_results_in_parallel,results)

        del results
        #dbutils.fs.rm('file:' + CDL_parts_path, recurse=True)
        try:
            # Remove the original directory after successful copy
            source_path = os.path.join(ouput_file_path,'temp')
            shutil.rmtree(source_path)
            print(f"Successfully copied and removed directory: {source_path}")
        except OSError as e:
            print(f"Error removing directory: {e}")
            print(f"Directory {source_path} might still exist.")
            #shutil.rmtree(CDL_parts_path, ignore_errors=False)  # Remove directory tree
            raise
        gc.collect()
# %%
#