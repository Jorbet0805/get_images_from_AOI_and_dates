from datetime import datetime
import os
import zipfile
import shutil

import pdb


def get_years_and_dates(start_date, end_date):
  """
  This function takes two dates in string format (%Y-%m-%dT%H:%M:%SZ) and 
  returns a list of dictionaries containing years and adjusted start/end dates 
  for data download.

  Args:
      start_date: The start date in string format (%Y-%m-%dT%H:%M:%SZ).
      end_date: The end date in string format (%Y-%m-%dT%H:%M:%SZ).

  Returns:
      A list of dictionaries, where each dictionary contains:
          - year: Integer representing the year to download data.
          - start_date: String representing the adjusted start date in YYYY-MM-DD format.
          - end_date: String representing the adjusted end date in YYYY-MM-DD format.
  """
  # Convert strings to datetime objects
  start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
  end_date = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")

  # Get the years from the start and end date
  start_year = start_date.year
  end_year = end_date.year

  # Create a list of dictionaries with years and adjusted dates
  years_and_dates = []
  for year in range(start_year, end_year + 1):
    year_data = {}
    year_data["year"] = year
    # Adjusted start date
    if year == start_year:
      start_date_obj = start_date
    else:
      start_date_obj = datetime(year, 1, 1)  # Set start to Jan 1st for subsequent years
    year_data["start_date"] = start_date_obj.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Adjusted end date
    if year == end_year:
      end_date_obj = end_date
    else:
      end_date_obj = datetime(year, 12, 31, hour=23, minute=59, second=59)  # Set end to Dec 31st for non-last years
    year_data["end_date"] = end_date_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    years_and_dates.append(year_data)

  return years_and_dates

def compress_and_remove_images(directory):
  """
  Compresses all images in the given directory into a ZIP file and removes them.

  Args:
      directory: The path to the directory containing the images.
  """

  # Create a ZIP file with a timestamped name
  zip_filename = os.path.join(directory, f"images_{os.path.getmtime(directory)}.zip")
  with zipfile.ZipFile(zip_filename, 'w') as zip_file:
    for filename in os.listdir(directory):
      # Check if the file is an image (adjust extension check as needed)
      #pdb.set_trace()
      if filename.lower().endswith((".tif", ".jpg", ".png", ".jpeg")):
        filepath = os.path.join(directory, filename)
        zip_file.write(filepath, filename)
        os.remove(filepath)  # Remove the original image after adding to ZIP

  print(f"Images compressed into: {zip_filename}")
  print("Original images removed.")

# Example usage (replace with your actual directory path)
#directory = "./FileStore/s2_images/bbox=426362, 1405686, 520508, 1432630/year=2019/tile=15SXV_0/scene_date=2019-06-30/"
#compress_and_remove_images(directory)


def copy_and_remove_temp_dir(source_path, destination_path):
  """
  Copies a nested temporary directory and its contents to a destination path,
  and then removes the original directory.

  Args:
      source_path (str): The path to the temporary directory to copy.
      destination_path (str): The path to the destination directory.
  """

  # Check if source directory exists
  if not os.path.exists(source_path):
    raise FileNotFoundError(f"Source directory not found: {source_path}")

  # Create the destination path (if it doesn't exist)
  os.makedirs(destination_path, exist_ok=True)  # Handle existing directories gracefully

  try:
    # Copy the entire directory tree (including subdirectories and files)
    shutil.copytree(source_path, destination_path, dirs_exist_ok=True)

  except shutil.Error as e:
    print(f"Error copying directory: {e}")
    return  # Handle potential copy errors

  # Remove the original directory after successful copy
  try:
    shutil.rmtree(source_path)
    print(f"Successfully copied and removed directory: {source_path}")
  except OSError as e:
    print(f"Error removing directory: {e}")
    print(f"Directory {source_path} might still exist.")



def remove_temp_dir(source_path):
  """
  Copies a nested temporary directory and its contents to a destination path,
  and then removes the original directory.

  Args:
      source_path (str): The path to the temporary directory to copy.
      destination_path (str): The path to the destination directory.
  """

  # Check if source directory exists
  if not os.path.exists(source_path):
    #raise FileNotFoundError(f"Source directory not found: {source_path}")
    return

  # Remove the original directory after successful copy
  try:
    shutil.rmtree(source_path)
    print(f"Successfully removed directory: {source_path}")
  except OSError as e:
    print(f"Error removing directory: {e}")
    print(f"Directory {source_path} might still exist.")


if __name__ == '__main__':
    # Example usage
    #start_date = "2023-01-15T00:00:00Z"
    #end_date = "2025-08-21T23:59:59Z"

    #years_and_dates = get_years_and_dates(start_date, end_date)

    #for year_data in years_and_dates:
    #    print(f"Year: {year_data['year']}")
    #    print(f"Start Date: {year_data['start_date']}")
    #    print(f"End Date: {year_data['end_date']}")
    #    print("-" * 20)
    
    # Example usage (replace with your actual directory path)
    directory = "./FileStore/s2_images/temp/bbox=426362, 1405686, 520508, 1432630/year=2019/tile=15SXV_0/scene_date=2019-09-21"
    compress_and_remove_images(directory)