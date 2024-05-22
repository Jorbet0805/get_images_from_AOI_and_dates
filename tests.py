import os
import zipfile

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
      if filename.lower().endswith((".tif", ".jpg", ".png", ".jpeg")):
        filepath = os.path.join(directory, filename)
        zip_file.write(filepath, filename)
        os.remove(filepath)  # Remove the original image after adding to ZIP

  print(f"Images compressed into: {zip_filename}")
  print("Original images removed.")

# Example usage (replace with your actual directory path)
directory = "./FileStore/s2_images/temp/bbox=426362, 1405686, 520508, 1432630/year=2019/tile=15SXV_0/scene_date=2019-10-03"
compress_and_remove_images(directory)
#get_images_from_AOI_and_dates/FileStore/s2_images/temp/bbox=426362, 1405686, 520508, 1432630/year=2019/tile=15SXV_0/scene_date=2019-10-03

