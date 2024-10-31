import iris_insee_utils_data
import geopandas as gpd
import os

path = iris_insee_utils_data.__path__[0]

def find_folder_with_substring(directory, substring):
    found_folders = []
    for root, dirs, files in os.walk(directory):
        for name in dirs:
            folder_path = os.path.join(root, name)
            if substring in folder_path:
                found_folders.append(folder_path)
    return found_folders

def return_shape_files_path(year):
    shape_folder_path = [
        folder_path
        for folder_path in find_folder_with_substring(
            path + "/../../data/raw/", f"SHP_LAMB93_FXX-{year}"
        )
        if "1_DONNEES_LIVRAISON" in folder_path
    ]
    assert len(shape_folder_path) == 1, f"Found {len(shape_folder_path)} folders"
    return shape_folder_path[0]


def extract_and_transform_Contours_IRIS_to_small_parquet_file(year: int):
    """
    For a given year:
      - Take the IRIS contour folder available at https://geoservices.ign.fr/contoursiris
      - Load only the lambert_93_FXX files
      - Extract the IRIS polygons and save them in a parquet file in the data folder
    """
    data_path = path + "/../../data/"
    shape_folder_path = return_shape_files_path(year)
    df_map = gpd.read_file(shape_folder_path)
    df_map.to_parquet(
        data_path + f"primary/iris_{year}.parquet",
        index=False,
        compression="brotli",
    )


if __name__ == "__main__":
    for year in range(2018, 2024):
        extract_and_transform_Contours_IRIS_to_small_parquet_file(year)
