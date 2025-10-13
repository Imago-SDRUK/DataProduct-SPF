import geopandas as gpd
import numpy as np
from shapely import box
from shapely.geometry import mapping
import xarray
from pystac_client import Client
import pystac
import datetime


def get_polygon_coordinates(p):
    """
    This function extracts the exterior coordinates of a polygon
    """

    X,Y = p.exterior.coords.xy

    return list(X[:-1]), list(Y[:-1]) #-1 to exclude the last coordinates as they are duplicated

def touched_tiles(aoi_path:str, tile_path:str) -> gpd.GeoDataFrame:
    """
    This function finds the tiles touched by the area of interest

    Parameters:
    aoi_path (str): path to the area of interest (city)
    tile_path (str): path to the dataset, containing all tiles

    Returns:
    touched (gpd.Geodataframe): tiles intersected by the area of interest
    aoi_crs (str): CRS of the area of interest
    tile_crs (str): CRS of the tiles
    """
    aoi = gpd.read_file(aoi_path)
    tiles = gpd.read_file(tile_path)
    xmin, xmax, ymin, ymax = aoi.total_bounds

    if aoi.crs != tiles.crs:
        print("CRS is not compliant")
        aoi = aoi.to_crs(tiles.crs)

    print(f"Area of interest is {aoi.crs}")

    aoi_union = aoi.union_all() # combine geometries for spatial filtering
    touched = tiles[tiles.intersects(aoi_union)]

    aoi_crs = aoi.crs
    tile_crs = tiles.crs
    return touched, aoi_crs, tile_crs
   
def extract_tile_ext(tiles:gpd.GeoDataFrame) -> np.ndarray:
    """
    This function extracts extents of the tiles from polygons, provided it's geopackage format (CRS-agnostic)
    gpkg_path(str): path to the geopackage file with tiles
    
    Parameters:
        tiles (gpd.Geodataframe): grid tiles of interest (already been intersected with area of interest)

    Returns:
        coords (np.ndarray): array of max/min coordinates for each tile of interest
    """
    # gdf = geopandas.read_file(gpkg_path, driver='GPKG')
    
    coords = np.asarray( list( map(get_polygon_coordinates,tiles['geometry']) ) ) 
    #print(coords)
    print(f"Coordinates shape is {coords.shape}") #NOTE:DEBUG
    return coords

def extract_min_max(coords: np.ndarray, crs: str, transl_to_wgs84: bool) -> gpd.GeoDataFrame:
    """
    This function extracts xmin, xmax, ymin, ymax from the duplicated tile coordinates
    Parameters:
        coords (gpd.Geodataframe): geodataframe with coordinates of each tile of interest
        crs (str): tile CRS
        transl_to_wgs84 (boolean): True, if you want to translate coordinates to WGS84 (EPSG:4326)

    Returns:
        min_max_coords (gpd.Geodataframe): geodataframe with reprojected coordinates (WGS84) for each tile of interest
    """

    data = []
    for x_vals, y_vals in coords:
        xmin, xmax = np.min(x_vals), np.max(x_vals)
        ymin, ymax = np.min(y_vals), np.max(y_vals)
        geom = box(xmin, ymin, xmax, ymax)  #polygon from box bounds
        data.append({'xmin': xmin, 'xmax': xmax,
                     'ymin': ymin, 'ymax': ymax,
                     'geometry': geom})

    min_max_coords = gpd.GeoDataFrame(data, crs=crs)
    print(f"original CRS is {min_max_coords.crs}") #DEBUG
    if transl_to_wgs84 is True:
        min_max_coords = min_max_coords.to_crs(epsg=4326)
        
        # recalc min and max from new bounds
        min_max_coords['xmin'] = min_max_coords.geometry.bounds['minx']
        min_max_coords['ymin'] = min_max_coords.geometry.bounds['miny']
        min_max_coords['xmax'] = min_max_coords.geometry.bounds['maxx']
        min_max_coords['ymax'] = min_max_coords.geometry.bounds['maxy']

    print(f"Target CRS is {min_max_coords.crs}") #DEBUG
    print(min_max_coords)
    return min_max_coords

def clip_scene_by_one_tile(array: xarray.DataArray, tiles: gpd.GeoDataFrame, index: int) -> xarray.DataArray:
    """
    This function clips retrieved scenes by the boundaries of tiles of interest
    Parameters:
        array (xarray.DataArray): geodataframe with coordinates of each tile of interest
        tiles (gpd.GeoDataFrame): geodataframe with tiles
        index (int): index of tile which you wish to clip the scene by

    Returns:
        clipped_array (xarray.DataArray): geodataframe with reprojected coordinates (WGS84) for each tile of interest
    """  

    if tiles.crs is not None and not array.rio.crs:
        raise ValueError ("Tiles GeoDataFrame does not have a CRS defined")
    if not array.rio.crs:
        raise ValueError("Scene must have a CRS to reproject to tile CRS.")
    
    if array.rio.crs != tiles.crs:
        array_proj = array.rio.reproject(tiles.crs)
    else:
        array_proj = array

    tile = tiles.iloc[index] # pick one tile

    clipped_array = array_proj.rio.clip([mapping(tile.geometry)], array_proj.rio.crs, drop=True)
    
    return clipped_array
# TODO - Big room for parallelisation of clipping (number of Arrays x number of Tiles)

def extract_stac_items(url: str, collection: str, datetime: str, asset: str, target_id: str=None) -> pystac.ItemCollection:
    """This function extract items from STAC, based on the parameters (dates, asset name, extent etc)
    
    Parameters:
    url (str): API endpoint to extract data from
    collection (str): name of collection you are interested, for example Sentinel 2 Collection 1 l2a
    datetime (str): timeframe you need to extract items from
    asset (str): name of the asset (band) you are interested, such as red or cloud probability
    target_id (str, Optional): for extracting item by ID code. Default: None

    Returns:
    items (pystac.ItemCollection): collection of filtered STAC items
    """
    # TODO - to use datetime format instead and transform it to STAC-required format in function
    
# TODO - to implement wrapper for all functions

if __name__ == "__main__":
    aoi_path = "data/NewcastleUponTyne.gpkg"
    tile_path = "data/uk_20km_grid.gpkg"

    # For extracting extents
    touched, aoi_crs, tile_crs = touched_tiles(aoi_path, tile_path)
    coords = extract_tile_ext(touched)
    min_max_coords = extract_min_max(coords, aoi_crs, transl_to_wgs84=True)

