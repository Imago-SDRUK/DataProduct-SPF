#pip install odc-stac pystac-client dask

#pip install odc-stac pystac-client dask
import sys
sys.path.append('/external_libs')

import numpy as np
import pandas as pd
import xarray as xr

from pystac_client import Client as STACClient
import geopandas as gpd
import rioxarray as xrx
from rioxarray import merge

import odc.stac

import dask.array as da
import dask.dataframe as dd

import yaml
import warnings
import time

from dask.distributed import Client as DaskClient, LocalCluster
from dask import delayed, compute
#from dask_jobqueue import SLURMCluster

#EXTERNAL
from imago_utils.io.downloader import load_tile, to_geotif
from imago_utils.utils.mask import apply_scl_mask
from imago_utils.utils.config import ResampleEnum, OutputFormatEnum, DataTypeEnum, CollectionEnum

def get_cfg(config, path, default=None):
    """Access nested dict values using dot notation, warn if missing."""
    keys = path.split(".")
    value = config
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            warnings.warn(f"=Missing key: '{path}' — using default={default!r}")
            return default
    return value
# TODO - design suppression of warnings if multiple (as multiple processes use function)

with open("config/config.yaml") as f:
    cfg = yaml.safe_load(f)
    
    tiles_path=get_cfg(cfg,"in_area.tiles_path")
    tiles_of_interest=get_cfg(cfg,"in_area.tiles_of_interest")

    api_url=get_cfg(cfg,"in_stac.api_url")
    collection=get_cfg(cfg,"in_stac.collection")
    year=get_cfg(cfg,"in_stac.time.year")
    season=get_cfg(cfg,"in_stac.time.season")
    bands_of_interest=get_cfg(cfg,"in_stac.bands_of_interest")

    area_name=get_cfg(cfg,"out.area_name")
    out_bands=get_cfg(cfg,"out.out_bands")
    res=get_cfg(cfg,"out.res")
    target_crs=get_cfg(cfg,"out.target_crs")
    resampling=get_cfg(cfg,"out.resampling")
    target_dtype=get_cfg(cfg,"out.target_dtype")
    out_format=get_cfg(cfg,"out.out_format")

    chunks_size=get_cfg(cfg,"dask.chunks_size")

def check_kernel_limit():
    """This checks the local kernel limitations on memory.
    Prints `-1` if inherited from OS and no restrictions."""
    import resource

    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    print(f"Address space (virtual memory) soft/hard: {soft}/{hard}")
    soft, hard = resource.getrlimit(resource.RLIMIT_DATA)
    print(f"Data segment size soft/hard: {soft}/{hard}")

def inspect_search(items,index=0):
    """Print inspect/debug info about the one of the assets (usually the first one).
    Parameters:
    items (pystac.ItemCollection): STAC collection
    index (int=0): random tile number to inspect (Default: 0)
    """
    item = items[index]
    try:
        print("-" * 40)
        print(f"Inspecting the asset #{index}")
        print(f"DATETIME is {item.datetime}")
        print(f"GEOMETRY is {item.geometry}")
        print(f"PROPERTIES are:\n{item.properties}")
        print(f"CRS: {item.properties.get('proj:code') or item.properties.get('proj:epsg')}")
        '''metadata=odc.stac.extract_collection_metadata(item, cfg=None, md_plugin=None)
        print(f"STAC metadata:\n{metadata}")'''
        print("ASSETS are:")
        assets = item.assets
        print(assets.keys())
        print(assets["thumbnail"].href)
        print("-" * 40)
    except Exception as e:
        print("-" * 40)
        print(f"Error checking item[{index}]: {e}")
        print("-" * 40)

def create_geobox(area, crs, resolution):
    """Creates a geobox object from the initial bounding box, reprojected to the target CRS. 
    The target CRS should be the same as the desired CRS of output GeoTIFF
    
    Parameters:
    area (gpd.GeoDataFrame): geodataframe with the area of interest (tiles)
    crs (int): target CRS
    resolution (int): target resolution
    Returns:
    geobox (odc.geo.geobox.GeoBox): GeoBox object aligned with the area of interest

    NOTES:
    - deprecated as another solution to clip datasets found
    """
    from odc.geo.geobox import GeoBox
    from shapely.ops import unary_union
    from odc.geo.geom import Geometry

    geom=area.geometry.iloc[0]
    """#merged_geom = unary_union(area.geometry)
    # polygon = merged_geom.convex_hull""" #not needed
    
    # wrap shapely geometry with CRS
    geopolygon = Geometry(geom, crs)

    # create geobox
    geobox = GeoBox.from_geopolygon(geopolygon, resolution=resolution, tight=True)
    print(geobox)

    return geobox

def process_cloud(cloud_masked):
    """
    This function calculates the median cloud probability, counts valid pixels from the xarray dataset, and then export it to GeoTIFFs.
    
    Parameters:
    dataset (xarray.Dataset): dataset with `cloud` and `scl` variables
    out_path (str): path to the output GeoTIFFs.
    
    Returns:
    cloud_out (xarray.Dataset): output xarray dataset with calculated median probability and valid pixels

    Notes:
    - do not skip `.where(cloud-masked!=-1)` because otherwise it just won't consider pixels covered by satellite, but with a -1 cloud probability.
    We use now `-1` instead of `0` because that's our new no data value.
    - if `groupby` is conducted for `time` it is excessive to define it as the mean calculations are also conducted by this dimension.
    Otherwise if both defined, it will break down the export to GeoTIFF
    - datetime accessor from `odc.stac.load`, `solar_day` doesn't seem to work as there is no such coordinate in the Dataset (only `time`).
    `id` accessor also doesn't work. `time.year` accessor works as it is the predefined xarray accessor
    (https://odc-stac.readthedocs.io/en/latest/_api/odc.stac.load.html#)
    `solar_day` doesn't have analogues in xarray (https://github.com/opendatacube/odc-stac/issues/54)
    - `.count` in xarray automatically calculates without NODATA values
    - occasional issue met loading data from AWS (TODO - to document once faced again).
    Aborting load due to failure while reading: https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/sentinel-2-c1-l2a/30/U/XG/2024/7/S2A_T30UXG_20240707T112127_L2A/SCL.tif:1
    """
    cloud_out = xr.Dataset({
        "cloud_prob": (
            cloud_masked
            .where(cloud_masked != -1)
            .groupby("time.year")
            .mean(dim="time", skipna=True)
            .astype("float32")
        ),
        "valid_count": (
            cloud_masked
            .where(cloud_masked != -1)
            .groupby("time.year")
            .count(dim="time")
            .astype("int16")
        )
    })

    # DEBUG
    print(cloud_out)
    print(cloud_out.dims)
    print(cloud_out.rio.crs)
    print(cloud_out.rio.bounds())
    print("-" * 40)

    return cloud_out

def export_s2_scenes(data, items, output_dir="data/test", band="cloud"):
    """
    Export Sentinel-2 scenes from an xarray Dataset or DataArray to individual GeoTIFFs.
    Useful for visual checks within a short timeframe, eg January images.
    Filenames are based on the STAC 's2:tile_id' property.

    # USAGE (to export unmasked scenes)
    # export_s2_scenes(data, items, output_dir="data/test/unmasked", band="cloud")
    """
    import os

    os.makedirs(output_dir, exist_ok=True)
    # extract tile IDs from STAC items
    tile_ids = [item.properties.get("s2:tile_id", f"scene_{i}") for i, item in enumerate(items)]
    # attach as the coordinate
    if "tile_id" not in data.coords:
        data = data.assign_coords(tile_id=("time", tile_ids))
    # wrap in a dataset if it's dataarray
    if isinstance(data, xr.DataArray):
        print("Input is a DataArray — converting to Dataset for export.")
        data = data.to_dataset(name=data.name or band)
    # if band exists
    if band not in data.data_vars:
        raise ValueError(f"Band '{band}' not found in dataset. Available bands: {list(data.data_vars)}")
        
    # loop over scenes
    for i, tile_id in enumerate(data.tile_id.values):
        print(f"Processing scene {i+1}/{len(data.time)} → {tile_id}")

        # select one scene and load into memory
        scene = data.isel(time=i).compute()
        out_path = os.path.join(output_dir, f"{band}_{tile_id}.tif")
        # to check if crs is written
        scene_band = scene[band]
        scene_band = scene_band.rio.write_crs(scene_band.rio.crs or data[band].rio.crs, inplace=False)
        
        scene_band.rio.to_raster(out_path)
        print(f"Exported: {out_path}")

    print(f"\n All {len(data.time)} scenes exported to '{output_dir}'.")

def check_dataset(obj):
    """General info about the output array or dataset"""
    print(f"Type: {type(obj)}")
    
    if isinstance(obj, xr.DataArray):
        print(f"Shape: {obj.shape}")
        print(f"Dimensions: {obj.dims}")
        print(f"Coordinates: {list(obj.coords)}")
        print(f"Attributes: {obj.attrs}")
    
    elif isinstance(obj, xr.Dataset):
        print(f"Variables: {list(obj.data_vars)}")
        print(f"Coordinates: {list(obj.coords)}")
        print(f"Attributes: {obj.attrs}")
        print(f"Dimensions: {obj.dims}")

def inspect_chunks(obj):
    """
    Inspects Dask chunking for an xarray Dataset or DataArray.
    Prints total number of chunks, average size, and alignment info.
    """
    # Handle Dataset (multiple variables)
    if isinstance(obj, xr.Dataset):
        print(f"Dataset with {len(obj.data_vars)} variables:")
        print("=" * 60)
        for var_name, da in obj.data_vars.items():
            print(f"\nVariable: {var_name}")
            inspect_chunks(da)
        return

    da = obj #handle dataarray (single variable)

    if not hasattr(da.data, "chunks"):
        print("Array not chunked (not a Dask array).")
        return

    chunks = da.data.chunks
    dtype_size = da.dtype.itemsize

    print("-" * 60)
    total_chunks = 1
    uneven = False

    for dim, sizes in zip(da.dims, chunks):
        total_chunks *= len(sizes)
        equal = len(set(sizes)) == 1
        if not equal:
            uneven = True
        print(f"{dim:>6}: {len(sizes)} chunks | sizes = {sizes[:5]}{'...' if len(sizes) > 5 else ''}")

    avg_chunk_elems = np.prod([np.mean(s) for s in chunks])
    avg_chunk_bytes = avg_chunk_elems * dtype_size
    avg_chunk_mb = avg_chunk_bytes / 1e6

    print("-" * 60)
    print(f"Total chunks: {total_chunks}")
    print(f"Average chunk size: {avg_chunk_mb:.2f} MB ({da.dtype})")
    print(f"Chunks evenly sized? {'Yes' if not uneven else 'No, uneven chunks'}")

def dask_local_cluster(
    n_workers: int | None = None,
    threads_per_worker: int | None = None,
    memory_limit: str | None = None,
    processes: bool = True,
    dashboard_address: str = "0.0.0.0:8787"
):
    """
    This function utilises the local Dask cluster (not suitable for HPC)

    Parameters:
    n_workers (int): number of workers to start in the cluster.
    threads_per_worker (int): number of threads per worker process
    memory_limit (str): memory limit per worker, eg "4GB"
    processes (bool): whether to use separate processes (`True`) or threads (`False`) per worker
    dashboard_address (str, optional, default "0.0.0.0:8787"): Aadress and port for the Dask dashboard. Binding to `0.0.0.0` allows access from outside a Docker container or remote Jupyter kernel

    Notes:
    - if no parameters customised, just call `dask_local_cluster()`
    - if Dask run outside of the main it might cause an issue: 
    `RuntimeError: An attempt has been made to start a new process before the current process has finished its bootstrapping phase.`
    It is required to run the multiprocessing process with Dask cluster and main steps in `if __name__ == "__main__":`
    - typical output is:
    LocalCluster(40311939, 'tcp://127.0.0.1:32973', workers=7, threads=14, memory=7.65 GiB)
    <Client: 'tcp://127.0.0.1:32973' processes=7 threads=14, memory=7.65 GiB>
    - Cluster is the Scheduler with linked Workers
    - It is possible to define custom dashboard address which would be accessible outside of the Docker container/Jupyter kernel
    - `processes` are more suitable for this job than threads: https://dask.discourse.group/t/understanding-how-dask-is-executing-processes-vs-threads/666/2
    - Print statements wil declare parameters are None if they are not passed, but Dask internally uses automatic parameter values
    """
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        processes=processes,
        dashboard_address=dashboard_address
    )
    client = DaskClient(cluster)

    # DEBUG + dashboard
    print("DASK CLUSTER")
    print(f"Scheduler: {cluster.scheduler_address}")
    print(f"Number of workers: {len(cluster.workers)}")
    print(f"Total threads: {sum(w.nthreads for w in cluster.workers.values())}")
    total_memory_bytes = sum(
        w.memory_limit for w in cluster.workers.values() if w.memory_limit is not None
    )   
    print(f"Total memory: {total_memory_bytes / 1e9:.2f} GiB")

    print("Workers:")
    for i, (addr, worker) in enumerate(cluster.workers.items()):
        mem_str = f"{worker.memory_limit / 1e9:.2f} GiB" if worker.memory_limit else "unlimited"
        print(f"Worker {i}: Address={addr}, Threads={worker.nthreads}, Memory={mem_str}")

    print("Client:")
    print(client)
    print(f"Dashboard: {client.dashboard_link}")
    print("-" * 40)

    return cluster,client

#### MAIN PROCESS
@delayed
def cloud_prob_wrapper (tile, export_scenes:bool=False, export_tile:bool=False):
    """
    Main wrapper process entails the following steps either for a single tile, or for a tile series:
    - 0. set up Dask cluster
    - 1. load tile (with inspection)
    - 2. mask cloud with scl
    - (optional) export masked and unmasked scenes to debug (for short datetime)
    - 3. calculate cloud probability and valid count
    - (optional) mosaic cloud probability, if multiple tiles
    - 4. export to geotiff

    These manipulations are done lazily in a Dask-backed array without actual computation.
    
    Parameters:
    tile (Any): name of tile, eg `NZ06`
    export_scenes (bool, default=False): whether exporting scenes is required or not for visual checks (recommended only for calculations with one tile and short time period). `False` if no export required
    mosaic (bool, default=True): whether tiles should be combined (mosaicked into one output). `True` if mosaic is required
    export_tile (bool, default=False): whether exporting each calculated output (per tile) into a separate GeoTIFF

    Notes:
    - `xr.merge (processed_tiles, compat="override")` didn't work - it's exporting the GeoTIFF with the merged extent, but overriding all pixels of other datasets (tiles) with NaN
    `xarray.combine_first` is used instead
    """
    # read tiles and define time range
    tiles = gpd.read_file(tiles_path)
    time_of_interest = f'{year}-01-01/{year}-12-31'
    # TODO - to rewrite to a function if we need seasons?

    # SINGLE TILE
    if tile:
        selected_tile = tiles[tiles["tile_name"] == tile]

        bbox_of_interest = selected_tile.total_bounds.tolist()
        print(f"Initial bbox is {bbox_of_interest})")

        data=load_tile(
            tile_bbox=bbox_of_interest,
            collection=CollectionEnum.ELEMENT84_SENTINEL_2,
            out_bands=bands_of_interest,
            start_date="2024-01-01",
            end_date="2024-12-31",
            out_resolution=res,
            out_crs=target_crs,
            resample_type=ResampleEnum.NEAREST,
            chunks=chunks_size
        )
        
        try:
            cloud_masked = apply_scl_mask(
                data=data,
                origin_band="cloud",
                scl_band="scl",
                dtype_origin="float32", 
                dtype_mask="int16",
                mask_values=0,
                fill_value=-1
            )

            print(f"Cloud masked with SCL.")
        except Exception as e:
            print(f"Failed to mask cloud with SCL band")

        cloud_out = process_cloud(cloud_masked)
        cloud_out.attrs["tile_name"] = tile
        print(f"Cloud out dimensions: {cloud_out.dims}") # DEBUG
        
        # NOTE: TODO - wrap into the separate @delayed function?
        if export_tile:
            out_path = f'data/{area_name}_{tile}_{year}.tif'

            print(f"Bands of interest are(1): {out_bands}")  # TODO - to fix the name of output bands - not defined
            try:
                out = to_geotif(
                    dataset=cloud_out,
                    out_bands=out_bands,
                    out_crs=target_crs,
                    out_path=out_path,
                    out_dtype=DataTypeEnum.FLOAT32,
                    out_format=OutputFormatEnum.COG
                )

                print(f"Output GeoTIFF saved to {out_path}")
            except Exception as e:
                print(f"Failed to save GeoTIFF: {e}")

        check_dataset(cloud_out)
        inspect_chunks(data)

        return cloud_out

# TODO - to develop this one
@delayed
def export_out_tile(dataset, out_path, bands, crs, out_format="COG"):
    """Wrapper for exporting separate tiles if needed"""
    return to_geotif(
        dataset=dataset,
        bands_of_interest=bands,
        reproject=crs,
        out_path=out_path,
        out_format=out_format
    )



@delayed
def mosaic_tiles(processed_tiles, area_name: str, year: int, out_bands, target_crs, out_format="GTiff"):
    """
    Creates a mosaic from multiple processed tile datasets (eg, annual cloud probability). 
    Starts from the first tile and combines others with this one.

    Parameters:
    - processed_tiles (list of xarray.Dataset): list of tiles already processed
    - area_name (str): area name for output file naming
    - year (int): data year
    - out_bands (list): bands to export
    - target_crs (str): CRS for reprojection
    - out_format (str, default="GTiff"): output format

    Returns:
    - xarray.Dataset: lazy Dask-backed dataset representing the mosaic
    """
    if not processed_tiles or len(processed_tiles) == 0:
        raise ValueError("No processed tiles provided for mosaicking.")

    if len(processed_tiles) == 1:
        mosaic = processed_tiles[0]
        tile_name = mosaic.attrs.get("tile_name", "unknown")
        print(f"Only one tile provided ({tile_name}). Mosaic is not provided.")
        return mosaic
    else:
        mosaic = processed_tiles[0]
        first_tile_name = mosaic.attrs.get("tile_name", "unknown")
        print(f"Starting mosaic with the first tile: {first_tile_name}")

        for tile_ds in processed_tiles[1:]:
            tile_name = tile_ds.attrs.get("tile_name", "unknown")
            print(f"Combining tile {tile_name} into mosaic...")
            mosaic = tile_ds.combine_first(mosaic)

    # NOTE: consider rechunking to avoid too many small chunks
    # mosaic = mosaic.chunk({"time": 20})  # TODO:- to find out if we need this

    # export mosaic to GeoTIFF (lazy Dask will compute here)
    out_path = f"data/{area_name}_mosaic_{year}.tif"
    try:
        out = to_geotif(
            dataset=mosaic,
            out_bands=out_bands,
            out_crs=target_crs,
            out_path=out_path,
            out_dtype=DataTypeEnum.FLOAT32,
            out_format=OutputFormatEnum.COG
        )

        print(f"Mosaic GeoTIFF saved to {out_path}")
    except Exception as e:
        print(f"Failed to save mosaic GeoTIFF: {e}")

    return mosaic

if __name__ == "__main__":
    """
    This will create number of tasks equal to the tile number and build the Dask graph
    """
    start_time = time.time()
    check_kernel_limit()
    
    cluster, client = dask_local_cluster()

    processed_tiles=[cloud_prob_wrapper(tile=tile,export_scenes=False, export_tile=True) for tile in tiles_of_interest]
    
    print(processed_tiles)
    # TODO - to develop
    """if cloud_prob_wrapper.export_tile:
        tile_exports=export_out_tile(processed_tiles, out_path, out_bands, target_crs, out_format)
        compute"""
    
    mosaic = mosaic_tiles(
        processed_tiles=processed_tiles,
        area_name=area_name,
        year=year,
        out_bands=out_bands,
        target_crs=target_crs,
        out_format=out_format
    )
    
    compute(mosaic)

    finish_time = time.time()
    elapsed_time = finish_time - start_time
    print(f"Elapsed time: {elapsed_time:.2f} seconds")