#pip install odc-stac pystac-client dask

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

from dask.distributed import Client as DaskClient, LocalCluster
from dask import delayed, compute
import time

#from dask_jobqueue import SLURMCluster

def get_cfg(config, path, default=None):
    """Access nested dict values using dot notation, warn if missing."""
    keys = path.split(".")
    value = config
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            warnings.warn(f"[INFO] Missing key: '{path}' — using default={default!r}")
            return default
    return value

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

def mask_with_band(data, origin_band, mask_band, dtype_origin=None, dtype_mask=None, mask_nodata=0, fill_value=-1):
    """
    Replaces values of one of the bands based on the no-data values of another band.
    
    In the case of SPF product, it replaces values of the `cloud` band if values in the `SCL` mask are 0.
    Rules:
    - if SCL == 0 → cloud = -1
    - if SCL != 0 → keep cloud as is
    - assign -1 as the new no-data value for the cloud band
    - ensure no NaNs remain (fill them with -1)
    
    Parameters:
    data (xarray.Dataset): original dataset with `cloud` and `scl` variables
    origin_band (str): original dataset band to mask
    mask_band (str): original dataset band used as a mask
    dtype_origin (str or np.dtype, optional): dtype for the origin band. If None, inferred from the origin band.
    dtype_mask (str or np.dtype, optional): dtype for the mask band. If None, inferred from the mask band.
    mask_nodata (int or float, optional): value in the mask which considered to be no data value. In Sentinel 2, SCL band, it is 0
    fill_value (int or float, optional): value assigned to the origin band where `mask == mask_nodata` (default=-1).
    
    Returns:
    masked (xarray.DataArray): output array with masked `cloud` band.


    Usage example:
    cloud_masked=mask_with_band(data, origin_band="cloud", mask_band="scl", dtype_origin="float32", dtype_mask="int16", mask_nodata=0, fill_value=-1):
    """
    
    print(("-" * 40 + "\n") * 2, end="")
    # DEBUG
    print(f"Starting masking of '{origin_band}' using '{mask_band}'...")
    
    origin = data[origin_band].astype(dtype_origin).copy()
    mask = data[mask_band].astype(dtype_mask)

    #DEBUG
    print(f"{origin_band} dimensions: {origin.dims}")
    print(f"{mask_band} dimensions: {mask.dims}")
    
    # apply rules
    masked = origin.where(mask != mask_nodata, fill_value)

    # replace any remaining NaNs with -1
    masked = masked.fillna(fill_value)
    # assign -1 as the nodata value for output
    masked.attrs["nodata"] = fill_value

    # DEBUG:
    print("Type:", type(masked))
    print("Name:", masked.name)
    print("Dimensions:", masked.dims)
    print("Coordinates:", list(masked.coords))
    print(f"{fill_value} is set as the no-data value (no NaNs remain).")
    print("-" * 40)
    
    return masked

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

def to_geotif(
    dataset,
    bands_of_interest:list=None,
    reproject:int|None=None,
    out_path:str|None=None,
    out_format:str="GTiff"
):
    """
    Saves an xarray Dataset or DataArray with multiple variables as Geotiff/COG.
    Parameters:
    dataset (xarray.Dataset or xarray.DataArray): processed dataset/array to be saved
    bands_of_interest (list): list of bands to include, if None all variables included
    reproject (int|None): EPSG code of target CRS (if needs reprojecting the dataset). If not given, the dataset will be written in its current CRS.
    out_path (str, optional): path for output Geotiff.
    out_format (str, default="GTiff"): output format, must match GDAL/rasterio drivers

    Returns
    image_out (str or None): path to the written GeoTIFF.

    Notes:
    - This is adapted from https://discourse.pangeo.io/t/comparing-odc-stac-load-and-stackstac-for-raster-composite-workflow/4097 and 
    https://discourse.pangeo.io/t/can-a-reprojection-change-of-crs-operation-be-done-lazily-using-rioxarray/4468
    - reprojection is implemented as optional if STAC collections were previously loaded without reprojection to avoid chunking artefacts.
    - `odc.geo.xr.xr_reproject` is used instead of `rio.reproject` as `rio.reproject` triggers Dask chunks breakdown
    - `odc.reproject` is not used as this command should contain odc metadata
    """
    from odc.geo.xr import xr_reproject
    
    if isinstance(dataset, xr.DataArray):
        print(f"Dataset/array: Array")
        image = dataset.squeeze('year')
    
    else:
        print(f"Dataset/array: Dataset")
        print(f"Bands of interest are (1): {bands_of_interest}")
        image = (
            dataset[bands_of_interest]
            .to_array(dim="band")
            .squeeze('year')
            .transpose('band','y', 'x')
            .astype(target_dtype)
        )

    """geobox=create_geobox(area=tiles, crs=reproject, resolution=res)"""
        
    # reproject if defined
    if reproject:
        if image.rio.crs is None:
            print(f"Assigning CRS: EPSG:{reproject}")
            image = image.rio.write_crs(f"EPSG:{reproject}")
        elif image.rio.crs.to_epsg() != reproject:
            print(f"Reprojecting to EPSG:{reproject}")
            image = xr_reproject(image, how=f'EPSG:{reproject}')
            print("Reprojection finished")
            
            # print(image.data_vars) - TODO - check where vars are being renamed to the invalid band names
            # This tested to check if band names are kept, otherwise will appear as `reproject-hash`, but didn't work out
            """image = image.assign_coords(band=bands_of_interest)"""
        else:
            print(f"Dataset already in EPSG:{reproject}")

    # DEBUG
    # force computation to see all chunks

    image_out=image.rio.to_raster(
        out_path,
        driver=out_format,
        compress="LZW",
        dtype=target_dtype
    )
    
    print("-" * 40)
    return image_out

def load_tile(
    tiles_of_interest: str,
    bbox: list | tuple,
    bands: list,
    datetime: str,
    resolution: int,
    chunks: dict | None = None,
    resampling: str | None = "nearest",
):
    """
    Load STAC items for a single tile as a Dask array if chunks are enabled (`dask.array.core.Array`)
    
    Parameters:
    tiles_of_interest (str): name of the tile.
    bbox (list or tuple): bounding box [minx, miny, maxx, maxy] for the tile
    bands (list): list of bands of interest
    datetime (str): time range in "YYYY-MM-DD/YYYY-MM-DD" format
    resolution (int): target resolution
    chunks (dict, optional): dictionary with chunk size for each dimension. None, if chunking not needed.
    resampling(str, optional): type of resampling used (Default: "nearest")
    
    Returns:
    dask.array.core.Array: loaded array for the tile
    pystac.ItemCollection: STAC items

    Notes:
    - `crs=crs` in `odc.stac.stac_load` was used before which led to on-fly tile reprojection
    and artefacts in chunked output compared to non-chunked one. This was omitted, so the loaded data keeps the original CRS.
    - `bilinear` and `cubic` provide smoother resampling, but `nearest` has less artefacts when chunking 
    and doesn't approximate the number of valid pixels (with float numbers).
    - `odc.stac.stac_load` is not covered by the documentation yet (04/11/2025). Only the old version, `odc.stac.load`, 0.39.0 available.
    To check the actual documentation use `help(odc.stac.stac_load)`.
    

    """
    print("-" * 40)
    print(f'Process for tile {tiles_of_interest} started.', flush=True)
    print(f"Bbox of interest is: {bbox}")

    catalog = STACClient.open(
        api_url
        )

    search = catalog.search(
        collections=collection,
        bbox=bbox,
        datetime=datetime
    )
    items = search.item_collection()
    print(f"Number of items: {len(items)}")
    print(f"Type of items: {type(items)}")
    inspect_search(items, index=0)

    data = odc.stac.stac_load(
        items=items,
        bands=bands,
        bbox=bbox,
        resolution=resolution,
        chunks=chunks,
        resampling=resampling
    )
    # TODO - to write clipping by grid tiles.
    # TODO - check out `xr.rio.clip_by_box`

    # DEBUG
    print("Dataset has been loaded")
    print(data)
    print(data.dims)

    return data, items

def process_cloud (cloud_masked):
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
    processes: bool = False,
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
    - It is unclear whether `processes` or `threads` are more suitable for this job: https://dask.discourse.group/t/understanding-how-dask-is-executing-processes-vs-threads/666/2
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
def cloud_prob_wrapper (export_scenes:bool=False, mosaic:bool=True, **cluster_kwargs):
    """
    Main wrapper process entails the following steps either for a single tile, or for a tile series:
    - 0. set up Dask cluster
    - 1. load tile (with inspection)
    - 2. mask cloud with scl
    - (optional) export masked and unmasked scenes to debug (for short datetime)
    - 3. calculate cloud probability and valid count
    - (optional) mosaic cloud probability, if multiple tiles
    - 4. export to geotiff
    
    Parameters:
    export_scenes (bool, default=False): whether exporting scenes is required or not for visual checks (recommended only for calculations with one tile and short time period). `False` if no export required
    mosaic (bool, default=True): whether tiles should be combined (mosaicked into one output). `True` if mosaic is required
    **cluster_kwargs (dict): optional keyword arguments passed to `dask_local_cluster`, eg n_workers=7, threads_per_worker=2 

    Notes:
    - `mosaic=False` is not implemented yet. It is unclear if we need mosaic for larger areas and if it would be better to convert pixel values to parquet
    - `xr.merge (processed_tiles, compat="override")` didn't work - it's exporting the GeoTIFF with the merged extent, but overriding all pixels of other datasets (tiles) with NaN
    `xarray.combine_first` is used instead
    """
    start_time = time.time()

    # set up Dask
    cluster, client = dask_local_cluster(**cluster_kwargs)

    # read tiles and define time range
    tiles = gpd.read_file(tiles_path).to_crs(4326)
    time_of_interest = f'{year}-01-01/{year}-12-31'
    # TODO - to rewrite to a function if we need seasons?

    check_kernel_limit()

    # SINGLE TILE
    if tiles_of_interest and len(tiles_of_interest) == 1:
        print(f"Calculating cloud probability for a tile of interest {tiles_of_interest}...")
        tile_name = tiles_of_interest[0]

        selected_tile = tiles[tiles["tile_name"] == tile_name]
        bbox_of_interest = selected_tile.total_bounds.tolist()

        data, items = load_tile(
            tiles_of_interest=tile_name,
            bbox=bbox_of_interest,
            bands=bands_of_interest,
            datetime=time_of_interest,
            resolution=res,
            chunks=chunks_size,
            resampling=resampling
        )

        try:
            cloud_masked = mask_with_band(data)
            print(f"Cloud masked with SCL.")
        except Exception as e:
            print(f"Failed to mask cloud with SCL band")

        # DEBUG: check each scene for test
        if export_scenes:
            export_s2_scenes(data, items, output_dir="data/test/unmasked_cloud", band="cloud")
            export_s2_scenes(data, items, output_dir="data/test/unmasked_scl", band="scl")
            export_s2_scenes(cloud_masked, items, output_dir="data/test/masked_cloud", band="cloud")

        cloud_out = process_cloud(cloud_masked)

        out_path = f'data/{area_name}_{tile_name}_{year}.tif'

        print(f"Bands of interest are(2): {out_bands}")  # TODO - to fix the name of output bands - not defined
        try:
            out = to_geotif(
                dataset=cloud_out,
                bands_of_interest=out_bands,
                reproject=target_crs,
                out_path=out_path,
                out_format=out_format
            )
            # TODO - move to Dask compute (@delayed) in one task
            print(f"Output GeoTIFF saved to {out_path}")
        except Exception as e:
            print(f"Failed to save GeoTIFF: {e}")

    # MULTIPLE TILES
    else:
        print(f"Calculating cloud probability for a tile series {tiles_of_interest}...")
        processed_tiles = []

        for tile_name in tiles_of_interest:
            selected_tile = tiles[tiles["tile_name"] == tile_name]
            bbox_of_interest = selected_tile.total_bounds.tolist()

            data, items = load_tile(
                tiles_of_interest=tile_name,
                bbox=bbox_of_interest,
                bands=bands_of_interest,
                datetime=time_of_interest,
                resolution=res,
                chunks=chunks_size,
                resampling=resampling
            )

            cloud_masked = mask_with_band(
                data,
                origin_band="cloud",
                mask_band="scl",
                dtype_origin="float32",
                dtype_mask="int16",
                mask_nodata=0,
                fill_value=-1
            )

            cloud_out = process_cloud(cloud_masked)
            print(f"Cloud out dimensions: {cloud_out.dims}") # DEBUG
            cloud_out.attrs["tile_name"] = tile_name  # NOTE: write the tilename to the dataset attributes

            processed_tiles.append(cloud_out)
            print("-" * 40)

        if mosaic:
        # mosaic files together
            first_tile = processed_tiles[0].attrs.get('tile_name')
            mosaic = processed_tiles[0]
            print(f"Starting mosaic with the first tile {first_tile}")
            for tile in processed_tiles[1:]:
                dataset_name = tile.attrs.get('tile_name')
                print(f"Combining tiled dataset {dataset_name} into mosaic for the base...")
                mosaic = tile.combine_first(mosaic)  # NOTE: this will combine the next tiles with the first one

            # TODO - to consider rechunking for `mosaic` by `time` dimension
            # NOTE: Dask will raise a warning for yearly mosaic:
            # /opt/conda/lib/python3.12/site-packages/dask/array/core.py:4996: PerformanceWarning: Increasing number of chunks by factor of 16
            # result = blockwise(

            out_path = f'data/{area_name}_mosaic_{year}.tif'
            print("Combining finished.")
            # TODO: Task 1 (tiling/calculation with Dask), Task 2 (mosaic + conversion to LSOA, probably Spark)
            try:
                out = to_geotif(
                    dataset=mosaic,
                    bands_of_interest=out_bands,
                    reproject=target_crs,
                    out_path=out_path,
                    out_format=out_format
                )
                print(f"Output GeoTIFF saved to {out_path}")
            except Exception as e:
                print(f"Failed to save GeoTIFF: {e}")
            # NOTE: with lazy Dask, time spent on GeoTIFF exporting, especially mosaic
        else:
            raise NotImplementedError("Non-mosaicked output for multiple tiles is not implemented yet.")

    finish_time = time.time()
    elapsed_time = finish_time - start_time
    print(f"Elapsed time: {elapsed_time:.4f} seconds")

    check_dataset(cloud_out)
    inspect_chunks(data)

if __name__ == "__main__":
    tasks = [delayed(cloud_prob_wrapper)(export_scenes=False, mosaic=True)]
    compute(*tasks)