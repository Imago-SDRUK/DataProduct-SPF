#pip install odc-stac pystac-client dask

### HPC config
# NOTE: if `imago-utils` is mounted as an extra volume to your docker, use the following:
import sys
sys.path.append('/mnt/imago-utils/src')

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
import os
import resource

from typing import List, Optional, Dict, Union

from dask.distributed import Client as DaskClient, LocalCluster
from dask import delayed, compute
import dask.bag as db
from functools import partial
#from dask_jobqueue import SLURMCluster

#INTERNAL UTILS
from imago_utils.io.downloader import load_tile, to_geotif
from imago_utils.utils.mask import apply_scl_mask
from imago_utils.utils.config import ResampleEnum, OutputFormatEnum, DataTypeEnum, CollectionEnum
from imago_utils.utils.tile_inspection import inspect_search, export_scenes


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

with open("config/config.yaml") as f:
    cfg = yaml.safe_load(f)
    
    tiles=get_cfg(cfg,"in_area.tiles")

    api_url=get_cfg(cfg,"in_stac.api_url")
    collection=get_cfg(cfg,"in_stac.collection")
    year=get_cfg(cfg,"in_stac.time.year")
    season=get_cfg(cfg,"in_stac.time.season")
    start_date=get_cfg(cfg,"in_stac.time.start_date")
    end_date=get_cfg(cfg,"in_stac.time.end_date")

    bands_of_interest=get_cfg(cfg,"in_stac.bands_of_interest")

    area_name=get_cfg(cfg,"out.area_name")
    out_bands=get_cfg(cfg,"out.out_bands")
    res=get_cfg(cfg,"out.res")
    target_crs=get_cfg(cfg,"out.target_crs")
    resampling=get_cfg(cfg,"out.resampling")
    target_dtype=get_cfg(cfg,"out.target_dtype")
    out_format=get_cfg(cfg,"out.out_format")

    chunks_size=get_cfg(cfg,"dask.chunks_size")
    flush=get_cfg(cfg,"dask.flush")

def check_hpc_mem_limit(flush: bool = True):
    """
    Checks memory limits on HPC systems.
    
    Parameters:
    flush (bool): Whether to flush print output immediately.
    
    Notes:
    - checks kernel memory limits (soft/hard)
    - checks slurm memory allocation if running under Slurm
    """
    # --- Kernel limits ---
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    soft = -1 if soft == resource.RLIM_INFINITY else soft
    hard = -1 if hard == resource.RLIM_INFINITY else hard
    print(f"-" * 40, flush=flush)
    print(f"[Kernel] Address space (virtual memory) soft/hard: {soft}/{hard}", flush=flush)

    soft, hard = resource.getrlimit(resource.RLIMIT_DATA)
    soft = -1 if soft == resource.RLIM_INFINITY else soft
    hard = -1 if hard == resource.RLIM_INFINITY else hard
    print(f"[Kernel] Data segment size soft/hard: {soft}/{hard}", flush=flush)

    # --- Slurm limits ---
    if 'SLURM_JOB_ID' in os.environ:
        mem_per_node = os.environ.get('SLURM_MEM_PER_NODE', 'unknown')
        mem_per_cpu = os.environ.get('SLURM_MEM_PER_CPU', 'unknown')
        print(f"[Slurm] Memory per node: {mem_per_node} MB", flush=flush)
        print(f"[Slurm] Memory per CPU: {mem_per_cpu} MB", flush=flush)
    else:
        print("[Slurm] Not running under Slurm", flush=flush)
    print(f"-" * 40, flush=flush)

def process_cloud(cloud_masked):
    """
    This function calculates the median cloud probability, counts valid pixels from the xarray dataset, and then export it to GeoTIFFs.
    
    Parameters:
    dataset (xarray.Dataset): dataset with `cloud` and `scl` variables
    
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
    print("-" * 40, flush=flush)
    print("Output dataset specifications:", flush=flush)
    print(cloud_out, flush=flush)
    print(cloud_out.dims,flush=flush)
    print(cloud_out.rio.crs, flush=flush)
    print(cloud_out.rio.bounds(), flush=flush)
    print("-" * 40, flush=flush)

    return cloud_out

def check_dataset(obj):
    """General info about the output array or dataset"""
    print(f"Type: {type(obj)}", flush=flush)
    
    if isinstance(obj, xr.DataArray):
        print(f"Shape: {obj.shape}", flush=flush)
        print(f"Dimensions: {obj.dims}", flush=flush)
        print(f"Coordinates: {list(obj.coords)}", flush=flush)
        print(f"Attributes: {obj.attrs}", flush=flush)
    
    elif isinstance(obj, xr.Dataset):
        print(f"Variables: {list(obj.data_vars)}", flush=flush)
        print(f"Coordinates: {list(obj.coords)}", flush=flush)
        print(f"Attributes: {obj.attrs}", flush=flush)
        print(f"Dimensions: {obj.dims}", flush=flush)

def inspect_chunks(obj, flush: bool = True):
    """
    Inspects Dask chunking for an xarray Dataset or DataArray.
    Prints total number of chunks, average size, and alignment info.
    
    Parameters:
    obj: xarray.Dataset or xarray.DataArray
    flush (bool): whether to flush print output immediately
    """
    # Handle Dataset (multiple variables)
    if isinstance(obj, xr.Dataset):
        print("=" * 60, flush=flush)
        print(f"Dataset with {len(obj.data_vars)} variables:", flush=flush)
        for var_name, da in obj.data_vars.items():
            print(f"\nVariable: {var_name}", flush=flush)
            inspect_chunks(da)
        return

    da = obj #handle dataarray (single variable)

    if not hasattr(da.data, "chunks"):
        print("Array not chunked (not a Dask array).", flush=flush)
        print("=" * 60, flush=flush)
        return

    chunks = da.data.chunks
    dtype_size = da.dtype.itemsize

    print("-" * 60, flush=flush)
    total_chunks = 1
    uneven = False

    for dim, sizes in zip(da.dims, chunks):
        total_chunks *= len(sizes)
        equal = len(set(sizes)) == 1
        if not equal:
            uneven = True
        print(f"{dim:>6}: {len(sizes)} chunks | sizes = {sizes[:5]}{'...' if len(sizes) > 5 else ''}", flush=flush)

    avg_chunk_elems = np.prod([np.mean(s) for s in chunks])
    avg_chunk_bytes = avg_chunk_elems * dtype_size
    avg_chunk_mb = avg_chunk_bytes / 1e6

    print("-" * 60, flush=flush)
    print(f"Total chunks: {total_chunks}", flush=flush)
    print(f"Average chunk size: {avg_chunk_mb:.2f} MB ({da.dtype})", flush=flush)
    print(f"Chunks evenly sized? {'Yes' if not uneven else 'No, uneven chunks'}", flush=flush)

def dask_local_cluster(
    n_workers: int | None = 18,
    threads_per_worker: int | None = 4,
    memory_limit: str | None = "5GB",
    dashboard_address: str = ":8787"
):
    """
    This function utilises the local Dask cluster (not suitable for HPC)

    Parameters:
    n_workers (int): number of workers to start in the cluster.
    threads_per_worker (int): number of threads per worker process
    memory_limit (str): memory limit per worker, eg "4GB"
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
    - `creating more workers` are more suitable for this job than threads: https://dask.discourse.group/t/understanding-how-dask-is-executing-processes-vs-threads/666/2
    - Print statements wil declare parameters are None if they are not passed, but Dask internally uses automatic parameter values
    """
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        dashboard_address=dashboard_address
    )
    client = DaskClient(cluster)

    # DEBUG + dashboard
    print("-" * 40, flush=flush)
    print("DASK CLUSTER", flush=flush)
    print(f"Scheduler: {cluster.scheduler_address}", flush=flush)
    print(f"Number of workers: {len(cluster.workers)}", flush=flush)
    print(f"Total threads: {sum(w.nthreads for w in cluster.workers.values())}", flush=flush)
    total_memory_bytes = sum(
        w.memory_limit for w in cluster.workers.values() if w.memory_limit is not None
    )   
    print(f"Total memory: {total_memory_bytes / 1e9:.2f} GiB", flush=flush)

    print("Workers:", flush=flush)
    for i, (addr, worker) in enumerate(cluster.workers.items()):
        mem_str = f"{worker.memory_limit / 1e9:.2f} GiB" if worker.memory_limit else "unlimited"
        print(f"Worker {i}: Address={addr}, Threads={worker.nthreads}, Memory={mem_str}", flush=flush)

    print(f"Client: {client}", flush=flush)
    print(f"Dashboard: {client.dashboard_link}", flush=flush)
    print("-" * 40, flush=flush)

    return cluster,client

#### MAIN PROCESS
@delayed
def cloud_tile_wrapper(idx, tile_name, tile_bbox, out_path: Optional[str], export_tile=True, inspect=False,
                       out_format: OutputFormatEnum = OutputFormatEnum.GTIFF,
                       out_bands: Optional[List[str]] = None):
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
    idx (int): Index of the tile in the tiles list or GeoDataFrame.
    tile_name (str): Unique name/identifier for the tile.
    tile_bbox (list or tuple of float): Bounding box of the tile in [minx, miny, maxx, maxy] format.
    out_path (str): output path
    export_tile (bool, default=False): whether exporting each calculated output (per tile) into a separate GeoTIFF
    inspect (bool, default=True): whether inspecting the first item from the STAC collection
    out_format (OutputFormatEnum, default=OutputFormatEnum.GTIFF): output file format, according to the internal config
    out_bands (Optional[List[str]]): names of output bands

    Returns
    tile_out (xarray.Dataset): processed dataset

    Notes:
    - `xr.merge (processed_tiles, compat="override")` didn't work - it's exporting the GeoTIFF with the merged extent, but overriding all pixels of other datasets (tiles) with NaN
    `xarray.combine_first` is used instead
    - Avoid creatig directories in the Python code on HPC, try to run jobs instead at the level aboves
    """

    row = tiles_gdf.loc[idx] 
    tile_bbox = list(row.geometry.bounds)

    data,items=load_tile(
        tile_bbox=tile_bbox,
        collection=collection,
        out_bands=bands_of_interest,
        start_date=start_date,
        end_date=end_date,
        out_resolution=res,
        out_crs=target_crs,
        chunks=chunks_size
    )

    inspect_search(items, index=0, flush=flush) if inspect else None
        
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

        print(f"Cloud masked with SCL.", flush=flush)
    except Exception as e:
        print(f"Failed to mask cloud with SCL band", flush=flush)

    tile_out = process_cloud(cloud_masked)

    tile_name = tiles_gdf.loc[idx, "tile_name"]
    tile_out.attrs["tile_name"] = tile_name
    print(f"Tile name: {tile_name}", flush=flush)
    print(f"Cloud out dimensions: {tile_out.dims}", flush=flush)
        
    if export_tile:

        try:
            out = to_geotif(
                dataset=tile_out,
                out_bands=out_bands,
                out_crs=target_crs,
                out_path=out_path,
                out_dtype=DataTypeEnum.FLOAT32, # TODO - to get from config 
                out_format=out_format
            )
            
            print(f"Output saved to {out_path}", flush=flush)
        except Exception as e:
            print(f"Failed to save: {e}", flush=flush)

    check_dataset(tile_out)
    inspect_chunks(data)

    return tile_out

@delayed
def mosaic_tiles(processed_tiles, area_name: str, year: int, target_crs, out_format: OutputFormatEnum = OutputFormatEnum.GTIFF, out_bands: Optional[List[str]] = None):
    """
    Creates a mosaic from multiple processed tile datasets (eg, annual cloud probability). 
    Starts from the first tile and combines others with this one.

    Parameters:
    - processed_tiles (list of xarray.Dataset): list of tiles already processed
    - area_name (str): area name for output file naming
    - year (int): data year
    - target_crs (str): CRS for reprojection
    - out_format (OutputFormatEnum = OutputFormatEnum.GTIFF): output format, eg GeoTIFF or COG
    - out_bands (Optional[List[str]]): bands of export

    Returns:
    - xarray.Dataset: lazy Dask-backed dataset representing the mosaic
    """
    if not processed_tiles or len(processed_tiles) == 0:
        raise ValueError("No processed tiles provided for mosaicking.")

    if len(processed_tiles) == 1:
        mosaic = processed_tiles[0]
        tile_name = mosaic.attrs.get("tile_name", "unknown")
        print(f"Only one tile provided ({tile_name}). Mosaic is not provided.", flush=flush)
        return mosaic
    else:
        mosaic = processed_tiles[0]
        first_tile_name = mosaic.attrs.get("tile_name", "unknown")
        print(f"Starting mosaic with the first tile: {first_tile_name}", flush=flush)

        for tile_ds in processed_tiles[1:]:
            tile_name = tile_ds.attrs.get("tile_name", "unknown")
            print(f"Combining tile {tile_name} into mosaic...", flush=flush)
            mosaic = tile_ds.combine_first(mosaic)

    # export mosaic to GeoTIFF (lazy Dask will compute here)
    out_path = f"data/{area_name}_mosaic_{year}.tif"
    try:
        out = to_geotif(
            dataset=mosaic,
            out_bands=out_bands,
            out_crs=target_crs,
            out_path=out_path,
            out_dtype=DataTypeEnum.FLOAT32,  # TODO - to get from CONFIOG
            out_format=out_format  # TODO - to get from CONFIOG
        )

        print(f"Mosaic saved to {out_path}", flush=flush)
    except Exception as e:
        print(f"Failed to save mosaic: {e}", flush=flush)

    return mosaic

if __name__ == "__main__":
    """
    This will create number of tasks equal to the tile number and build the Dask graph.
    Mosaic is not included.
    """
    
    check_hpc_mem_limit()
    
    cluster, client = dask_local_cluster()

    # open tiles and compute indices
    tiles_gdf = gpd.read_file(tiles)
    indices = list(range(len(tiles_gdf)))

    tile_names = tiles_gdf["tile_name"].tolist()  # list of tile names in the same order as indices
    tile_bboxes = [list(geom.bounds) for geom in tiles_gdf.geometry]
    out_paths = [f"data/output/{name}.tif" for name in tile_names]

    # list comprehension - build tasks
    tasks = [
        cloud_tile_wrapper(
            idx,
            tile_name=tile_names[idx],
            tile_bbox=tile_bboxes[idx],
            out_path=out_paths[idx],
            export_tile=True,
            inspect=False,
            out_format=OutputFormatEnum.GTIFF,
            out_bands=out_bands
        )
        for idx in indices
    ]

    # actual computation
    start_time = time.time()
    results = compute(*tasks)
    finish_time = time.time()
    elapsed_time = finish_time - start_time
    print(f"Elapsed time: {elapsed_time:.2f} seconds", flush=flush)
    