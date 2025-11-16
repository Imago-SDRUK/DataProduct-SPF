from dask.distributed import Client as DaskClient
import dask.bag as db
from dask.distributed import LocalCluster
from dask_jobqueue import SLURMCluster
from dask import delayed, compute
from imago.io.downloader import load_tile, to_geotif
from imago.utils.mask import apply_scl_mask
from imago.spf.process import process_cloud
import geopandas as gpd
import yaml, argparse, time

def parse_args():
    parser = argparse.ArgumentParser(
        prog="SPF",
        description="Processing of cloud probability for the SPF data product",
    )
    parser.add_argument(
        "--config_path",
        "-i",
        help="Absolute or relative path to a config.yaml file",
        default="config.yaml",
        required=False,
    )
    return parser.parse_args()

def process_tiles(tile_bbox, tile_name):
    tile = load_tile(
        tile_bbox=tile_bbox,
        collection=collection,
        out_bands=out_bands,
        start_date=start_date,
        end_date=end_date,
        out_resolution=out_resolution,
        out_crs=out_crs,
        chunks=chunks,
    )

    masked_tile = apply_scl_mask(tile, origin_band="cloud")
    ds = process_cloud(masked_tile)
    to_geotif(ds,
              out_format=out_format,
              out_dtype = out_dtype, 
              out_path=f"{output_storage}/{output_filename}_{tile_name}.tif", 
              out_bands=list(ds.data_vars),
              time_dim = time_dim)

def main():
    args = parse_args()

    with open(args.config_path, "r") as f:
        config = yaml.safe_load(f)
    
    # extract sections
    input_cfg = config["input"]
    output_cfg = config["output"]
    dask_cfg = config["dask"]

    # make globals accessible to process_tiles
    global tiles_file, collection, start_date, end_date, out_bands
    global input_storage, output_storage, output_filename, out_crs
    global out_resolution, out_dtype, resampling, out_format, time_dim
    global chunks, npartitions, n_workers, threads_per_worker, memory_limit, dashboard_address
    global local_cluster, core

    # for each section, go through and get the variables of interest
    # input params
    tiles_file= input_cfg["tiles"]
    collection = input_cfg["collection"]
    start_date = input_cfg["time"]["start_date"]
    end_date = input_cfg["time"]["end_date"]
    out_bands = input_cfg["bands_of_interest"]
    input_storage = input_cfg["input_storage"]
    # output params
    output_storage = output_cfg["output_storage"]
    output_filename = output_cfg["output_filename"]
    out_crs = output_cfg["out_crs"]
    out_resolution = output_cfg["res"]
    out_dtype = output_cfg["out_dtype"]
    resampling = output_cfg["resampling"]
    out_format = output_cfg["out_format"]
    time_dim = output_cfg["time_dim"]
    # dask/optimisation paramsprocess.py
    chunks = dask_cfg["chunks"]
    npartitions = dask_cfg["npartitions"]
    n_workers = dask_cfg["n_workers"]
    threads_per_worker = dask_cfg["threads_per_worker"]
    memory_limit = dask_cfg["memory_limit"]
    dashboard_address = dask_cfg["dashboard_address"]
    local_cluster = dask_cfg["local_cluster"]
    core = dask_cfg["core"]
    """Main function that generates the cluster and performs the computation"""
    tiles = gpd.read_file(f"{input_storage}/{tiles_file}")

    if local_cluster:

        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            memory_limit=memory_limit,
            dashboard_address=8787,
        )
        client = DaskClient(cluster)
    else:
        # TODO: adjust SLURMCluster parameters as needed based on your HPC environment
        cluster = SLURMCluster(
            cores=core,
            processes=threads_per_worker,
            memory=memory_limit,
            shebang="#!/usr/bin/env bash",
            walltime="12:00:00",
            local_directory="/tmp",
            log_directory="/tmp",
            death_timeout="30s",
            job_extra_directives=[ 
                "--gres=gpu:0" 
                ]
        )

        client = DaskClient(cluster)
        cluster.scale(jobs=n_workers)

    while len(cluster.scheduler.workers) < 5: # wait for at least 5 workers to be ready
        time.sleep(1)


    tasks= [
        process_tiles([row.minx, row.miny, row.maxx, row.maxy], tiles.loc[row.name, "tile_name"])
        for _, row in tiles.bounds.iterrows()
    ]
    # Break tasks into chunks
    for i in range(0, len(tasks), npartitions):
        chunk = tasks[i : i + npartitions]
        results = compute(*chunk)

    return results

if __name__ == "__main__":
    results = main()