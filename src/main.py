from dask.distributed import Client as DaskClient
from dask.distributed import LocalCluster
from dask_jobqueue import SLURMCluster
from dask import compute, delayed
from imago.io.downloader import load_tile
from imago.io.output import to_geotif, to_netcdf
from imago.utils.mask import apply_scl_mask
from imago.spf.process import process_cloud
import geopandas as gpd
import yaml, argparse

def parse_args():
    """Load in the config filepath from the command line, or set it to `config.yaml`"""
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

def validate_config(input_cfg, output_cfg, dask_cfg):
    """Check that fields in the config files have acceptable values"""
    # JE - This should be an evolving thing as we add more config stuff. I include a pattern
    # here as an example for e.g. output format
    acceptable_output_formats = ["geotiff", "cog", "netcdf", "gtiff"]
    # Default to netCDF if not specified
    if not "output_format" in output_cfg:
        input_cfg["output_format"] = "netcdf"

    if output_cfg["output_format"].lower() not in acceptable_output_formats:
        raise ValueError(f'Output format {output_cfg["output_format"]} is not acceptable, '
                         f'need one of {acceptable_output_formats}')
    # if "geotiff" coerce to GTiff as this is the required format
    elif output_cfg["output_format"].lower() == "geotiff":
        output_cfg["output_format"] = "GTiff"

    # pass-by-reference so don't need to return

@delayed
def process_tiles(tile_bbox, tile_name, input_cfg, output_cfg, dask_cfg):
    """
    Given a tilename and bounding box:
        - Load in the tile and the bands we want at the specified resolution
        - Apply masking and obtain the aggregated cloud probability
        - Write it out to either geotiff, COG or netcdf
    """
    tile = load_tile(
        tile_bbox=tile_bbox,
        collection=input_cfg["collection"],
        out_bands=input_cfg["bands_of_interest"],
        start_date=input_cfg["time"]["start_date"],
        end_date=input_cfg["time"]["end_date"],
        out_resolution=output_cfg["res"],
        out_crs=output_cfg["out_crs"],
        chunks=dask_cfg["chunks"],
    )

    masked_tile = apply_scl_mask(tile, origin_band="cloud")
    ds = process_cloud(masked_tile)
    if output_cfg["output_format"].lower() in ['gtiff', 'cog']:
        out_path = to_geotif(ds,
                  out_format=output_cfg["output_format"],
                  out_dtype=output_cfg["out_dtype"],
                  out_path=f"{output_cfg['output_storage']}/{output_cfg['output_filename']}_{tile_name}.tif",
                  out_bands=list(ds.data_vars),
                  time_dim=output_cfg["time_dim"])

    elif output_cfg["output_format"].lower() == 'netcdf':
        out_path = to_netcdf(ds,
                  out_path=f"{output_cfg['output_storage']}/{output_cfg['output_filename']}_{tile_name}.nc",
                  out_bands=list(ds.data_vars),
                  out_dtype=output_cfg["out_dtype"])

    return out_path

def main():
    """Main function that generates the cluster and performs the computation"""

    args = parse_args()
    with open(args.config_path, "r") as f:
        config = yaml.safe_load(f)

    # extract sections and validate
    input_cfg = config["input"]
    output_cfg = config["output"]
    dask_cfg = config["dask"]
    validate_config(input_cfg, output_cfg, dask_cfg)

    # load in tiles file
    tiles = gpd.read_file(f"{input_cfg['input_storage']}/{input_cfg['tiles']}")

    if dask_cfg["local_cluster"]:
        cluster = LocalCluster(
            n_workers=dask_cfg["n_workers"],
            threads_per_worker=dask_cfg["threads_per_worker"],
            memory_limit=dask_cfg["memory"],
            dashboard_address=dask_cfg["dashboard_address"],
        )
        client = DaskClient(cluster)
    else:
        cluster = SLURMCluster(
            cores=dask_cfg["cores"],
            processes=dask_cfg["threads_per_worker"],
            memory=dask_cfg["memory"],
            shebang="#!/usr/bin/env bash",
            walltime=dask_cfg["worker_walltime"],
            local_directory="/tmp",
            log_directory="/tmp",
            death_timeout="30s",
            job_extra_directives=dask_cfg["job_extra_directives"],
        )

        client = DaskClient(cluster)
        cluster.scale(jobs=dask_cfg["n_workers"])

    # Wait until we have at least 25% of the requested workers
    client.wait_for_workers(n_workers=dask_cfg["n_workers"] // 4, timeout=dask_cfg["client_worker_timeout"])

    # build tasks lazily using dask.delayed
    # JE - this should run lazily now I think? I recall we had something decorated with @delayed
    # before but I can't see it in any of the current repositories.
    # Do we need to_geotif and to_netcdf to be delayed also?
    tasks = [
        process_tiles(
            [row.minx, row.miny, row.maxx, row.maxy],
            tiles.loc[row.name, "tile_name"],
            input_cfg, output_cfg, dask_cfg
        )
        for _, row in tiles.bounds.iterrows()
    ]

    # create chunks - this should return the output paths now too
    out_paths = []
    for i in range(0, len(tasks), dask_cfg["npartitions"]):
        chunk = tasks[i: i + dask_cfg["npartitions"]]
        # compute returns a tuple of results for *chunk
        out_paths.extend(compute(*chunk))

    return out_paths


if __name__ == "__main__":
    results = main()