from dask.distributed import Client as DaskClient
from dask.distributed import LocalCluster
from dask import delayed, compute
from imago_utils.io.downloader import load_tile, to_geotif
from imago_utils.utils.mask import apply_scl_mask
import xarray as xr
import geopandas as gpd
import yaml

with open("config_test.yaml", "r") as f:
    config = yaml.safe_load(f)

# extract sections
input_cfg = config["input"]
output_cfg = config["output"]
dask_cfg = config["dask"]

# for each section, go through and get the variables of interest
# input params
tiles_file= input_cfg["tiles"]
api_url = input_cfg["api_url"]
collection = input_cfg["collection"]
start_date = input_cfg["time"]["start_date"]
end_date = input_cfg["time"]["end_date"]
out_bands = input_cfg["bands_of_interest"]
input_storage = input_cfg["input_storage"]
# output params
output_storage = output_cfg["output_storage"]
output_filename = output_cfg["output_filename"]
target_crs = output_cfg["target_crs"]
out_resolution = output_cfg["res"]
target_dtype = output_cfg["target_dtype"]
resampling = output_cfg["resampling"]
out_format = output_cfg["out_format"]
area_name = output_cfg["area_name"]
# dask/optimisation params
chunks = dask_cfg["chunks"]
n_workers = dask_cfg["n_workers"]
threads_per_worker = dask_cfg["threads_per_worker"]
memory_limit = dask_cfg["memory_limit"]
dashboard_address = dask_cfg["dashboard_address"]

# TODO: Find a sutable function name so that it can be realted to the SPF project
# TODO: Move the function to imago-utils in to process.py
def process_cloud(cloud_masked):
    """This function calculates the median cloud probability, counts valid pixels from the xarray dataset, and then export it to GeoTIFFs.
    Parameters:
    dataset (xarray.Dataset): dataset with `cloud` and `scl` variables
    out_path (str): path to the output GeoTIFFs.
    Returns:
    cloud_out (xarray.Dataset): output xarray dataset with calculated median probability and valid pixels
    """
    cloud_out = xr.Dataset(
        {
            "cloud_prob": (
                cloud_masked.where(cloud_masked != -1)
                .groupby("time.year")
                .mean(dim="time", skipna=True)
                .astype("float32")
            ),
            "valid_count": (
                cloud_masked.where(cloud_masked != -1)
                .groupby("time.year")
                .count(dim="time")
                .astype("int16")
                # NOTE: `.count` in xarray automatically calculates without NODATA values
            ),
        }
    )

    return cloud_out

@delayed
def process_tiles(tile_bbox, idx):
    tile = load_tile(
        tile_bbox=tile_bbox,
        collection=collection,
        out_bands=out_bands,
        start_date=start_date,
        end_date=end_date,
        out_resolution=out_resolution,
        out_crs=target_crs,
        chunks=chunks,
    )

    masked_tile = apply_scl_mask(tile, origin_band="cloud")
    ds = process_cloud(masked_tile)
    to_geotif(ds, out_path=f"{output_storage}/{output_filename}", out_bands=list(ds.data_vars))

def main():
    """Main function that generates the cluster and performs the computation"""
    tiles = gpd.read_file(f"{input_storage}/{tiles_file}")
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        dashboard_address=dashboard_address,
    )

    dask_client = DaskClient(cluster)
    dash_url = cluster.dashboard_link
    scheduler_host = dash_url.split("//")[1].split(":")[0]
    print("\nto access the Dask dashboard, do:")
    print(f"ssh -L {dashboard_address.lstrip(':')}{scheduler_host}{dashboard_address} "
          f"<hpc_username>@<login_node>")
    print(f"then open: http://localhost{dashboard_address}\n")
    # print(dask_client.get_versions(check=True))

    tasks = [
        process_tiles([row.minx, row.miny, row.maxx, row.maxy], idx)
        for idx, row in tiles.bounds.iterrows()
    ]
    results = compute(*tasks)
    return results

if __name__ == "__main__":
    results = main()