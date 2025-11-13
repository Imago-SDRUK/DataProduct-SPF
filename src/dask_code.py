from dask.distributed import Client as DaskClient
import dask.bag as db
from dask.distributed import LocalCluster
from dask import delayed, compute
from imago_utils.io.downloader import load_tile, to_geotif
from imago_utils.utils.mask import apply_scl_mask
from imago_utils.utils.process import process_cloud
import geopandas as gpd
import yaml

with open("/home/u5da/bvsh15.u5da/codebook/config_template.yaml", "r") as f:
    config = yaml.safe_load(f)

# extract sections
input_cfg = config["input"]
output_cfg = config["output"]
dask_cfg = config["dask"]

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
n_workers = dask_cfg["n_workers"]
threads_per_worker = dask_cfg["threads_per_worker"]
memory_limit = dask_cfg["memory_limit"]
dashboard_address = dask_cfg["dashboard_address"]

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
    print("\nto access the Dask dashboard, do:",flush=True)
    print(f"ssh -L {dashboard_address.lstrip(':')}:{scheduler_host}{dashboard_address} "
          f"<hpc_username>@<login_node>", flush=True)
    print(f"then open: http://localhost{dashboard_address}\n",flush=True)

    tile_list = [
        ([row.minx, row.miny, row.maxx, row.maxy], tiles.loc[row.name, "tile_name"])
        for _, row in tiles.bounds.iterrows()
    ]

    
    bag = db.from_sequence(tile_list, npartitions=16) #Break tiles into 16 groups
    bag = bag.map(lambda x: process_tiles(x[0], x[1])) # Map the processing function to each tile
    results = bag.compute()
    return results

if __name__ == "__main__":
    results = main()