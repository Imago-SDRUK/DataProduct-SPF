from dask.distributed import Client as DaskClient
from dask.distributed import LocalCluster
from dask import delayed, compute
from imago_utils.io.downloader import *
from imago_utils.utils.mask import *
import geopandas as gpd
import time

start_time = time.time()
tile_bbox = [400000.0, 540000.0, 420000.0, 560000.0]
collection = 'e84-s2'
out_bands = ['cloud_prob', 'scl']
start_date = '2024-01-01'
end_date = '2024-12-31'
out_resolution = 20
out_crs = '27700'
chunks = None,

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
            # NOTE: `.count` in xarray automatically calculates without NODATA values
        )
    })

    return cloud_out


#TODO: Storage should be changed and follow defined format
storage = '/home/u5da/bvsh15.u5da/storage/TemporaryData'
tiles = gpd.read_file(f'{storage}/newcastle_tiles.gpkg')



cluster = LocalCluster(
    n_workers=18,              # 18 workers × 4 threads = 72 total threads
    threads_per_worker=4,
    memory_limit="5GB",        # 18 × 5 GB = 90 GB, leaves ~10 GB for system overhead
    dashboard_address=":8787"  # Access dashboard at http://localhost:8787
)
dask_client = DaskClient(cluster)
# print(dask_client.get_versions(check=True))

@delayed
def dask_fuc(tile_bbox,idx):
    tile = load_tile(
        tile_bbox = tile_bbox,
        collection = collection,
        out_bands = out_bands,
        start_date = start_date,
        end_date = end_date,
        out_resolution = out_resolution,
        out_crs = out_crs,
        chunks = chunks
    )

    masked_tile = apply_scl_mask(tile,origin_band='cloud')
    ds = process_cloud(masked_tile)
    to_geotif(ds, out_path=f'{storage}/test_{idx}.tif',out_bands=list(ds.data_vars))

if __name__ == "__main__":
    tasks = [
        dask_fuc([row.minx, row.miny, row.maxx, row.maxy],idx)
        for idx, row in tiles.bounds.iterrows()
    ]

results = compute(*tasks)
