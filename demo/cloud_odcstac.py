# pip install odc-stac
# pip install pystac-client
# pip install dask
import os
import resource
import numpy as np
import xarray as xr
import geopandas as gpd
import odc
import odc.stac
import rioxarray as xrx
from odc.geo.xr import xr_reproject
from pystac_client import Client

# config/constants

DATA_DIR = "/Users/jonathanelsey/Projects/Imago/DataProduct-SPF/demo/data"
tiles_path=f"{DATA_DIR}/uk_20km_grid.gpkg"
tiles_of_interest=["NZ26","NZ24","NZ04","NZ06","NZ44","NZ46"]
#tiles_of_interest=["NZ26"] # NOTE: replace with the tile name if you want calcs for just a single tile (for example, "NZ26")
collection="sentinel-2-c1-l2a"
year=2024
time_of_interest=f'{year}-01-01/{year}-02-28'
bands_of_interest=["cloud", "scl"]
api_url="https://earth-search.aws.element84.com/v1"
res=20
target_crs=27700
target_dtype="float32" #NOTE:we save to GeoTIFF, so dtype must be the same across bands (can't specify integer for the band with pixel count)
resample_type="nearest" # DEFAULT odc-stac: "nearest"

chunking = True
if chunking:
    chunked = "_chunked"
    chunks_size = {'time': 20, 'x': 300, 'y': 300}

else:
    chunked = ''
    chunks_size = None

area="Newcastle"

out_bands=['cloud_prob','valid_count']
out_format="COG"

tiles = gpd.read_file(tiles_path).to_crs(4326)
# DASK config
# chunks_size={'time': 40} #NOTE: if you want to avoid the artefacts in the chunked versions of loaded STACs

#NOTE: there is no info about modes available for resampling in the documentation

#TODO: to design config.yaml


def check_kernel_limit():
    """This checks the local kernel limitations on memory.
    Prints `-1` if inherited from OS and no restrictions."""

    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    print(f"Address space (virtual memory) soft/hard: {soft}/{hard}")
    soft, hard = resource.getrlimit(resource.RLIMIT_DATA)
    print(f"Data segment size soft/hard: {soft}/{hard}")

check_kernel_limit()



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


def mask_with_scl(data):
    """
    Replace cloud values based on SCL mask.
    Rules:
    - if SCL == 0 → cloud = -1
    - if SCL != 0 → keep cloud as is
    - assign -1 as the new no-data value for the cloud band
    - ensure no NaNs remain (fill them with -1)

    Parameters:
    data (xarray.Dataset): original dataset with `cloud` and `scl` variables
    Returns:
    cloud (xarray.DataArray): output array with masked `cloud` band
    """
    print(("-" * 40 + "\n") * 2, end="")
    print(f"Starting masking...")
    cloud = data["cloud"].astype("float32").copy()
    scl = data["scl"].astype("int16")

    print(f"Cloud dimensions: {cloud.dims}")
    print(f"SCL dimensions: {scl.dims}")

    # apply rules
    cloud_masked = cloud.where(scl != 0, -1)

    # replace any remaining NaNs with -1
    cloud_masked = cloud_masked.fillna(-1)
    # assign -1 as the nodata value for output
    cloud_masked.attrs["nodata"] = -1

    print("Type:", type(cloud_masked))
    print("Name:", cloud_masked.name)
    print("Dimensions:", cloud_masked.dims)
    print("Coordinates:", list(cloud_masked.coords))
    print("-1 is set as the no-data value (no NaNs remain).")
    print("-" * 40)

    return cloud_masked

    """
    #NOTE: DEBUG for checking the cloud masked 
    #NOTE: heavy calculation as it opens the whole numpy array (computes)
    num_nodata = cloud_masked.isnull().sum().compute().item()
    print(f"Number of no-data (NaN) values: {num_nodata}")

    # select the first time slice if 'time' is one of the dimensions
    if "time" in cloud_masked.dims:
        first_scene = cloud_masked.isel(time=0)
    else:
        first_scene = cloud_masked

    # ensure CRS and spatial transform are defined
    first_scene = first_scene.rio.write_crs(data["cloud"].rio.crs, inplace=False)
    # export to GeoTIFF
    output_path = "cloud_masked_first_scene.tif"
    first_scene.rio.to_raster(output_path)
    print(f"Exported first scene to {output_path}")
    """
    """
    # DEPRECATED
    # define bad pixels
    nodatas = bitmask
    all_bad_pixels = nodatas(dim="time")

    # Expand dimensions to match data shape
    all_bad_expanded = all_bad_pixels.broadcast_like(nodatas)

    # For these pixels, we’ll override and mark them as good
    effective_bad_mask = nodatas.where(~all_bad_expanded, other=False)

    # Apply the mask: keep data where bad == False
    masked = data.where(~effective_bad_mask)
    return masked
    """


def to_geotif(
        dataset,
        bands_of_interest: list = None,
        reproject: int | None = None,
        out_path: str | None = None,
        out_format: str = "GTiff"
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
    - `odc.geo.xr.xr_reproject` is used instead of `rio.reproject`
    - `odc.reproject` is not used as this command should contain odc metadata
    """

    if isinstance(dataset, xr.DataArray):
        print(f"Dataset/array: Array")
        image = dataset.squeeze('year')

    else:
        print(f"Dataset/array: Dataset")
        image = (
            dataset[bands_of_interest]
            .to_array(dim="band")
            .squeeze('year')
            .transpose('band', 'y', 'x')
            .astype(target_dtype)
        )

    # reproject if defined
    if reproject:
        if image.rio.crs is None:
            print(f"Assigning CRS: EPSG:{reproject}")
            image = image.rio.write_crs(f"EPSG:{reproject}")
        elif image.rio.crs.to_epsg() != reproject:
            print(f"Reprojecting to EPSG:{reproject}")
            """image = image.rio.reproject(f"EPSG:{reproject}")"""
            image = xr_reproject(image, how=f'EPSG:{reproject}')
            print("Reprojection finished")
        else:
            print(f"Dataset already in EPSG:{reproject}")

    # DEBUG
    """
    # force computation to see all chunks
    image.compute()
    print("Image shape:", image.shape)
    print("X extent:", image['x'].min().item(), image['x'].max().item())
    print("Y extent:", image['y'].min().item(), image['y'].max().item())"""

    image_out = image.rio.to_raster(
        out_path,
        driver=out_format,
        compress="LZW",
        dtype=target_dtype
    )

    print("-" * 40)
    return image_out

# NOTE: version with cloud_prob, masked by scl
# NOTE: do not skip `.where(cloud-masked!=-1)` because otherwise it just won't consider pixels covered by satellite, but with a -1 cloud probability
# NOTE: we use now `-1` instead of 0 because that's our new no data value
def process_cloud (cloud_masked):
    """This function calculates the median cloud probability, counts valid pixels from the xarray dataset, and then export it to GeoTIFFs.
    Parameters:
    dataset (xarray.Dataset): dataset with `cloud` and `scl` variables
    out_path (str): path to the output GeoTIFFs.
    Returns:
    cloud_out (xarray.Dataset): output xarray dataset with calculated median probability and valid pixels
    """
    # cloud_out effectively is a pair of reductions - in time dimension
    # cloud_prob - mean cloud probability. (not median!)
    # valid_count - variable corresponding to total number of non-nan cells
    # TODO - changed mean to median
    cloud_out = xr.Dataset({
        "cloud_prob": (
            cloud_masked
            .where(cloud_masked != -1)
            .groupby("time.year")
            .median(dim="time", skipna=True)
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

    print(cloud_out)
    print(cloud_out.dims)
    print(cloud_out.rio.crs)
    print(cloud_out.rio.bounds()) # NOTE: bounds are extended
    print("-" * 40)

    return cloud_out
    # NOTE: occasional issue met loading data from AWS (TODO - to document once faced again)
    # Aborting load due to failure while reading: https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/sentinel-2-c1-l2a/30/U/XG/2024/7/S2A_T30UXG_20240707T112127_L2A/SCL.tif:1

def load_tile(
    tiles_of_interest: str,
    bbox: list | tuple,
    bands: list,
    datetime: str,
    resolution: int,
    crs: int,
    chunks: dict | None = None,
    resampling: str | None = "nearest",
):
    """
    Load STAC items for a single tile
    Parameters:
    tiles_of_interest (str): name of the tile.
    bbox (list or tuple): bounding box [minx, miny, maxx, maxy] for the tile
    bands (list): list of bands of interest
    datetime (str): time range in "YYYY-MM-DD/YYYY-MM-DD" format
    resolution (int): target resolution
    crs (int): target CRS
    chunks (dict, optional): dictionary with chunk size for each dimension. None, if chunking not needed.
    resampling(str, optional): type of resampling used (Default: "nearest")
    Returns:
    xr.Dataset (loaded dataset for the tile)
    pystac.ItemCollection (STAC items)
    """
    print("-" * 40)
    print(f'Process for tile {tiles_of_interest} started.', flush=True)
    print(f"Bbox of interest is: {bbox_of_interest}")

    catalog = Client.open(
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
        #crs=crs,
        chunks=chunks,
        resampling=resampling #TODO - work on the configuration, depends on the HPC config
    )

    # NOTE: parameters to align the data borders: `like`, `anchor` or `geobox`
    # NOTE: `bilinear` and `cubic` provide smoother resampling, but `nearest` has less artefacts when chunking and doesn't approximate the number of valid pixels (with float numbers)

    # NOTE: odc.stac.stac_load is not covered by the documentation yet (only the old version - odc.stac.load in 0.39.0)
    # TO check the actual documentation: `help(odc.stac.stac_load)`
    # TODO - to write clipping by grid tiles.
    # It is recommended to use `geobox=GeoBox.from_bbox(bbox, resolution=gsd, tight=True)` (https://github.com/opendatacube/odc-stac/issues/124)

    print("Dataset has been loaded")
    print(data)
    print(data.dims)

    return data, items

def export_s2_scenes(data, items, output_dir=f"{DATA_DIR}/test", band="cloud"):
    """
    Export Sentinel-2 scenes from an xarray Dataset or DataArray to individual GeoTIFFs.
    Useful for visual checks within a short timeframe, eg January images.
    Filenames are based on the STAC 's2:tile_id' property."""


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
        print(f"Processing scene {i + 1}/{len(data.time)} → {tile_id}")

        # select one scene and load into memory
        scene = data.isel(time=i).compute()
        out_path = os.path.join(output_dir, f"{band}_{tile_id}.tif")
        # to check if crs is written
        scene_band = scene[band]
        scene_band = scene_band.rio.write_crs(scene_band.rio.crs or data[band].rio.crs, inplace=False)

        scene_band.rio.to_raster(out_path)
        print(f"Exported: {out_path}")

    print(f"\n All {len(data.time)} scenes exported to '{output_dir}'.")

# USAGE (to export unmasked scenes)
# export_s2_scenes(data, items, output_dir=f"{DATA_DIR}/test/unmasked", band="cloud")

### MAIN PROCESS
# The block below utilises functions either for a single tile, or for a tile series:
# 1. load tile (with inspection)
# 2. mask cloud with scl
# (optional) export masked and unmasked scenes to debug (for short datetime)
# 3. calculate cloud probability and valid count
# (optional) mosaic cloud probability, if multiple tiles
# 4. export to geotiff

if tiles_of_interest and len(tiles_of_interest) == 1:  # Single tile
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
        crs=target_crs,
        chunks=chunks_size,
        resampling="nearest",
    )

    # DEBUG: check each scene (for test dataset) - unmasked
    # export_s2_scenes(data, items, output_dir=f"{DATA_DIR}/test/unmasked", band="cloud")
    # export_s2_scenes(data, items, output_dir=f"{DATA_DIR}/test/unmasked_scl", band="scl")

    try:
        cloud_masked = mask_with_scl(data)
        print(f"Cloud masked with SCL.")
    except Exception as e:
        print(f"Failed to mask cloud with SCL band")

    # DEBUG: check each scene (for test dataset) - masked
    # export_s2_scenes(cloud_masked, items, output_dir=f"{DATA_DIR}/test/masked", band="cloud")

    cloud_out = process_cloud(cloud_masked)

    out_path = f'{DATA_DIR}/{area}_{tile_name}_{year}{chunked}.tif'
    try:
        out = to_geotif(
            dataset=cloud_out,
            bands_of_interest=out_bands,
            reproject=target_crs,
            out_path=out_path,
            out_format=out_format
        )
        # TODO - Dask configuration with compute (@delayed) in one task
        print(f"Output GeoTIFF saved to {out_path}")
    except Exception as e:
        print(f"Failed to save GeoTIFF: {e}")

else:  # tile series
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
            crs=target_crs,
            chunks=chunks_size,
            resampling="nearest",
        )

        cloud_masked = mask_with_scl(data)

        cloud_out = process_cloud(cloud_masked)
        print(f"CLOUD OUT dimensions: {cloud_out.dims}")
        cloud_out.attrs["tile_name"] = tile_name  # NOTE: write the tilename to the dataset attributes

        processed_tiles.append(cloud_out)  # NOTE: should be fine as it's a lazy operation
        print("-" * 40)

        """
        out_path = f'{DATA_DIR}/{tile_name}_2024.tif'
        out_bands=['cloud_prob','valid_count']

        try:
            to_geotif(cloud_out,out_bands, out_path=out_path)
            print(f"Output GeoTIFF saved to {out_path}")
        except Exception as e:
            print(f"Failed to save GeoTIFF: {e}")
        """
    # mosaic files together
    first_tile = processed_tiles[0].attrs.get('tile_name')
    mosaic = processed_tiles[0]
    print(f"Starting mosaic with the first tile {first_tile}")
    for tile in processed_tiles[1:]:
        dataset_name = tile.attrs.get('tile_name')
        print(f"Combining tiled dataset {dataset_name} into mosaic for the base...")
        mosaic = tile.combine_first(mosaic)  # NOTE: this will combine the next tiles with the first one
        # TODO - check if there should be rio.crs
    # NOTE: Dask will raise a warning for yearly mosaic:
    # /opt/conda/lib/python3.12/site-packages/dask/array/core.py:4996: PerformanceWarning: Increasing number of chunks by factor of 16
    # result = blockwise(

    out_path = f'{DATA_DIR}/{area}_mosaic_{year}.tif'
    out_bands = ['cloud_prob', 'valid_count']
    print("Combining finished.")
    # NOTE: xr.merge (processed_tiles, compat="override") didn't work - it's exporting the GeoTIFF with the merged extent, but overriding all pixels of other datasets (tiles) with NaN
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
    # NOTE: with lazy Dask, mainly time spent on GeoTIFF exporting, especially mosaic


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


check_dataset(cloud_out)
# TODO - to add logic for checking Dataset, in addition to Array

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

inspect_chunks(data)

import cartopy.crs as ccrs
from matplotlib import pyplot as plt
from matplotlib.animation import FuncAnimation
# Plot the cloud probability in the initial dataset as an animation.
fig, ax = plt.subplots()
ax.contourf(data['cloud'][0,:,:])

def update(frame):
    cont = ax.contourf(data['cloud'][frame,:,:])
    plt.title(f'cloud probability, frame {frame}')
    print(frame)
    return cont

# anim = FuncAnimation(fig, update, frames=int(len(data['cloud'])/10))