import ee
from ee_lst.landsat_lst import fetch_landsat_collection
from ee_lst.broadband_emiss import add_band
ee.Initialize()

# Define the Landsat LST calculation functions (equivalent to JS modules)
def get_specific_collection(satellite, date_start, date_end, geometry, cloud_threshold, use_ndvi):
    try:
        landsat_coll = fetch_landsat_collection(
        satellite, date_start, date_end, geometry, cloud_threshold, use_ndvi
        )
    except ValueError as e:
        print(e)
    return landsat_coll

def make_get_collection(date_start, date_end, geometry, cloud_threshold, use_ndvi):
    def get_collection(satellite):
        return get_specific_collection(satellite, date_start, date_end, geometry, cloud_threshold, use_ndvi)
    return get_collection

# 创建一个包装函数来处理map
def create_add_band_mapper(use_ndvi):
    def mapper(image):
        return add_band(use_ndvi, image)
    return mapper

site = ee.Geometry.Point([-116.01947, 36.62373])
geometry = site.buffer(30)
date_start = '1982-08-01'
date_end = '2020-01-31'
use_ndvi = True
add_bbe = create_add_band_mapper(True)
cloud_threshold = 20

get_collection = make_get_collection(date_start, date_end, geometry, cloud_threshold, use_ndvi)
L8coll = get_collection('L8').map(add_bbe)
L7coll = get_collection('L7').map(add_bbe)
L5coll = get_collection('L5').map(add_bbe)
L4coll = get_collection('L4').map(add_bbe)

def rename_band(image, sat):
    return image.select('LST').rename(f'LST_{sat}')

landsat_coll = ee.ImageCollection([])
for sat, coll in [('L8', L8coll), ('L7', L7coll), ('L5', L5coll), ('L4', L4coll)]:
    landsat_coll = landsat_coll.merge(coll.map(lambda img: rename_band(img, sat)))

# Generate time series chart (Note: Direct charting in Python requires altair/folium)
# For Python, we typically export the data instead
# This shows how to create a feature collection for export
def create_feature(image):
    date = ee.Date(image.get('system:time_start'))
    props = {
        'year': date.get('year'),
        'month': date.get('month'),
        'day': date.get('day'),
        'hour': date.get('hour'),
        'minute': date.get('minute'),
        'lst': image.select('LST').reduceRegion(ee.Reducer.mean(), geometry, 30).get('LST'),
        'tpw': image.select('TPW').reduceRegion(ee.Reducer.mean(), geometry, 30).get('TPW'),
        'em': image.select('EM').reduceRegion(ee.Reducer.mean(), geometry, 30).get('EM'),
        'fvc': image.select('FVC').reduceRegion(ee.Reducer.mean(), geometry, 30).get('FVC'),
        'bbe': image.select('BBE').reduceRegion(ee.Reducer.mean(), geometry, 30).get('BBE')
    }
    return ee.Feature(site, props)

# Export to Drive
task = ee.batch.Export.table.toDrive(
    collection=ee.FeatureCollection(L8coll.map(create_feature)),
    description='export_landsat_lst_timeseries',
    folder='landsat_lst_timeseries',
    fileNamePrefix='landsat_lst_timeseries',
    fileFormat='CSV'
)
task.start()

print("Export task started.") 