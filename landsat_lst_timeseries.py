import ee
from ee_lst.landsat_lst import fetch_landsat_collection
from ee_lst.broadband_emiss import add_band
from functools import partial
import random
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

def get_collection_wapper(date_start, date_end, geometry, cloud_threshold, use_ndvi):
    def get_collection(satellite):
        return get_specific_collection(satellite, date_start, date_end, geometry, cloud_threshold, use_ndvi)
    return get_collection

def create_add_band_mapper():
    def mapper(image):
        return add_band(True, image)
    return mapper

def create_feature(geometry, site, satellite, image):
    date = ee.Date(image.get('system:time_start'))
    scale = 30 # 30m resolution 
    props = {
        'year': date.get('year'),
        'month': date.get('month'),
        'day': date.get('day'),
        'lst': image.select('LST').reduceRegion(ee.Reducer.mean(), geometry, scale).get('LST'),
        'tpw': image.select('TPW').reduceRegion(ee.Reducer.mean(), geometry, scale).get('TPW'),
        'em': image.select('EM').reduceRegion(ee.Reducer.mean(), geometry, scale).get('EM'),
        'satellite': satellite,
        'bbe': image.select('BBE').reduceRegion(ee.Reducer.mean(), geometry, scale).get('BBE')
    }
    return ee.Feature(site, props)

def get_collection(date_start, date_end, site, cloud_threshold, use_ndvi):
    geometry = site.buffer(30)
    add_bbe = create_add_band_mapper()

    get_collection = get_collection_wapper(date_start, date_end, geometry, cloud_threshold, use_ndvi)
    L8coll = get_collection('L8').map(add_bbe)
    L7coll = get_collection('L7').map(add_bbe)
    L5coll = get_collection('L5').map(add_bbe)
    L4coll = get_collection('L4').map(add_bbe)

    landsat_coll = ee.ImageCollection([])
    basic_wrapper = partial(create_feature, geometry, site)
    for sat, coll in [('L8', L8coll), ('L7', L7coll), ('L5', L5coll), ('L4', L4coll)]:
        feature_wrapper = partial(basic_wrapper, sat)
        landsat_coll = landsat_coll.merge(coll.map(feature_wrapper))
    return landsat_coll.filter(ee.Filter.notNull(['lst']))

def export_to_drive(landsat_coll, point_name):
    task = ee.batch.Export.table.toDrive(
        collection=ee.FeatureCollection(landsat_coll),
        description='export_landsat_lst_timeseries_for_'+point_name,
        folder='landsat_lst_timeseries',
        fileNamePrefix=point_name,
        fileFormat='CSV'
    )
    task.start()
    print("Export task started.") 

def create_series(lat,lon):
    # use current time to generate a random number
    random_number = random.randint(10000, 99999)
    point_name = str(lat).replace('.', '') + '_' + str(lon).replace('.', '') + '_' + str(random_number)
    site = ee.Geometry.Point([lat, lon])
    date_start = '1982-08-01'
    date_end = '2024-01-31'
    cloud_threshold = 20
    use_ndvi = True
    landsat_coll = get_collection(date_start, date_end, site, cloud_threshold, use_ndvi)
    export_to_drive(landsat_coll, point_name)

def __main__():
    lat = 114.35
    lon = 30.35
    create_series(lat, lon)

if __name__ == '__main__':
    __main__()