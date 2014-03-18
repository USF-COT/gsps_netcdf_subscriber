from datetime import datetime
from netCDF4 import default_fillvals as NC_FILL_VALUES


def generate_set_key(message):
    return '%s-%s' % (message['glider'], message['start'])


def generate_global_id(configs, dataset):
    glider_name = dataset.glider
    glider_id = configs[glider_name]['deployment']['platform']['id']
    start_time = datetime.fromtimestamp(dataset.times[0])

    global_id = '%s_%04d%02d%02dT%02d%02d%02d' % (
        glider_id,
        start_time.year,
        start_time.month,
        start_time.day,
        start_time.hour,
        start_time.minute,
        start_time.second
    )

    return global_id


def generate_filename(configs, dataset):
    glider_name = dataset.glider.replace('-', '')
    global_id = generate_global_id(configs, dataset)

    filename = '%s-%s_rt0.nc' % (glider_name, global_id)
    return filename


def max_excluding_nc_fill(data, default_min):
    maximum = default_min

    for datum in data:
        if datum != NC_FILL_VALUES['f8'] and datum > maximum:
            maximum = datum

    return maximum


def set_bounds(bounds, datatypes,
               glider_type, bound_type, resolution, units):
    if glider_type in datatypes:
        bounds[bound_type+'_min'] = min(datatypes[glider_type])
        bounds[bound_type+'_max'] = (
            max_excluding_nc_fill(
                datatypes[glider_type],
                bounds[bound_type+'_min']
            )
        )
        bounds[bound_type+'_resolution'] = resolution
        bounds[bound_type+'units'] = units

    return bounds


def generate_geospatial_bounds(dataset):
    bounds = {}
    datatypes = dataset.data_by_type

    bounds = set_bounds(bounds, datatypes,
                        'm_lat-lat', 'geospatial_lat',
                        'point', 'degrees_north')

    bounds = set_bounds(bounds, datatypes,
                        'm_lon-lon', 'geospatial_lon',
                        'point', 'degrees_east')

    bounds = set_bounds(bounds, datatypes,
                        'm_depth-m', 'geospatial_vertical',
                        'point', 'meters')

    bounds['geospatial_vertical_positive'] = 'down'

    return bounds


def generate_time_bounds(dataset):
    bounds = {}
    now_time = datetime.utcnow()

    bounds['history'] = (
        "Created on %s" % now_time.strftime('%a %b %d %H:%M:%S %Y')
    )

    start_time = datetime.fromtimestamp(min(dataset.times))
    end_time = datetime.fromtimestamp(max(dataset.times))
    bounds['time_coverage_start'] = start_time.isoformat()
    bounds['time_coverage_end'] = end_time.isoformat()
    bounds['time_coverage_resolution'] = 'point'

    return bounds


def generate_global_attributes(configs, dataset):
    glider_name = dataset.glider
    global_attributes = configs['global_attributes']
    deployment_global = (
        configs[glider_name]['deployment']['global_attributes']
    )
    global_attributes.update(deployment_global)

    geospatial_global = generate_geospatial_bounds(dataset)
    global_attributes.update(geospatial_global)

    time_global = generate_time_bounds(dataset)
    global_attributes.update(time_global)

    global_id = generate_global_id(configs, dataset)
    global_attributes['id'] = global_id

    return global_attributes
