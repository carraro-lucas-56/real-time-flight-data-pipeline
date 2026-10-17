REQUIRED_FIELDS=[
    "icao24", 
    "last_contact",
    "on_ground",
]

NON_NEGATIVE_FIELDS=[
    "velocity",
    "baro_altitude"
]

COORDINATE_FIELDS=["longitude", "latitude"]

FIELDS_METRIC_1 = ["icao24", "on_ground", "last_contact", "latitude", "longitude"]

FIELDS_METRIC_2 = ["icao24", "last_contact", "velocity", "on_ground"]

VERTICAL_ACCELERATION_THRESHOLD = (-1.0)

countries_dict = {
        'AR': 'Argentina',
        'BO': 'Bolivia',
        'BR': 'Brazil',
        'CL': 'Chile',
        'CO': 'Colombia',
        'EC': 'Ecuador',
        'FK': 'Falkland_Islands',
        'GF': 'French_Guiana',
        'GY': 'Guyana',
        'PY': 'Paraguay',
        'PE': 'Peru',
        'SR': 'Suriname',
        'UY': 'Uruguay',
        'VE': 'Venezuela'
    }

countries_list = list(countries_dict.values())

