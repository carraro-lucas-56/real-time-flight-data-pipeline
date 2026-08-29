
REQUIRED_FIELDS=[
    "icao24", 
    "last_contact",
    "on_ground",
]

NON_NEGATIVE_FIELDS=[
    "velocity",
    "baro_altitude"
]

COORDINATES_FIELDS=["longitude", "latitude"]

DESIRED_FIELDS_METRIC_1 = ["icao24", "last_contact", "latitude", "longitude"]

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

