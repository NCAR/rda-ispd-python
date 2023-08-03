"""
Variable definitions for ISPD tables.

Precision: 
   0 - string
   1 - integer
   Otherwise - float
"""
ISPDMETA = {
   'uid': {
      'index': 0,
      'precision': 0,
      'field_index': 0,
      'size': 19,
      'missing': "9"*19,
      'position': None,
      'description': "Unique record ID"
   },
   'timestamp': {
      'index': 1,
      'precision': 0,
      'field_index': 0,
      'size': 12,
      'missing': "9"*12,
      'position': 0,
      'description': "Timestamp"
   },
   'unoc': {
      'index': 2,
      'precision': 0,
      'field_index': 0,
      'size': 7,
      'missing': "9"*7,
      'position': 12,
      'description': "Unique observation number code"
   },
   'id': {
      'index': 3,
      'precision': 0,
      'field_index': 32,
      'size': 13,
      'missing': "9"*13,
      'position': None,
      'description': "Observation/Station ID"
   },
   'year': {
      'index': 4,
      'precision': 1,
      'field_index': 0,
      'size': 4,
      'missing': 9999,
      'position': 0,
      'description': "year"
   },
   'month': {
      'index': 5,
      'precision': 1,
      'field_index': 0,
      'size': 2,
      'missing': -9,
      'position': 4,
      'description': "month"
   },
   'day': {
      'index': 6,
      'precision': 1,
      'field_index': 0,
      'size': 2,
      'missing': -9,
      'position': 6,
      'description': "day"
   },
   'hour': {
      'index': 7,
      'precision': 1,
      'field_index': 0,
      'size': 2,
      'missing': -9,
      'position': 8,
      'description': "hour"
   },
   'minute': {
      'index': 8,
      'precision': 1,
      'field_index': 0,
      'size': 2,
      'missing': -9,
      'position': 10,
      'description': "minute"
   },
   'second': {
      'index': 9,
      'precision': 1,
      'field_index': 0,
      'size': 0,
      'missing': 0,
      'position': 0,
      'description': "second"
   },
   'lat': {
      'index': 10,
      'precision': 0.01,
      'field_index': 5,
      'size': 6,
      'missing': -99.99,
      'position': None,
      'description': "Latitude"
   },
   'lon': {
      'index': 11,
      'precision': 0.01,
      'field_index': 4,
      'size': 7,
      'missing': 999.99,
      'position': None,
      'description': "Longitude"
   },
   'elev': {
      'index': 12,
      'precision': 1,
      'field_index': 6,
      'size': 6,
      'missing': 9999,
      'position': None,
      'description': "Elevation"
   },
   'ant_offset': {
      'index': 13,
      'precision': 0.01,
      'field_index': 7,
      'size': 6,
      'missing': 999.99,
      'position': None,
      'description': "Time offset"
   }
}
ISPDOBS = {
   'uid': {
      'index': 0,
      'precision': 0,
      'field_index': 0,
      'size': 19,
      'missing': "9"*19,
      'position': None,
      'description': "Unique record ID"
   },
   'timestamp': {
      'index': 1,
      'precision': 0,
      'field_index': 0,
      'size': 12,
      'missing': "9"*12,
      'position': 0,
      'description': "Timestamp"
   },
   'unoc': {
      'index': 2,
      'precision': 0,
      'field_index': 0,
      'size': 7,
      'missing': "9"*7,
      'position': 12,
      'description': "Unique observation number code"
   },
   'slp': {
      'index': 3,
      'precision': 0.01,
      'field_index': 9,
      'size': 8,
      'missing': 9999.99,
      'position': None,
      'description': "Observed sea level pressure"
   },
   'slpe': {
      'index': 4,
      'precision': 0.01,
      'field_index': 10,
      'size': 6,
      'missing': -9.99,
      'position': None,
      'description': "Sea level pressure error"
   },
   'slpqc': {
      'index': 5,
      'precision': 1,
      'field_index': 11,
      'size': 1,
      'missing': 9,
      'position': None,
      'description': "Sea level pressure flag"
   },
   'sfp': {
      'index': 6,
      'precision': 0.01,
      'field_index': 12,
      'size': 8,
      'missing': 9999.99,
      'position': None,
      'description': "Surface level pressure"
   },
   'sfpe': {
      'index': 7,
      'precision': 0.01,
      'field_index': 13,
      'size': 6,
      'missing': -9.99,
      'position': None,
      'description': "Surface level pressure error"
   },
   'sfpqc': {
      'index': 8,
      'precision': 1,
      'field_index': 14,
      'size': 1,
      'missing': 9,
      'position': None,
      'description': "Surface level pressure flag"
   },
   'obp': {
      'index': 9,
      'precision': 0.01,
      'field_index': 8,
      'size': 8,
      'missing': 9999.99,
      'position': None,
      'description': "Observed pressure"
   },
   'id_type': {
      'index': 10,
      'precision': 1,
      'field_index': 2,
      'size': 2,
      'missing': -9,
      'position': None,
      'description': "Observation ID type"
   },
   'ncep_type': {
      'index': 11,
      'precision': 1,
      'field_index': 1,
      'size': 3,
      'missing': -99,
      'position': None,
      'description': "NCEP observation type code"
   },
   'ispdbcid': {
      'index': 12,
      'precision': 0,
      'field_index': 3,
      'size': 6,
      'missing': "-99999",
      'position': None,
      'description': "ISPDB collection ID"
   }
}
ISPDTRACK = {
   'uid': {
      'index': 0,
      'precision': 0,
      'field_index': 0,
      'size': 19,
      'missing': "9"*19,
      'position': None,
      'description': "Unique record ID"
   },
   'timestamp': {
      'index': 1,
      'precision': 0,
      'field_index': 0,
      'size': 12,
      'missing': "9"*12,
      'position': 0,
      'description': "Timestamp"
   },
   'unoc': {
      'index': 2,
      'precision': 0,
      'field_index': 0,
      'size': 7,
      'missing': "9"*7,
      'position': 12,
      'description': "Unique observation number code"
   },
   'sname': {
      'index': 3,
      'precision': 0,
      'field_index': 31,
      'size': 30,
      'missing': "-"+"9"*29,
      'position': None,
      'description': "Station or ship name"
   },
   'slib': {
      'index': 4,
      'precision': 0,
      'field_index': 33,
      'size': 3,
      'missing': "999",
      'position': None,
      'description': "Name of station library used for station position, if different from source"
   },
   'icoads_sid': {
      'index': 5,
      'precision': 1,
      'field_index': 34,
      'size': 3,
      'missing': -99,
      'position': None,
      'description': "ICOADS source ID"
   },
   'icoads_dck': {
      'index': 6,
      'precision': 1,
      'field_index': 35,
      'size': 3,
      'missing': -99,
      'position': None,
      'description': "ICOADS deck ID"
   },
   'icoads_pt': {
      'index': 7,
      'precision': 1,
      'field_index': 36,
      'size': 2,
      'missing': -9,
      'position': None,
      'description': "ICOADS platform type"
   },
   'sflsd': {
      'index': 8,
      'precision': 0,
      'field_index': 37,
      'size': 1,
      'missing': "9",
      'position': None,
      'description': "Source flag for land station data"
   },
   'rtc': {
      'index': 9,
      'precision': 0,
      'field_index': 38,
      'size': 5,
      'missing': "99999",
      'position': None,
      'description': "Report type code"
   },
   'qcislp': {
      'index': 10,
      'precision': 0,
      'field_index': 39,
      'size': 5,
      'missing': "99999",
      'position': None,
      'description': "Quality control indicators for sea level pressure value from source"
   },
   'qcisfp': {
      'index': 11,
      'precision': 0,
      'field_index': 40,
      'size': 5,
      'missing': "99999",
      'position': None,
      'description': "Quality control indicators for surface pressure value from source"
   }
}
ISPDFEEDBACK = {
   'uid': {
      'index': 0,
      'precision': 0,
      'field_index': 0,
      'size': 19,
      'missing': "9"*19,
      'position': None,
      'description': "Unique record ID"
   },
   'timestamp': {
      'index': 1,
      'precision': 0,
      'field_index': 0,
      'size': 12,
      'missing': "9"*12,
      'position': 0,
      'description': "Timestamp"
   },
   'unoc': {
      'index': 2,
      'precision': 0,
      'field_index': 0,
      'size': 7,
      'missing': "9"*7,
      'position': 12,
      'description': "Unique observation number code"
   },
   'mdpavims': {
      'index': 3,
      'precision': 0.01,
      'field_index': 15,
      'size': 7,
      'missing': 9999.99,
      'position': None,
      'description': "Modified observed pressure after vertically interpolating to model surface"
   },
   'epvims': {
      'index': 4,
      'precision': 0.01,
      'field_index': 16,
      'size': 5,
      'missing': -9.99,
      'position': None,
      'description': "Error in observed pressure vertically interpolated to model surface"
   },
   'bias': {
      'index': 5,
      'precision': 0.01,
      'field_index': 17,
      'size': 7,
      'missing': 0.00,
      'position': None,
      'description': "Difference between observation and analysis averaged over past sixty days"
   },
   'sfsfp': {
      'index': 6,
      'precision': 1,
      'field_index': 18,
      'size': 1,
      'missing': 9,
      'position': None,
      'description': "Status flag for observed surface pressure"
   },
   'ai': {
      'index': 7,
      'precision': 1,
      'field_index': 19,
      'size': 1,
      'missing': 9,
      'position': None,
      'description': "Assimilation indicator"
   },
   'uc': {
      'index': 8,
      'precision': 1,
      'field_index': 20,
      'size': 1,
      'missing': 9,
      'position': None,
      'description': "Usability check for reanalysis"
   },
   'bcf': {
      'index': 9,
      'precision': 1,
      'field_index': 21,
      'size': 1,
      'missing': 9,
      'position': None,
      'description': "QC background check flag indicator"
   },
   'bf': {
      'index': 10,
      'precision': 1,
      'field_index': 22,
      'size': 1,
      'missing': 9,
      'position': None,
      'description': "Buddy flag indicator"
   },
   'qc': {
      'index': 11,
      'precision': 1,
      'field_index': 23,
      'size': 1,
      'missing': 9,
      'position': None,
      'description': "QC control indicator"
   },
   'emfg': {
      'index': 12,
      'precision': 0.01,
      'field_index': 24,
      'size': 7,
      'missing': 9999.99,
      'position': None,
      'description': "Ensemble mean first guess pressure"
   },
   'sdeg': {
      'index': 13,
      'precision': 0.01,
      'field_index': 25,
      'size': 5,
      'missing': -9.99,
      'position': None,
      'description': "Standard deviation of ensemble first guess pressure"
   },
   'mpmemfg': {
      'index': 14,
      'precision': 0.01,
      'field_index': 26,
      'size': 6,
      'missing': 999.99,
      'position': None,
      'description': "Ensemble mean first guess pressure minus modified observation pressure"
   },
   'emap': {
      'index': 15,
      'precision': 0.01,
      'field_index': 27,
      'size': 7,
      'missing': 9999.99,
      'position': None,
      'description': "Ensemble mean analysis pressure"
   },
   'sdeap': {
      'index': 16,
      'precision': 0.01,
      'field_index': 28,
      'size': 5,
      'missing': -9.99,
      'position': None,
      'description': "Standard deviation of ensemble analysis pressure"
   },
   'mpmema': {
      'index': 17,
      'precision': 0.01,
      'field_index': 29,
      'size': 6,
      'missing': 999.99,
      'position': None,
      'description': "Ensemble mean analysis pressure minus modified observation pressure"
   },
   'melv': {
      'index': 18,
      'precision': 1,
      'field_index': 30,
      'size': 4,
      'missing': 9999,
      'position': None,
      'description': "Modified elevation"
   }
}

# define ISPD sections

ISPDS = {
   'ispdmeta': {
      'tindex': 0,
      'attm': ISPDMETA,
      'tname': 'Meta'
   },
   'ispdobs': {
      'tindex': 1,
      'attm': ISPDOBS,
      'tname': 'Obs'
   },
   'ispdtrack': {
      'tindex': 2,
      'attm': ISPDTRACK,
      'tname': 'Track'
   },
   'ispdfeedback': {
      'tindex': 3, 
      'attm': ISPDFEEDBACK,
      'tname': 'Feedback'
   }
}

TABLECOUNT = len(ISPDS)
ISPD_NAMES = list(ISPDS.keys())

DBCNTL = "ispddb"

MULTI_NAMES = []
ATTI2NAME = {}
ATTCPOS = INVENTORY = CURTIDX = CURIIDX = 0
CURIUID = ''
ISPD_COUNTS = []
UIDIDX = 0
AUTHREFS = {}
NUM2NAME = {}             # cache field names from component/field numbers
NAME2NUM = {}             # cache component/field numbers from field names
UMATCH = {}               # unique matches, use iidx as key
TINFO = {}
FIELDINFO = {}            # cache the field metadata info for quick find
ATTM_VARS = {}
MISSING = -999999
ERROR = -999999
LEADUID = 0
CHKEXIST = 0
UIDLENGTH = 0  # uid record len
UIDOFFSET = 0  # uid value offset
ATTMNAME = None      # standalone attm section name to fill