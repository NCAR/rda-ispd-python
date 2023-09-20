"""
Variable definitions for ISPD tables.
"""
ISPDMETA = {
   'uid': {
      'index': 0,
      'type': str,
      'field_index': 0,
      'size': 19,
      'missing': "9"*19,
      'position': None,
      'description': "Unique record ID"
   },
   'timestamp': {
      'index': 1,
      'type': str,
      'field_index': 0,
      'size': 12,
      'missing': "9"*12,
      'position': 0,
      'description': "Timestamp"
   },
   'unoc': {
      'index': 2,
      'type': str,
      'field_index': 0,
      'size': 7,
      'missing': "9"*7,
      'position': 12,
      'description': "Unique observation number code"
   },
   'id': {
      'index': 3,
      'type': str,
      'field_index': 31,
      'size': 13,
      'missing': "9"*13,
      'position': None,
      'description': "Observation/Station ID"
   },
   'year': {
      'index': 4,
      'type': int,
      'field_index': 0,
      'size': 4,
      'missing': 9999,
      'position': 0,
      'description': "year"
   },
   'month': {
      'index': 5,
      'type': int,
      'field_index': 0,
      'size': 2,
      'missing': -9,
      'position': 4,
      'description': "month"
   },
   'day': {
      'index': 6,
      'type': int,
      'field_index': 0,
      'size': 2,
      'missing': -9,
      'position': 6,
      'description': "day"
   },
   'hour': {
      'index': 7,
      'type': int,
      'field_index': 0,
      'size': 2,
      'missing': -9,
      'position': 8,
      'description': "hour"
   },
   'minute': {
      'index': 8,
      'type': int,
      'field_index': 0,
      'size': 2,
      'missing': -9,
      'position': 10,
      'description': "minute"
   },
   'second': {
      'index': 9,
      'type': int,
      'field_index': 0,
      'size': 0,
      'missing': 0,
      'position': 0,
      'description': "second"
   },
   'lat': {
      'index': 10,
      'type': float,
      'field_index': 5,
      'size': 6,
      'missing': -99.99,
      'position': None,
      'description': "Latitude"
   },
   'lon': {
      'index': 11,
      'type': float,
      'field_index': 4,
      'size': 7,
      'missing': 999.99,
      'position': None,
      'description': "Longitude"
   },
   'elev': {
      'index': 12,
      'type': int,
      'field_index': 6,
      'size': 6,
      'missing': 9999,
      'position': None,
      'description': "Elevation"
   },
   'ant_offset': {
      'index': 13,
      'type': float,
      'field_index': 7,
      'size': 7,
      'missing': 999.99,
      'position': None,
      'description': "Time offset"
   }
}
ISPDOBS = {
   'uid': {
      'index': 0,
      'type': str,
      'field_index': 0,
      'size': 19,
      'missing': "9"*19,
      'position': None,
      'description': "Unique record ID"
   },
   'timestamp': {
      'index': 1,
      'type': str,
      'field_index': 0,
      'size': 12,
      'missing': "9"*12,
      'position': 0,
      'description': "Timestamp"
   },
   'unoc': {
      'index': 2,
      'type': str,
      'field_index': 0,
      'size': 7,
      'missing': "9"*7,
      'position': 12,
      'description': "Unique observation number code"
   },
   'slp': {
      'index': 3,
      'type': float,
      'field_index': 9,
      'size': 8,
      'missing': 9999.99,
      'position': None,
      'description': "Observed sea level pressure"
   },
   'slpe': {
      'index': 4,
      'type': float,
      'field_index': 10,
      'size': 6,
      'missing': -9.99,
      'position': None,
      'description': "Sea level pressure error"
   },
   'slpqc': {
      'index': 5,
      'type': int,
      'field_index': 11,
      'size': 1,
      'missing': 9,
      'position': None,
      'description': "Sea level pressure flag"
   },
   'sfp': {
      'index': 6,
      'type': float,
      'field_index': 12,
      'size': 8,
      'missing': 9999.99,
      'position': None,
      'description': "Surface level pressure"
   },
   'sfpe': {
      'index': 7,
      'type': float,
      'field_index': 13,
      'size': 6,
      'missing': -9.99,
      'position': None,
      'description': "Surface level pressure error"
   },
   'sfpqc': {
      'index': 8,
      'type': int,
      'field_index': 14,
      'size': 1,
      'missing': 9,
      'position': None,
      'description': "Surface level pressure flag"
   },
   'obp': {
      'index': 9,
      'type': float,
      'field_index': 8,
      'size': 8,
      'missing': 9999.99,
      'position': None,
      'description': "Observed pressure"
   },
   'id_type': {
      'index': 10,
      'type': int,
      'field_index': 2,
      'size': 2,
      'missing': -9,
      'position': None,
      'description': "Observation ID type"
   },
   'ncep_type': {
      'index': 11,
      'type': int,
      'field_index': 1,
      'size': 3,
      'missing': -99,
      'position': None,
      'description': "NCEP observation type code"
   },
   'ispdbcid': {
      'index': 12,
      'type': str,
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
      'type': str,
      'field_index': 0,
      'size': 19,
      'missing': "9"*19,
      'position': None,
      'description': "Unique record ID"
   },
   'timestamp': {
      'index': 1,
      'type': str,
      'field_index': 0,
      'size': 12,
      'missing': "9"*12,
      'position': 0,
      'description': "Timestamp"
   },
   'unoc': {
      'index': 2,
      'type': str,
      'field_index': 0,
      'size': 7,
      'missing': "9"*7,
      'position': 12,
      'description': "Unique observation number code"
   },
   'sname': {
      'index': 3,
      'type': str,
      'field_index': 30,
      'size': 30,
      'missing': "-"+"9"*29,
      'position': None,
      'description': "Station or ship name"
   },
   'slib': {
      'index': 4,
      'type': str,
      'field_index': 32,
      'size': 3,
      'missing': "999",
      'position': None,
      'description': "Name of station library used for station position, if different from source"
   },
   'sflsd': {
      'index': 5,
      'type': str,
      'field_index': 33,
      'size': 1,
      'missing': "9",
      'position': None,
      'description': "Source flag for land station data"
   },
   'rtc': {
      'index': 6,
      'type': str,
      'field_index': 34,
      'size': 5,
      'missing': "9"*5,
      'position': None,
      'description': "Report type code"
   },
   'qcislp': {
      'index': 7,
      'type': str,
      'field_index': 35,
      'size': 5,
      'missing': "9"*5,
      'position': None,
      'description': "Quality control indicators for sea level pressure value from source"
   },
   'qcisfp': {
      'index': 8,
      'type': str,
      'field_index': 36,
      'size': 5,
      'missing': "9"*5,
      'position': None,
      'description': "Quality control indicators for surface pressure value from source"
   },
   'icoads_sid': {
      'index': 9,
      'type': int,
      'field_index': 37,
      'size': 3,
      'missing': -99,
      'position': None,
      'description': "ICOADS source ID"
   },
   'icoads_dck': {
      'index': 10,
      'type': int,
      'field_index': 38,
      'size': 3,
      'missing': -99,
      'position': None,
      'description': "ICOADS deck ID"
   },
   'icoads_pt': {
      'index': 11,
      'type': int,
      'field_index': 39,
      'size': 2,
      'missing': -9,
      'position': None,
      'description': "ICOADS platform type"
   },
   'icoads_uid': {
       'index': 12,
       'type': str,
       'field_index': 40,
       'size': 6,
       'missing': " "*6,
       'position': None,
       'description': "ICOADS record unique ID"
   }
}
ISPDFEEDBACK = {
   'uid': {
      'index': 0,
      'type': str,
      'field_index': 0,
      'size': 19,
      'missing': "9"*19,
      'position': None,
      'description': "Unique record ID"
   },
   'timestamp': {
      'index': 1,
      'type': str,
      'field_index': 0,
      'size': 12,
      'missing': "9"*12,
      'position': 0,
      'description': "Timestamp"
   },
   'unoc': {
      'index': 2,
      'type': str,
      'field_index': 0,
      'size': 7,
      'missing': "9"*7,
      'position': 12,
      'description': "Unique observation number code"
   },
   'sfsfp': {
       'index': 3,
       'type': float,
       'field_index': 15,
       'size': 7,
       'missing': 9999.99,
       'position': None,
       'description': "Observed station pressure or sea level pressure"
   },
   'pobtime_offset': {
       'index': 4, 
       'type': float, 
       'field_index': 16,  
       'size': 6,
       'missing': 9.9,
       'position': None,
       'description': "Time offset of observation time from analysis time"       
   },
   'poberrvar_orig_out': {
       'index': 5, 
       'type': float,
       'field_index': 17,
       'size': 6,
       'missing': 99.9,
       'position': None,
       'description': "Original Error Variance"       
   },
   'poberrvar_out': {
       'index': 6,
       'type': float,
       'field_index': 18,
       'size': 7,
       'missing': 99.9,
       'position': None,
       'description': "Observation error variance if modified by analysis"
   },
   'poberrvaruse': {
       'index': 7,
       'type': float,
       'field_index': 19,
       'size': 7,
       'missing': 99.9,
       'position': None,
       'description': "Observation error variance used in analysis"
   },
   'paoverpb_save': {
       'index': 8,
       'type': float,
       'field_index': 20,
       'size': 9,
       'missing': 99.9,
       'position': None,
       'description': "H(pa)H^T/H(Pb)H^T analysis ensemble covariance at space-time location divided by forecast ensemble covariance"
   },
   'local_lscale': {
       'index': 9,
       'type': float,
       'field_index': 21,
       'size': 8,
       'missing': 9999.9,
       'position': None,
       'description': "Local length scale"
   },
   'ai': {
       'index': 10,
       'type': int,
       'field_index': 22,
       'size': 1,
       'missing': 9,
       'position': None,
       'description': "Assimilation indicator 1=skipped"
   },
   'bias': {
       'index': 11,
       'type': float,
       'field_index': 23,
       'size': 10,
       'missing': 9.9999,
       'position': None,
       'description': "Bias ov-(ob-fg)-ensmean_obnobc"
   },
   'pge': {
       'index': 12,
       'type': float,
       'field_index': 24,
       'size': 5,
       'missing': 9.9999,
       'position': None,
       'description': "Probability of gross error"
   },
   'qc': {
       'index': 13,
       'type': int,
       'field_index': 25,
       'size': 1,
       'missing': 9,
       'position': None,
       'description': "Quality Control failure Indicator 1=failed"
   },
   'pobsprd_prior': {
       'index': 14,
       'type': float,
       'field_index': 26,
       'size': 8,
       'missing': 99.9,
       'position': None,
       'description': "Variance of Hx^f Variance of Ensemble Guess Pressure interpolated to observation time and location"
   },
   'pobfit_prior': {
       'index': 15,
       'type': float,
       'field_index': 27,
       'size': 8,
       'missing': 9999.9,
       'position': None,
       'description': "(Observation-Bias) minus First Guess Pressure"
   },
   'pobsprd_post': {
       'index': 16,
       'type': float,
       'field_index': 28,
       'size': 8,
       'missing': 99.9,
       'position': None,
       'description': "Variance of Hx^a Variance of Ensemble Analysis interpolated to observation time and location"
   },
   'pobfit_post': {
       'index': 17,
       'type': float,
       'field_index': 29,
       'size': 8,
       'missing': 9999.99,
       'position': None,
       'description': "Observation-Bias) minus Ensemble Mean Analysis Pressure"
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
   'ispdv4feedback': {
      'tindex': 3, 
      'attm': ISPDFEEDBACK,
      'tname': 'Feedback'
   }
}

TABLECOUNT = len(ISPDS)
ISPD_NAMES = list(ISPDS.keys())
FIELD_COUNT = 41

DBCNTL = "ispddb"

# field delimiter in ASCII input records
ISPD_DELIM = '<:>'

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