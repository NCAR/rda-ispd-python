"""
Module supporting tools and utilities to interact with 
the ISPD database (ISPDDB)
"""

import os
from PgLOG import pgexit
from PgDBI import ispddb_dbname
from .ispd_common import *
import logging

logger = logging.getLogger(__name__)

class FillISPD:
   def __init__(self, add_inventory=None, lead_uid=None, check_existing=None):
      self.add_inventory = add_inventory
      self.lead_uid = lead_uid
      self.check_existing = check_existing
      self.pvals = {'names': None, 'files': [], 'uatti': ''}
      self.pvals['names'] = '/'.join(ISPD_NAMES)

   def initialize_db(self):
      ispddb_dbname()

   def close_db(self):
      pgexit()

   def initialize_indices(self):
      init_current_indices(self.lead_uid, self.check_existing)

   def get_input_files(self, files):
      if files is None:
         logger.error("At least one input file is required.")
      
      self.pvals['files'] = files
   
   def fill_ispd_data(self):
      """ Insert ISPD data into ISPDDB """

      fcnt = 0
      tcounts = [0]*TABLECOUNT
      for file in self.pvals['files']:
         fcnt += 1
         logger.debug("Processing input file {}".format(file))
         acnts = self.process_ispd_file(file)
         for i in range(TABLECOUNT): tcounts[i] += acnts[i]

      if fcnt > 1: 
         logger.info("{} ({}) filled for {} files".format('/'.join(map(str, tcounts)), self.pvals['names'], fcnt))

      return

   def process_ispd_file(self, fname):
      """ Read ISPD record from given file name and save into ISPDDB """

      iname = fname if self.add_inventory else None

      logger.info("Recording ISPD records from file '{}' into ISPDDB".format(fname))

      ISPD = open(fname, 'r', encoding = 'latin_1')
      acounts = [0]*TABLECOUNT
      records = {}

      # get the first valid date and do initialization
      line = ISPD.readline()
      while line:
         idate = cdate = get_ispd_date(line)
         if cdate:
            init_indices_for_date(cdate, iname)
            records = get_ispd_records(line, cdate, records)
            break
         line = ISPD.readline()

      line = ISPD.readline()
      while line:
         idate = get_ispd_date(line)
         if idate:
            if idate != cdate:
               acnts = add_ispd_records(cdate, records)
               for i in range(TABLECOUNT): acounts[i] += acnts[i]
               records = {}
               cdate = idate
               init_indices_for_date(cdate, iname)
            records = get_ispd_records(line, idate, records)
         line = ISPD.readline()

      ISPD.close()

      acnts = add_ispd_records(cdate, records)
      for i in range(TABLECOUNT): acounts[i] += acnts[i]

      logger.info("{} ({}) filled from {}".format(' '.join(map(str, acounts)), PVALS['names'], os.path.basename(fname)))
   
      return acounts

