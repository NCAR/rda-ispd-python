#!/usr/bin/env python3

"""
Script to read observation records from the original ISPD HDF5 files and
insert the records into the ISPD database (ISPDDB) at the NCAR RDA.
"""

import logging
import logging.handlers
import sys

from rda_ispd_python.ispddb import FillISPD

#=========================================================================================
def main(args):

   add_inventory = args.addinventory
   lead_uid = args.leaduid
   check_existing = args.checkexisting

   fill_ispd = FillISPD(add_inventory=add_inventory, lead_uid=lead_uid, check_existing=check_existing)
   fill_ispd.initialize_db()
   fill_ispd.get_input_files(args.files)
   fill_ispd.initialize_indices()
   fill_ispd.fill_ispd_data()
   fill_ispd.close_db()

#=========================================================================================
def configure_log(**kwargs):
   """ Congigure logging """
   LOGPATH = '/glade/scratch/tcram/logs/ispd/'
   LOGFILE = '{}.log'.format(__file__)

   if 'level' in kwargs:
      loglevel = kwargs['level']
   else:
      loglevel = 'info'

   LEVELS = { 'debug':logging.DEBUG,
              'info':logging.INFO,
              'warning':logging.WARNING,
              'error':logging.ERROR,
              'critical':logging.CRITICAL,
            }

   level = LEVELS.get(loglevel, logging.INFO)
   logger.setLevel(level)

   formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

   """ Rotating file handler """
   rfh = logging.handlers.RotatingFileHandler(LOGPATH+'/'+LOGFILE,maxBytes=200000000,backupCount=1)
   rfh.setLevel(level)
   rfh.setFormatter(formatter)
   logger.addHandler(rfh)

   return

#=========================================================================================
def parse_opts():
   """ Parse command line arguments """
   import argparse
   import textwrap
	
   desc = "Read ISPD records from original HDF5 data files and store information in ISPDDB."	
   epilog = textwrap.dedent('''\
   Example:
      - Read the ISPD file 1950123100.h5 and store the information in ISPDDB:
         fill_ispddb.py -i -e 1950123100.h5
   ''')

   parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description=desc, epilog=textwrap.dedent(epilog))
   parser.add_argument('files', nargs="+", help="Input ISPD file names.  A minimum of one file name is required.")
   parser.add_argument('-i', '--addinventory', action="store_true", default="False", help='Add daily counting records into inventory table.')
   parser.add_argument('-u', '--leaduid', action="store_true", default="False", help='Standalone attachment records with leading 6-character UID.')
   parser.add_argument('-e', '--checkexisting', action="store_true", default="False", help='Check for existing record before adding record to DB.')

   if len(sys.argv)==1:
      parser.print_help()
      sys.exit(1)

   args = parser.parse_args(sys.argv[1:])
   logger.info("{0}: {1}".format(sys.argv[0], args))

   return args

#=========================================================================================

logger = logging.getLogger(__name__)
configure_log(level='info')

if __name__ == "__main__":
   args = parse_opts()
   main(args)