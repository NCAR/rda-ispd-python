"""
Common library with ISPD dataset and database utilities
"""

import re
import PgLOG
import PgUtil
import PgDBI

from .ispddb_config import *

import logging
logger = logging.getLogger(__name__)

#
#  initialize the database table information
#
def init_table_info():

   global ISPD_COUNTS, UIDATTI

   ISPD_COUNTS = [0]*TABLECOUNT
   UIDATTI = ISPDS['ispdmeta']['attm']

   return 1

def get_ispd_records(line, cdate, records):
   """ 
   Append the individual fields and return ispd records for one line of input
   """

   global CURIIDX, CURIUID, ISPD_COUNTS
   llen = len(line)
   if llen == 0:
      return records
   
   if not CURIUID:
      CURIIDX += 1
      pgrecs = {}

   for aname in ISPDS:
      ispd = ISPDS[aname]
      pgrec = get_one_attm(ispd['attm'], line)
      if aname not in records:
         records[aname] = initialize_attm_records(ispd['attm'])
      if CURIUID:
         # append attm to records
         for var in ispd['attm']:
            val = pgrec[var]
            records[aname][var].append(val)
            ISPD_COUNTS[ispd['tindex']] += 1  # row index for individual table
      else:
         pgrecs[aname] = pgrec

   if CURIUID:
      return records

   if 'uid' not in pgrecs['ispdmeta']:
      logger.error("Missing 'uid' in the ispdmeta table: {}".format(line))

   records['ispdmeta']['date'].append(cdate)

   for aname in pgrecs:
      ispd = ISPDS[aname]
      # append each attm to records
      for var in ispd['attm']:
         val = pgrecs[aname][var]
         records[aname][var].append(val)
         ISPD_COUNTS[ispd['tindex']] += 1  # row index for individual table
   
   return records

def get_one_attm(attm, line):
   """ Gets all field values for a single table in an observation record """

   # strip newline character from line
   line = line.rstrip('\n')

   # strip trailing delimiter if present and split ASCII record into fields
   fields = line.rstrip(ISPD_DELIM).split(ISPD_DELIM)

   # check number of fields
   if len(fields) != FIELD_COUNT:
      logger.error("Incorrect number of fields in record: {}".format(line))

   pgrec = {}

   for var in attm:
      field = attm[var]
      field_type = field['type']
      field_index = field['field_index']
      size = field['size']
      missing = str(field['missing'])

      if field['position'] is not None:
         position = field['position']
         val = fields[field_index][position:position+size]
      else:
         if len(fields[field_index]) > size:
            val = missing
         else:
            val = fields[field_index]
      
      logger.debug("var: {}, val: {}".format(var, val))
      if re.search("nan", val) and field_type != str:
         val = missing
      val = val.rstrip()

      # convert val from string to correct type
      if len(val) > 0:
         pgrec[var] = field_type(val)
      else:
         pgrec[var] = None

   return pgrec

#
# Initialize dict records for specified attm table
#
def initialize_attm_records(attm):

   pgrecs = {}
   for var in attm:
      pgrecs[var] = []
   if 'year' in attm: 
      pgrecs['date'] = []

   return pgrecs

def add_ispd_records(cdate, records):
   """ Add multiple ispd records into their respective tables in RDADB """

   global INVENTORY, CURTIDX

   if INVENTORY and ISPD_NAMES[0] in records:   # add counting record into inventory table
      ulen = len(records[ISPD_NAMES[0]]['uid'])
      if ulen > 0:
         INVENTORY = add_inventory_record(INVENTORY['fname'], cdate, ulen, INVENTORY)
      if CURTIDX < INVENTORY['tidx']:
         CURTIDX = INVENTORY['tidx']
      tidx = CURTIDX
   else:
      tidx = date2tidx(cdate)
   
   acnts = [0]*TABLECOUNT

   for i in range(TABLECOUNT):
      if not ISPD_COUNTS[i]:
         continue
      aname = ISPD_NAMES[i]
      acnts[i] = add_records_to_table(aname, str(tidx), records[aname], cdate)
      ISPD_COUNTS[i] = 0

   return acnts

def add_inventory_record(fname, cdate, count, inventory, cntopt = 0):
   """ add inventory information into control db """

   didx = 0
   table = "{}.ispd_inventory".format(DBCNTL)

   if cntopt == 2:
      cnd = "date = '{}'".format(cdate)
      pgrec = PgDBI.pgget(table, "didx, count", cnd, PgLOG.LGEREX)
      if not pgrec:
         logger.error("{}: error get record for {}".format(table, cnd))
      count = pgrec['count']
      didx = pgrec['didx']
      record = {}
   else:
      record = {'date' : cdate, 'fname' : fname, 'count' : count}

   if cntopt != 1:
      record['tidx'] = inventory['tidx']
      record['tcount'] = inventory['tcount'] + count
      record['miniidx'] = inventory['maxiidx'] + 1
      record['maxiidx'] = inventory['maxiidx'] + count
      if record['tcount'] > PgDBI.PGDBI['MAXICNT']:
         record['tidx'] += 1
         record['tcount'] = count

   if didx:
      cnd = "didx = {}".format(didx)
      if not PgDBI.pgupdt(table, record, cnd, PgLOG.LGEREX):
         logger.error("{}: error updating table for {}".format(table, cnd))
   else:
      didx = PgDBI.pgadd(table, record, PgLOG.LGEREX|PgLOG.AUTOID)

   record['didx'] = didx
   if cntopt == 2:
      record['count'] = count
      record['date'] = cdate

   return record

def get_ispd_date(line):
   """ Get ISPD date """

   return get_record_date(line[0:4], line[4:6], line[6:8])

#
# get the itidx record from given uid
#
def get_itidx_date(uid):

   global CURIUID, CURIIDX, CURTIDX
   uidx = uid[0:2].lower()
   suid = uid[2:6]
   table = "cntldb.itidx_{}".format(uidx)

   pgrec = PgDBI.pgget(table, "*", "suid = '{}'".format(suid), PgLOG.LGEREX)
   if not pgrec:
      logger.warning("{}: SKIP suid not in {}".format(suid, table))
      return

   if CHKEXIST:    # check
      table = "{}_{}".format(ATTMNAME, pgrec['tidx'])
      cnd = "iidx = {}".format(pgrec['iidx'])
      if ATTMNAME in MUNIQUE:
         for fname in MUNIQUE[ATTMNAME]: 
            cnd += " AND {} = '{}'".format(fname, pgrec[fname])

      if PgDBI.pgget(table, "", cnd): return None

   CURIUID = uid
   CURIIDX = pgrec['iidx']
   CURTIDX = pgrec['tidx']

   return pgrec['date']

#
# get record date for given year, month and day
#
def get_record_date(yr, mo, dy):

   global CURIUID
   mo = mo.strip()
   dy = dy.strip()
   if not mo:
      logger.error("missing month")

   nyr = int(yr)
   nmo = int(mo)
   sym = "{}-{}".format(yr, mo)
   if dy:
      ndy = int(dy)
      if ndy < 1:
         ndy = 1
         logger.info("{}-{}: set dy {} to 1".format(yr, mo, dy))
   else:
      ndy = 1
      logger.info("{}-{}: set missing dy to 1".format(yr, mo))

   CURIUID = ''

   cdate = PgUtil.fmtdate(nyr, nmo, ndy)

   # check for invalid dates (e.g. June 31 or Feb 29 in non-leap year)
   if ndy > 30 or nmo == 2 and ndy > 28:
      edate = PgUtil.enddate(sym+"-01", 0, 'M')
      if cdate > edate:
         cdate = edate
         logger.warning("{}: set {}-{} to {}".format(cdate, sym, dy, edate))

   return cdate

#
# get the tidx from table inventory for given date
#
def date2tidx(cdate):

   table = "{}.ispd_inventory".format(DBCNTL)
   pgrec = PgDBI.pgget(table, "tidx", "date = '{}'".format(cdate), PgLOG.LGEREX)
   if pgrec:
      return pgrec['tidx']

   pgrec = PgDBI.pgget(table, "max(tidx) tidx, max(date) date", "", PgLOG.LGEREX)
   if pgrec and PgUtil.diffdate(cdate, pgrec['date']) > 0:
      return pgrec['tidx']
   else:
      return 1

#
# get the date from table inventory for given iidx
#
def iidx2date(iidx):

   table = "{}.ispd_inventory".format(DBCNTL)
   pgrec = PgDBI.pgget(table, "date", "miniidx <= {} AND maxiidx >= {}".format(iidx, iidx), PgLOG.LGEREX)
   return (pgrec['date'] if pgrec else None)

#
# get max inventory index
#
def get_inventory_record(didx = 0, cntopt = 0):

   table = "{}.ispd_inventory".format(DBCNTL)

   if not didx:
      if cntopt == 2:
         pgrec = PgDBI.pgget(table, "min(date) mdate", "tcount = 0", PgLOG.LGEREX)
         if not (pgrec and pgrec['mdate']):
            logger.error(table+": no counted-only inventory record exists")
         didx = get_inventory_didx(pgrec['mdate'], 1)
      elif cntopt == 0:
         pgrec = PgDBI.pgget(table, "max(didx) idx", "", PgLOG.LGEREX)
         didx = (pgrec['idx'] if pgrec else 0)
   if didx:
      cnd = "didx = {}".format(didx)
      pgrec = PgDBI.pgget(table, "*", cnd, PgLOG.LGEREX)
      if not pgrec:
         logger.error("{}: record not found for {}".format(table, cnd))
   else:
      pgrec = {'date' : '', 'fname' : '', 'miniidx' : 0, 'maxiidx' : 0,
               'didx' : 0, 'count' : 0, 'tcount' : 0, 'tidx' : 1}

   return pgrec

#
# get previous/later inventory didx for given date
#
def get_inventory_didx(cdate, prev):

   table = "ispd_inventory"
   fld = "didx, date"
   if prev:
      cnd = "date < '{}' ORDER BY date DESC".format(cdate)
   else:
      cnd = "date > '{}' ORDER BY date ASC".format(cdate)

   pgrec = PgDBI.pgget(table, fld, cnd, PgLOG.LGEREX)
   if not pgrec:
      logger.error("{}: record not found for {}".format(table, cnd))

   return pgrec['didx']

#
# initialize the global indices
#
def init_current_indices(lead_uid = 0, check_existing = 0):

   global UIDIDX, CURIIDX, CURTIDX, CURIUID, AUTHREFS, LEADUID, CHKEXIST
   # leading info for iuida
   UIDIDX = ISPDS['ispdmeta']['tindex']
   CURIIDX = 0
   CURTIDX = 1
   CURIUID = ''
   AUTHREFS = {}
   LEADUID = lead_uid
   CHKEXIST = check_existing

#
# initialize indices for givn date
#
def init_indices_for_date(cdate, fname):

   global INVENTORY, CURIIDX, CURTIDX
   if fname:
      if not INVENTORY: INVENTORY = get_inventory_record()
      INVENTORY['fname'] = fname
      CURIIDX = INVENTORY['maxiidx']
      CURTIDX = INVENTORY['tidx']
   else:
      table = "{}.ispd_inventory".format(DBCNTL)
      pgrec = PgDBI.pgget(table, "*", "date = '{}'".format(cdate), PgLOG.LGEREX)
      if not pgrec:
         logger.error("{}: given date not in inventory yet".format(cdate))
      if CURIIDX < pgrec['miniidx']:
         CURIIDX = pgrec['miniidx'] - 1
         CURTIDX = pgrec['tidx'] - 1
   
   return

#
# update or add control tables
#
def update_control_tables(cdate, acnts, iuida, tidx = 0):

   if not tidx: tidx = date2tidx(cdate)

   if iuida and acnts[0]:
      tname = "cntldb.itidx"
      records = {}
      for i in range(acnts[UIDIDX]):
         auid = iuida['uid'][i][0:2].lower()
         if auid not in records:
            records[auid] = {'suid' : [], 'date' : [], 'tidx' : [], 'iidx' : []}
         records[auid]['suid'].append(iuida['uid'][i][2:6])
         records[auid]['date'].append(cdate)
         records[auid]['tidx'].append(tidx)
         records[auid]['iidx'].append(iuida['iidx'][i])

      for auid in records:
         add_records_to_table(tname, auid, records[auid], cdate)

   tname = "cntldb.iattm"
   dname = tname + "_daily"
   for i in range(TABLECOUNT):
      if not acnts[i]: continue
      aname = IMMA_NAMES[i]
      cnd = "attm = '{}' AND tidx = {}".format(aname, tidx)
      pgrec = PgDBI.pgget(tname, "aidx, count", cnd, PgLOG.LGWNEX)
      if pgrec:
         record = {'count' : (pgrec['count'] + acnts[i])}
         PgDBI.pgupdt(tname, record, "aidx = {}".format(pgrec['aidx']), PgLOG.LGWNEX)
      else:
         record = {'tidx' : tidx, 'attm' : aname, 'count' : acnts[i]}
         PgDBI.pgadd(tname, record, PgLOG.LGWNEX)

      cnd = "attm = '{}' AND date = '{}'".format(aname, cdate)
      pgrec = PgDBI.pgget(dname, "aidx, count", cnd, PgLOG.LGWNEX)
      if pgrec:
         record = {'count' : (pgrec['count'] + acnts[i])}
         PgDBI.pgupdt(dname, record, "aidx = {}".format(pgrec['aidx']), PgLOG.LGWNEX)
      else:
         record = {'date' : cdate, 'tidx' : tidx, 'attm' : aname, 'count' : acnts[i]}
         PgDBI.pgadd(dname, record, PgLOG.LGWNEX)

def add_records_to_table(tname, suffix, records, cdate):
   """ Add records to a table """
   table =  "{}_{}".format(tname, suffix)
   if not PgDBI.pgcheck(table):
      pgcmd = PgDBI.get_pgddl_command(tname)
      PgLOG.pgsystem("{} -x {}".format(pgcmd, suffix), PgLOG.LGWNEX)

   cnt = PgDBI.pgmadd(table, records, PgLOG.LGEREX)

   ess = 's' if cnt > 1 else ''
   logger.info("{}: {} record{} added to {}".format(cdate, cnt, ess, table))

   return cnt

# call to initialize table info when the module is loaded
init_table_info()
