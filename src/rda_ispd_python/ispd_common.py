"""
Common library with ISPD dataset and database utilities
"""

import re
import math
import numpy
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

#
# identify and return the ATTM name from a given line of standalone attm input
#
def identify_attm_name(line):

   global UIDOFFSET, UIDLENGTH, ATTMNAME
   if LEADUID:
      UIDOFFSET = 0
      UIDLENGTH = 6
      atti = line[6:8]
   elif re.match(r'^9815', line):
      UIDOFFSET = 4
      UIDLENGTH = 15
      atti = line[15:17]
   else:
      atti = None

   if atti and atti in ATTI2NAME:
      ATTMNAME = ATTI2NAME[atti]
   else:
      ATTMNAME = None

   return ATTMNAME

#
# cache field information for given attm and variable name
#
def cache_field_info(aname, var, uidopt = 0):

   global FIELDINFO, LEADUID
   if imma not in IMMAS: PgLOG.pglog("{}: Unkown attm name provided to fill field {}".format(aname, var), PgLOG.LGEREX)
   imma = IMMAS[aname]
   attm = imma[3]
   if var not in attm: PgLOG.pglog("{}: Field name not in attm {}".format(var, aname), PgLOG.LGEREX)
   fld = attm[var]

   if uidopt: LEADUID = uidopt
   FIELDINFO = {'aname' : aname, 'atti' : imma[1], 'attl' : imma[2], 'var' : var, 'fld' : fld, 'prec' : fld[2]}

def get_ispd_records(line, cdate, records):
   """ 
   Append the individual fields and return ispd records for one line of input
   """

   global CURIIDX, CURIUID
   llen = len(line)
   if llen == 0:
      return records

   for aname in ISPDS:
      ispd = ISPDS[aname]
      pgrec = get_one_attm(ispd['attm'], line)
      if aname not in records:
         records[aname] = initialize_attm_records(ispd['attm'])
      if CURIUID:
         append_one_attm(uid, ispd['tindex'], ispd['attm'], pgrec, records[aname])
      else:
         pgrecs[aname] = pgrec

   if CURIUID:
      return records

   if 'uid' not in pgrecs:
      logger.error("Missing 'uid' in the ispdmeta table: {}".format(line))

   uid = pgrecs['ispdmeta']['uid']
   records['ispdmeta']['date'].append(cdate)



   if 'iuida' not in pgrecs: PgLOG.pglog("Miss UID attm: " + line, PgLOG.LGEREX)
   uid = pgrecs['iuida']['uid']
   records['icoreloc']['date'].append(cdate)

   for aname in pgrecs:
      ispd = ISPDS[aname]
      append_one_attm(uid, ispd['tindex'], ispd['attm'], pgrecs[aname], records[aname])

   return records

#
# append the individual fields and return imma records for one line of multi-attm record
#
def get_imma_multiple_records(line, records):

   llen = len(line)
   if llen == 0: return records
   uid = line[4:10]
   offset = 15
   aname = ATTI2NAME[line[15:17]]
   imma = IMMAS[aname]
   if aname not in records: records[aname] = initialize_attm_records(imma[3])

   while (llen-offset) > 3:
      pgrec = get_one_attm(imma[3], offset, line)
      append_one_attm(uid, imma[0], imma[3], pgrec, records[aname])
      offset += imma[2]

   return records

#
# read file line and fill a single field value into db
# 
def set_imma_field(line):

   llen = len(line)
   var = FIELDINFO['var']
   pgrec = {}

   if ATTMNAME:      # attm name is provided
      coreidx = 2    # skip core sections
      offset = UIDLENGTH
      uid = line[UIDOFFSET:UIDOFFSET+6]
      cont = 1
   else:
      coreidx = 0
      offset = 0
      uid = None
      cont = 2

   getval = 0
   while (llen-offset) > 3:
      if coreidx < 2:
         aname = IMMA_NAMES[coreidx]
         coreidx += 1
         if aname == FIELDINFO['aname']: getval = 1
      else:
         atti = line[offset:offset+2]
         if atti == UIDATTI:
            uid = line[offset+4:offset+10]
            cont -= 1
         if atti == FIELDINFO['atti']:
            getval = 1
         else:
            aname = ATTI2NAME[atti]

      if getval:
         fld = FIELDINFO['fld']
         pos = offset + fld[3]
         if fld[1] > 0:
            val = line[pos:pos+fld[1]]
         else:
            val = line[pos:]
         val = val.rstrip(val)  # remove trailing whitespaces
         if len(val) > 0:
            if fld[2] > 0:
               cnd = "{} = {}".format(var, val)
               pgrec[var] = int(val)
            else:
               cnd = "{} = '{}'".format(var, val)
               pgrec[var] = val
         getval = 0
         cont -= 1
         offset += FIELDINFO['attl']
      else:
         offset += IMMAS[aname][2]

      if cont <= 0: break

   if not pgrec: return 0

   if not uid: PgLOG.pglog("Miss UID attm: " + line, PgLOG.LGEREX)
   if not get_itidx_date(uid): return 0

   tname = "{}_{}".format(FIELDINFO['aname'], CURTIDX)
   if PgDBI.pgget(tname, "", "iidx = {} AND {}".format(CURIIDX, cnd)): return 0
   return PgDBI.pgupdt(tname, pgrec, "iidx = {}".format(CURIIDX), PgLOG.LGEREX)

def get_one_attm(attm, offset, line):
   """ Gets all field values for a single table in an observation record """

   fields = line.split(',')

   # check number of fields (should be 41)
   if len(fields) != 41:
      logger.error("Incorrect number of fields in record: {}".format(line))

   pgrec = {}

   for var in attm:
      field = attm[var]
      precision = field['precision']
      field_index = field['field_index']
      size = field['size']
      missing = field['missing']

      if field['position']:
         position = field['position']
         val = fields[field_index][position:size]
      else:
         if len(fields[field_index] > size):
            val = missing
         else:
            val = fields[field_index]
      
      if re.search("nan", val) and precision > 0:
         val = missing
      val = val.rstrip()

      if len(val) > 0:
         if precision > 0.01:
            pgrec[var] = int(val)
         else:
            pgrec[var] = val
      else:
         pgrec[var] = None

   return pgrec

#
# Initialize dict records for specified attm table
#
def initialize_attm_records(attm):

   pgrecs = {'iidx' : [], 'uid' : []}
   for var in attm: pgrecs[var] = []
   if 'year' in attm: pgrecs['date'] = []

   return pgrecs

#
#  append one attm record to the multiple attm records
#
def append_one_attm(uid, aidx, attm, pgrec, pgrecs):

   global ISPD_COUNTS
   pgrecs['iidx'].append(CURIIDX)
   if 'uid' not in attm:
      pgrecs['uid'].append(uid)
   for var in attm: 
      pgrecs[var].append(pgrec[var])
   ISPD_COUNTS[aidx] += 1  # row index for individual table

#
# get imma attm counts for a given line of imma record
#
def get_imma_counts(line, acnts):

   global CURIIDX, CURIUID
   llen = len(line)
   if llen == 0: return acnts

   if CURIUID:
      coreidx = 2
      offset = UIDLENGTH
   else:
      coreidx = 0
      offset = 0
      CURIIDX += 1

   while (llen-offset) > 3:
      if coreidx < 2:
         aname = IMMA_NAMES[coreidx]
         coreidx += 1
      else:
         aname = ATTI2NAME[line[offset:offset+2]]
      imma = IMMAS[aname]
      acnts[imma[0]] += 1
      if not imma[2]: break
      offset += imma[2]

   return acnts

#
# get imma multiple attm countsfor a given line of multi-attm record
#
def get_imma_multiple_counts(line, acnts):

   llen = len(line)
   if llen == 0: return acnts
   offset = 15
   aname = ATTI2NAME[line[15:17]]
   imma = IMMAS[aname]

   while (llen-offset) > 3:
      acnts[imma[0]] += 1
      offset += imma[2]

   return acnts

#
# add multiple ispd records into different tables in RDADB
#
def add_ispd_records(cdate, records):

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

#
# read attm records for given date
#
def read_attm_for_date(aname, cdate, tidx = None):

   if tidx is None:
      tidx = date2tidx(cdate)
      if tidx is None: return None

   if aname == IMMA_NAMES[0]: return read_coreloc_for_date(cdate, tidx)

   table = "{}_{}".format(aname, tidx)
   if not PgDBI.pgcheck(table): return None
   loctable = "{}_{}".format(IMMA_NAMES[0], tidx)
   jtables = "{} a, {} b".format(table, loctable)

   return PgDBI.pgmget(jtables, "a.*", "b.date = '{}' AND a.iidx = b.iidx ORDER BY iidx".format(cdate), PgLOG.LGEREX)

#
# read core records for given date
#
def read_coreloc_for_date(cdate, tidx = None):

   global CURTIDX
   if not tidx:
      tidx = date2tidx(cdate)
      if not tidx: return None
   CURTIDX = tidx

   return PgDBI.pgmget("{}_{}".format(IMMA_NAMES[0], tidx), '*', "date = '{}' ORDER BY iidx".format(cdate))

#
# read attm record for given uid
#
def read_attm_for_uid(aname, uid, tidx):

   if aname == IMMA_NAMES[0]: return read_coreloc_for_uid(uid, tidx)

   table = "{}_{}".format(aname, tidx)
   if not PgDBI.pgcheck(table): return None

   return PgDBI.pgget(table, "*", "uid = '{}'".format(uid), PgLOG.LGEREX)

#
# read core records for given uid
#
def read_coreloc_for_uid(uid, tidx):

   return PgDBI.pgget("{}_{}".format(IMMA_NAMES[0], tidx), '*', "uid = '{}'".format(uid))

#
# write IMMA records to file
#
def write_imma_file(fh, corelocs):

   (acount, anames, atables, aindices) = get_attm_names(CURTIDX)
   rcnt = len(corelocs['iidx'])
   acnts = [0]*TABLECOUNT

   for r in range(rcnt):
      coreloc = PgUtil.onerecord(corelocs, r)
      line = get_attm_line(IMMA_NAMES[0], None, 0, coreloc)
      acnts[0] += 1
      ilines = []
      acnt = -1
      for a in range(acount):
         if anames[a] in MUNIQUE:
            (icnt, iline) = get_multiple_attm_line(anames[a], atables[a], coreloc['iidx'])
            if icnt > 0:
               acnt += icnt    # un-comment if multiple attms are counted
               acnts[aindices[a]] += icnt
               ilines.append(iline)
         else:
            aline = get_attm_line(anames[a], atables[a], coreloc['iidx'])
            if not aline: continue
            line += aline
            if anames[a] == 'iuida' and ilines:
               for i in len(ilines):
                  ilines[i] = aline + ilines[i]
            acnts[aindices[a]] += 1
            acnt += 1

      if acnt != coreloc['attc']: line[ATTCPOS] = B36(acnt)

      fh.write(line + "\n")               # main record
      if ilines:
         for il in ilines:
            fh.write(il + "\n")    # add standalone multiple attm line

   return acnts

#
# write IMMA records for given date
#
def write_imma_records(fh, cdate, tidx, dumpall):

   acnts = [0]*TABLECOUNT
   if not tidx:
      tidx = date2tidx(cdate)
      if not tidx: return None

   dcnd = "date = '{}' ORDER BY iidx".format(cdate)
   mtable = "{}_{}".format(IMMA_NAMES[0], tidx)
   pgrecs = PgDBI.pgmget(mtable, "*", dcnd)
   if not pgrecs: return None
   acnts[0] = count = len(pgrecs['iidx'])
   minidx = pgrecs['iidx'][0]
   jcnd = "m.iidx = n.iidx AND " + dcnd
   tcnd = "tidx = {} AND attm =".format(tidx)
   atable = "cntldb.iattm"

   lines = ['']*count
   attcs = [-2]*count
   if dumpall: ulines = ['']*count
   build_imma_lines(IMMA_NAMES[0], minidx, count, pgrecs, lines, attcs)
   atsave = pgrecs['attc']

   # dump main record
   for a in range(1, TABLECOUNT):
      aname = IMMA_NAMES[a]
      if aname in MUNIQUE: continue
      if PgDBI.pgget(atable, "", "{} '{}'".format(tcnd, aname)):
         ntable = "{}_{}".format(aname, tidx)
         pgrecs = PgDBI.pgmget("{} m, {} n".format(mtable, ntable), "n.*", jcnd)
         if not pgrecs: continue
         acnts[a] = len(pgrecs['iidx'])
         if dumpall and aname == "iuida":
            build_imma_lines(aname, minidx, acnts[a], pgrecs, ulines, attcs)
            for i in range(count):
               lines[i] += ulines[i]
         else:
            build_imma_lines(aname, minidx, acnts[a], pgrecs, lines, attcs)

   if dumpall:   # append the multi-line attms
      for a in range(1, TABLECOUNT):
         aname = IMMA_NAMES[a]
         if MUNIQUE[aname] is None: continue
         if PgDBI.pgget(atable, "", "{} '{}'".format(tcnd, aname)):
            ntable = "{}_{}".format(aname, tidx)
            pgrecs = PgDBI.pgmget("{} m, {} n".format(mtable, ntable), "n.*", jcnd)
            if not pgrecs: continue
            acnts[a] = len(pgrecs['iidx'])
            append_imma_lines(aname, minidx, acnts[a], pgrecs, ulines, lines, attcs)

   for i in range(count):
      if attcs[i] != atsave[i]:
         acnt = attcs[i]
         line = lines[i]
         lines[i] = line[0:ATTCPOS] + B36(acnt) + line[ATTCPOS+1:]
      fh.write(lines[i] + "\n")

   return acnts

#
# build daily imma lines by appending each attm 
#
def build_imma_lines(aname, minidx, count, pgrecs, lines, attcs):

   imma = IMMAS[aname]
   attm = imma[3]

   if aname in ATTM_VARS:
      vars = ATTM_VARS[aname]
   else:
      ATTM_VARS[aname] = vars = order_attm_variables(attm)

   for i in range(count):
      pgrec = PgUtil.onerecord(pgrecs, i)
      line = ''
      for var in vars:
         vlen = attm[var][1]
         if vlen > 0:
            if pgrec[var] is None:
               line += "{:{}}".format(' ', vlen)
            elif attm[var][2] > 0:
               line += "{:{}}".format(pgrec[var], vlen)
            else:
               line += "{:{}}".format(pgrec[var], vlen)
         elif pgrec[var] is not None:
            line += pgrec[var]     # append note

      idx = pgrec['iidx'] - minidx
      lines[idx] += imma[1] + imma[4] + line
      attcs[idx] += 1 

#
# append daily imma lines for each multi-line attm 
#
def append_imma_lines(aname, minidx, count, pgrecs, ulines, lines, attcs):

   imma = IMMAS[aname]
   attm = imma[3]

   if ATTM_VARS[aname]:
      vars = ATTM_VARS[aname]
   else:
      ATTM_VARS[aname] = vars = order_attm_variables(attm)

   pidx = -1
   for i in range(count):
      pgrec = PgUtil.onerecord(pgrecs, i)
      cidx = pgrec['iidx'] - minidx
      if cidx > pidx: lines[cidx] +=  "\n" + ulines[cidx]
      line = imma[1] + imma[4]
      for var in vars:
         vlen = attm[var][1]
         if pgrec[var] is None:
            line += "{:{}}".format(' ', vlen)
         elif attm[var][2] > 0:
            line += "{:{}}".format(pgrec[var], vlen)
         else:
            line += "{:{}}".format(pgrec[var], vlen)
      lines[cidx] += line
      attcs[cidx] += 1 
      pidx = cidx

#
# count IMMA records for given date
#
def count_imma_records(cdate, tidx, cntall):

   acnts = [0]*TABLECOUNT
   if not tidx:
      tidx = date2tidx(cdate)
      if not tidx: return None

   atable = "cntldb.iattm"
   tcnd = "tidx = {}".format(tidx)
   dcnd = "date = '{}'".format(cdate)
   mtable = "{}_{}".format(IMMA_NAMES[0], tidx)
   jcnd = "m.iidx = n.iidx AND " + dcnd
   acnts[0] = PgDBI.pgget(mtable, "", dcnd)
   if not acnts[0]: return None

   for i in range(1,TABLECOUNT):
      aname = IMMA_NAMES[i]
      if not cntall and aname in MUNIQUE: continue
      if PgDBI.pgget(atable, "", "{} AND attm = '{}'".format(tcnd, aname)):
         ntable = "{}_{}".format(aname, tidx)
         acnts[i] = PgDBI.pgget("{} m, {} n".format(mtable, ntable), "", jcnd)

   return acnts

#
# add inventory information into control db
#
def add_inventory_record(fname, cdate, count, inventory, cntopt = 0):

   didx = 0
   table = "cntldb.inventory"

   if cntopt == 2:
      cnd = "date = '{}'".format(cdate)
      pgrec = PgDBI.pgget(table, "didx, count", cnd, PgLOG.LGEREX)
      if not pgrec: PgLOG.pglog("{}: error get record for {}".format(table, cnd), PgLOG.LGEREX)
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
         PgLOG.pglog("{}: error update table for {}".format(table, cnd), PgLOG.LGEREX)
   else:
      didx = PgDBI.pgadd(table, record, PgLOG.LGEREX|PgLOG.AUTOID)

   record['didx'] = didx
   if cntopt == 2:
      record['count'] = count
      record['date'] = cdate

   return record

#
# get the attm names for the current tidx
#
def get_attm_names(tidx):

   anames = []
   atables = []
   aindices = []
   attms = {}
   acnt = 0
   pgrecs = PgDBI.pgmget("cntldb.iattm", "attm", "tidx = {}".format(tidx), PgLOG.LGEREX)
   if not pgrecs: PgLOG.pglog("miss iattm record for tidx = {}".format(tidx), PgLOG.LGEREX)
   for aname in pgrecs['attm']: attms[aname] = 1

   for i in range(1, TABLECOUNT):
      aname = IMMA_NAMES[i]        
      if aname in attms:
         anames.append(aname)
         atables.append("{}_{}".format(aname, tidx))
         aindices.append(i)
         acnt += 1

   return (acnt, anames, atables, aindices)

#
# get the attm line for the attm name and current iidx
#
def get_attm_line(aname, atable, iidx, pgrec):

   if not pgrec: pgrec = PgDBI.pgget(atable, "*", "iidx = {}".format(iidx), PgLOG.LGEREX)
   return build_one_attm_line(aname, pgrec) if pgrec else None

#
# get the attm line for the multiple attms of current iidx
#
def get_multipe_attm_line(aname, atable, iidx):

   pgrecs = PgDBI.pgmget(atable, "*", "iidx = {} ORDER BY lidx".format(iidx), PgLOG.LGEREX)
   icnt = (len(pgrecs['lidx']) if pgrecs else 0)
   if not icnt: return (0, None)

   iline = ''
   for i in range(icnt):
      iline += build_one_attm_line(aname, PgUtil.onerecord(pgrecs, i))

   return (icnt, iline)

#
# build the string line for the attm record and current iidx
#
def build_one_attm_line(aname, pgrec):

   imma = IMMAS[aname]
   attm = imma[3]
   line = imma[1] + imma[4]

   if aname in ATTM_VARS:
      vars = ATTM_VARS[aname]
   else:
      ATTM_VARS[aname] = vars = order_attm_variables(attm)

   for var in vars:
      vlen = attm[var][1]
      if vlen > 0:
         if pgrec[var] is None:
            line += "{:{}}".format(' ', vlen)
         elif attm[var][2] > 0:
            line += "{:{}}".format(pgrec[var], vlen)
         else:
            line += "{:{}}".format(pgrec[var], vlen)
      elif pgrec[var] is not None:
         line += pgrec[var]     # append note

   return line

#
# find an existing imma record in RDADB
#
def find_imma_record(coreloc):

   cnd = "date = '{}'".format(coreloc['date'])
   if coreloc['dy'] is None:
      cnd += " AND dy IS NULL"
   if coreloc['hr'] is not None:
      cnd += " AND hr = {}".format(coreloc['hr'])
   else:
      cnd += " AND hr IS NULL"
   if coreloc['lat'] is not None:
      cnd += " AND lat = {}".format(coreloc['lat'])
   else:
      cnd += " AND lat IS NULL"
   if coreloc['lon'] is not None:
      cnd += " AND lon = {}".format(coreloc['lon'])
   else:
      cnd += " AND lon IS NULL"
   if coreloc['id'] is not None:
      cnd += " AND id = '{}'".format(coreloc['id'])
   else:
      cnd += " AND id IS NULL"

   pgrec = PgDBI.pgget("coreloc", "iidx", cnd, PgLOG.LGWNEX)

   return (pgrec['iidx'] if pgrec else 0)

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
   if not pgrec: return PgLOG.pglog("{}: SKIP suid not in {}".format(suid, table), PgLOG.WARNLG)

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
         PgLOG.pglog("{}: set {}-{} to {}".format(cdate, sym, dy, edate), PgLOG.LOGWRN)

   return cdate

#
# get the tidx from table inventory for given date
#
def date2tidx(cdate):

   table = "ispd_inventory"
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

   pgrec = PgDBI.pgget("cntldb.inventory", "date", "miniidx <= {} AND maxiidx >= {}".format(iidx, iidx), PgLOG.LGEREX)
   return (pgrec['date'] if pgrec else None)

#
# get field name from the component number and field number
#
def number2name(cn, fn):

   key = cn * 100 + fn
   if key in NUM2NAME: return NUM2NAME[key]

   if cn > 0:
      offset = 3
      for i in range(2, TABLECOUNT):
         aname = IMMA_NAMES[i]
         if cn == int(IMMAS[aname][1]): break
      if i >= TABLECOUNT: PgLOG.pglog("{}: Cannot find Component".format(cn), PgLOG.LGEREX)
   elif fn < 17:
      offset = 1
      aname = IMMA_NAMES[0]
   else:
      offset = 17
      aname = IMMA_NAMES[1]

   attm = IMMAS[aname][3]
   for fname in attm:
      if fn == (attm[fname][0]+offset):
         NUM2NAME[key] = [fname, aname]
         return NUM2NAME[key]

   PgLOG.pglog("{}: Cannot find field name in Component '{}'".format(fn, aname), PgLOG.LGEREX)

#
# get component number and field number from give field name
#
def name2number(fname):

   if fname in NAME2NUM: return NAME2NUM[fname]

   for i in range(TABLECOUNT):
      aname = IMMA_NAMES[i]
      attm = IMMAS[aname][3]
      if fname in attm:
         cn = int(IMMAS[aname][1]) if IMMAS[aname][1] else 0
         fn = attm[fname][0]
         if i == 0:
            fn += 1
         elif i == 1:
            fn += 17
         else:
            fn += 3

         NAME2NUM[fname] = [cn, fn, aname]
         return NAME2NUM[fname]

   PgLOG.pglog(fname + ": Cannot find Field Name", PgLOG.LGEREX)

#
# convert integers to floating values
#
def float_imma_record(record):

   for aname in IMMA_NAMES:
      if aname not in record: continue
      attm = IMMAS[aname][3]
      for key in attm:
         prec = attm[key][2]
         if prec == 1 or prec == 0: continue
         val = record[aname][key] if record[aname][key] else 0
         if not val: continue
         record[aname][key] = val * prec

   return record

#
# convert the floating values to integers
#
def integer_imma_record(record):

   for aname in IMMA_NAMES:
      if aname not in record: continue
      attm = IMMAS[aname][3]
      for key in attm:
         prec = attm[key][2]
         if prec == 1 or prec == 0: continue
         val = record[aname][key] if record[aname][key] else 0
         if not val: continue
         if val > 0:
            record[aname][key] = int(val/prec + 0.5)
         else:
            record[aname][key] = int(val/prec - 0.5)

   return record

#
# order attm fields according FN and return ordered field array
#
def order_attm_variables(attm, aname = None):

   if not attm: attm = IMMAS[aname][3]

   return list(attm)

#
# get max inventory index
#
def get_inventory_record(didx = 0, cntopt = 0):

   table = "ispd_inventory"

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
      pgrec = PgDBI.pgget("ispd_inventory", "*", "date = '{}'".format(cdate), PgLOG.LGEREX)
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

#
# add records to a table
#
def add_records_to_table(tname, suffix, records, cdate):

   table =  "{}_{}".format(tname, suffix)
   if not PgDBI.pgcheck(table):
      pgcmd = PgDBI.get_pgddl_command(tname)
      PgLOG.pgsystem("{} -x {}".format(pgcmd, suffix), PgLOG.LGWNEX)

   cnt = PgDBI.pgmadd(table, records, PgLOG.LGEREX)
   s = 's' if cnt > 1 else ''
   PgLOG.pglog("{}: {} records added to {}".format(cdate, cnt, table), PgLOG.LOGWRN)

   return cnt

#
# match imma records for given time/space, and matching variables if provided
#
# TODO: need more work (avoid of itable)
#
# return maching count
#
def match_imma_records(cdate, t1, t2, w, e, s, n, vt):

   if cdate in TINFO:
      tinfo = TINFO[cdate]
      if not tinfo: return 0
   else:
      tinfo = PgDBI.pgget("cntldb.itable", "*", "bdate <= '{}' AND edate >= '{}'".format(cdate, cdate), PgLOG.LGWNEX)
      if not tinfo:
         TINFO[cdate] = 0
         return 0

   # match time/latitude
   mrecs = PgDBI.pgmget("icoreloc_{}".format(tinfo['tidx']), "*",
                  "date = '{}' AND hr BETWEEN {} AND {} AND lat BETWEEN {} AND {}".format(cdate, t1, t2, s, n), PgLOG.LGWNEX)
   if not mrecs: return 0  # no match

   cnt = len(mrecs['iidx'])
   # match longitude, and/or variables
   m = 0
   for i in range(cnt):
      mrec = PgUtil.onerecord(mrecs, i)
      if w < e:
         if mrec['lon'] < w or mrec['lon'] > e: continue
      else:
         if mrec['lon'] > e and mrec['lon'] < w: continue

      if not vt or match_imma_vars(tinfo, mrec, vt):
         iidx = mrec['iidx']
         if iidx not in UMATCH: UMATCH[iidx] = 1
         m += 1

   return m

#
# return 1 if a value is found for any given variable 0 otherwise
#
# TODO: need more work
#
def match_imma_vars(tinfo, mrec, vt):

   mrecs = {}
   mrecs['icoreloc'] = mrec
   iidx = mrec['iidx']
   tidx = tinfo['tidx']
   for v in vt:
      name = vt[v]
      if name not in mrecs:
         if name in tinfo:
            mrecs[name] = PgDBI.pgget("{}_{}".format(name, tidx), "*", "iidx = {}".format(iidx))
            if not mrecs[name]: mrecs[name] = 0
         else:
            mrecs[name] = 0

      if mrecs[name] and mrecs[name][v] is not None: return 1   # found value for variable

   return 0

#
# distant in km to degree in 0.01deg
#
def distant2degree(la, lo, d, b):

   P = 3.14159265
   R = 6371

   la *= P/18000
   lo *= P/18000
   b *= P/18000

   lat = int(18000 * math.asin(math.sin(la)*math.cos(d/R) + math.cos(la)*math.sin(d/R)*math.cos(b))/P + 0.5)
   lon = int(18000 * (lo + math.atan2(math.sin(b)*math.sin(d/R)*math.cos(la), math.cos(d/R) - math.sin(la)*math.sin(la)))/P + 0.5)

   return (lat, lon)

# integer to 36-based string
def B36(I36):

   STR36 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
   B36 = STR36[I36%36]
   while I36 >= 36:
      I36 = int(I36/36)
      B36 = STR36[I36%36] + B36

   return B36

# 36-based string to integer
def I36(B36):

   STR36 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
   I36 = 0
   for ch in B36:
      I36 = I36*36 + int(STR36.find(ch))

   return I36

#
# convert from trimqc2.f:01C by Steve Worley
# modified for trinqc2.f01D to trim on RH
#
def TRIMQC2(record, options = None):

   values = {}
   values['sst'] = record['icorereg']['sst']
   values['at']  = record['icorereg']['at']
   values['d']   = record['icorereg']['d']
   values['w']   = record['icorereg']['w']
   values['slp'] = record['icorereg']['slp']
   values['wbt'] = record['icorereg']['wbt']
   values['dpt'] = record['icorereg']['dpt']
   values['rh']  = record['iimmt5']['rh'] if 'iimmt5' in record and record['iimmt5'] else None 

   # default to enhenced trimming
   if not options: options = {'OPDN' : 0, 'OPPT' : 1, 'OPSE' : 0, 'OPCQ' : 0, 'OPTF' : 2, 'OP11' : 1}

   # GET TRIMMING AND OTHER QUALITY CONTROL FLAGS
   TRFLG = GETTRF(record)
   flags = TRIMQC0(record['icoreloc']['yr'], record['iicoads']['dck'], record['iicoads']['sid'],
                   record['iicoads']['pt'], record['iicoads']['dups'], TRFLG, options)

   if flags['ZZQF'] == 1: return None    # RECORD REJECTED

   if flags['SZQF'] == 1: values['sst'] = None   # SST FLAG AND QC APPLICATION
   if flags['AZQF'] == 1: values['at'] = None    # AT FLAG AND QC APPLICATION
   if flags['WZQF'] == 1: values['d'] = values['w'] = None   # WIND, D AND W FLAG AND QC APPLICATION
   if flags['PZQF'] == 1: values['slp'] = None    # SLP FLAG AND QC APPLICATION
   if flags['RZQF'] == 1: values['rh'] = values['wbt'] = values['dpt'] = None  # WBT AND DPT FLAG AND QC APPLICATION

   return values

#
# converted from Fortran code trimqc0.f:01D by Sandy Lubker
#
def TRIMQC0(YR, DCK, SID, PT, DS, TRFLG, options):

   flags = {'ZZQF' : 1}   # INITIAL REPORT REJECTION

   # CHECK IF TRIMMING FLAGS MISSING
   if TRFLG[0] == 0:
      PgLOG.pglog('TRIMMING FLAGS MISSING', PgLOG.LGEREX)
      return flags

   # CHECK RANGES OF OPTIONS
   if options['OPDN'] < 0 or options['OPDN'] > 2:
      PgLOG.pglog("OPDN={}".format(options['OPDN']), PgLOG.LGEREX)
   if options['OPPT'] < 0 or options['OPPT'] > 1:
      PgLOG.pglog("OPPT={}".format(options['OPPT']), PgLOG.LGEREX)
   if options['OPSE'] < 0 or options['OPSE'] > 1:
      PgLOG.pglog("OPSE={}".format(options['OPSE']), PgLOG.LGEREX)
   if options['OPCQ'] < 0 or options['OPCQ'] > 1:
      PgLOG.pglog("OPCQ={}".format(options['OPCQ']), PgLOG.LGEREX)
   if options['OPTF'] < 0 or options['OPTF'] > 3:
      PgLOG.pglog("OPTF={}".format(options['OPTF']), PgLOG.LGEREX)
   if options['OP11'] < 0 or options['OP11'] > 1:
      PgLOG.pglog("OP11={}".format(options['OP11']), PgLOG.LGEREX)

   B2 = TRFLG[0]
   ND = TRFLG[1]
   SF = TRFLG[2]
   AF = TRFLG[3]
   UF = TRFLG[4]
   VF = TRFLG[5]
   PF = TRFLG[6]
   RF = TRFLG[7]
   ZQ = TRFLG[8]
   SQ = TRFLG[9]
   AQ = TRFLG[10]
   WQ = TRFLG[11]
   PQ = TRFLG[12]
   RQ = TRFLG[13]
   XQ = TRFLG[14]
   CQ = TRFLG[15]
   EQ = TRFLG[16]
   LZ = TRFLG[17]
   SZ = TRFLG[18]
   AZ = TRFLG[19]
   WZ = TRFLG[20]
   PZ = TRFLG[21]
   RZ = TRFLG[22]

   if PT is None: PT = -1
   if options['OPDN'] == 1:
      if ND == 2: return flags
   elif options['OPDN'] == 2:
      if ND == 1: return flags
   if DS > 2 and (YR >= 1950 or DS != 6): return flags
   if LZ == 1: return flags
   if (ZQ == 1 or ZQ == 3) and YR >= 1950: return flags
   if YR >= 1980:
      if SID == 25 and YR > 1984: return flags
      if SID == 30 and YR > 1984: return flags
      if SID == 33 and YR < 1986: return flags
      if options['OPPT'] == 0:
         if not (PT  == 2 or PT == 5 or PT == -1 and DCK == 888): return flags
         if SID == 70 or SID == 71: return flags
      elif options['OPPT'] == 0:
         if PT > 5: return flags
         if SID == 70 or SID == 71: return flags

   # REMOVE ELEMENT REJECTION
   flags['ZZQF'] = flags['SZQF'] = flags['AZQF'] = flags['WZQF'] = flags['PZQF'] = flags['RZQF'] = 0

   # SOURCE EXCLUSION FLAGS
   if YR < 1980 or (PT != 13 and PT != 14 and PT != 16):
      if options['OPSE'] == 0:
         if SZ == 1: flags['SZQF'] = 1
         if AZ == 1: flags['AZQF'] = 1
         if WZ == 1: flags['WZQF'] = 1
         if YR >= 1980:
            if SID == 70 or SID == 71: flags['WZQF'] = 1
         if PZ == 1: flags['PZQF'] = 1
         if RZ == 1: flags['RZQF'] = 1

   # COMPOSITE QC FLAGS
   if options['OPCQ'] == 0:
      if SQ > 0: flags['SZQF'] = 1
      if AQ > 0: flags['AZQF'] = 1
      if WQ > 0: flags['WZQF'] = 1
      if PQ > 0: flags['PZQF'] = 1
      if RQ > 0: flags['RZQF'] = 1

   # TRIMMING FLAGS
   if options['OPTF'] < 3:
      if SF > (options['OPTF']*2+1):
         if options['OP11'] == 0 or SF != 11: flags['SZQF'] = 1
      if AF > (options['OPTF']*2+1): flags['AZQF'] = 1
      if UF > (options['OPTF']*2+1) or VF > (options['OPTF']*2+1): flags['WZQF'] = 1
      if PF > (options['OPTF']*2+1):
         if options['OP11'] == 0 or PF != 11: flags['PZQF'] = 1
      if RF > (options['OPTF']*2+1): flags['RZQF'] = 1
   elif options['OPTF'] == 3:
      if SF > 12: flags['SZQF'] = 1
      if AF > 12: flags['AZQF'] = 1
      if UF > 12 or VF > 12: flags['WZQF'] = 1
      if PF > 12: flags['PZQF'] = 1
      if RF > 12: flags['RZQF'] = 1

   return flags

#
# get trim flags
#
def GETTRF(record):

   (ZNC,WNC,BNC,XNC,YNC,PNC,ANC,GNC,DNC,SNC,CNC,ENC,FNC,TNC) = (0,1,2,3,4,5,6,7,8,9,10,11,12,13)
   (SF,AF,UF,VF,PF,RF) = (0,1,2,3,4,5)

   QCFLG = [None]*14
   cstr = record['iicoads']['nqcs']
   if cstr:
      i = 0
      for c in cstr: 
         if c != ' ': QCFLG[i] = I36(c)
         i += 1

   TRIMS = [None]*6
   cstr = record['iicoads']['trms']
   if cstr:
      i = 0
      for c in cstr: 
         if c != ' ': TRIMS[i] = I36(c)
         i += 1

   TRFLG = [0]*23
   if record['icoreloc']['lat'] is not None and record['icoreloc']['lon'] is not None and record['iicoads']['b10'] is not None:
      TRFLG[0] = B2QXY(QB10(record['iicoads']['b10']),record['icoreloc']['lon'],record['icoreloc']['lat'])
   if record['iicoads']['nd'] is not None: TRFLG[1] = record['iicoads']['nd']
   if TRIMS[SF] is not None:
      TRFLG[2] = TRIMS[SF]
      TRFLG[3] = TRIMS[AF]
      TRFLG[4] = TRIMS[UF]
      TRFLG[5] = TRIMS[VF]
      TRFLG[6] = TRIMS[PF]
      TRFLG[7] = TRIMS[RF]

   if QCFLG[ZNC]:
      if QCFLG[ZNC] >= 7 and QCFLG[ZNC] != 10: TRFLG[8] = 1
      if QCFLG[SNC] >= 8 and QCFLG[SNC] != 10: TRFLG[9] = 1
      if QCFLG[ANC] >= 8 and QCFLG[ANC] != 10: TRFLG[10] = 1
      d = record['icorereg']['d']
      w = record['icorereg']['w']
      if d != None and w != None and d >= 1 and d <= 360 and w == 0 and QCFLG[WNC] == 7: TRFLG[11] = 1
      if QCFLG[PNC] >= 8 and QCFLG[PNC] != 10: TRFLG[12] = 1
      if QCFLG[GNC] >= 8 and QCFLG[GNC] != 10 or QCFLG[DNC] >= 8 and QCFLG[DNC] != 10: TRFLG[13] = 1
      if QCFLG[XNC] != 10:
         if QCFLG[XNC] >= 7:
            TRFLG[14] = 3
         elif QCFLG[XNC] >= 4:
            TRFLG[14] = 2
         elif QCFLG[XNC] >= 2:
            TRFLG[14] = 1
      if QCFLG[CNC] != 10:
         if QCFLG[CNC] >= 7:
            TRFLG[15] = 3
         elif QCFLG[CNC] >= 4:
            TRFLG[15] = 2
         elif QCFLG[CNC] >= 2:
            TRFLG[15] = 1
      if QCFLG[ENC] != 10:
         if QCFLG[ENC] >= 7:
            TRFLG[16] = 3
         elif QCFLG[ENC] >= 4:
            TRFLG[16] = 2
         elif QCFLG[ENC] >= 2:
            TRFLG[16] = 1

   QC = record['iicoads']['qce']
   if QC:
      for i in range(13, 7, -1):
         TRFLG[i] += 2*(QC%2)
         QC >>= 1

   if record['iicoads']['lz'] != None: TRFLG[17] = record['iicoads']['lz']

   QC = record['iicoads']['qcz']
   if QC:
      for i in range(22, 17, -1):
         TRFLG[i] += 2*(QC%2)
         QC >>= 1

   return TRFLG

#
#  B10 to Q value
#
def QB10(B10):

   if B10 < 1 or B10 > 648:
      return -1
   else:
      return 2 + (int((B10-1)/324))*2 - int(((B10-1+3)%36)/18)

#
# Q, X and Y to B2 values 
#
def B2QXY(Q, X, Y):

   YY = (-Y) if Y < 0 else Y

   if Q < 1 or Q > 4 or X < 0 or X > 35999 or YY > 9000: return 0

   if YY < 9000:
      if (Q%2) == 0:
         C = int(X/200)
         if C > 89: C = 89
      else:
         C = int(((36000-X)%36000)/200)
         if C > 89: C = 89
         C = 179-C

      if int(Q/3) == 0:
          R = 89-int((9000+Y)/200)
      else:
          R = int((9000-Y)/200)

      B2 = 2+R*180+C
   elif Y == 9000:
      B2 = 1
   else:
      B2 = 16202

   return B2

#
# wind speed/directory to component U and V
#
def wind2uv(w, d):

   u = v = None

   if w == 0 or d == 361:
      u = v = 0
   elif d > 0 and d < 361:
      d = numpy.deg2rad(d + 180)
      u = w * math.cos(d)
      v = w * math.sin(d)

   return (u, v)

# call to initialize table info when the module is loaded
init_table_info()
