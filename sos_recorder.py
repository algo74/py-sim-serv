"""
 Copyright (c) 2024 Alexander Goponenko. University of Central Florida.
 
 Permission is hereby granted, free of charge, to any person obtaining
 a copy of this software and associated documentation files (the
 “Software”), to deal in the Software without restriction, including
 without limitation the rights to use, copy, modify, merge, publish,
 distribute, sublicense, and/or sell copies of the Software, and
 to permit persons to whom the Software is furnished to do so,
 subject to the following conditions:
 
 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.
 
 THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
 FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

'''
Created on Apr 21, 2020

@author: alex
'''

import logging
import os
import threading
import numpy as np
from sosdb import Sos
from numsos.DataSource import SosDataSource

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class EmptyDatabaseError(Exception):
  pass


class _Recorder(object):
  
  def __init__(self, path, schema_template, schema_name):
    self.gDB_lock = threading.Lock()
    db = Sos.Container()
    self.db = db
    self.path = path
    
    try:
      db.open(path)
    except:
      db.create(path)
      db.open(path)
      db.part_create(schema_name)
      part = db.part_by_name(schema_name)
      part.state_set("PRIMARY")
      
      schema = Sos.Schema()
      schema.from_template(schema_name, schema_template)
      schema.add(db)
    else:
      schema = db.schema_by_name(schema_name)
    
    self.schema = schema



class Recorder(_Recorder):

  def __init__(self, path="/LDMS_data/SOS/results"):
    schema_name = "summary"
    schema_template = [
      {"name": "variety_id", "type": "char_array"},
      {"name": "parameter", "type": "char_array"},
      {"name": "avg", "type": "double"},
      {"name": "var", "type": "double"},
      {"name": "w_count", "type": "double"},
      {"name": "w_sum", "type": "double"},
      {"name": "vid_par", "type": "join", "join_attrs": ["variety_id", "parameter"], "index": {}}
    ]
    super().__init__(path, schema_template, schema_name)
    self.key_attr = self.schema['vid_par']

  def getRecord(self, variety_id, param):
    key = self.key_attr.key(variety_id, param)
    record = self.key_attr.find(key)
    if not record:
      return None
    else:
      with self.gDB_lock:
        return record[2:6]

  def saveRecord(self, variety_id, param, avg, var, w_count, w_sum):
    logger.debug("saving variety_id: \"%s\", parameter: \"%s\", avg: %f, var: %f, count: %f, sum: %f",
                 variety_id, param, avg, var, w_count, w_sum)
    key = self.key_attr.key(variety_id, param)
    record = self.key_attr.find(key)
    if not record:
      logger.debug("creating new record variety_id: \"%s\", parameter: \"%s\"", variety_id, param)
      record = self.schema.alloc()
      record[0:6] = variety_id, param, avg, var, w_count, w_sum
      with self.gDB_lock:
        record.index_add()
        self.db.commit()
    else:
      with self.gDB_lock:
        record[2:6] = avg, var, w_count, w_sum
        self.db.commit()



class CanaryStore(_Recorder):

  def __init__(self, path="/LDMS_data/SOS/canary"):
    schema_name = "canary"
    schema_template = [
      {"name": "time", "type": "uint64", "index": {}},
      {"name": "ost", "type": "uint64"},
      {"name": "value", "type": "double"}
    ]
    super().__init__(path, schema_template, schema_name)
    self.key_attr = self.schema['time']
    self.src = SosDataSource()
    self.src.config(path=path)


  def saveRecord(self, time, ost, duration):
    logger.debug("saving canary probe (time: \"%d\", ost: \"%d\", duration: %f)",
                 time, ost, duration)
    record = self.schema.alloc()
    record[0:3] = time, ost, duration
    with self.gDB_lock:
      record.index_add()
      self.db.commit()


  def getAverageValue(self, start_time, end_time, wiggle_time, limit=1000):
    st = start_time - wiggle_time
    et = end_time + wiggle_time
    # get records from st to et
    with self.gDB_lock: #TODO: is the lock needed here?
      self.src.select(['value'], where=[['time', Sos.COND_GE, st], ['time', Sos.COND_LE, et]], order_by='time')
      total = 0
      count = 0
      data = self.src.get_results(limit=limit)
      while data:
        total += np.sum(data.array('value'))
        count += data.get_series_size()
        data = self.src.get_results(limit=limit, reset=False)
    logger.debug("Read {} canary records for the interval {}-{}".format(count, st, et))
    if count >= 2:
      return total/count

    # if less than two records, check whether the whole database has at least two records
    self.src.select(['value'], order_by='time')
    data = self.src.get_results(limit=3)
    if not data or data.get_series_size() < 2:
      raise EmptyDatabaseError("Database {} has too little records".format(self.path))
    # increase wiggle time and repeat
    if wiggle_time <= 0:
      raise EmptyDatabaseError("No records to calculate canary average and non-positive wiggle time ({})".format(wiggle_time))
    logger.debug("Increasing wiggle time ({}) for the interval {}-{}".format(wiggle_time, start_time, end_time))
    return self.getAverageValue(start_time, end_time, 2*wiggle_time, limit)
