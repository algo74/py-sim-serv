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

#
# NOTE: make sure that SOS library is in the path
#
from __future__ import division

# import os
#print(os.environ['LD_LIBRARY_PATH'])
#if not os.environ['LD_LIBRARY_PATH']: 
#  print('no LD_LIBRARY_PATH')

import sys
import math
import time
import SocketServer
import json
import threading
import logging
import hashlib 
import numpy as np

import Queue

from sosdb import Sos
from numsos.DataSource import SosDataSource

from sos_recorder import Recorder
import table_log

###################################################
#
# configuration switch
#
###################################################
is_production = False

###################################################
#
# "constants"
#
###################################################
MAX = 1024 * 4 # maximum lenght of network message
QUERY_LIMIT = 4096 # maximum number of rows to be returned by queries
OVERFLOW = 1 + 0xffffffffffffffff # uint64 overflow value
ZERO_TIME = np.datetime64('1970-01-01T00:00:00Z') # used to convert date to int
#DEFAULT_DT = 1 # default interval between samples in seconds

###################################################
#
# parameters for processing of finished jobs
#
###################################################
PROCESSING_DELAY = 20  # seconds
N_TRIES = 3 # number of tries before giving up on a job
DELTAS = ("user",)
if is_production:
  DELTAS = ("lustre",)
PATH = '/LDMS_data/SOS/sos1/procstat/' # path to the LDMS records
REC_PATH = "/LDMS_data/SOS/results" # path to the DB that keeps summaries of usage
if is_production:
  PATH = '/LDMS_data/SOS/sos1/lustre2_client/'
  REC_PATH = "/LDMS_data/SOS/results"
# set of metrics that should be calculated
PARAMS = list(DELTAS)
PARAMS.append("timelimit")
# decay constants (alphas) for the metrics
DECAY = {
  "timelimit" : 0.2, 
  "user" : 0.2,
  "lustre" : 0.2
}
# how many sigmas shall we add to averages when predicting usage
K_SIGMA = {
  "timelimit" : 3, 
  "lustre" : 0,
  "user" : 1
}

# functions to calculate data for each deltas
def simple_array(data_source_result, val_name):
  return data_source_result.array(val_name)

SUM_SOURCES = {
    "lustre" : ["client.read_bytes#llite.testfs",
                "client.write_bytes#llite.testfs"]
  }
def sum_array(data_source_result, val_name):
  res = 0
  for name in SUM_SOURCES[val_name]:
    res = data_source_result.array(name) + res
  return res

DELTAS_DATA_GETTERS = {
  'user' : simple_array,
  'lustre' : sum_array
  }

###################################################
#
# columns to read from db
#
###################################################
COLUMNS = ["timestamp", "component_id", 'user']
if is_production:
  COLUMNS = ["timestamp", 
	    "component_id", 
	    "client.read_bytes#llite.testfs",
	    "client.write_bytes#llite.testfs"]


###################################################
#
# additional parameters for calculating current utilization
#
###################################################
CU_QUERY_TIME = 10 # seconds
CU_PERIOD = CU_QUERY_TIME/2 # 1/(frequency of updates) (seconds)


###################################################
#
# communication protocol related parameters
#
###################################################
# names for the metrics that should be used in communications 
TRANSLATE = {
  "timelimit" : "time_limit", 
  "user" : "lustre"
}
# the metrics should be multiplied by the following values
FACTORS = {
  "timelimit" : 1.0/60.0, 
  "user" : 1,
  "lustre" : 10000.0/14000000.0
}


###################################################
#
# parameters for saving to tables
#
###################################################

doSaveTables = True

if doSaveTables:
  prefixSaveTables = "/xch/results/" if is_production else ""
  gJobsTable = table_log.TableLog(prefixSaveTables+"jobs_results_table.csv")
  titles = ["job_id", "variety_id", "start_time", "end_time", "dt"]
  for val_name in DELTAS:
    titles.extend([val_name+"_avg", val_name+"_var"])
  gJobsTable.log(titles)
  gSummaryTable = table_log.TableLog(prefixSaveTables+"summary_results_table.csv")
  titles = ["variety_id", "time", "param", "avg", "var"]
  gSummaryTable.log(titles)
  # current utilization
  gCUTable = table_log.TableLog(prefixSaveTables+"current_utilization_table.csv")
  titles = ["time"]
  for val_name in DELTAS:
    titles.extend([val_name])
  gCUTable.log(titles)


###################################################
#
# globals
#
###################################################



gMessageQueue = Queue.Queue(0)

gCU_lock = threading.Lock()
gCU_avg = dict.fromkeys(DELTAS, 0.0)

gRecorder = Recorder(REC_PATH)

logging.basicConfig()
TCPlog = logging.getLogger("TCP");
TCPlog.setLevel(logging.INFO)


###################################################
#
# the rest
#
###################################################

def DT64toTS(dt64):
  return (dt64 - ZERO_TIME) / np.timedelta64(1, 's') if dt64.size else dt64


def format_value(param, val):
    return str(int(math.ceil(val * FACTORS[param])))


class State:
  
  def __init__(self):
    self.lustre_usage = 103
    
g_state = State()


class Message:
    
    def __init__(self, time, job_id, variety_id):
        self.time = time
        self.job_id = job_id
        self.variety_id = variety_id


class DeltaParameter:
  """
  Assuming that the total parameter is the sum of the parameters of the nodes 
  
  Average is the sum of averages of the nodes
  Variance is the sum ov variances of the nodes
  
  NOTE:
  "timestamp" which methods require must be a number, not a datatime64 or such 
  """
  
  def __init__(self, log=None):
    self.tot_avg = 0
    self.tot_var = 0
    if log: 
      self.log = log
    else:
      self.log = logging.getLogger("DeltaParameter")
      self.log.setLevel(logging.DEBUG)
    
  def finish_node(self):  
    if self.cur_time == self.node_init_time:
      self.log.warn("start time and end time are equal for a node")
      return 
    node_time = (self.cur_time - self.node_init_time) #/ np.timedelta64(1, 's')
    node_avg = (self.cur_val - self.node_init_val + self.node_rollover) / node_time
    node_var = self.node_square_total/node_time - node_avg*node_avg
    self.tot_avg += node_avg
    self.tot_var += node_var
    self.log.debug("finish node; node_time: %f, node avg: %f, node_square_total: %f, node_var: %f",
                  node_time, node_avg, self.node_square_total, node_var)
  
  
  def new_node(self, timestamp, value):
    self.finish_node()
    self.init_node(timestamp, value)
  
  
  def init_node(self, timestamp, value):
    self.node_square_total = 0
    self.node_init_val = value
    self.node_init_time = timestamp
    self.cur_time = timestamp
    self.cur_val = value
    self.node_rollover = 0
    self.log.debug("init node; value: %f, time: %s", value, str(timestamp))
  
  
  def same_node(self, timestamp, value):
    self.log.debug("same_node; value: %f, timestamp: %s, ", value, str(timestamp))
    delta_value = value - self.cur_val
    delta_time = (timestamp - self.cur_time) #/ np.timedelta64(1, 's')
    # in case the value roll over
    if delta_value < 0 :
      self.log.info("!!!!!!!!!!!!!!!!!!!!! -- value rollover -- !!!!!!!!!!!!!!!!!")
      delta_value += OVERFLOW
      self.node_rollover += OVERFLOW
    if delta_time < 0 :
      self.log.error("timestamp rollover - should not happen")
    self.cur_time = timestamp
    self.cur_val = value
    if delta_time > 0:
      square = delta_value * delta_value / delta_time
      self.node_square_total += square
    else:
      square = 0
    self.log.debug("delta_value: %f, delta_time: %f, square: %f",
                  delta_value, delta_time, square)
  
  
  def finish_all(self):
    self.finish_node()
    avg = self.tot_avg
    var = self.tot_var
    return (avg, var)



def process_job(dataSource, message, log):
  """ 
  called by process_thread for each job 
  
  #NOTE
  
  ## Calculating time
  
  To calculate time we determine-
  * minimum timestamp min_time
  * maximum timestamp max_time
  * average interval between records avg_dt
  The time requirement for the job is then max_time - min_time + 2*avg_dt
  because up to 2*avg_dt could have not been acounted for
  """
  log.info("Processing job_id: %i, variety_id: %s", message.job_id, message.variety_id)
  dataSource.select(COLUMNS,
             where=[['job_id', Sos.COND_EQ, message.job_id]],
             order_by='job_comp_time')
  a = dataSource.get_results(limit=1)
  if not a:
    log.info("No records for the job")
    return None
  
  total_records = 1
  total_components = 1
  cur_comp_id = a.array('component_id')[0]
  cur_deltas = {}
  for val_name in DELTAS:
    cur_deltas[val_name] = DELTAS_DATA_GETTERS[val_name](a, val_name)[0]
  cur_time = a.array('timestamp')[0]
  cur_timestamp = DT64toTS(cur_time)
  log.debug("first time: "+ str(cur_time))
  min_time = cur_timestamp
  max_time = cur_timestamp
  dt_sample = 0
  dt_total = 0
  delta_params = {}
  delta_data = {}
  for val_name in DELTAS:
    delta_params[val_name] = DeltaParameter(log)
    delta_params[val_name].init_node(cur_timestamp, cur_deltas[val_name])
  while True:
    a = dataSource.get_results(limit=QUERY_LIMIT, reset = False)
    if not a:
      # end cycle 
      break
    
    timestamps = DT64toTS(a.array('timestamp'))
    comp_ids = a.array('component_id')
    for val_name in DELTAS:
      delta_data[val_name] = DELTAS_DATA_GETTERS[val_name](a, val_name)
    nrecords = len(timestamps)
    total_records += nrecords
    log.debug("total records so far: %d", total_records)
    for i in range(nrecords):
      
      if comp_ids[i] != cur_comp_id:
        # finish previous component
        max_time = max(max_time, cur_timestamp)
        total_components += 1
        log.debug("finished component #%d: %d", total_components, cur_comp_id)
        #  start new component id
        cur_comp_id = comp_ids[i]
        cur_timestamp = timestamps[i]
        for val_name in DELTAS:
          cur_deltas[val_name] = delta_data[val_name][i]
          delta_params[val_name].new_node(cur_timestamp, cur_deltas[val_name])
        min_time = min(min_time, cur_timestamp)
      else:
        # process one more record for current component id
        new_timestamp = timestamps[i]
        dt_total += new_timestamp - cur_timestamp
        dt_sample += 1
        cur_timestamp = new_timestamp
        for val_name in DELTAS:
          cur_deltas[val_name] = delta_data[val_name][i]
          delta_params[val_name].same_node(cur_timestamp, cur_deltas[val_name])
    if nrecords < QUERY_LIMIT:
      # end cycle
      break
  
  log.debug("finishing last component: %d", cur_comp_id)
  max_time = max(max_time, cur_timestamp)
  if dt_sample == 0 or dt_total == 0:
    log.info("No useful records for the job")
    return None
  
  dt_avg = dt_total /  dt_sample
  delta_time = 2*dt_avg + (max_time - min_time) #/ np.timedelta64(1, 's')
  results = {}
  for val_name in DELTAS:
    results[val_name] = delta_params[val_name].finish_all()
  a_value = next(iter(DELTAS))
  avg, var = results[a_value]
  log.info("d_time: %f, %s avg: %f, var: %f", delta_time, a_value, avg, var)
  
  if doSaveTables:
    row = [message.job_id, message.variety_id, min_time, max_time, delta_time]
    for val_name in DELTAS:
      row.extend(results[val_name])
    gJobsTable.log(row)
  
  return delta_time, results


def normalizeRecord(record):
  return record if record else (0.0, 0.0, 0.0, 0.0)

def update_param(variety_id, param_name, avg, var):
  alpha = DECAY[param_name]
  pAvg, pVar, pCount, pSum = normalizeRecord(gRecorder.getRecord(variety_id, param_name))
  nCount = 1 + (1-alpha) * pCount
  nSum = avg + (1-alpha) * pSum
  nAvg = nSum / nCount
  nVar = (var + (avg-nAvg)**2 + (1-alpha)*pCount*(pVar +(pAvg-nAvg)**2))/nCount
  if doSaveTables:
    time_now = DT64toTS(np.datetime64('now'))
    gSummaryTable.log([variety_id, time_now, param_name, nAvg, nVar])
  gRecorder.saveRecord(variety_id, param_name, nAvg, nVar, nCount, nSum)

def processing_thread():
  """ the job for the thread that monitores the queue and processes jobs  from it"""
  log = logging.getLogger("job_util")
  log.setLevel(logging.INFO)
  log.info("Job processing thread started")
  src = SosDataSource()
  src.config(path=PATH)
  src.select(['timestamp'],
             where = [['timestamp', Sos.COND_GE, time.time() - 60]],
             order_by='time_comp_job')
  a = src.get_results()
  if a:
    log.info("Last minute results length: %s", len(a.array('timestamp')))
    # src.show(30)
  else:
    log.info("No recent results")
  
  while True:
    if gMessageQueue.empty():
      time.sleep(PROCESSING_DELAY)
    else:
      m = gMessageQueue.get()
      now = time.time()
      if now < m.time:
        time.sleep(m.time - now)
      # try processing 3 times
      for _ in range(N_TRIES):
        # process job data
        result = process_job(src, m, log)
        if result != None:
          break
        time.sleep(PROCESSING_DELAY)
      if not result:
        log.error("Could not find any records for job %d, giving up", m.job_id)
      else: 
        dt, param_results = result
        # process timelimit
        update_param(m.variety_id, 'timelimit', dt, 0)
        for param_name in param_results:
          avg, var = param_results[param_name]
          update_param(m.variety_id, param_name, avg, var)



def calculate_current_utilization(dataSource, log):
  global gCU_avg
  global gCU_lock
  dataSource.select(COLUMNS,
             where = [['timestamp', Sos.COND_GE, time.time() - CU_QUERY_TIME]],
             order_by='time_comp_job')
  
  delta_data = {}
  
  total_records = 0
  total_components = 0
  mins = {}
  maxs = {}
  a = dataSource.get_results(limit=QUERY_LIMIT, reset = True)
  
  while True:
    if not a:
      # end cycle 
      break
    
    timestamps = DT64toTS(a.array('timestamp'))
    comp_ids = a.array('component_id')
    for val_name in DELTAS:
      delta_data[val_name] = DELTAS_DATA_GETTERS[val_name](a, val_name)
    nrecords = len(timestamps)
    total_records += nrecords
    log.debug("total records so far: %d", total_records)
    for i in range(nrecords):
      cur_comp_id = comp_ids[i]
      # prepare the record
      record = {"timestamp" : timestamps[i]}
      for val_name in DELTAS:
        record[val_name] = delta_data[val_name][i]
      # if the first row for this component, add it to mins
      if cur_comp_id not in mins:
        total_components += 1
        mins[cur_comp_id] = record
      # update maxs
      maxs[cur_comp_id] = record
    if nrecords < QUERY_LIMIT:
      # end cycle
      break
    
    a = dataSource.get_results(limit=QUERY_LIMIT, reset = False)
  
  if  total_records == 0:
    log.info("No recent data for current utilization")
    return False
  
  #now process each component
  total_deltas = {}
  for val_name in DELTAS:
    total_deltas[val_name] = 0
  for comp_id in mins:
    minrow = mins[comp_id]
    maxrow = maxs[comp_id]
    dt = maxrow["timestamp"] - minrow["timestamp"]
    if dt > 0:
      for val_name in DELTAS:
        #TODO: take care of rollover
        total_deltas[val_name] += (maxrow[val_name] - minrow[val_name]) / dt
  a_value = next(iter(DELTAS))
  cu = total_deltas[a_value]
  log.info("%s cu: %f from %d records of %d components",
            a_value, cu, total_records, total_components)
  # save results
  with gCU_lock:
    gCU_avg = total_deltas
  
  if doSaveTables:
    time_now = DT64toTS(np.datetime64('now'))
    row = [time_now]
    for val_name in DELTAS:
      row.append(total_deltas[val_name])
    gCUTable.log(row)


def current_utilization_thread():
  log = logging.getLogger("curr_util")
  log.setLevel(logging.INFO)
  log.info("Current utilization thread started")
  dataSource = SosDataSource()
  dataSource.config(path=PATH)
  
  while True:
    calculate_current_utilization(dataSource, log)
    time.sleep(CU_PERIOD)
  


class MyTCPHandler(SocketServer.BaseRequestHandler):
  """
  The request handler class for our server.

  It is instantiated once per connection to the server, and must
  override the handle() method to implement communication to the
  client.
  """
  
  
  def str_to_variety_id(self, string):
    return hashlib.sha256(string.encode('utf-8')).hexdigest()
    
    
  def handle(self):
    global g_state
    while 1:
      # self.request is the TCP socket connected to the client
      self.data = self.request.recv(MAX).strip()
      TCPlog.info("{} wrote: {}".format(self.client_address[0], self.data))
      
      if not self.data:
        TCPlog.info("closing connection")
        break
      
      resp = {"status":"error"}
      
      req = json.loads(self.data)
      
      resp["req_id"] = req.get("req_id", "error")
      
      if "type" in req:
        req_type = req["type"]
        
        if req_type == "process_job":
            if "job_id" in req and "variety_id" in req:
              resp["status"] = "ACK"
                                      
              m = Message(time.time() + PROCESSING_DELAY,
                          int(req['job_id']),
                          req['variety_id'])
              gMessageQueue.put(m)
            
        elif req_type.startswith("variety_id"):
            if req_type == "variety_id/manual":
              if "variety_name" in req:
                resp["status"] = "OK"
                resp["variety_id"] = self.str_to_variety_id(req["variety_name"])
              else:
                resp["status"] = "error"
                resp["error"] = "variety_name not specified"
            elif req_type == "variety_id/auto":
              # TODO: improve the algorithm
              if "script_args" in req:
                args = req["script_args"]
                if args:
                  resp["status"] = "OK"
                  args_str = json.dumps(args)
                  resp["variety_id"] = self.str_to_variety_id(args_str)
                else:
                  resp["error"] = "script_args is empty"
              else:
                resp["error"] = "script_args is not specified"
            else:
              resp["error"] = "wrond variety_id option"
                
        elif req_type == "usage":
          with gCU_lock:
            cu_avg = gCU_avg
          resp["status"] = "OK"
          # TODO: change to new protocol
          name = DELTAS[0]
          resp["response"] = {"lustre" : format_value(name, cu_avg[name])}
              
        elif req_type == "job_utilization":
            if "variety_id" in req:
              variety_id = req["variety_id"]
              utilization = {}
              for param in PARAMS:
                record = gRecorder.getRecord(variety_id, param)
                if record:
                  avg, var = record[0:2]
                  name = TRANSLATE.get(param, param) 
                  val = avg + K_SIGMA[param] * math.sqrt(var)
                  utilization[name] = format_value(param, val)
              resp["response"] = utilization
              resp["status"] = "OK"
            else:
              resp["error"] = "variety_id not specified"
            
        else:
            resp["status"] = "not implemented"
            
      resp = json.dumps(resp)
      
      TCPlog.info("response: %s", resp)

      resp += "\n"
      self.request.sendall(resp)


def _start_server(host, port):
    
    
    # Create the server, binding to host on port
    server = SocketServer.ThreadingTCPServer((host, port), MyTCPHandler, bind_and_activate=True)
    server.daemon_threads = True;
    
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()


logging.getLogger().setLevel(logging.DEBUG)

if __name__ == "__main__":
    
    host, port = "localhost", 9999
    
    # overwrite host and port is supplied in the command string
    argv = sys.argv
    if len(argv) > 1:
      host = argv[1]
      if len(argv) > 2:
        port = int(argv[2])
    
    # start thread that processes completed jobs 
    th_cj = threading.Thread(target=processing_thread)
    th_cj.daemon = True
    th_cj.start()
    
    # start thread that measures "current utilization"
    th_cu  = threading.Thread(target=current_utilization_thread)
    th_cu.daemon = True
    th_cu.start()
    
    _start_server(host, port)
    
   
