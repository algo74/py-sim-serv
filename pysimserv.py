#
# NOTE: make sure that SOS library is in the path
#
from __future__ import division
import os

print(os.environ['LD_LIBRARY_PATH'])

import time
import SocketServer
import json
import threading
import logging
import hashlib 
import numpy as np

import Queue
import threading

from sosdb import Sos
from numsos.DataSource import SosDataSource

# "constants"
MAX = 1024 * 4 # maximum lenght of network message
QUERY_LIMIT = 4096
OVERFLOW = 1 + 0xffffffffffffffff # uint64 overflow value
ZERO_TIME = np.datetime64('1970-01-01T00:00:00Z')

# parameters to calculate for finished jobs
PROCESSING_DELAY = 20  # seconds
N_TRIES = 10 # number of tries before giving up on a job
DELTAS = set(("user",))
PATH = '/LDMS_data/SOS/sos1/procstat/'

# parameters to calculate current utilization
CU_QUERY_TIME = 10 # seconds
CU_PERIOD = CU_QUERY_TIME/2 # 1/(frequency of updates) (seconds)

# init globals
COLUMNS = ["timestamp", "component_id"]
COLUMNS.extend(DELTAS)

print "COLUMNS: ", COLUMNS

gMessageQueue = Queue.Queue(0)

gCU_lock = threading.Lock()
gCU_avg = {}


def DT64toTS(dt64):
  return (dt64 - ZERO_TIME) / np.timedelta64(1, 's')


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
  
  def __init__(self):
    self.tot_avg = 0
    self.tot_var = 0
    
    
  def finish_node(self):  
    if self.cur_time == self.node_init_time:
      logging.warn("start time and end time are equal for a node")
      return 
    node_time = (self.cur_time - self.node_init_time) #/ np.timedelta64(1, 's')
    node_avg = (self.cur_val - self.node_init_val + self.node_rollover) / node_time
    node_var = self.node_square_total/node_time - node_avg*node_avg
    self.tot_avg += node_avg
    self.tot_var += node_var
    logging.debug("finish node; node_time: %f, node avg: %f, node_square_total: %f, node_var: %f",
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
    logging.debug("init node; value: %f, time: %s", value, str(timestamp))
  
  
  def same_node(self, timestamp, value):
    delta_value = value - self.cur_val
    delta_time = (timestamp - self.cur_time) #/ np.timedelta64(1, 's')
    # in case the value roll over
    if delta_value < 0 :
      logging.info("!!!!!!!!!!!!!!!!!!!!! -- value rollover -- !!!!!!!!!!!!!!!!!")
      delta_value += OVERFLOW
      self.node_rollover += OVERFLOW
    if delta_time < 0 :
      logging.error("timestamp rollover - should not happen")
    self.node_square_total += delta_value * delta_value / delta_time
    self.cur_time = timestamp
    self.cur_val = value
    logging.debug("same_node; value: %f, timestamp: %s, ", value, str(timestamp))
    logging.debug("delta_value: %f, delta_time: %f, square: %f",
                  delta_value, delta_time, delta_value * delta_value / delta_time)
  
  
  def finish_all(self):
    self.finish_node()
    avg = self.tot_avg
    var = self.tot_var
    return (avg, var)



def process_job(dataSource, message):
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
  logging.info("Processing job_id: %i, variety_id: %s", message.job_id, message.variety_id)
  dataSource.select(COLUMNS,
             where=[['job_id', Sos.COND_EQ, message.job_id]],
             order_by='comp_time_job')
  a = dataSource.get_results(limit=1)
  if not a:
    logging.info("No records for the job")
    return False
  
  total_records = 1
  total_components = 1
  cur_comp_id = a.array('component_id')[0]
  #cur_user = a.array('user')[0]
  cur_deltas = {}
  for val_name in DELTAS:
    cur_deltas[val_name] = a.array(val_name)[0]
  cur_time = a.array('timestamp')[0]
  cur_timestamp = DT64toTS(cur_time)
  logging.debug("first time: "+ str(cur_time))
  min_time = cur_timestamp
  max_time = cur_timestamp
  dt_sample = 0
  dt_total = 0
  #user_param = DeltaParameter()
  #user_param.init_node(cur_timestamp, cur_user)
  delta_params = {}
  delta_data = {}
  for val_name in DELTAS:
    delta_params[val_name] = DeltaParameter()
    delta_params[val_name].init_node(cur_timestamp, cur_deltas[val_name])
  while True:
    a = dataSource.get_results(limit=QUERY_LIMIT, reset = False)
    if not a:
      # end cycle 
      break
    
    timestamps = DT64toTS(a.array('timestamp'))
    comp_ids = a.array('component_id')
    #user_data = a.array('user')
    for val_name in DELTAS:
      delta_data[val_name] = a.array(val_name)
    nrecords = len(timestamps)
    total_records += nrecords
    logging.debug("total records so far: %d", total_records)
    for i in range(nrecords):
      
      if comp_ids[i] != cur_comp_id:
        # finish previous component
        max_time = max(max_time, cur_timestamp)
        total_components += 1
        logging.debug("finished component #%d: %d", total_components, cur_comp_id)
        #  start new component id
        cur_comp_id = comp_ids[i]
        cur_timestamp = timestamps[i]
        #cur_user = user_data[i]
        #user_param.new_node(cur_timestamp, cur_user)
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
        #cur_user = user_data[i]
        #user_param.same_node(cur_timestamp, cur_user)
        for val_name in DELTAS:
          cur_deltas[val_name] = delta_data[val_name][i]
          delta_params[val_name].same_node(cur_timestamp, cur_deltas[val_name])
    if nrecords < QUERY_LIMIT:
      # end cycle
      break
  
  logging.debug("finishing last component: %d", cur_comp_id)
  max_time = max(max_time, cur_timestamp)
  dt_avg = dt_total /  dt_sample
  delta_time = 2*dt_avg + (max_time - min_time) #/ np.timedelta64(1, 's')
  #avg, var = user_param.finish_all()
  results = {}
  for val_name in DELTAS:
    results[val_name] = delta_params[val_name].finish_all()
  avg, var = results['user']
  logging.info("d_time: %f, avg: %f, var: %f", delta_time, avg, var)
          
  return True


def processing_thread():
  """ the job for the thread that monitores the queue and processes jobs  from it"""
  logging.info("Job processing thread started")
  src = SosDataSource()
  src.config(path=PATH)
  src.select(['timestamp'],
             where = [['timestamp', Sos.COND_GE, time.time() - 60]],
             order_by='time_comp_job')
  a = src.get_results()
  if a:
    logging.info("Last minute results length: %s", len(a.array('timestamp')))
    src.show(30)
  else:
    logging.info("No recent results")
  
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
        if process_job(src, m):
          break
        time.sleep(PROCESSING_DELAY)
      

def calculate_current_utilization(dataSource):
  global gCU_avg
  global gCU_lock
  a = dataSource.get_results(limit=1, reset = True)
  if not a:
    logging.info("No recent data for current utilization")
    return False
  
  total_records = 1
  total_components = 1
  cur_comp_id = a.array('component_id')[0]
  cur_time = a.array('timestamp')[0]
  cur_timestamp = DT64toTS(cur_time)
  start_timestamp = cur_timestamp
  logging.debug("first time: "+ str(cur_time))
  cur_deltas = {}
  start_deltas = {}
  avg_deltas = {}
  delta_data = {}
  for val_name in DELTAS:
    cur_deltas[val_name] = a.array(val_name)[0]
    start_deltas[val_name] = cur_deltas[val_name]
    avg_deltas[val_name] = 0
  while True:
    a = dataSource.get_results(limit=QUERY_LIMIT, reset = False)
    if not a:
      # end cycle 
      break
    
    timestamps = DT64toTS(a.array('timestamp'))
    comp_ids = a.array('component_id')
    for val_name in DELTAS:
      delta_data[val_name] = a.array(val_name)
    nrecords = len(timestamps)
    total_records += nrecords
    logging.debug("total records so far: %d", total_records)
    for i in range(nrecords):
      if comp_ids[i] != cur_comp_id:
        # finish previous component
        dt = cur_timestamp - start_timestamp
        if dt > 0:
          for val_name in DELTAS:
            #TODO: take care of rollover
            dv = cur_deltas[val_name] - start_deltas [val_name]
            if dv < 0:
              dv += OVERFLOW
            avg_deltas[val_name] += dv / dt
        total_components += 1
        logging.debug("finished component #%d: %d", total_components, cur_comp_id)
        #  start new component id
        cur_comp_id = comp_ids[i]
        cur_timestamp = timestamps[i]
        start_timestamp = cur_timestamp
        for val_name in DELTAS:
          cur_deltas[val_name] = delta_data[val_name][i]
          start_deltas[val_name] = cur_deltas[val_name]
      else:
        # process one more record for current component id
        cur_timestamp = timestamps[i]
        for val_name in DELTAS:
          cur_deltas[val_name] = delta_data[val_name][i]
    if nrecords < QUERY_LIMIT:
      # end cycle
      break
  
  dt = cur_timestamp - start_timestamp
  if dt > 0:
    for val_name in DELTAS:
      #TODO: take care of rollover
      avg_deltas[val_name] += (cur_deltas[val_name] - start_deltas [val_name]) / dt
  total_components += 1
  logging.debug("finishing last component: %d", cur_comp_id)
  avg = avg_deltas['user']
  logging.info("avg: %f", avg)
  # save results
  with gCU_lock:
    gCU_avg = avg_deltas


def current_utilization_thread():
  logging.info("Current utilization thread started")
  dataSource = SosDataSource()
  dataSource.config(path=PATH)
  dataSource.select(COLUMNS,
             where = [['timestamp', Sos.COND_GE, time.time() - CU_QUERY_TIME]],
             order_by='comp_time_job')
  while True:
    calculate_current_utilization(dataSource)
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
      logging.info("{} wrote: {}".format(self.client_address[0], self.data))
      
      if not self.data:
        logging.info("closing connection")
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
          if "request" in req:
            with gCU_lock:
              cu_avg = gCU_avg
            if req["request"] == "lustre":
              resp["status"] = "OK"
              # FIXME: change 'user' to 'lustre'
              resp["response"] = str(cu_avg['user'])
            elif req["request"].startswith("set "):
              ## FIXME: temporary way to change usage
              g_state.lustre_usage = int(req["request"][4,])
              resp["status"] = "Got it"
            else:
              resp["status"] = "not implemented"
              
        elif req_type == "job_utilization":
            if "variety_id" in req:
              resp["status"] = "OK"
              resp["lustre"] = "5000"
              resp["time_limit"] = "5"
            else:
              resp["error"] = "variety_id not specified"
            
        else:
            resp["status"] = "not implemented"
            
      resp = json.dumps(resp)
      
      logging.info("response: %s", resp)

      resp += "\n"
      self.request.sendall(resp)


def _start_server():
    HOST, PORT = "localhost", 9999
    
    # Create the server, binding to HOST on PORT
    server = SocketServer.ThreadingTCPServer((HOST, PORT), MyTCPHandler, bind_and_activate=True)
    server.daemon_threads = True;
    
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()


logging.getLogger().setLevel(logging.DEBUG)

if __name__ == "__main__":
    
    
    # start thread that processes completed jobs 
    th_cj = threading.Thread(target=processing_thread)
    th_cj.daemon = True
    th_cj.start()
    
    # start thread that measures "current utilization"
    th_cu  = threading.Thread(target=current_utilization_thread)
    th_cu.daemon = True
    th_cu.start()
    
    _start_server()
    
   
