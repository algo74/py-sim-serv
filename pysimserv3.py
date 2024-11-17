


#
# Python 3 version of py-sim-serv
#
# NOTE: make sure that SOS library is in the path
#
# from __future__ import division

# import os
# print(os.environ['LD_LIBRARY_PATH'])
# if not os.environ['LD_LIBRARY_PATH']:
#  print('no LD_LIBRARY_PATH')

import copy
import math
import time
import socketserver
import json
import threading
import logging
import hashlib
import numpy as np
import yaml
import argparse
import traceback
import csv
import os

# from analyze_job_option1 import analyze_job
from combined_queue import CombinedQueue
from JSON_fsqueue import JSONFSQueue

from sosdb import Sos
from numsos.DataSource import SosDataSource

from message import Message
from sos_recorder import Recorder, CanaryStore
import table_log
from delta_parameter_totalized import DeltaParameter


def update_dict(base, overrides):
  """
   Update a nested dictionary or similar mapping.
   Modifies ``base`` in-place.
   """
  for k, v in overrides.items():
    base[k] = copy.deepcopy(v)


parser = argparse.ArgumentParser()
parser.add_argument('--production', action='store_true', help='switch to the "production" configuration')
parser.add_argument('-c', '--config_file', type=str, default=None, help="configuration file")
args, _ = parser.parse_known_args()

# reset default production flag from the config file
if args.config_file is not None:
  with open(args.config_file) as conf_file:
    pre_conf = yaml.safe_load(conf_file)
  if 'production' in pre_conf:
    parser.set_defaults(production=pre_conf['production'])
  args, _ = parser.parse_known_args()
###################################################
#
# configuration switch
#
###################################################
is_production = args.production

# TODO: read default address and port from an environmental variable
parser.add_argument('-a', '--address', type=str, default='0.0.0.0',
                    help="address (hostname or ip) to which the server should listen")
parser.add_argument('-p', '--port', type=int, default=9999, help="port to which the server should listen")
parser.add_argument('--FACTORS', type=yaml.safe_load, default={
  "timelimit": 1.0 / 60.0,
  "user": 1,
  "lustre": 10000.0 / 14000000.0
}, help="multipliers for the metrics")
parser.add_argument('--grid_step', type=int, default=10, help="grid step for gridding (in seconds)")
parser.add_argument('--use_canary', type=str, default=None, help="location of canary probe database (None if disabled)")
parser.add_argument('--zero_current_utilization', type=bool, default=False, help='whether to report always zero untilization')

###################################################
#
# parameters for saving to tables
#
###################################################
parser.add_argument('--doSaveTables', type=bool, default=True, help='whether to save diagnostic tables or not')
parser.add_argument('--prefixSaveTables', type=str, default="/xch/results/" if is_production else "",
                    help="location where diagnostic tables are saved")

# TODO: add all parameters to parser


args = parser.parse_args()

if args.config_file is not None:
  # set defaults from the configuration file and re-parse the command line arguments
  with open(args.config_file) as conf_file:
    custom_conf = yaml.safe_load(conf_file)
  parser.set_defaults(**custom_conf)
  args = parser.parse_args()

###################################################
#
# "constants"
#
###################################################
conf = {}
conf['MAX'] = 1024 * 4  # maximum lenght of network message
conf['QUERY_LIMIT'] = 4096  # maximum number of rows to be returned by queries
conf['OVERFLOW'] = 1 + 0xffffffffffffffff  # uint64 overflow value
# DEFAULT_DT = 1 # default interval between samples in seconds

###################################################
#
# parameters for processing of finished jobs
#
###################################################
conf['PROCESSING_DELAY'] = 20  # seconds
conf['N_TRIES'] = 3  # number of tries before giving up on a job
conf['DELTAS'] = ["user", ]
if is_production:
  conf['DELTAS'] = ["lustre", ]
conf['PATH'] = '/LDMS_data/SOS/sos1/procstat/'  # path to the LDMS records
conf['REC_PATH'] = "/LDMS_data/SOS/results"  # path to the DB that keeps summaries of usage
if is_production:
  conf['PATH'] = '/LDMS_data/SOS/sos1/lustre2_client/'
  conf['REC_PATH'] = "/LDMS_data/SOS/results"
# decay constants (alphas) for the metrics
conf['DECAY'] = {
  "timelimit": 0.2,
  "user": 0.2,
  "lustre": 0.2
}
# how many sigmas shall we add to averages when predicting usage
conf['K_SIGMA'] = {
  "timelimit": 0,  # 3,
  "lustre": 0,
  "user": 0  # 1
}
conf['SUM_SOURCES'] = {
  "lustre": ["client.read_bytes#llite.testfs",
             "client.write_bytes#llite.testfs"]
}

###################################################
#
# columns to read from db
#
###################################################
conf['COLUMNS'] = ["timestamp", "component_id", 'user']
if is_production:
  conf['COLUMNS'] = ["timestamp",
                     "component_id",
                     "client.read_bytes#llite.testfs",
                     "client.write_bytes#llite.testfs"]

###################################################
#
# additional parameters for calculating current utilization
#
###################################################
conf['CU_QUERY_TIME'] = 10  # seconds
conf['CU_PERIOD'] = conf['CU_QUERY_TIME'] / 2  # 1/(frequency of updates) (seconds)

###################################################
#
# additional parameters for the canary probe
#
###################################################
conf['wiggle_time'] = 10  # seconds

###################################################
#
# communication protocol related parameters
#
###################################################
# names for the metrics that should be used in communications
conf['TRANSLATE'] = {
  "timelimit": "time_limit",
  "user": "lustre"
}

#  Parameters for communication through file system
conf['file_queue_path'] = None
conf['file_canary_queue_path'] = None
conf['CANARY_FS_PROCESSING_DELAY'] = 20  # seconds

###################################################

conf.update(vars(args))

###################################################
#
# calculated parameters (not configurable)
#
###################################################

conf['ZERO_TIME'] = np.datetime64('1970-01-01T00:00:00Z', 'us')  # used to convert date to int (microseconds)

conf['PARAMS'] = list(conf['DELTAS'])
conf['PARAMS'].append("timelimit")


# functions to calculate data for each deltas
def simple_array(data_source_result, val_name):
  return data_source_result.array(val_name)


def sum_array(data_source_result, val_name):
  res = 0
  for name in conf['SUM_SOURCES'][val_name]:
    res = data_source_result.array(name) + res
  return res


conf['DELTAS_DATA_GETTERS'] = {
  'user': simple_array,
  'lustre': sum_array
}

print("VVVVV Config VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV")
print(yaml.dump(conf))
print("^^^^^ Config ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^", flush=True)


###################################################
#
# globals
#
###################################################


gMessageQueue = CombinedQueue(conf['file_queue_path'])

gCU_lock = threading.Lock()
gCU_avg = dict.fromkeys(conf['DELTAS'], 0.0)

gRecorder = Recorder(conf['REC_PATH'])
gCanaryStore = CanaryStore(conf['use_canary']) if conf['use_canary'] else None

logging.basicConfig(format='[%(asctime)s] %(levelname)s:%(name)s: %(message)s')
TCPlog = logging.getLogger("TCP")
TCPlog.setLevel(logging.DEBUG)

if 'hostlist' in conf and conf['hostlist']:
  # read hostlist from file
  with open(conf['hostlist']) as f:
    reader = csv.reader(f)
    gHostList = {str(row[0]): int(row[1]) for row in list(reader)}
else:
  gHostList = None

if conf['doSaveTables']:
  prefixSaveTables = conf['prefixSaveTables']
  # job table
  titles = ["job_id", "variety_id", "start_time", "end_time", "dt"]
  for val_name in conf['DELTAS']:
    titles.extend([val_name + "_avg", val_name + "_var"])
  if conf['use_canary']:
    titles.append("canary_time")
  gJobsTable = table_log.TableLog(prefixSaveTables + "jobs_results_table.csv", title=titles)
  # Summary table
  gSummaryTable = table_log.TableLog(prefixSaveTables + "summary_results_table.csv",
                                     title=["variety_id", "time", "param", "avg", "var"])
  # current utilization table
  # NOTE: this table is now defined locally in current utilization thread TODO: remove
  # titles = ["time"]
  # for val_name in conf['DELTAS']:
  #   titles.extend([val_name])
  # # if conf['use_canary']:
  # #   titles.append('canary_time')
  # gCUTable = table_log.TableLog(prefixSaveTables + "current_utilization_table.csv", title=titles)
  # TODO: improve for many parameters (add column with metric name)
  gNodeLog = table_log.TableLog(prefixSaveTables + "node_allocation_log.csv",
                                title=["job_id", "component_id", "start_time", "end_time", "avg_metric"])
  # canary probe table
  if conf['use_canary']:
    gCanaryTable = table_log.TableLog(prefixSaveTables + "canary_probe_table.csv", title=["time", "ost", "value"])
  else:
    gCanaryTable = None
else:
  gNodeLog = None
  gCanaryTable = None

###################################################
#
# the rest
#
###################################################

def DT64toTS(dt64):
  # return (dt64 - conf['ZERO_TIME']) / np.timedelta64(1, 's') if dt64.size else dt64
  return (dt64 - conf['ZERO_TIME']) / np.timedelta64(1, 's')


def format_value(param, val):
  return str(int(math.ceil(val * conf['FACTORS'][param])))


def get_components_from_string(s):
  """
  Convert a string of comma separated numbers and ranges to a list of numbers
  with prefix outside the brackets. A single hostname may have not bracket.
  Example:
  "pre1[1,3-5],pre27,pre3[8-10]" -> [pre11,pre12,pre14,pre15,pre27,pre38,pre39,pre310]
  """
  s = s.strip()
  if not s:
    return []
  # split by comma
  comps = s.split(',')
  res = []
  prefix = ''
  for comp in comps:
    comp = comp.strip()
    if '[' in comp:
      if prefix:
        raise ValueError("Unexpected '[' in {}".format(comp))
      prefix, comp = comp.split('[')
      if not prefix:
        raise ValueError("Empty prefix before [{}".format(comp))
      if ']' in prefix:
        raise ValueError("Unexpected ']' in {}".format(prefix))
      if not comp:
        raise ValueError("Empty field after {}[".format(prefix))
    if ']' in comp:
      if not prefix:
        raise ValueError("Mismatched ']' in {}".format(comp))
      comp, suffix = comp.split(']')
      if not comp:
        raise ValueError("Empty field before ]{}".format(suffix))
      if suffix:
        raise ValueError("Unexpected {} after ']'".format(suffix))
      finish_prefix = True
    else:
      finish_prefix = False
    # split by dash
    if '-' in comp:
      a, b = comp.split('-')
      res.extend([prefix+str(i) for i in range(int(a), int(b) + 1)])
    else:
      if not comp:
        raise ValueError("Empty field")
      res.append(prefix + comp)
    if finish_prefix:
      prefix = ''

  # convert to component ids
  if gHostList:
    res = [gHostList[str(i)] for i in res]
  else:
    res = [int(i) for i in res]
  return res

class State:

  def __init__(self):
    self.lustre_usage = 103


g_state = State()


# class DeltaParameter:
#   """
#
#   TODO: use delta_parameter_totalized.py instead
#
#   Assuming that the total parameter is the sum of the parameters of the nodes
#
#   Average is the sum of averages of the nodes
#   Variance is the sum ov variances of the nodes
#
#   NOTE:
#   "timestamp" which methods require must be a number, not a datatime64 or such
#   """
#
#   def __init__(self, log=None, nodeLog=None, nodeLogPrefill=tuple()):
#     self.tot_avg = 0
#     self.tot_var = 0
#     if log:
#       self.log = log
#     else:
#       self.log = logging.getLogger("DeltaParameter")
#       self.log.setLevel(logging.DEBUG)
#     self.nodeLog = nodeLog
#     self.nodeLogPrefill = nodeLogPrefill
#
#   def finish_node(self):
#     if self.cur_time == self.node_init_time:
#       self.log.warn("start time and end time are equal for a node")
#       return
#     node_time = (self.cur_time - self.node_init_time) #/ np.timedelta64(1, 's')
#     node_avg = (self.cur_val - self.node_init_val + self.node_rollover) / node_time
#     node_var = self.node_square_total/node_time - node_avg*node_avg
#     self.tot_avg += node_avg
#     self.tot_var += node_var
#     self.log.debug("finish node; node_time: %f, node avg: %f, node_square_total: %f, node_var: %f",
#                   node_time, node_avg, self.node_square_total, node_var)
#     if self.nodeLog:
#       nodeLogLine = []
#       nodeLogLine.extend(self.nodeLogPrefill)
#       nodeLogLine.extend([self.node_name, self.node_init_time, self.cur_time, node_avg])
#       self.nodeLog.log(nodeLogLine)
#
#
#   def new_node(self, timestamp, value, node_name):
#     self.finish_node()
#     self.init_node(timestamp, value, node_name)
#
#
#   def init_node(self, timestamp, value, node_name):
#     self.node_name = node_name
#     self.node_square_total = 0
#     self.node_init_val = value
#     self.node_init_time = timestamp
#     self.cur_time = timestamp
#     self.cur_val = value
#     self.node_rollover = 0
#     self.log.debug("init node; value: %f, time: %s", value, str(timestamp))
#
#
#   def same_node(self, timestamp, value):
#     self.log.debug("same_node; value: %f, timestamp: %s, ", value, str(timestamp))
#     delta_value = value - self.cur_val
#     delta_time = (timestamp - self.cur_time) #/ np.timedelta64(1, 's')
#     # in case the value roll over
#     if delta_value < 0 :
#       self.log.info("!!!!!!!!!!!!!!!!!!!!! -- value rollover -- !!!!!!!!!!!!!!!!!")
#       delta_value += conf['OVERFLOW']
#       self.node_rollover += conf['OVERFLOW']
#     if delta_time < 0 :
#       self.log.error("timestamp rollover - should not happen")
#     self.cur_time = timestamp
#     self.cur_val = value
#     if delta_time > 0:
#       square = delta_value * delta_value / delta_time
#       self.node_square_total += square
#     else:
#       square = 0
#     self.log.debug("delta_value: %f, delta_time: %f, square: %f",
#                   delta_value, delta_time, square)
#
#
#   def finish_all(self):
#     self.finish_node()
#     avg = self.tot_avg
#     var = self.tot_var
#     return (avg, var)


def analyze_job(dataSource, job_id, log, nodeLog=None):
  log.debug("Analyzing job_id: %i", job_id)
  dataSource.select(conf['COLUMNS'],
                    where=[['job_id', Sos.COND_EQ, job_id]],
                    desc=True,
                    order_by='job_time_comp')
  a = dataSource.get_results(limit=1)
  if not a:
    log.info("No records for the job %i", job_id)
    return None
  max_time = DT64toTS(a.array('timestamp')[0])

  dataSource.select(conf['COLUMNS'],
                    where=[['job_id', Sos.COND_EQ, job_id]],
                    order_by='job_time_comp')
  a = dataSource.get_results(limit=1)
  min_time = DT64toTS(a.array('timestamp')[0])

  if min_time >= max_time:
    log.info("min time(%i) is not less than max time(%i) for the job %i", min_time, max_time, job_id)
    return None

  dataSource.select(conf['COLUMNS'],
                    where=[['job_id', Sos.COND_EQ, job_id]],
                    order_by='job_comp_time')
  a = dataSource.get_results(limit=1)
  total_records = 1
  total_components = 1
  cur_comp_id = a.array('component_id')[0]
  cur_deltas = {}
  for val_name in conf['DELTAS']:
    cur_deltas[val_name] = conf['DELTAS_DATA_GETTERS'][val_name](a, val_name)[0]
  cur_time = a.array('timestamp')[0]
  cur_timestamp = DT64toTS(cur_time)
  log.debug("first time: " + str(cur_time))
  # min_time = cur_timestamp
  # max_time = cur_timestamp
  dt_sample = 0
  dt_total = 0
  delta_params = {}
  delta_data = {}
  for val_name in conf['DELTAS']:
    delta_params[val_name] = DeltaParameter(min_time, max_time, conf['grid_step'], log, nodeLog, (job_id,))
    delta_params[val_name].init_node(cur_timestamp, cur_deltas[val_name], cur_comp_id)
  while True:
    a = dataSource.get_results(limit=conf['QUERY_LIMIT'], reset=False)
    if not a:
      # end cycle
      break

    timestamps = DT64toTS(a.array('timestamp'))
    comp_ids = a.array('component_id')
    for val_name in conf['DELTAS']:
      delta_data[val_name] = conf['DELTAS_DATA_GETTERS'][val_name](a, val_name)
    nrecords = len(timestamps)
    total_records += nrecords
    log.debug("total records so far: %d", total_records)
    for i in range(nrecords):
      if comp_ids[i] != cur_comp_id:
        # finish previous component
        max_time = max(max_time, cur_timestamp)
        log.debug("finished component #%d: %d", total_components, cur_comp_id)
        total_components += 1
        #  start new component id
        cur_comp_id = comp_ids[i]
        cur_timestamp = timestamps[i]
        for val_name in conf['DELTAS']:
          cur_deltas[val_name] = delta_data[val_name][i]
          delta_params[val_name].new_node(cur_timestamp, cur_deltas[val_name], cur_comp_id)
        min_time = min(min_time, cur_timestamp)
      else:
        # process one more record for current component id
        new_timestamp = timestamps[i]
        dt_total += new_timestamp - cur_timestamp
        dt_sample += 1
        cur_timestamp = new_timestamp
        for val_name in conf['DELTAS']:
          cur_deltas[val_name] = delta_data[val_name][i]
          delta_params[val_name].same_node(cur_timestamp, cur_deltas[val_name])
    if nrecords < conf['QUERY_LIMIT']:
      # end cycle
      break

  log.debug("finishing last component: %d", cur_comp_id)
  max_time = max(max_time, cur_timestamp)
  if dt_sample == 0 or dt_total == 0:
    log.info("No useful records for the job %i", job_id)
    return None

  dt_avg = dt_total / dt_sample
  delta_time = 2 * dt_avg + (max_time - min_time)  # / np.timedelta64(1, 's')
  results = {}
  for val_name in conf['DELTAS']:
    results[val_name] = delta_params[val_name].finish_all()
  if conf['use_canary']:
    results['canary_time'] = (gCanaryStore.getAverageValue(min_time,
                                                           max_time,
                                                           conf['wiggle_time'],
                                                           conf['QUERY_LIMIT']),
                              0)
  a_value = next(iter(conf['DELTAS']))
  avg, var = results[a_value]
  log.info("d_time: %f, %s avg: %f, var: %f", delta_time, a_value, avg, var)

  return min_time, max_time, delta_time, results


def _sample_inbetween(pre, post, between):
  between = DT64toTS(between)
  if pre:
    pre_time = DT64toTS(pre.array('timestamp')[0])
    pre_val = [conf['DELTAS_DATA_GETTERS'][val_name](pre, val_name)[0] for val_name in conf['DELTAS']]
  if post:
    post_time = DT64toTS(post.array('timestamp')[0])
    post_val = [conf['DELTAS_DATA_GETTERS'][val_name](post, val_name)[0] for val_name in conf['DELTAS']]
  if pre:
    if post:
      pre_weight = (post_time - between) / (post_time - pre_time)
      post_weight = 1 - pre_weight
      between_val = [pre_val[i] * pre_weight + post_val[i] * post_weight for i in range(len(pre_val))]
      rc = 3
    else:
      between_val = pre_val
      rc = 1
  elif post:
    between_val = post_val
    rc = 2
  else:
    between_val = None
    rc = 0
  logging.debug("between_val: %s", str(between_val))
  return between_val, rc


def _convert_DataSet_to_str(data_set):
  if data_set is None:
    return "None"
  res = ", ".join([val_name + ": " + str(data_set.array(val_name)[0]) for val_name in conf['COLUMNS']])
  return res


def alt_analyze_job(dataSource, message, log, nodeLog=None):
  log.debug("Alt. analyzing job_id: %i", message.job_id)
  try:
    min_time = np.datetime64(message.job_start, 'us') # - np.timedelta64(1, 's') # NOTE: we DON'T take one second before the start
    min_ts = DT64toTS(min_time)
    max_time = np.datetime64(message.job_end, 'us') + np.timedelta64(1, 's')   # NOTE: we take one second after the end
    max_ts = DT64toTS(max_time)
    log.debug("min_time: %s(%f), max_time: %s(%f)", str(min_time), min_ts, str(max_time), max_ts)
    job_id = message.job_id
    components = get_components_from_string(message.job_nodes)
    log.debug("components: %s", str(components))
  except Exception as e:
    log.error("Could not parse message: %s", str(message))
    log.error("Exception: %s", str(e))
    return None

  if min_time >= max_time:
    log.info("min time(%s) is not less than max time(%s) for the job %i",
             str(min_time), str(max_time), job_id)
    return None

  duration = max_ts - min_ts
  deltas = None # will be an array later
  # NOTE: we only calcuate average for the deltas (no variance)
  total_components = 0
  for comp_id in components:
    # get the value before min_time
    dataSource.select(
        conf['COLUMNS'],
        where=[['timestamp', Sos.COND_LE, min_time],
               ['component_id', Sos.COND_EQ, comp_id]],
        desc=True,
        # NOTE: we use comp_time_job instead of time_comp_job
        # because with time_comp_job, the search stops at first record that matches time and component_id less than comp_id
        order_by='comp_time_job')
    pre_min = dataSource.get_results(limit=1, reset=True)
    log.debug("Component %i pre_min: %s", comp_id, _convert_DataSet_to_str(pre_min))
    # get the value after min_time
    dataSource.select(
        conf['COLUMNS'],
        where=[['timestamp', Sos.COND_GE, min_time],
               ['component_id', Sos.COND_EQ, comp_id]],
        desc=False,
        order_by='comp_time_job')
    post_min = dataSource.get_results(limit=1, reset=True)
    log.debug("Component %i post_min: %s", comp_id, _convert_DataSet_to_str(post_min))
    # get the value before max_time
    dataSource.select(
        conf['COLUMNS'],
        where=[['timestamp', Sos.COND_LE, max_time],
               ['component_id', Sos.COND_EQ, comp_id]],
        desc=True,
        order_by='comp_time_job')
    pre_max = dataSource.get_results(limit=1, reset=True)
    log.debug("Component %i pre_max: %s", comp_id, _convert_DataSet_to_str(pre_max))
    # get the value after max_time
    dataSource.select(
        conf['COLUMNS'],
        where=[['timestamp', Sos.COND_GE, max_time],
               ['component_id', Sos.COND_EQ, comp_id]],
        desc=False,
        order_by='comp_time_job')
    post_max = dataSource.get_results(limit=1, reset=True)
    log.debug("Component %i post_max: %s", comp_id, _convert_DataSet_to_str(post_max))

    if pre_max and pre_max.array('timestamp')[0] <= min_time:
      pre_max = None
    if post_min and post_min.array('timestamp')[0] >= max_time:
      post_min = None

    if (pre_min or post_min) and (pre_max or post_max):
      # can proceed with this component
      start_val, rc = _sample_inbetween(pre_min, post_min, min_time)
      if rc == 0:
        log.error(
            "Second check (alt_analyze_job): could not find start value for job %i component %i",
            job_id, comp_id)
        continue
      elif rc != 3:
        log.warning("alt_analyze_job: rc=%i for start sample for job %i component %i", rc, job_id, comp_id)
      end_val, rc = _sample_inbetween(pre_max, post_max, max_time)
      if rc == 0:
        log.error(
            "Second check (alt_analyze_job): could not find end value for job %i component %i",
            job_id, comp_id)
        continue
      elif rc != 3:
        log.warning("alt_analyze_job: rc=%i for end sample for job %i component %i", rc, job_id, comp_id)
      total_components += 1
      comp_deltas = [(end_val[i] - start_val[i]) / duration for i in range(len(start_val))]
      log.debug("Component #%i(%i) totals: %s, deltas: %s",
                total_components, comp_id, str([(end_val[i] - start_val[i]) for i in range(len(start_val))]), str(comp_deltas))
      deltas = np.add(deltas, comp_deltas) if deltas else comp_deltas
    else:
      if not pre_max and not post_max:
        log.warning("alt_analyze_job: could not find end value for job %i component %i", job_id, comp_id)
      if not pre_min and not post_min:
        log.warning("alt_analyze_job: could not find start value for job %i component %i", job_id, comp_id)
        # skip this component
  # make the results
  if total_components == 0:
    log.warning("No useful records for the job %i", job_id)
    return None
  results = {}
  for i in range(len(conf['DELTAS'])):
    results[conf['DELTAS'][i]] = deltas[i], 0 # average, variance
  a_value = next(iter(conf['DELTAS']))
  log.info("d_time: %f, %s avg: %f, var: %f", duration, a_value, results[a_value][0], results[a_value][1])

  if conf['use_canary']:
    results['canary_time'] = (gCanaryStore.getAverageValue(min_ts,
                                                          max_ts,
                                                          conf['wiggle_time'],
                                                          conf['QUERY_LIMIT']),
                              0)

  return min_ts, max_ts, duration, results



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
  log.info("Job start: %s, end: %s, nodes: %s",str(message.job_start), str(message.job_end), str(message.job_nodes))
  if message.job_start is None or message.job_end is None or message.job_nodes is None:
    res = analyze_job(dataSource, message.job_id, log, gNodeLog)
  else:
    res = alt_analyze_job(dataSource, message, log, gNodeLog)
  if res is None:
    return res

  min_time, max_time, delta_time, results = res
  if conf['doSaveTables']:
    row = [message.job_id, message.variety_id, min_time, max_time, delta_time]
    for val_name in conf['DELTAS']:
      row.extend(results[val_name])
    if conf['use_canary']:
      row.append(results['canary_time'][0])  # add only average so far
    gJobsTable.log(row)
  return delta_time, results


def normalizeRecord(record):
  return record if record else (0.0, 0.0, 0.0, 0.0)


def update_param(variety_id, param_name, avg, var):
  # logging.debug("arg types: %s, %s", str(type(avg)), str(type(var)))
  alpha = conf['DECAY'][param_name]
  pAvg, pVar, pCount, pSum = normalizeRecord(gRecorder.getRecord(variety_id, param_name))
  # logging.debug("param types: %s, %s, %s, %s", str(type(pAvg)), str(type(pVar)), str(type(pCount)), str(type(pSum)))
  nCount = 1 + (1 - alpha) * pCount
  nSum = avg + (1 - alpha) * pSum
  nAvg = nSum / nCount
  nVar = (var + (avg - nAvg) ** 2 + (1 - alpha) * pCount * (pVar + (pAvg - nAvg) ** 2)) / nCount
  if conf['doSaveTables']:
    time_now = DT64toTS(np.datetime64('now'))
    gSummaryTable.log([variety_id, time_now, param_name, nAvg, nVar])
  gRecorder.saveRecord(variety_id, param_name, nAvg, nVar, nCount, nSum)


def processing_thread():
  """ the job for the thread that monitores the queue and processes jobs  from it"""
  log = logging.getLogger("job_util")
  log.setLevel(logging.DEBUG)
  log.info("Job processing thread started")
  src = SosDataSource()
  src.config(path=conf['PATH'])
  src.select(['timestamp'],
             where=[['timestamp', Sos.COND_GE, time.time() - 60]],
             order_by='time_comp_job')
  a = src.get_results()
  if a:
    log.info("Last minute results length: %s", len(a.array('timestamp')))
    # src.show(30)
  else:
    log.info("No recent results")

  while True:
    if gMessageQueue.empty():
      time.sleep(conf['PROCESSING_DELAY'])
    else:
      m = gMessageQueue.get()
      now = time.time()
      if now < m.time:
        time.sleep(m.time - now)
      # try processing 3 times
      for _ in range(conf['N_TRIES']):
        # process job data
        result = process_job(src, m, log)
        if result != None:
          break
        time.sleep(conf['PROCESSING_DELAY'])
      if not result:
        log.error("Could not find any records for job %d, giving up", m.job_id)
      else:
        dt, param_results = result
        # process timelimit
        update_param(m.variety_id, 'timelimit', dt, 0)
        # TODO use canary to calculate loads
        if 'canary_time' in param_results:
          del param_results['canary_time']
        for param_name in param_results:
          avg, var = param_results[param_name]
          update_param(m.variety_id, param_name, avg, var)


def process_canary_probe(req, resp):
  for param in ['start_time', 'end_time', 'OST', 'duration']:
    if param not in req:
      resp["status"] = "error"
      resp["error"] = "{} not specified".format(param)
      return resp

  start_time = int(req['start_time'])
  end_time = int(req['end_time'])
  OST = int(req['OST'])
  duration = float(req['duration'])
  time = (start_time + end_time) // 2
  gCanaryStore.saveRecord(time, OST, duration)
  if conf['doSaveTables']:
    gCanaryTable.log((time, OST, duration))
  resp["status"] = "ACK"
  return resp


def canary_processing_tread():
  """ the job for the thread that monitores the queue and processes jobs  from it"""
  log = logging.getLogger("canary_processing_tread")
  log.setLevel(logging.INFO)
  log.info("Canary processing thread started")
  try:
    queue = JSONFSQueue(conf['file_canary_queue_path'])
  except:
    log.error("could not open canary FS queue: {}. Aborting...".format(conf['file_canary_queue_path']))
    return

  while True:
    if queue.empty():
      time.sleep(conf['CANARY_FS_PROCESSING_DELAY'])
    else:
      req = "<not converted>"
      try:
        req = queue.get()
        resp = {}
        process_canary_probe(req, resp)
        if resp['status'] != 'ACK':
          raise resp['status']
      except:
        log.error("could not process canary fs request: {}".format(str(req)))
      log.debug("Successfully processed canary fs request: {}".format(str(req)))


def calculate_current_utilization(dataSource, log, table, privateNodes=None):
  global gCU_avg
  global gCU_lock
  if not privateNodes:
    privateNodes = []
  cur_time = time.time() - conf['CU_QUERY_TIME']
  dataSource.select(conf['COLUMNS'],
                    where=[['timestamp', Sos.COND_GE, cur_time]],
                    order_by='time_comp_job')

  delta_data = {}

  total_records = 0
  total_components = 0
  mins = {}
  maxs = {}
  a = dataSource.get_results(limit=conf['QUERY_LIMIT'], reset=True)

  while a:
    timestamps = DT64toTS(a.array('timestamp'))
    comp_ids = a.array('component_id')
    for val_name in conf['DELTAS']:
      delta_data[val_name] = conf['DELTAS_DATA_GETTERS'][val_name](a, val_name)
    nrecords = len(timestamps)
    total_records += nrecords
    log.debug("total records so far: %d", total_records)
    for i in range(nrecords):
      cur_comp_id = comp_ids[i]
      # prepare the record
      record = {"timestamp": timestamps[i]}
      for val_name in conf['DELTAS']:
        record[val_name] = delta_data[val_name][i]
      # if the first row for this component, add it to mins
      if cur_comp_id not in mins:
        total_components += 1
        mins[cur_comp_id] = record
      # update maxs
      maxs[cur_comp_id] = record
    if nrecords < conf['QUERY_LIMIT']:
      # end cycle
      break

    a = dataSource.get_results(limit=conf['QUERY_LIMIT'], reset=False)

  if total_records == 0:
    log.info("No recent data for current utilization")
    return False

  # now process each component
  total_deltas = {}
  total_priv_deltas = {}
  for val_name in conf['DELTAS']:
    total_deltas[val_name] = 0
    total_priv_deltas[val_name] = 0
  for comp_id in mins:
    minrow = mins[comp_id]
    maxrow = maxs[comp_id]
    dt = maxrow["timestamp"] - minrow["timestamp"]
    if dt > 0:
      for val_name in conf['DELTAS']:
        # TODO: take care of rollover
        cur_delta = (maxrow[val_name] - minrow[val_name]) / dt
        if cur_delta < 0:
          log.warning("Negative delta for %s in component %s: %d at %d and %d at %d", 
          val_name, str(comp_id), minrow[val_name], minrow["timestamp"], maxrow[val_name], maxrow["timestamp"])
          cur_delta = 0
        total_deltas[val_name] += cur_delta
        if comp_id in privateNodes:
          total_priv_deltas[val_name] += cur_delta
  a_value = next(iter(conf['DELTAS']))
  cu = total_deltas[a_value]
  pcu = total_priv_deltas[a_value]
  log.info("[%s] %s cu: %f (private: %f) from %d records of %d components",
           str(cur_time), a_value, cu, pcu, total_records, total_components)
  # save results
  with gCU_lock:
    gCU_avg = total_deltas

  if conf['doSaveTables']:
    time_now = DT64toTS(np.datetime64('now'))
    row = [time_now]
    for val_name in conf['DELTAS']:
      row.append(total_deltas[val_name])
      if privateNodes:
        row.append(total_priv_deltas[val_name])
    table.log(row)


def current_utilization_thread():
  log = logging.getLogger("curr_util")
  log.setLevel(logging.INFO)
  log.info("Current utilization thread started")
  dataSource = SosDataSource()
  dataSource.config(path=conf['PATH'])

  # get private nodes from env. variable $SLURM_JOB_NODELIST 
  # (but only if doSaveTables is True; otherwise, we have no use for it)
  privateNodes = None
  if conf['doSaveTables']:
    try:
      privateNodes = get_components_from_string(os.environ.get('SLURM_JOB_NODELIST', ''))
    except Exception as e:
      log.error("Could not parse SLURM_JOB_NODELIST: %s", str(e))
      privateNodes = None
    # create table for current utilization
    titles = ["time"]
    for val_name in conf['DELTAS']:
      titles.extend([val_name])
      if privateNodes:
        titles.extend([val_name + "_priv"])
    # if conf['use_canary']:
    #   titles.append('canary_time')
    CUTable = table_log.TableLog(
        conf['prefixSaveTables'] + "current_utilization_table.csv",
        title=titles)

  while True:
    calculate_current_utilization(dataSource, log, CUTable, privateNodes)
    time.sleep(conf['CU_PERIOD'])


class MyTCPHandler(socketserver.BaseRequestHandler):
  """
  The request handler class for our server.

  It is instantiated once per connection to the server, and must
  override the handle() method to implement communication to the
  client.
  """

  def str_to_variety_id(self, string):
    return hashlib.sha256(string.encode('utf-8')).hexdigest()

  def handle(self):
    global g_state  # TODO: Not used

    while 1:
      # self.request is the TCP socket connected to the client
      self.data = self.request.recv(conf['MAX']).strip()
      TCPlog.info("{} wrote: {}".format(self.client_address[0], self.data))

      if not self.data:
        TCPlog.info("closing connection")
        break

      resp = {"status": "error"}

      try:
        req = json.loads(self.data)
      except json.decoder.JSONDecodeError:
        resp["error"] = "JSON decode error"
        req = {}

      resp["req_id"] = req.get("req_id", "error")

      if "type" in req:
        req_type = req["type"]

        try:
          if req_type == "process_job":
            if "job_id" in req and "variety_id" in req:
              if "job_start" in req or "job_end" in req or "job_nodes" in req:
                if "job_start" in req and "job_end" in req and "job_nodes" in req:
                  m = Message(time.time() + conf['PROCESSING_DELAY'],
                              int(req['job_id']),
                              req['variety_id'],
                              req['job_start'],
                              req['job_end'],
                              req['job_nodes'])
                  gMessageQueue.put(m)
                  resp["status"] = "ACK"
                else:
                  resp["status"] = "error"
                  resp["error"] = "job_start, job_end, and job_nodes must be specified together"
              else:
                m = Message(time.time() + conf['PROCESSING_DELAY'],
                            int(req['job_id']),
                            req['variety_id'])
                gMessageQueue.put(m)
                resp["status"] = "ACK"

          elif req_type == "analyze_job":
            if "job_id" not in req:
              resp["status"] = "error"
              resp["error"] = "job_id not specified"
            else:
              job_id = int(req['job_id'])
              resp['job_id'] = job_id
              datasource = SosDataSource()
              datasource.config(path=conf['PATH'])
              res = analyze_job(datasource, job_id, TCPlog)
              if res is None:
                resp["status"] = "error"
                resp["error"] = "no data for the job"
              else:
                min_time, max_time, delta_time, results = res
                resp["status"] = ""
                resp['start_time'] = min_time
                resp['end_time'] = max_time
                resp['duration'] = delta_time
                for k in results:
                  name = conf['TRANSLATE'].get(k, k)
                  avg, var = results[k]
                  resp[name] = {'avg': format_value(k, avg), 'std': format_value(k, math.sqrt(var))}


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
            name = conf['DELTAS'][0]
            resp["response"] = {"lustre" : "0" if conf['zero_current_utilization'] else format_value(name, cu_avg[name])}

          elif req_type == "job_utilization":
            if "variety_id" in req:
              variety_id = req["variety_id"]
              utilization = {}
              for param in conf['PARAMS']:
                record = gRecorder.getRecord(variety_id, param)
                if record:
                  avg, var = record[0:2]
                  name = conf['TRANSLATE'].get(param, param)
                  val = avg + conf['K_SIGMA'][param] * math.sqrt(var)
                  utilization[name] = format_value(param, val)
              resp["response"] = utilization
              resp["status"] = "OK"
            else:
              resp["error"] = "variety_id not specified"

          elif req_type == 'process_canary_probe':
            if gCanaryStore is None:
              resp["status"] = "error"
              resp["error"] = "Not configured"
            else:
              process_canary_probe(req, resp)
          else:
            resp["status"] = "not implemented"
        except Exception as err:
          resp["status"] = "error"
          resp["error"] = "Exception: {}".format(err)
          print("---- Error caught... ----")
          traceback.print_exc()
          print("---- ...continuing  -----")

      resp = json.dumps(resp)

      TCPlog.info("response: %s", resp)

      resp += "\n"
      self.request.sendall(resp.encode('utf-8'))


def _start_server(host, port):
  # prepare global state

  # Create the server, binding to host on port
  server = socketserver.ThreadingTCPServer((host, port), MyTCPHandler, bind_and_activate=True)
  server.daemon_threads = True

  # Activate the server; this will keep running until you
  # interrupt the program with Ctrl-C
  server.serve_forever()


logging.getLogger().setLevel(logging.DEBUG)

if __name__ == "__main__":

  host = args.address
  port = args.port

  # start thread that processes completed jobs
  th_cj = threading.Thread(target=processing_thread)
  th_cj.daemon = True
  th_cj.start()

  # start thread that processes completed jobs
  if gCanaryStore and conf['file_canary_queue_path']:
    th_canary = threading.Thread(target=canary_processing_tread)
    th_canary.daemon = True
    th_canary.start()

  # start thread that measures "current utilization"
  th_cu = threading.Thread(target=current_utilization_thread)
  th_cu.daemon = True
  th_cu.start()

  _start_server(host, port)
