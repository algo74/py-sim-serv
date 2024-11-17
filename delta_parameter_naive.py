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

import logging

from pysimserv3 import conf


class DeltaParameter:
  """

  TODO: use delta_parameter_totalized.py instead

  Assuming that the total parameter is the sum of the parameters of the nodes

  Average is the sum of averages of the nodes
  Variance is the sum ov variances of the nodes

  NOTE:
  "timestamp" which methods require must be a number, not a datatime64 or such
  """

  def __init__(self, start, end, step, log=None, nodeLog=None, nodeLogPrefill=tuple()):
    self.tot_avg = 0
    self.tot_var = 0
    if log:
      self.log = log
    else:
      self.log = logging.getLogger("DeltaParameter")
      self.log.setLevel(logging.DEBUG)
    self.nodeLog = nodeLog
    self.nodeLogPrefill = nodeLogPrefill

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
    if self.nodeLog:
      nodeLogLine = []
      nodeLogLine.extend(self.nodeLogPrefill)
      nodeLogLine.extend([self.node_name, self.node_init_time, self.cur_time, node_avg])
      self.nodeLog.log(nodeLogLine)


  def new_node(self, timestamp, value, node_name):
    self.finish_node()
    self.init_node(timestamp, value, node_name)


  def init_node(self, timestamp, value, node_name):
    self.node_name = node_name
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
      delta_value += conf['OVERFLOW']
      self.node_rollover += conf['OVERFLOW']
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