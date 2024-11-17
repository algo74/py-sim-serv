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

import math
import logging
import numpy as np

# from pysimserv3 import conf


class DeltaParameter:
  """
  Discretize data, calculate total profile,
  then calculate average and variance of the total profile

  NOTE:
  "timestamp" which methods require must be a number, not a datatime64 or such
  """

  def __init__(self, start, end, step, log=None, nodeLog=None, nodeLogPrefill=tuple()):
    """
    :param start: min grid time
    :param end: max gird time
    :param step: grid step
    :param log: logger for messages (from logging)
    :param nodeLog: logger for node for analysis (outputs to csv)
    :param nodeLogPrefill: tuple of values to be prepended to the nodeLog line
    """
    assert start < end
    assert step > 0
    self.n_points = max(1, round((end - start) / step))
    self.start = start
    self.step = (end - start) / self.n_points
    self.end = end
    self.t_profile = np.zeros(self.n_points)
    self.total = 0
    if log:
      self.log = log
    else:
      self.log = logging.getLogger("DeltaParameter")
      self.log.setLevel(logging.DEBUG)
    self.nodeLog = nodeLog
    self.nodeLogPrefill = nodeLogPrefill
    log.debug("DeltaParameter; start: %s, end: %s, step: %s, n_points: %d",
              str(start), str(end), str(self.step), self.n_points)


  def init_node(self, timestamp, value, node_name):
    """ starts a new node of the analysis
    :param timestamp: time of the first point
    :param value: value of the first point
    :param node_name: name of the node (for logging)
    """
    self.node_name = node_name
    self.cur_point = math.floor((timestamp - self.start) / self.step)
    if self.cur_point < 0:
      self.log.warning("init_node; timestamp (%f) is less than start time (%f)",
                       timestamp, self.start)
      self.cur_point = 0
    elif self.cur_point >= self.n_points:
      if timestamp > self.end:
        self.log.warning("init_node; timestamp (%f) is over the end time (%f)",
                       timestamp, self.end)
      self.cur_point = self.n_points - 1
    # 'ps' is "point start"
    self.ps_val = value
    self.point_roll_over = 0
    # 'ns' is "node start"
    self.ns_time = timestamp
    self.ns_val = value
    self.node_roll_over = 0
    self.last_time = timestamp
    self.last_val = value
    self.log.debug("init node %s; time: %s value: %f", str(node_name), str(timestamp), value)


  def finish_node(self):
    """ finishes the computation for the current node
    """
    if self.last_time == self.ns_time:
      self.log.warning("node %s start time and end time are equal when finishing a node", str(self.node_name))
      return

    pd_val = self.last_val + self.point_roll_over - self.ps_val
    point_rate = pd_val / self.step
    self.log.debug("finish node %s; point: %d, adding: %f", str(self.node_name), self.cur_point, point_rate)
    self.t_profile[self.cur_point] += point_rate

    node_time = (self.last_time - self.ns_time)  # / np.timedelta64(1, 's')
    node_total = self.node_roll_over + self.last_val - self.ns_val
    node_avg = node_total / node_time
    self.log.debug("finish node %s; node time: %s, node total: %f, node rate: %f",
                    str(self.node_name), str(node_time), node_total, node_avg)
    self.total += node_total

    if self.nodeLog:
      nodeLogLine = []
      nodeLogLine.extend(self.nodeLogPrefill)
      nodeLogLine.extend([self.node_name, self.ns_time, self.last_time, node_avg])
      self.nodeLog.log(nodeLogLine)


  def new_node(self, timestamp, value, node_name):
    self.finish_node()
    self.init_node(timestamp, value, node_name)


  def same_node(self, timestamp, value):
    self.log.debug("same_node; time: %s, value: %f", str(timestamp), value)
    roll_over = self.last_val if value < self.last_val else 0
    # ^ we man loose some due to overflow, but reset is more likely and we should not risk it
    self.point_roll_over += roll_over
    self.node_roll_over += roll_over
    new_point = max(0, math.floor((timestamp - self.start) / self.step))
    assert new_point >= self.cur_point
    if new_point >= self.n_points:
      if timestamp > self.end:
        self.log.warning("same_node; timestamp (%f) is over the end time (%f)",
                       timestamp, self.end)
      new_point = self.n_points - 1
    if new_point > self.cur_point:
      # we have to finish the point and advance
      delta_value = value + roll_over - self.last_val
      delta_time = (timestamp - self.last_time)  # / np.timedelta64(1, 's')
      assert delta_time > 0
      while self.cur_point < new_point:
        # finish cur_point and init a new
        pe_time = self.start + (self.cur_point + 1) * self.step # point end time
        pe_val = value - delta_value * (timestamp - pe_time) / delta_time
        pd_val = pe_val + self.point_roll_over - self.ps_val # point delta value
        point_rate = pd_val / self.step
        self.log.debug("same_node; point: %d, end: (%f, %f), adding: %f", self.cur_point, pe_time, pe_val, point_rate)
        self.t_profile[self.cur_point] += point_rate
        self.cur_point += 1
        self.ps_val = pe_val
        self.point_roll_over = 0
    assert new_point == self.cur_point

    self.last_time = timestamp
    self.last_val = value

  def finish_all(self):
    self.finish_node()
    avg = self.t_profile.mean()
    var = self.t_profile.var()
    return avg, var,  #  self.total
