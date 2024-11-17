'''
Created on Apr 24, 2020

@author: alex
'''
from __future__ import division
import unittest
import numpy as np
import pysimserv

QUERY_LIMIT = 4096




class TestProcessJob(unittest.TestCase):


    def setUp(self):
        self.log = MockLog()
        self.m = MockMessage()
        self.oldDeltas = pysimserv.DELTAS
        # also save PARAMS if needed in the future
        self.oldSUM_SOURCES = pysimserv.SUM_SOURCES

    def tearDown(self):
        pysimserv.DELTAS = self.oldDeltas
        pysimserv.SUM_SOURCES = self.oldSUM_SOURCES

    def test_case0(self):
        """
        simple one component of many records
        """
        ds = MockDataSourceCase0()
        result = pysimserv.process_job(ds, self.m, self.log)
        true_result = ds.true_results()
        self.assertAlmostEqual(result[0], true_result[0])
        self.assertAlmostEqual(result[1]['user'][0], true_result[1])
        self.assertAlmostEqual(result[1]['user'][1], true_result[2])
        
    def test_caseEmpty(self):
        ds = MockDataSourceCaseEmpty()
        result = pysimserv.process_job(ds, self.m, self.log)
        self.assertEqual(result, None)
        
    def test_SameRecord(self):
        ds = MockDataSourceCaseSameRecord()
        result = pysimserv.process_job(ds, self.m, self.log)
        self.assertEqual(result, None)
        
    def test_case1(self):
        """
        many components of varios number of records
        """
        ds = MockDataSourceCase1()
        result = pysimserv.process_job(ds, self.m, self.log)
        true_result = ds.true_results()
        self.assertAlmostEqual(result[0], true_result[0])
        self.assertAlmostEqual(result[1]['user'][0], true_result[1])
        self.assertAlmostEqual(result[1]['user'][1], true_result[2])
        
    def test_case2(self):
        """
        many components of varios number of records
        """
        ds = MockDataSourceCase2()
        result = pysimserv.process_job(ds, self.m, self.log)
        true_result = ds.true_results()
        self.assertAlmostEqual(result[0], true_result[0])
        self.assertAlmostEqual(result[1]['user'][0], true_result[1])
        self.assertAlmostEqual(result[1]['user'][1], true_result[2])

    def test_lustre(self):
        pysimserv.DELTAS = set(("lustre",))
        fields = ["client.read_bytes#llite.testfs-01",
                      "client.write_bytes#llite.testfs-01"]
        pysimserv.SUM_SOURCES = {
          "lustre" : fields
        }
        ds = MockDataSourceLustre0(fields)
        result = pysimserv.process_job(ds, self.m, self.log)
        true_result = ds.true_results()
        self.assertAlmostEqual(result[0], true_result[0])
        self.assertAlmostEqual(result[1]['lustre'][0], true_result[1])
        self.assertAlmostEqual(result[1]['lustre'][1], true_result[2])

class MockLog:
  def info(self, *args):
    pass
  
  def debug(self, *args):
    pass
  
  def error(self, *args):
    pass
  
  def warn(self, *args):
    pass


class MockMessage:
  def __init__(self):
    self.job_id = 10
    self.variety_id = "SHA256"



class MockData:
  def __init__(self):
    self.data = {}
    
  def array(self, column):
    return self.data[column]




class MockDataSource:
  
  def __init__(self):
    self.cur_record = 0
    self.starttime = np.datetime64('2011-07-11')
    self.cur_time = self.starttime
    
  
  def select(self, colums, where = None, order_by = None):
    pass
  
  def _reset(self):
    self.cur_record = 0
    self.cur_time = self.starttime
  
  def _get_row(self):
    return self.cur_time, 0, 0
  
  def get_results(self, limit=QUERY_LIMIT, reset=True):
    if reset: 
      self._reset()
    results = MockData()
    results.data['timestamp'] = []
    results.data['component_id'] = []
    results.data['user'] = []
    
    for _ in range(limit):
      row = self._get_row()
      if row:
        t, c, u = row
      else:
        break
      
      results.data['timestamp'].append(t)
      results.data['component_id'].append(c)
      results.data['user'].append(u)
      
    for name in results.data:
      results.data[name] = np.array(results.data[name])
    
    return results
    
  
  
class MockDataSourceCase0(MockDataSource):
  
  def __init__(self):
    MockDataSource.__init__(self)
    self.N = 5000
    self.deltas = np.random.uniform(4, 16, self.N)
    self._reset()
    
  def _reset(self):
    MockDataSource._reset(self)
    self.cur_value = 10
    
    
  def _get_row(self):
    if self.cur_record >= self.N:
      return None
    else: 
      t, c, u = self.cur_time, 1, self.cur_value
      self.cur_time += np.timedelta64(1, 's')
      self.cur_value += self.deltas[self.cur_record]
      self.cur_record += 1
      return t, c, u
    
  def true_results(self):
    return (self.N+1, np.mean(self.deltas[:-1]), np.var(self.deltas[:-1]))


class MockDataSourceCase1(MockDataSource):
  
  def __init__(self):
    MockDataSource.__init__(self)
    self.lengths = [0, # zero is needed here to calculate borders 
                    1, 2, 3, 8, 
                    4096-14, 4097, 4098, 
                    8192-4, 8192, 8103, 8194, 8195, 8196]
    self.n_comp = len(self.lengths)-1
    self.borders = np.cumsum(self.lengths)
    self.N = np.sum(self.lengths)
    self.deltas = np.random.uniform(4, 16, self.N)
    self.comps = np.empty(self.N)
    for i in range(self.n_comp):
      self.comps[self.borders[i]:self.borders[i+1]] = i
    self._reset()
    
  def _reset(self):
    MockDataSource._reset(self)
    self.cur_value = 10
    
    
  def _get_row(self):
    if self.cur_record >= self.N:
      return None
    else: 
      t, c, u = self.cur_time, self.comps[self.cur_record], self.cur_value
      self.cur_time += np.timedelta64(1, 's')
      self.cur_value += self.deltas[self.cur_record]
      self.cur_record += 1
      return t, c, u
    
  def true_results(self):
    n = self.n_comp - 1 # first component has no records
    sum_of_means = 0
    sum_of_vars = 0
    for i in range(1, self.n_comp):
      sum_of_means += np.mean(self.deltas[self.borders[i]:self.borders[i+1]-1])
      sum_of_vars += np.var(self.deltas[self.borders[i]:self.borders[i+1]-1])
    return (self.N+1, sum_of_means, sum_of_vars)



class MockDataSourceCase2(MockDataSource):
  
  def __init__(self):
    MockDataSource.__init__(self)
    self.N = 5000
    self.n_doubles = 1000
    self.n_triples = 1000
    self.deltas = np.random.uniform(4, 16, self.N)
    self.orig = self.deltas[:-1].copy()
    self.deltas[0:self.n_doubles] *= 2
    self.deltas[-self.n_triples-1:-1] *= 3
    self.dt = np.ones(self.N)
    self.dt[0:self.n_doubles] *= 2
    self.dt[-self.n_triples-1:-1] *= 3
    
    self._reset()
    
  def _reset(self):
    MockDataSource._reset(self)
    self.cur_value = 10
    
    
  def _get_row(self):
    if self.cur_record >= self.N:
      return None
    else: 
      t, c, u = self.cur_time, 1, self.cur_value
      self.cur_time += np.timedelta64(int(self.dt[self.cur_record]), 's')
      self.cur_value += self.deltas[self.cur_record]
      self.cur_record += 1
      return t, c, u
    
  def true_results(self):
    total_weight = np.sum(self.dt[:-1])
    avg = np.average(self.orig, weights=self.dt[:-1])
    var = np.sum(self.orig**2 * self.dt[:-1]) / total_weight - avg**2
    return ((self.N+1) * (self.n_doubles + self.n_triples*2 + self.N - 1) / (self.N-1),
            avg, 
            var)



class MockDataSourceCaseEmpty(MockDataSource):
  
  def __init__(self):
    MockDataSource.__init__(self)
    self.N = 1
    self._reset()
    
  def _reset(self):
    MockDataSource._reset(self)
    self.cur_value = 10
    
  def _get_row(self):
    if self.cur_record >= self.N:
      return None
    else: 
      t, c, u = self.cur_time, 1, 1011
      self.cur_record += 1
      return t, c, u
    

class MockDataSourceCaseSameRecord(MockDataSource):
  
  def __init__(self):
    MockDataSource.__init__(self)
    self.N = 3
    self._reset()
    
  def _reset(self):
    MockDataSource._reset(self)
    self.cur_value = 10
    
  def _get_row(self):
    if self.cur_record >= self.N:
      return None
    else: 
      t, c, u = self.cur_time, 1, 1011
      self.cur_record += 1
      return t, c, u
    
  def true_results(self):
    return (2, 1011, 0)


class MockDataSourceLustre0():
  
  def __init__(self, columns):
    self.columns = columns
    self.N = 5000
    self.deltas = {}
    self.sum = 0
    self.cur_values = {}
    self.starttime = np.datetime64('2011-07-11')
    for c in self.columns:
      self.deltas[c] = np.random.uniform(4, 16, self.N)
      self.sum = self.deltas[c] + self.sum
    self._reset()
    
  def _reset(self):
    self.cur_record = 0
    self.cur_time = self.starttime
    for c in self.columns:
      self.cur_values[c] = 10
    
    
  def _get_row(self):
    if self.cur_record >= self.N:
      return None
    else: 
      row = [self.cur_time, 1]
      self.cur_time += np.timedelta64(1, 's')
      for c in self.columns:
        row.append(self.cur_values[c])
        self.cur_values[c] += self.deltas[c][self.cur_record]
      self.cur_record += 1
      return row
    
  def get_results(self, limit=QUERY_LIMIT, reset=True):
    if reset: 
      self._reset()
    results = MockData()
    results.data['timestamp'] = []
    results.data['component_id'] = []
    for c in self.columns:
      results.data[c] = []
    
    for _ in range(limit):
      row = self._get_row()
      if row:
        pass
      else:
        break
      
      results.data['timestamp'].append(row[0])
      results.data['component_id'].append(row[1])
      i = 2
      for c in self.columns:
        results.data[c].append(row[i])
        i+=1
      
    for name in results.data:
      results.data[name] = np.array(results.data[name])
    
    return results
    
  def select(self, colums, where = None, order_by = None):
    pass
  
  def true_results(self):
    return (self.N+1, np.mean(self.sum[:-1]), np.var(self.sum[:-1]))



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()