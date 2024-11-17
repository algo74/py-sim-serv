'''
Created on Apr 28, 2020

@author: alex
'''

from __future__ import division
import unittest
import numpy as np
import pysimserv


class Test(unittest.TestCase):


  def setUp(self):
    # self.oldRecorder = pysimserv.gRecorder
    self.recorder = MockRecorder()
    pysimserv.gRecorder = self.recorder
    pysimserv.DECAY = {
        'timelimit' : 0.2,
        'user' : 0.1
      }


  def tearDown(self):
    # pysimserv.gRecorder = self.oldRecorder
    pass


  def testOneUpdate(self):
    variety_id = "test_variety_id"
    param_name = "timelimit"
    avg = 1.1
    var = 2.2
    pysimserv.update_param(variety_id, param_name, avg, var)
    h = self.recorder.history
    self.assertEqual(len(h), 2)
    get = h[0]
    stored = h[1]
    self.assertEqual(get[0], "getRecord")
    self.assertEqual(get[1], self.recorder._key(variety_id, param_name))
    self.assertEqual(stored[0], "saveRecord")
    self.assertEqual(stored[1], self.recorder._key(variety_id, param_name))
    self.assertEqual(stored[2], (1.1, 2.2, 1, 1.1))
    
    
  def testTimeUpdateCase1(self):
    """
    When alpha==0, the formulas reduce to traditional formulas
    """
    variety_id = "test_variety_id"
    param = "timelimit"
    pysimserv.DECAY['timelimit'] = 0
    # first update
    dt = 1.1
    pysimserv.update_param(variety_id, param, dt, 0)
    res = self.recorder.getRecord(variety_id, param)
    self.assertTrue(res, "record must exist after first update")
    pAvg, pVar, pCount, pSum = res
    self.assertAlmostEqual(pAvg, dt)
    self.assertAlmostEqual(pVar, 0)
    self.assertAlmostEqual(pCount, 1)
    self.assertAlmostEqual(pSum, dt)
    # second update
    pysimserv.update_param(variety_id, param, dt, 0)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    self.assertAlmostEqual(pAvg, dt)
    self.assertAlmostEqual(pVar, 0)
    self.assertAlmostEqual(pCount, 2)
    self.assertAlmostEqual(pSum, 2.2)
    # what we have so far
    sequence = [1.1, 1.1]
    # third update
    dt = 3.3
    sequence.append(dt)
    pysimserv.update_param(variety_id, param, dt, 0)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    self.assertAlmostEqual(pAvg, np.mean(sequence))
    self.assertAlmostEqual(pVar, np.var(sequence))
    self.assertAlmostEqual(pCount, len(sequence))
    self.assertAlmostEqual(pSum, np.sum(sequence))
    # forth update
    dt = 3.3
    sequence.append(dt)
    pysimserv.update_param(variety_id, param, dt, 0)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    self.assertAlmostEqual(pAvg, np.mean(sequence))
    self.assertAlmostEqual(pVar, np.var(sequence))
    self.assertAlmostEqual(pCount, len(sequence))
    self.assertAlmostEqual(pSum, np.sum(sequence))
    # fifth update
    dt = 10
    sequence.append(dt)
    pysimserv.update_param(variety_id, param, dt, 0)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    self.assertAlmostEqual(pAvg, np.mean(sequence))
    self.assertAlmostEqual(pVar, np.var(sequence))
    self.assertAlmostEqual(pCount, len(sequence))
    self.assertAlmostEqual(pSum, np.sum(sequence))
    # sixth update
    dt = 0
    sequence.append(dt)
    pysimserv.update_param(variety_id, param, dt, 0)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    self.assertAlmostEqual(pAvg, np.mean(sequence))
    self.assertAlmostEqual(pVar, np.var(sequence))
    self.assertAlmostEqual(pCount, len(sequence))
    self.assertAlmostEqual(pSum, np.sum(sequence))

  def testUserUpdateAlpha0(self):
    """
    When alpha==0, the formulas reduce to traditional formulas
    """
    variety_id = "test_variety_id"
    param = "user"
    pysimserv.DECAY['user'] = 0
    # first update
    avg = 1.1
    var = 3.33
    pysimserv.update_param(variety_id, param, avg, var)
    res = self.recorder.getRecord(variety_id, param)
    self.assertTrue(res, "record must exist after first update")
    pAvg, pVar, pCount, pSum = res
    self.assertAlmostEqual(pAvg, avg)
    self.assertAlmostEqual(pVar, var)
    self.assertAlmostEqual(pCount, 1)
    self.assertAlmostEqual(pSum, avg)
    # second update
    pysimserv.update_param(variety_id, param, avg, var)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    self.assertAlmostEqual(pAvg, avg)
    self.assertAlmostEqual(pVar, var)
    self.assertAlmostEqual(pCount, 2)
    self.assertAlmostEqual(pSum, 2.2)
    # what we have so far
    avg_seq = [1.1, 1.1]
    var_seq = [3.33, 3.33]
    # third update
    avg = 3.3
    var = 4.0
    avg_seq.append(avg)
    var_seq.append(var)
    pysimserv.update_param(variety_id, param, avg, var)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    nAvg = np.mean(avg_seq)
    nSum = np.sum(avg_seq)
    nCount = len(avg_seq)
    nVar = (np.sum(var_seq) + np.sum((np.array(avg_seq) - nAvg)**2)) / nCount
    self.assertAlmostEqual(pAvg, nAvg)
    self.assertAlmostEqual(pVar, nVar)
    self.assertAlmostEqual(pCount, nCount)
    self.assertAlmostEqual(pSum, nSum)
    # forth update
    avg = 3.3
    var = 4.0
    avg_seq.append(avg)
    var_seq.append(var)
    pysimserv.update_param(variety_id, param, avg, var)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    nAvg = np.mean(avg_seq)
    nSum = np.sum(avg_seq)
    nCount = len(avg_seq)
    nVar = (np.sum(var_seq) + np.sum((np.array(avg_seq) - nAvg)**2)) / nCount
    self.assertAlmostEqual(pAvg, nAvg)
    self.assertAlmostEqual(pVar, nVar)
    self.assertAlmostEqual(pCount, nCount)
    self.assertAlmostEqual(pSum, nSum)
    # fifth update
    avg = 0
    var = 0
    avg_seq.append(avg)
    var_seq.append(var)
    pysimserv.update_param(variety_id, param, avg, var)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    nAvg = np.mean(avg_seq)
    nSum = np.sum(avg_seq)
    nCount = len(avg_seq)
    nVar = (np.sum(var_seq) + np.sum((np.array(avg_seq) - nAvg)**2)) / nCount
    self.assertAlmostEqual(pAvg, nAvg)
    self.assertAlmostEqual(pVar, nVar)
    self.assertAlmostEqual(pCount, nCount)
    self.assertAlmostEqual(pSum, nSum)
    # sixth update
    avg = 0
    var = 1001
    avg_seq.append(avg)
    var_seq.append(var)
    pysimserv.update_param(variety_id, param, avg, var)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    nAvg = np.mean(avg_seq)
    nSum = np.sum(avg_seq)
    nCount = len(avg_seq)
    nVar = (np.sum(var_seq) + np.sum((np.array(avg_seq) - nAvg)**2)) / nCount
    self.assertAlmostEqual(pAvg, nAvg)
    self.assertAlmostEqual(pVar, nVar)
    self.assertAlmostEqual(pCount, nCount)
    self.assertAlmostEqual(pSum, nSum)
    # seventh update
    avg = 1001
    var = 0
    avg_seq.append(avg)
    var_seq.append(var)
    pysimserv.update_param(variety_id, param, avg, var)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    nAvg = np.mean(avg_seq)
    nSum = np.sum(avg_seq)
    nCount = len(avg_seq)
    nVar = (np.sum(var_seq) + np.sum((np.array(avg_seq) - nAvg)**2)) / nCount
    self.assertAlmostEqual(pAvg, nAvg)
    self.assertAlmostEqual(pVar, nVar)
    self.assertAlmostEqual(pCount, nCount)
    self.assertAlmostEqual(pSum, nSum)
    
  def testAlpha1Update(self):
    """
    When alpha==1, the formulas reduce to last value
    """
    variety_id = "test_variety_id"
    param = "timelimit"
    pysimserv.DECAY['timelimit'] = 1
    # first update
    dt = 1.1
    var = 6.66
    pysimserv.update_param(variety_id, param, dt, var)
    res = self.recorder.getRecord(variety_id, param)
    self.assertTrue(res, "record must exist after first update")
    pAvg, pVar, pCount, pSum = res
    self.assertAlmostEqual(pAvg, dt)
    self.assertAlmostEqual(pVar, var)
    self.assertAlmostEqual(pCount, 1)
    self.assertAlmostEqual(pSum, dt)
    # second update
    pysimserv.update_param(variety_id, param, dt, var)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    self.assertAlmostEqual(pAvg, dt)
    self.assertAlmostEqual(pVar, var)
    self.assertAlmostEqual(pCount, 1)
    self.assertAlmostEqual(pSum, dt)
    # what we have so far
    sequence = [1.1, 1.1]
    # third update
    dt = 3.3
    sequence.append(dt)
    pysimserv.update_param(variety_id, param, dt, var)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    self.assertAlmostEqual(pAvg, dt)
    self.assertAlmostEqual(pVar, var)
    self.assertAlmostEqual(pCount, 1)
    self.assertAlmostEqual(pSum, dt)
    # forth update
    dt = 3.3
    var = 0
    sequence.append(dt)
    pysimserv.update_param(variety_id, param, dt, var)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    self.assertAlmostEqual(pAvg, dt)
    self.assertAlmostEqual(pVar, var)
    self.assertAlmostEqual(pCount, 1)
    self.assertAlmostEqual(pSum, dt)
    # fifth update
    dt = 10
    var = 5.555
    sequence.append(dt)
    pysimserv.update_param(variety_id, param, dt, var)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    self.assertAlmostEqual(pAvg, dt)
    self.assertAlmostEqual(pVar, var)
    self.assertAlmostEqual(pCount, 1)
    self.assertAlmostEqual(pSum, dt)
    # sixth update
    dt = 0
    var = 2000
    sequence.append(dt)
    pysimserv.update_param(variety_id, param, dt, var)
    pAvg, pVar, pCount, pSum = self.recorder.getRecord(variety_id, param)
    self.assertAlmostEqual(pAvg, dt)
    self.assertAlmostEqual(pVar, var)
    self.assertAlmostEqual(pCount, 1)
    self.assertAlmostEqual(pSum, dt)

class MockRecorder:
  
  def __init__(self):
    self.records = {}
    self.history = []
    
  def _key(self, variety_id, param):
    return str(variety_id) + "/" + str(param)


  def getRecord(self, variety_id, param):
    self.history.append( ("getRecord", self._key(variety_id, param)) )
    return self.records.get(variety_id, None)

  
  def saveRecord(self, variety_id, param, avg, var, w_count, w_sum):
    self.history.append( ("saveRecord", self._key(variety_id, param), (avg, var, w_count, w_sum)) )
    self.records[variety_id] = [avg, var, w_count, w_sum]



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()