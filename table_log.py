'''
Created on May 5, 2020

@author: alex
'''

import csv
import threading
from os.path import exists as file_exists


class TableLog:
  
  def __init__(self, filename, title=None):
    self.filename = filename
    self.lock = threading.Lock()
    if title and not file_exists(filename):
      self.log(title)

  def log(self, data):
    with self.lock:
      with open(self.filename, 'a', newline='') as f:
        w = csv.writer(f)
        w.writerow(data)


class NoneLog:
  
  def __init__(self, filename):
    pass

  def log(self, data):
    pass
  