import context

import unittest
from unittest import mock

import sys

sys.modules['sosdb'] = mock.MagicMock()
sys.modules['numsos'] = mock.MagicMock()
sys.modules['numsos.DataSource'] = mock.MagicMock()

from pysimserv3 import get_components_from_string

class Test(unittest.TestCase):


  def setUp(self):
    # mock pysimserv3 gHostList
    gHostList = {
      'component1': 1001,
      'component2': 1002,
      'component3': 1003,
      'component4': 1004,
      'component5': 1005,
      'component6': 1006,
      'component7': 1007,
      'component8': 1008,
      'component9': 1009,
      'component10': 1010,
      'other_1': 2001,
      'other_2': 2002,
      'other_3': 2003,
      'other_4': 2004,
      'other_5': 2005,
      'other_6': 2006,
    }
    sys.modules['pysimserv3'].gHostList = gHostList
    pass


  def tearDown(self):
    pass

  def test_get_components_from_string(self):
    # Test case 1: Empty string
    self.assertEqual(get_components_from_string(""), [])

    # Test case 2: String with only whitespace
    self.assertEqual(get_components_from_string("   "), [])

    # Test case 3: String with one component
    self.assertEqual(get_components_from_string("component1"), [1001])

    # Test case 4: String with multiple components separated by comma
    self.assertEqual(get_components_from_string("component1,component2,component3"), [1001, 1002, 1003])

    # Test case 5: String with multiple components separated by comma and whitespace
    self.assertEqual(get_components_from_string("component1, component2, component3"), [1001, 1002, 1003])

    # Test case 6: String with multiple components separated by comma and whitespace
    self.assertEqual(get_components_from_string("component1, component2, other_1"), [1001, 1002, 2001])

    # Test case 7: Range of components
    self.assertEqual(get_components_from_string("component[1-3]"), [1001, 1002, 1003])

    # Test case 8: Range of components
    self.assertEqual(get_components_from_string("component[1-3], component5"), [1001, 1002, 1003, 1005])

    # Test case 9: Range of components
    self.assertEqual(get_components_from_string("component[1-3], component[5], component[7-9]"), [1001, 1002, 1003, 1005, 1007, 1008, 1009])

    # Test case 10: Range of components
    self.assertEqual(get_components_from_string("component[1-3,5,7-9], component10"), [1001, 1002, 1003, 1005, 1007, 1008, 1009, 1010])

    # Test case 11: Range of components
    self.assertEqual(get_components_from_string("component[1-3,5,7-9], other_[1,3]"), [1001, 1002, 1003, 1005, 1007, 1008, 1009, 2001, 2003])
if __name__ == "__main__":
  #import sys;sys.argv = ['', 'Test.testName']
  unittest.main()
