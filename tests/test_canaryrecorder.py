'''

Created by Alexander Goponenko at 7/26/2022

'''
import context

from pathlib import Path
import os
import tempfile


import logging


from sos_recorder import CanaryStore

if __name__ == "__main__":
    # This sets the root logger to write to stdout (your console).
    # Your script/app needs to call this somewhere at least once.
    logging.basicConfig()

    print("Manual tests")

    with tempfile.TemporaryDirectory() as tmp_dirname:
        cs = CanaryStore(tmp_dirname)

        print("adding first records")

        cs.saveRecord(1658857331, 1, 10.3)
        cs.saveRecord(1658857331, 2, 10.3)
        cs.saveRecord(1658857331, 3, 30.3)
        cs.saveRecord(1658857331, 4, 40.3)

        print("checking average 1")
        print(cs.getAverageValue(1658857321, 1658857341, 1))

        print("checking average 2")
        print(cs.getAverageValue(1658857321, 1658857321, 1))

        print("checking average 3")
        print(cs.getAverageValue(1658857341, 1658857341, 1))

        cs.saveRecord(1658857331, 1, 20.3)
