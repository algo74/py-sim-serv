"""From https://codereview.stackexchange.com/questions/221189/file-system-based-queue-in-python"""

import os
import queue
import time
import json
import uuid

from pathlib import Path


class JSONFSQueue:
    def __init__(self, dir):
        self._dir = Path(dir)
        self._dir.mkdir(exist_ok=True, parents=True)

    def _read_files(self):
        return self._dir.glob("[!.]*-*")


    def put(self, data):
        """Adds data to the queue by dumping it to a pickle file."""
        now = int(time.time() * 10000)
        uid = uuid.uuid4().hex
        while True:
            try:
                seq = "{now:016}-{uid}".format(now=now, uid=uid)
                target = self._dir / seq
                fn = target.with_suffix('.lock')
                json.dump(data, fn.open('x'))  # Write to locked file
                if target.exists():
                    fn.unlink()
                else:
                    fn.rename(target)
                    break
            except FileExistsError:
                # FIXME: what if we leave *.lock behind?
                pass


    def get(self):
        files = self._read_files()
        # FIFO
        files = sorted(files)
        for f in files:
            if f.match('*.lock'):
                continue  # Someone is writing or reading the file
            try:
                # fn = self._dir / f
                target = f.with_suffix('.lock')
                f.rename(target)
                data = json.load(target.open('r'))
                target.unlink()
                return data
            except FileNotFoundError:
                pass  # The file was locked by another get()
        raise queue.Empty()



    def qsize(self):
        """Returns the approximate size of the queue."""
        files = self._read_files()
        n = 0
        for f in files:
            if f.match('*.lock'):
                continue  # Someone is reading the file
            n += 1
        return n


    def empty(self):
        return self.qsize() == 0

