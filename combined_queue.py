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

'''

Created by Alexander Goponenko at 4/7/2022

'''
import queue
from simple_fsqueue import SimpleFSQueue as FSQueue
import time

import message



class CombinedQueue(object):

    def __init__(self, watched_path=None):
        self.inner_queue = queue.Queue(0)
        self.file_queue = FSQueue(watched_path) if watched_path else None
        self.file_item = None
        self.inner_item = None


    def empty(self) -> bool:
        if self.inner_item or self.file_item:
            return False
        if not self.inner_queue.empty():
            return False
        elif self.file_queue is None:
            return False
        else:
            return self.file_queue.empty()


    def put(self, item) -> None:
        self.inner_queue.put(item)


    def get(self):
        if self.inner_item is None and not self.inner_queue.empty():
            self.inner_item = self.inner_queue.get()
        if self.file_item is None and self.file_queue and not self.file_queue.empty():
            self.file_item = self.file_queue.get()
        if self.file_item:
            if self.inner_item and self.inner_item.time <= self.file_item.time:
                item = self.inner_item
                self.inner_item = None
                return item
            else:
                item = self.file_item
                self.file_item = None
                return item
        elif self.inner_item:
            item = self.inner_item
            self.inner_item = None
            return item
        else:
            return self.inner_queue.get()



def external_put(path, job_id, variety_id, delay):
    m = message.Message(time.time() + int(delay),
                        int(job_id),
                        variety_id)
    FSQueue(path).put(m)