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

import argparse

from JSON_fsqueue import JSONFSQueue

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--value', type=str, required=True, help="comma-separated values")
    parser.add_argument('-p', '--path', type=str, required=True, help="path to the file queue")

    args = parser.parse_args()

    start_time, end_time, ost, duration, file_size = args.value.split(',')


    JSONFSQueue(args.path).put({
        'type': 'process_canary_probe',
        'start_time': start_time,
        'end_time': end_time,
        'OST': ost,
        'duration': duration
    })
