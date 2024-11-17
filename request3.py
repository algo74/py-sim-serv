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

"""
CLI interface implementation for pysimserv3

"""

import socket
import json
import sys


class Communicatior(object):
  """

  """

  def __init__(self, host, port):
    self.host = host
    self.port = port
    self.socket = None


  def send_receive(self, message):
    if self.socket is None:
      # try to establish the connection
      self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.socket.connect((self.host, self.port))

    self.socket.send(message.encode('utf-8'))
    data = self.socket.recv(1024)

    return data



def main():
  def exit():
    raise SystemExit(f"Usage: {sys.argv[0]} [-<parameter>=<value>] ...")

  params = {}
  try:
    row_params = sys.argv[1:]
    for row in row_params:
      if not row.startswith('-'):
        exit()
      x, y = row[1:].split('=', 2)
      params[x] = y
  except:
    exit()


  port = int(params.pop('p', '9999'))
  host = params.pop('h', 'localhost')
  type = params.pop('type', 'usage')
  req_id = params.pop('req_id', '1')


  req = {
    'req_id': req_id,
    'type': type,
  }

  if type == 'usage':
    req['request'] = 'lustre'
  elif type.startswith('variety_id'):
    nodes = params.pop('nodes', 1)
    req['UID'] = 1000
    req['GID'] = 1000
    req['min_nodes'] = nodes
    req['max_nodes'] = nodes
    if type == "variety_id/manual":
      req["variety_name"] = 'test'
    elif type == "variety_id/auto":
      script_args = params.pop("script_args", "test1,test2,test3")
      script_args = script_args.split(',')
      req['script_args'] = script_args
      req['script_name'] = 'test'
  elif type == "job_utilization":
    req['nodes'] = 1

  for key in params:
    req[key] = params[key]

  message = json.dumps(req)

  print("Request to {}:{}".format(host, port))
  print(message)

  c = Communicatior(host, port)
  resp = c.send_receive(message)
  print("Responce:")
  print(resp)

if __name__ == "__main__":
    # execute only if run as a script
    main()