Connection protocol for SLURM-LDMS middleman server
===================================================

General
--------------------------------

* JSON request:  {“req_id: “...”, “type”: ”...”, ...}\n
* JSON response: {“req_id: “...”, “status”: ”error|OK|ACK|not implemented”, ...}\n

"type”: ”usage”
--------------------------------

* Request: 
  - “request”: [”lustre”, ...] -- ignored by the middlemant server

* Response: 
  - “status”: ”OK”
  - “response” : {”lustre” : “\<int>”, ... }
  
"type" : "variety_id"
--------------------------------

* Request:
  - "UID" : "..."
  - "GID" : "..."
  - "min_nodes" : "\<int>"
  - "max_nodes" : "\<int>"
  
* Response: 
  - “status”: ”OK”, 
  - “variety_id”: ”...”

### "type" : "variety_id/manual"

* Request:
  - "variety_name" : "..."

### "type" : "variety_id/auto"

* Request: 
  - “script_args”: [“\<arg0>”, “\<arg1>”, ...], 
  - “script_name”: ”...”


“type”: ”process_job”
--------------------------------

* Request: “variety_id”: ”...”, “job_id”: ”\<int>”
* Response: “status”: ”ACK”


“type”: ”process_canary_probe”
--------------------------------
> only implemented in pysimserv3

* Request: 
  - “start_time”: ”\<timestamp>” 
  - “end_time”: ”\<timestamp>”
  - “OST”: ”\<int>”
  - “duration”: ”\<float>”
* Response: “status”: ”ACK|error”, ”error”: ”Not configured | ...”

“type”: ”job_utilization”
--------------------------------

* Request: 
  - “variety_id”: ”...”
  - "nodes" : "\<int>"
  
* Response: 
  - “status”: ”OK”, 
  - “response” : {”lustre” : “\<int>”, ...}


“type”: ”analyze_job”
--------------------------------
> only implemented in pysimserv3

* Request: “job_id”: ”\<int>”
* Response:
  - “status”: ”OK”, 
  - ”lustre” : {"avg": “\<int>”, "var": “\<int>”}


------
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
