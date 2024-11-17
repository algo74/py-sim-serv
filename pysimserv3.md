# Documentation for `pysimserv3.py`

`pysimserv3.py` is a script that implements "Analytical Services" server.


## Configuration
`pysimserv3.py` has many configuration options. 
A sub-set of the options can be set with CLI  (these options are indicated by *). 
More options can be provided through a YAML configuration file. 
Some configuration options can be changed only by changing the script itself.


### Description of the options

`config_file`*  
This option is only available through CLI when running the script.
It allows to provide a configuration file's name.

`production`* (_default:_ `False`)   
Setting `production` `true` or `false` switches a set of default option values. `production=false` makes most sense for debugging `pysimserv3.py` on a system without Lustre file system because in this case the defaults set `user` metric from `procstat` LDMS plugin to be used in place of Lustre throughput.


#### Site-specific options

`address`* (_default:_ `"0.0.0.0"`)  
`port`*  (_default:_ `9999`)  
Address and port, to which the script should listen.

`COLUMNS`  
The list of columns to be read from the LDMS records. It depends, for instance, on what Lustre sample plugin is used (and possibly the name of the Lustre file system).

_Example_
```yaml
COLUMNS:
- timestamp
- component_id
- read_bytes.sum
- write_bytes.sum
```
```python
  conf['COLUMNS'] = ["timestamp",
                    "component_id",
                    "client.read_bytes#llite.testfs",
                    "client.write_bytes#llite.testfs"]
```

`FACTORS`*  
A dictionary used to convert the values computed from LDMS records to the scales used in Slurm. 
For instance, the value for "lustre" converts the throughput in B/s into the number of licenses (which, by default, the Slurm would limit to 10,000 at scheduling). 
"timelimit" is converted from seconds to minutes.

_Example_
```yaml
FACTORS:
  lustre: 4.657661854e-07
  timelimit: 0.016666666666666666
```
```python
parser.add_argument('--FACTORS', type=yaml.safe_load, default={
  "timelimit": 1.0 / 60.0,
  "lustre": 10000.0 / 14000000.0
}, help="multipliers for the metrics")
```

`PATH`  
The path to the SOS database with LDMS records.

`REC_PATH`  
The path to the SOS database used to save current job resource usage predictions.

`DELTAS` (default: `["Lustre"]` in `production`, `["user"]` otherwise)  
Likely, this option should not be changed.
"Delta" is the only implemented way of computing a metric. This method constitutes of computing differences between datapoints for metrics that represent "counts".

`DELTAS_DATA_GETTERS` 
"Data getters" allow to control the method of getting the metric data from the database.
Currently, `simple_array` gets an "array" with the name of the metric from the database, while `sum_array` derives the metric data by adding together several arrays (see `SUM_SOURCES`).
> This option is not configurable through a configuration file.

`SUM_SOURCES`  
A directory that contains lists of columns added together to produce derived metrics that use `sum_array` as their "data getter". Currently, the directory contains one entry, "lustre".


_Example_
```yaml
SUM_SOURCES:
  lustre:
  - read_bytes.sum
  - write_bytes.sum
```
```python
conf['SUM_SOURCES'] = {
  "lustre": ["client.read_bytes#llite.testfs",
             "client.write_bytes#llite.testfs"]
}
```


#### Options for diagnostics and experiments

`doSaveTables`*  
This flag instructs the script to save CSV files (tables) that contain information such as current resource utilization, computed resource requirements for finished jobs, and newly computer predictions for the finished job's variety id.
> _Effect of `$SLURM_JOB_NODELIST`_  
The prototype may be used in a "private" mode, that is in a part of a cluster created by allocating nodes within a global Slurm.
In this case, it may be important to check that other jobs started by the global Slurm do not interfere with experiment.
Therefore, if `$SLURM_JOB_NODELIST` env. variable is set, the script computes separately "private" resource utilization, that is the resource utilization computed for nodes that are listed in `$SLURM_JOB_NODELIST`. 

`prefixSaveTables`*
Location where diagnostic tables are saved.

`zero_current_utilization`* (_default:_ `False`)  
This flag may be used to make Analytical serivces always report current total Lustre resource utilization as zero.
It may be useful to compare the performance of the Slurm-based prototype with and without attempting to mitigate the file system overload due to underprediciton of Lustre usage. 

`use_canary`* (_default:_ `None`)  
Provides an option to specify the location of a canary probe database.
The canary probe is an experimental feature that does not yet perform well.
Setting this option `None` disables the feature.

`K_SIGMA`  
The predictions can be adjusted by adding the computed standard deviations multiplied by `K_SIGMA` to the computed average.
Usually, `K_SIGMA` is set to zero for every predicted job parameter.


#### Fine-tuning parameters

`grid_step`* (_default:_ 10)  
To compute a job's resource utilization, the resource usage data from every node used to run the job are "gridded" to the same grid.
After that, the gridded values are added together.
This option allows to set the interval between datapoints in the grid (in seconds).

`DECAY`  
The decay parameters for use in the exponetially decaying weighted average prediction of resource utilization of jobs.

`MAX` (_default:_ `4096`)  
Maximum lenght of network message in bytes.

`QUERY_LIMIT` (_default:_ `4096`)  
Maximum number of rows to be returned by queries to databases.

`PROCESSING_DELAY` (_default:_ `20`)  
Delay (in seconds) between recieving a signal of a job termination and the processing the job.

`N_TRIES` (_default:_ `3`)  
Number of tries before giving up on  processing the job.

`wiggle_time` (_default:_ `10` [seconds])  
> This parameter is probably used only to compute "canary time" if `use_canary` is set.
When no records are found in the database, the search window may be extended by `wiggle_time`. The exact procedure may be more complicated.

#### Other parameters

`OVERFLOW` (_default:_ 2^64)  
Value used to detect overflow (LDMS uses uint64).

`TRANSLATE`  
Dictionary used to translate internal names of the metrics into the names that should be used in communications (see [protocol.md](protocol.md)).

`hostlist`  (_default:_ _not set_)  
A name of a file that associates hostnames and their component ids.

`file_queue_path` (_default:_ `None`) 
Enables communicating terminated jobs through the file system.

`file_canary_queue_path` (_default:_ `None`)  
`CANARY_FS_PROCESSING_DELAY` (_default:_ `20` [second])  
Enables communicating results of canary probe through the file system.

`hostlist`  (_default:_ _not set_)  
A name of a file that associates hostnames and their component ids.
This assosiation is reqired for _"Workaround for LDMS records with not properly set job_id field"_.


## Workarounds

The script has several modes to allow operating (to certain extent) in less-than-ideal circumstances.


### Workaround for LDMS records with not properly set job_id field

In case LDMS is not configured to set job_id fields in the Lustre plugin's records, `pysimserv3.py` can detect the records belonging to the processed job with an alternative method. 
To enable the method, the request (that includes `“type”: ”process_job”`) should include, in addition to `“variety_id”` and `“job_id”`, fields: 
- `"job_start"`  
  a string (that `numpy.datetime64` can convert; "us" location) that indicates the time the job starts.
- `"job_end"`  
  a string that inicates the end time of the job.
- `"job_nodes"`  
  a list of nodes the job used (the list can be in Slurm's "condensed" form; the configuration parameter `hostlist`  should provide a path to a file that associates hostnames tocomponent ids)

If these fields present,  `pysimserv3.py` uses its `alt_analyze_job` routine, instead of `analyze_job`, to process the job.


### Workaround for the case that compute nodes cannot make connection with the analytical services

When `slurmctrld` cannot make a network connection to `pysimserv3.py`, the signal to process job can be delivered through a shared file system.
The configuration option `file_queue_path` sets the path to a directory used as a request queue.
Script `extjobfinishreq.py` can be used to make the request through the the file system queue.

Similarly, canary probes can send information through the shared file system as well. 
The queue path in this case is set using the option `file_canary_queue_path`. 
The helper script is `fscanaryrequest.py`.

-----
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