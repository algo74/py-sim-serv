#!/bin/bash
TOP=/source/ovis
SOS=/source/sos

export LD_LIBRARY_PATH=$SOS/lib/:$TOP/lib/:$TOP/lib64:$LD_LIBRARY_PATH
export LDMSD_PLUGIN_LIBPATH=$TOP/lib/ovis-ldms/ 
export ZAP_LIBPATH=$TOP/lib/ovis-ldms/ 
export PATH=$SOS/bin:$TOP/sbin:$TOP/bin:$PATH 
export PYTHONPATH=$TOP/lib/python2.7/site-packages/:$SOS/lib/python2.7/site-packages/:$PYTHONPATH

export SAMPLE_INTERVAL=1000000

export HOSTINDEX=0
if [[ $HOSTNAME =~ cl([0-9]) ]] ; then
  export HOSTINDEX=${BASH_REMATCH[1]}
fi

echo "LDMS setup complete, index: ${HOSTINDEX}"
