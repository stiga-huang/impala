#!/bin/bash

source /opt/impala/bin/common.sh
/opt/impala/bin/catalogd --flagfile=conf/catalogserver_flags
