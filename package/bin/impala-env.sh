#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if [[ -z "$JAVA_HOME" ]]; then
  echo "JAVA_HOME not set!"
  exit 1
fi

echo "Using JAVA_HOME: $JAVA_HOME"
LIB_JVM_DIR=$(dirname $(find $JAVA_HOME -type f -name libjvm.so))
LIB_JSIG_DIR=$(dirname $(find $JAVA_HOME -type f -name libjsig.so))

if [[ -n "$HADOOP_HOME" ]]; then
  echo "Using HADOOP_HOME: $HADOOP_HOME"
  export HADOOP_LIB_DIR="${HADOOP_HOME}/lib"
  export LIBHDFS_OPTS="${LIBHDFS_OPTS:-} -Djava.library.path=${HADOOP_LIB_DIR}/native/"
  echo "Using hadoop native libs in ${HADOOP_LIB_DIR}/native/"
else
  echo "WARNING: HDFS short-circuit reads are not enabled due to HADOOP_HOME not set."
fi

export LC_ALL=en_US.utf8
export LD_LIBRARY_PATH="/opt/impala/lib/:$LIB_JVM_DIR:$LIB_JSIG_DIR"
export CLASSPATH="/opt/impala/conf:/opt/impala/jar/*"
