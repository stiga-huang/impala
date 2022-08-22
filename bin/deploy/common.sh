#!/bin/bash

if [[ -z "$JAVA_HOME" ]]; then
  echo "JAVA_HOME not set!"
  exit 1
fi

echo "Using JAVA_HOME: $JAVA_HOME"
LIB_JVM_DIR=$(dirname $(find $JAVA_HOME -type f -name libjvm.so))
LIB_JSIG_DIR=$(dirname $(find $JAVA_HOME -type f -name libjsig.so))

export LC_ALL=en_US.utf8
export LD_LIBRARY_PATH="/opt/impala/lib/:$LIB_JVM_DIR:$LIB_JSIG_DIR"
export CLASSPATH="/opt/impala/conf:/opt/impala/jar/*"
