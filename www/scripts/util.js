// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

function getReadableSize(bytes) {
    var units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB'];
    if (bytes <= 0) return bytes + ' B';
    var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
    return (bytes / Math.pow(1024, i)).toFixed(2) + ' ' + units[i];
}

function getReadableTimeNS(value) {
    if (value >= 1000000000) {
        return getReadableTimeMS(value / 1000000)
    } else if (value >= 1000000) {
        return (value / 1000000).toFixed(3) + "ms"
    } else if (value >= 1000) {
        return (value / 1000).toFixed(3) + "us"
    } else {
        return value + "ns"
    }
}

function getReadableTimeMS(value) {
    var hour = false;
    var minute = false;
    var second = false;
    var re = "";
    if (value >= 3600000) {
        re += (Math.floor(value / 3600000) + "h");
        value = value % 3600000;
        hour = true;
    }
    if (value >= 60000) {
        re += (Math.floor(value / 60000) + "m");
        value = value % 60000;
        minute = true;
    }
    // if hour is true, the time is large enough and we should
    // ignore the remaining time on second level
    if (!hour && value >= 1000) {
        re += (Math.floor(value / 1000) + "s");
        value = value % 1000;
        second = true;
    }
    if (!hour && !minute) {
        if (second) {
            while (value.toString().length < 3) {
                value = "0" + value;
            }
        }
        re += (value + "ms")
    }
    return re;
}

/*
 * Useful render function used in DataTable. It renders the size
 * value into human readable format in 'display' and 'filter' modes,
 * and uses the original value in 'sort' mode.
 */
function renderSize(data, type, row) {
    // If display or filter data is requested, format the data
    if (type === 'display' || type === 'filter') {
        return getReadableSize(data);
    }
    return data;
}

/*
 * Useful render function used in DataTable. It renders the time
 * value (nano seconds) into human readable format in 'display'
 * and 'filter' modes, and uses the original value in 'sort' mode.
 */
function renderTime(data, type, row) {
    // If display or filter data is requested, format the data
    if (type === 'display' || type === 'filter') {
        return getReadableTimeNS(data);
    }
    return data;
}