#! /bin/bash

#
# Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Exit immediately on error.
set -e
# Treat unset variables an parameters as an error.
set -u

duration="${DURATION:-}"
if [ "x$duration" != "x" ]; then
  duration="-d $duration"
fi

# build the test
go build ./map/main.go

bash hz.sh start
./main $duration
bash hz.sh stop
