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

MEMBER_COUNT=3
TEST_FLAGS=
env MEMBER_COUNT=${MEMBER_COUNT} go test ${TEST_FLAGS} -count=1 -coverpkg "$(go list ./... | grep -v /internal/it | grep -v /benchmarks | grep -v /stress_tests | tr "\n" ",")" -coverprofile=coverage.out ./...