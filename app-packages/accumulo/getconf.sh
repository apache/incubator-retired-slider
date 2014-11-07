# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CLUSTER=$1

slider registry --getconf accumulo-site --name $CLUSTER --format xml --dest accumulo-site.xml
slider registry --getconf client --name $CLUSTER --format properties --dest client.conf
slider registry --getconf accumulo-env --name $CLUSTER --format json --dest accumulo-env.json
python -c "import json; file = open('accumulo-env.json'); content = json.load(file); file.close(); print content['content']" > accumulo-env.sh
