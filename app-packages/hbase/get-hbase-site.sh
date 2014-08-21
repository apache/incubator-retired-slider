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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#
tuple=`slider status $1 | grep "info.am.web.url"`
echo $tuple
FS=":"
url=`echo $tuple | awk '{split($0,array,": ")} END{print array[2]}'`
url="${url%,}"
url="${url%\"}"
url="${url#\"}"
siteurl="${url}ws/v1/slider/publisher/slider/hbase-site.xml"
curl -k -o hbase-site.dnld $siteurl
grep -v 'hbase.tmp.dir' hbase-site.dnld > hbase-site.xml

linksurl="${url}ws/v1/slider/publisher/slider/quicklinks"
curl -k -o links.json $linksurl
python $DIR/links.py
#| sed -e 's/\/\///g' | awk 'BEGIN { FS = ":" } ; { print $2 }'
