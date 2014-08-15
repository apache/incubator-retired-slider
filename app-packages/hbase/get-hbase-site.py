#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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


"""Gets hbase-site.xml from running HBase instance
First argument is the name of cluster instance
"""

import sys
import urllib2
import subprocess

f=subprocess.Popen("slider status "+sys.argv[1], shell=True, stdout=subprocess.PIPE).stdout
for line in f:
  pos = line.find("info.am.web.url")
  if pos > 0 :
    part = line[(pos+20) :]
    endPos = part.find("\"")
    url = part[: (endPos-1)]
    url = url + "/ws/v1/slider/publisher/slider/hbase-site.xml"
    print url
    response = urllib2.urlopen(url)
    html = response.read()

    fout=open("hbase-site.xml", "w")
    fout.write(html)
    fout.close()
    f.close()

    sys.exit(0)

print "info.am.web.url key was not found for " + sys.argv[1]
sys.exit(1)
