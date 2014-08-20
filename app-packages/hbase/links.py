#!/usr/bin/env python

'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

from __future__ import print_function
import logging
import json

file = open("links.json")
links = json.load(file)
file.close()
if links.has_key("entries"):
  entries = links["entries"]
  if entries.has_key("org.apache.slider.hbase.rest"):
    print("org.apache.slider.hbase.rest : %s" % entries["org.apache.slider.hbase.rest"])
  if entries.has_key("org.apache.slider.hbase.thrift"):
    print("org.apache.slider.hbase.thrift : %s" % entries["org.apache.slider.hbase.thrift"])
  if entries.has_key("org.apache.slider.hbase.thrift2"):
    print("org.apache.slider.hbase.thrift2 : %s" % entries["org.apache.slider.hbase.thrift2"])
