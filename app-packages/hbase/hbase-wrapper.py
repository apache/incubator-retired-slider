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


"""Invokes hbase shell after retrieving effective hbase-site.xml from a live Slider HBase cluster
First argument is the name of cluster instance
"""
from subprocess import call
import os
from os.path import expanduser
from os.path import exists
import glob
import re
import fnmatch
import shutil
import logging
import socket
from string import Template
import time
import fileinput
import sys
import tempfile
import json
import datetime
from xml.dom import minidom
from xml.dom.minidom import parseString
import xml.etree.ElementTree as ET
import urllib2
import hashlib
import random
import httplib, ssl

# Write text into a file
# wtext - Text to write
def writeToFile(wtext, outfile, isAppend=False):
    mode = 'w'
    if isAppend:
        mode = 'a+'
    outf = open(outfile, mode)
    try:
        outf.write(wtext)
    finally:
        outf.close()

# Update the XML configuration properties and write to another file
# infile - Input config XML file
# outfile - Output config XML file
# propertyMap - Properties to add/update
#               {'name1':'value1', 'name2':'value2',...}
def writePropertiesToConfigXMLFile(infile, outfile, propertyMap):
    xmldoc = minidom.parse(infile)
    cfgnode = xmldoc.getElementsByTagName("configuration")
    if len(cfgnode) == 0:
        raise Exception("Invalid Config XML file: " + infile)
    cfgnode = cfgnode[0]
    propertyMapKeys = propertyMap.keys()
    removeProp = []
    modified = []
    for node in xmldoc.getElementsByTagName("name"):
        name = node.childNodes[0].nodeValue.strip()
        if name in propertyMapKeys:
            modified.append(name)
            for vnode in node.parentNode.childNodes:
                if vnode.nodeName == "value":
                   if vnode.childNodes == []:
                     removeProp.append(name)
                     modified.remove(name)
                   else:
                     vnode.childNodes[0].nodeValue = propertyMap[name]
    remaining = list(set(propertyMapKeys) - set(modified))
    # delete properties whose value is set to None e.g.<value></value>
    for node in xmldoc.getElementsByTagName("name"):
        name = node.childNodes[0].nodeValue.strip()
        if name in removeProp:
          parent = node.parentNode
          super = parent.parentNode
          super.removeChild(parent)
    for property in remaining:
        pn = xmldoc.createElement("property")
        nn = xmldoc.createElement("name")
        ntn = xmldoc.createTextNode(property)
        nn.appendChild(ntn)
        pn.appendChild(nn)
        vn = xmldoc.createElement("value")
        vtn = xmldoc.createTextNode(str(propertyMap[property]))
        vn.appendChild(vtn)
        pn.appendChild(vn)
        cfgnode.appendChild(pn)
    writeToFile(xmldoc.toxml(), outfile)

home = expanduser("~")
if len(sys.argv) < 2:
  print "the name of cluster instance is required"
  sys.exit(1)

cluster_instance=sys.argv[1]

hbase_conf_dir="/etc/hbase/conf"
local_conf_dir=os.path.join(home, cluster_instance, 'conf')
if not exists(local_conf_dir):
  shutil.copytree(hbase_conf_dir, local_conf_dir)
tmpHBaseConfFile=os.path.join('/tmp', "hbase-site.xml")

call(["slider", "registry", "--getconf", "hbase-site", "--user", "hbase", "--format", "xml", "--dest", tmpHBaseConfFile, "--name", cluster_instance])
HBaseConfFile=os.path.join(local_conf_dir, "hbase-site.xml")
propertyMap = {'hbase.tmp.dir' : '/tmp/hbase-tmp', "instance" : cluster_instance}
writePropertiesToConfigXMLFile(tmpHBaseConfFile, HBaseConfFile, propertyMap)

call(["hbase", "--config", local_conf_dir, "shell"])
