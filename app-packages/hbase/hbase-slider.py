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
import os
import subprocess
from os.path import expanduser
from os.path import exists
import glob
import getopt
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

# find path to given command
def which(program):
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None

SLIDER_DIR = os.getenv('SLIDER_HOME', None)
if SLIDER_DIR == None or (not os.path.exists(SLIDER_DIR)):
  SLIDER_CMD = which("slider")
  if SLIDER_DIR == None:
    if os.path.exists("/usr/bin/slider"):
      SLIDER_CMD = "/usr/bin/slider"
    else:
      print "Unable to find SLIDER_HOME or slider command. Please configure SLIDER_HOME before running hbase-slider"
      sys.exit(1)
else:
  SLIDER_CMD = os.path.join(SLIDER_DIR, 'bin', 'slider.py')

HBASE_TMP_DIR=os.path.join(tempfile.gettempdir(), "hbase-temp")

# call slider command
def call(cmd):
  print "Running: " + " ".join(cmd)
  retcode = subprocess.call(cmd)
  if retcode != 0:
    raise Exception("return code from running %s was %d" % (cmd[0], retcode))

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

def install(cluster_instance, dir):
  """Syntax: [hbase-slider cluster_instance install dir]
  Installs a fully configured hbase client in the specified dir
  The resulting client may be used on its own without hbase-slider
  """
  if os.path.exists(dir):
    raise Exception("Install dir must not exist: " + dir)

  workdir = os.path.join(tempfile.gettempdir(), 'install-work-dir')

  statusfile = os.path.join(workdir, 'status.json')
  cmd = [SLIDER_CMD, "status", cluster_instance, "--out", statusfile]
  call(cmd)

  infile = open(statusfile)
  try:
    content = json.load(infile)
  finally:
    infile.close()

  appdef = content['options']['application.def']
  appdeffile = appdef[appdef.rfind('/')+1:]
  cmd = ["hadoop", "fs", "-copyToLocal", appdef, workdir]
  call(cmd)

  cmd = ["unzip", os.path.join(workdir, appdeffile), "-d", workdir]
  call(cmd)

  gzfile = glob.glob(os.path.join(workdir, 'package', 'files',  'hbase*gz'))
  if len(gzfile) != 1:
    raise Exception("got " + gzfile + " from glob")
  cmd = ["tar", "xvzf", gzfile[0], '-C', workdir]
  call(cmd)

  tmp_hbase = glob.glob(os.path.join(workdir, 'hbase-[.0-9]*'))
  if len(tmp_hbase) != 1:
    raise Exception("got " + tmp_hbase + " from glob")
  tmp_hbase = tmp_hbase[0]

  confdir = os.path.join(tmp_hbase, 'conf')
  tmpHBaseConfFile=os.path.join(tempfile.gettempdir(), "hbase-site.xml")

  call([SLIDER_CMD, "registry", "--getconf", "hbase-site", "--user", "hbase", "--format", "xml", "--dest", tmpHBaseConfFile, "--name", cluster_instance])
  global HBASE_TMP_DIR
  propertyMap = {'hbase.tmp.dir' : HBASE_TMP_DIR, "instance" : cluster_instance}
  writePropertiesToConfigXMLFile(tmpHBaseConfFile, os.path.join(confdir, "hbase-site.xml"), propertyMap)

  libdir = os.path.join(tmp_hbase, 'lib')
  for jar in glob.glob(os.path.join(workdir, 'package', 'files', '*jar')):
    shutil.move(jar, libdir)
  shutil.move(tmp_hbase, dir)

def quicklinks(app_name):
  """Syntax: [hbase-slider appname quicklinks]
  Prints the quicklinks information of hbase-slider registry
  """
  cmd = [SLIDER_CMD, "registry", "--getconf", "quicklinks", "--format", "json",
         "--name", app_name]

  call(cmd)

home = expanduser("~")
if len(sys.argv) < 2:
  print "optionally you can specify the output directory for conf dir using:"
  print "  --appconf=<dir>"
  print "optionally you can specify the (existing) directory for hbase conf files (as template) using:"
  print "  --hbaseconf=<dir>"
  print "optionally you can specify the age in number of hours beyond which hbase-site.xml would be retrieved from slider cluster"
  print "  --ttl=<age of hbase-site.xml>"
  print "the name of cluster instance is required as the first parameter following options"
  print "the second parameter can be:"
  print "  shell (default) - activates hbase shell based on retrieved hbase-site.xml"
  print "  quicklinks      - prints quicklinks from registry"
  print "  install <dir>   - installs hbase client into <dir>"
  sys.exit(1)

try:
  opts, args = getopt.getopt(sys.argv[1:], "", ["appconf=", "hbaseconf=", "ttl="])
except getopt.GetoptError as err:
  # print help information and exit:
  print str(err)
  sys.exit(2)

cluster_instance=args[0]
local_conf_dir=os.path.join(home, cluster_instance, 'conf')
hbase_conf_dir="/etc/hbase/conf"
ttl=0
for o, a in opts:
  if o == "--appconf":
    local_conf_dir = a
  elif o == "--hbaseconf":
    hbase_conf_dir = a
  elif o == "--ttl":
    ttl = a

if len(args) > 1:
  if args[1] == 'quicklinks':
    quicklinks(cluster_instance)
    sys.exit(0)
  elif args[1] == 'install':
    install(cluster_instance, args[2])
    sys.exit(0)

needToRetrieve=True
HBaseConfFile=os.path.join(local_conf_dir, "hbase-site.xml")
if not exists(local_conf_dir):
  shutil.copytree(hbase_conf_dir, local_conf_dir)
else:
  shutil.copy2(os.path.join(hbase_conf_dir, "hbase-env.sh"), local_conf_dir)
  if exists(HBaseConfFile):
    diff = os.path.getmtime(HBaseConfFile)-int(time.time())
    diff = diff / 60 / 60
    print HBaseConfFile + " is " + str(diff) + " hours old"
    if diff < ttl:
      needToRetrieve=False

if needToRetrieve:
  tmpHBaseConfFile=os.path.join(tempfile.gettempdir(), "hbase-site.xml")

  call([SLIDER_CMD, "registry", "--getconf", "hbase-site", "--user", "hbase", "--format", "xml", "--dest", tmpHBaseConfFile, "--name", cluster_instance])
  propertyMap = {'hbase.tmp.dir' : HBASE_TMP_DIR, "instance" : cluster_instance}
  writePropertiesToConfigXMLFile(tmpHBaseConfFile, HBaseConfFile, propertyMap)
  print "hbase configuration is saved in " + HBaseConfFile

call(["hbase", "--config", local_conf_dir, "shell"])
