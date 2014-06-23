# !/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys
import os
import subprocess

CONF = "conf"

LIB = "lib"

SLIDER_CONF_DIR = "SLIDER_CONF_DIR"
SLIDER_JVM_OPTS = "SLIDER_JVM_OPTS"
SLIDER_CLASSPATH_EXTRA = "SLIDER_CLASSPATH_EXTRA"

SLIDER_CLASSNAME = "org.apache.slider.Slider"
DEFAULT_JVM__OPTS = "-Djava.net.preferIPv4Stack=true -Djava.awt.headless=true -Xmx256m -Djava.confdir=%s"

"""Launches slider


"""



print os.environ['HOME']


def scriptDir():
  """ 
  get the script path
  """
  return os.path.dirname(os.path.realpath(__file__))

def sliderDir():
  return os.path.dirname(scriptDir())

def libDir(sliderdir) :
  return os.path.join(sliderdir, LIB)

def confDir(sliderdir):
  """
  determine the active configuration directory 
  :param sliderdir: slider directory 
  :return: the configuration directory -any env var will
  override the relative path
  """
  localconf = os.path.join(sliderdir, CONF)
  return os.environ.get(SLIDER_CONF_DIR,localconf) 

def dirMustExist(dir):
  if not os.path.exists(dir):
    raise Exception("Directory does not exist: %s " % dir)
  return dir


def usage():
  print "Usage: slider <action> <arguments>"
  return 1


def main():
  """
  Slider main method
  :return: exit code of the process
  """
  if len(sys.argv)==1 :
    return usage()
  args = sys.argv[1:]
  slider_home = sliderDir()
  libdir = dirMustExist(libDir(slider_home))
  confdir = dirMustExist(confDir(slider_home))
  default_jvm_opts = DEFAULT_JVM__OPTS % confdir
  slider_jvm_opts = os.environ.get(SLIDER_JVM_OPTS, default_jvm_opts)
  slider_classpath_extra = os.environ.get(SLIDER_CLASSPATH_EXTRA, "")
  p = os.pathsep    # path separator
  d = os.sep        # dir separator
  slider_classpath = '"' + \
                     libdir + d + "*" + p \
                     + confdir + p \
                     + slider_classpath_extra \
                     + '"'

  print "slider_home = \"%s\"" % slider_home
  print "slider_jvm_opts = \"%s\"" % slider_jvm_opts
  print "slider_classpath = \"%s\"" % slider_classpath
  
  
  commandline = ["java",]
  # commandline.append(slider_jvm_opts)
  commandline.append("-classpath")
  commandline.append(slider_classpath)
  commandline.append(SLIDER_CLASSNAME)
  commandline.extend(args)
  print "ready to exec : %s" % commandline
  # docs warn of using PIPE on stderr 
  return subprocess.call(commandline,
                         stdin=None,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=False)





if __name__ == '__main__':
  print "slider python script"
  try:
    rv = main()
    if rv != 0:
      print "exit code = %d" % rv
  except Exception as e:
    print "Exception: %s " % e.message
    rv = -1
  
  sys.exit(rv)
