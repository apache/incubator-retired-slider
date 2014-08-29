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

"""
Launches slider


"""



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

def dirMustExist(dirname):
  if not os.path.exists(dirname):
    raise Exception("Directory does not exist: %s " % dirname)
  return dirname

def read(pipe, line):
  """
  read a char, append to the listing if there is a char that is not \n
  :param pipe: pipe to read from 
  :param line: line being built up
  :return: (the potentially updated line, flag indicating newline reached)
  """

  c = pipe.read(1)
  if c != "":
    o = c.decode('utf-8')
    if o != '\n':
      line += o
      return line, False
    else:
      return line, True
  else:
    return line, False


def runProcess(commandline):
  """
  Run a process
  :param commandline: command line 
  :return:the return code
  """
  print "ready to exec : %s" % commandline
  exe = subprocess.Popen(commandline,
                         stdin=None,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=False)
  stdout = exe.stdout
  stderr = exe.stderr
  outline = ""
  errline = ""
  while exe.poll() is None:
    # process is running; grab output and echo every line
    outline, done = read(stdout, outline)
    if done:
      print outline
      outline = ""
    errline, done = read(stderr, errline)
    if done:
      print errline
      errline = ""

  # get tail
  out, err = exe.communicate()
  print outline + out.decode()
  print errline + err.decode()
  return exe.returncode

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

def java(classname, args, classpath, jvm_opts_list):
  """
  Execute a java process, hooking up stdout and stderr
  and printing them a line at a time as they come in
  :param classname: classname
  :param args:  arguments to the java program
  :param classpath: classpath
  :param jvm_opts_list: list of JVM options
  :return: the exit code.
  """
  # split the JVM opts by space
  # java = "/usr/bin/java"
  prg = "java"
  if which("java") is None:
    prg = os.environ["JAVA_HOME"] + "/bin/java"
  commandline = [prg]
  commandline.extend(jvm_opts_list)
  commandline.append("-classpath")
  commandline.append(classpath)
  commandline.append(classname)
  commandline.extend(args)
  return runProcess(commandline)


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
  # print "stdout encoding: "+ sys.stdout.encoding
  args = sys.argv[1:]
  slider_home = sliderDir()
  libdir = dirMustExist(libDir(slider_home))
  confdir = dirMustExist(confDir(slider_home))
  default_jvm_opts = DEFAULT_JVM__OPTS % confdir
  slider_jvm_opts = os.environ.get(SLIDER_JVM_OPTS, default_jvm_opts)
  jvm_opts_split = slider_jvm_opts.split()
  slider_classpath_extra = os.environ.get(SLIDER_CLASSPATH_EXTRA, "")
  p = os.pathsep    # path separator
  d = os.sep        # dir separator
  slider_classpath = libdir + d + "*" + p \
                     + confdir + p \
                     + slider_classpath_extra 
                     

  print "slider_home = \"%s\"" % slider_home
  print "slider_jvm_opts = \"%s\"" % slider_jvm_opts
  print "slider_classpath = \"%s\"" % slider_classpath

  return java(SLIDER_CLASSNAME,
              args,
              slider_classpath,
              jvm_opts_split)

if __name__ == '__main__':
  """
  Entry point
  """
  try:
    returncode = main()
  except Exception as e:
    print "Exception: %s " % e.message
    returncode = -1
  
  sys.exit(returncode)
