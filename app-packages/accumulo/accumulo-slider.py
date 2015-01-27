#!/usr/bin/python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import json
import glob
import tempfile
import subprocess
import shutil

SLIDER_DIR = os.getenv('SLIDER_HOME', None)
if SLIDER_DIR == None or (not os.path.exists(SLIDER_DIR)):
  print "Unable to find SLIDER_HOME. Please configure SLIDER_HOME before running accumulo-slider"
  sys.exit(1)
SLIDER_CMD = os.path.join(SLIDER_DIR, 'bin', 'slider.py')

TMP_DIR = os.path.join(tempfile.gettempdir(), "accumulo-slider-tmp."+str(os.getpid()))

CMD_OPTS = {}

def call(cmd):
  print "Running: " + " ".join(cmd)
  retcode = subprocess.call(cmd)
  if retcode != 0:
    raise Exception("return code from running %s was %d" % (cmd[0], retcode))

def exec_accumulo_command(command, args=[]):
  ACCUMULO_DIR = os.getenv('ACCUMULO_HOME', None)
  if ACCUMULO_DIR == None or (not os.path.exists(ACCUMULO_DIR)):
    print "Unable to find ACCUMULO_HOME. Please configure ACCUMULO_HOME before running accumulo-slider " + command
    sys.exit(1)
  ACCUMULO_CMD = os.path.join(ACCUMULO_DIR, 'bin', 'accumulo')

  confdir = get_all_conf()
  if command == 'shell' or command == 'admin':
    cmd = [ACCUMULO_CMD, command, '--config-file', os.path.join(confdir, 'client.conf')] + list(args)
  else:
    cmd = [ACCUMULO_CMD, command] + list(args)
  print "Setting ACCUMULO_CONF_DIR=" + confdir
  os.putenv("ACCUMULO_CONF_DIR", confdir)
  call(cmd)

def exec_tool_command(jarfile, mainclass, args=[]):
  ACCUMULO_DIR = os.getenv('ACCUMULO_HOME', None)
  if ACCUMULO_DIR == None or (not os.path.exists(ACCUMULO_DIR)):
    print "Unable to find ACCUMULO_HOME. Please configure ACCUMULO_HOME before running accumulo-slider tool"
    sys.exit(1)
  TOOL_CMD = os.path.join(ACCUMULO_DIR, 'bin', 'tool.sh')

  confdir = get_all_conf()
  cmd = [TOOL_CMD, jarfile, mainclass] + list(args)
  print "Setting ACCUMULO_CONF_DIR=" + confdir
  os.putenv("ACCUMULO_CONF_DIR", confdir)
  call(cmd)

def jar(jarfile, *args):
  """Syntax: [accumulo-slider --app appname[ --appconf confdir] jar jarfile [mainclass]]
    OR: [accumulo-slider --appconf confdir jar jarfile [mainclass]]
  Runs a class from a specified jar
  """
  exec_accumulo_command("jar", jarfile, args=args)

def classname(mainclass, *args):
  """Syntax: [accumulo-slider --app appname[ --appconf confdir] classname mainclass]
    OR: [accumulo-slider --appconf confdir classname mainclass]
  Runs a specified class on the existing accumulo classpath
  """
  exec_accumulo_command(mainclass, args=args)

def shell(*args):
  """Syntax: [accumulo-slider --app appname[ --appconf confdir] shell]
    OR: [accumulo-slider --appconf confdir shell]
  Runs an accumulo shell
  """
  exec_accumulo_command("shell", args=args)

def admin(*args):
  """Syntax: [accumulo-slider --app appname[ --appconf confdir] admin cmd]
    OR: [accumulo-slider --appconf confdir admin cmd]
  Executes an admin command (run without cmd argument for a list of commands)
  """
  exec_accumulo_command("admin", args=args)

def classpath(*args):
  """Syntax: [accumulo-slider --app appname[ --appconf confdir] classpath]
    OR: [accumulo-slider --appconf confdir classpath]
  Prints the classpath of the accumulo client install
  """
  exec_accumulo_command("classpath", args=args)

def info(*args):
  """Syntax: [accumulo-slider --app appname[ --appconf confdir] info]
    OR: [accumulo-slider --appconf confdir info]
  Prints information about an accumulo instance (monitor, masters, and zookeepers)
  """
  exec_accumulo_command("info", args=args)

def rfileinfo(*args):
  """Syntax: [accumulo-slider --app appname[ --appconf confdir] rfile-info rfilename]
    OR: [accumulo-slider --appconf confdir rfile-info rfilename]
  Prints information about a specified RFile
  """
  exec_accumulo_command("rfile-info", args=args)

def logininfo(*args):
  """Syntax: [accumulo-slider --app appname[ --appconf confdir] login-info]
    OR: [accumulo-slider --appconf confdir login-info]
  Prints the supported authentication token types for the accumulo instance
  """
  exec_accumulo_command("login-info", args=args)

def createtoken(*args):
  """Syntax: [accumulo-slider --app appname[ --appconf confdir] create-token]
    OR: [accumulo-slider --appconf confdir create-token]
  Saves a given accumulo authentication token to a file
  """
  exec_accumulo_command("create-token", args=args)

def version(*args):
  """Syntax: [accumulo-slider --app appname[ --appconf confdir] version]
    OR: [accumulo-slider --appconf confdir version]
  Prints the version of the accumulo client install
  """
  exec_accumulo_command("version", args=args)

def tool(jarfile, mainclass, *args):
  """Syntax: [accumulo-slider --app appname[ --appconf confdir] tool jarfile classname]
    OR: [accumulo-slider --appconf confdir tool jarfile classname]
  Runs a mapreduce job using accumulo's tool.sh
  """
  exec_tool_command(jarfile, mainclass, args=args)

def quicklinks():
  """Syntax: [accumulo-slider --app appname quicklinks]
  Prints the quicklinks information of accumulo-slider registry
  """
  global CMD_OPTS
  if not 'app_name' in CMD_OPTS.keys():
    print_usage()
    sys.exit(1)

  cmd = [SLIDER_CMD, "registry", "--getconf", "quicklinks", "--format", "json",
         "--name", CMD_OPTS['app_name']]

  if 'user' in CMD_OPTS.keys():
    cmd.append( "--user "+CMD_OPTS['user'])

  call(cmd)

def proxies():
  """Syntax: [accumulo-slider --app appname proxies]
  Prints the componentinstancedata information of accumulo-slider registry
  """
  global CMD_OPTS
  if not 'app_name' in CMD_OPTS.keys():
    print_usage()
    sys.exit(1)

  cmd = [SLIDER_CMD, "registry", "--getconf", "componentinstancedata",
              "--format", "json", "--name", CMD_OPTS['app_name']]

  if 'user' in CMD_OPTS.keys():
    cmd.append( "--user "+CMD_OPTS['user'])

  call(cmd)

def install(dir):
  """Syntax: [accumulo-slider --app appname install dir]
  Installs a fully configured accumulo client in the specified dir
  The resulting client may be used on its own without accumulo-slider
  """
  global CMD_OPTS
  if not 'app_name' in CMD_OPTS.keys():
    print_usage()
    sys.exit(1)
  if os.path.exists(dir):
    raise Exception("Install dir must not exist: " + dir)

  global TMP_DIR
  workdir = os.path.join(TMP_DIR, 'install-work-dir')

  statusfile = os.path.join(workdir, 'status.json')
  cmd = [SLIDER_CMD, "status", CMD_OPTS['app_name'], "--out", statusfile]
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

  gzfile = glob.glob(os.path.join(workdir, 'package', 'files',  'accumulo*gz'))
  if len(gzfile) != 1:
    raise Exception("got " + gzfile + " from glob")
  cmd = ["tar", "xvzf", gzfile[0], '-C', workdir]
  call(cmd)

  tmp_accumulo = glob.glob(os.path.join(workdir, 'accumulo-[0-9]*'))
  if len(tmp_accumulo) != 1:
    raise Exception("got " + tmp_accumulo + " from glob")
  tmp_accumulo = tmp_accumulo[0]

  confdir = os.path.join(tmp_accumulo, 'conf')
  tmpconf = os.path.join(workdir, 'conf-tmp')
  shutil.move(confdir, tmpconf)
  make_conf(os.path.join(tmpconf, 'templates'), confdir)

  libdir = os.path.join(tmp_accumulo, 'lib')
  for jar in glob.glob(os.path.join(workdir, 'package', 'files', '*jar')):
    shutil.move(jar, libdir)
  shutil.move(tmp_accumulo, dir)

def get_all_conf():
  """Syntax: [accumulo-slider --app appname [--appconf confdir] getconf]
  Downloads configuration for an accumulo instance
  If --appconf is specified, creates the specified conf dir and populates it
  """
  ACCUMULO_CONF_DIR = os.getenv('ACCUMULO_CONF_DIR', None)
  if ACCUMULO_CONF_DIR == None or (not os.path.exists(ACCUMULO_CONF_DIR)):
    ACCUMULO_DIR = os.getenv('ACCUMULO_HOME', None)
    if ACCUMULO_DIR == None or (not os.path.exists(ACCUMULO_DIR)):
      print "Unable to find ACCUMULO_HOME. Please configure ACCUMULO_HOME before running this command"
      sys.exit(1)
    ACCUMULO_CONF_DIR = os.path.join(ACCUMULO_DIR, 'conf')

  global CMD_OPTS
  global TMP_DIR
  confdir = os.path.join(TMP_DIR, 'conf')
  if 'app_conf' in CMD_OPTS.keys():
    confdir = CMD_OPTS['app_conf']
    if os.path.exists(confdir):
      print "Using existing app conf instead of downloading it again: " + confdir
      return confdir

  if not 'app_name' in CMD_OPTS.keys():
    print_usage()
    sys.exit(1)

  make_conf(ACCUMULO_CONF_DIR, confdir)
  return confdir

def make_conf(oldconf, newconf):
  client_file = os.path.join(newconf, 'client.conf')
  site_file = os.path.join(newconf, 'accumulo-site.xml')
  env_file = os.path.join(newconf, 'accumulo-env.sh')
  env_json = os.path.join(newconf, 'accumulo-env.json')

  print "Copying base conf from " + oldconf + " to " + newconf
  shutil.copytree(oldconf, newconf)

  try_remove(client_file)
  try_remove(site_file)
  try_remove(env_file)
  try_remove(env_json)

  get_conf("client", "properties", client_file)
  get_conf("accumulo-site", "xml", site_file)
  get_conf("accumulo-env", "json", env_json)

  infile = open(env_json)
  outfile = open(env_file, 'w')
  try:
    content = json.load(infile)
    outfile.write(content['content'])
  finally:
    outfile.close()
    infile.close()

def try_remove(path):
  try:
    os.remove(path)
  except:
    if os.path.exists(path):
      raise

def get_conf(confname, fileformat, destfile):
  if os.path.exists(destfile):
    print "Conf file " + destfile + " already exists, remove it to re-download"
    return

  cmd = [SLIDER_CMD, "registry", "--getconf", confname, "--format",
         fileformat, "--dest", destfile, "--name", CMD_OPTS['app_name']]
  if 'user' in CMD_OPTS.keys():
    cmd.append("--user " + CMD_OPTS['user'])

  call(cmd)
  if not os.path.exists(destfile):
    raise Exception("Failed to read slider deployed accumulo config " + confname)

def print_commands():
  """Print all client commands and link to documentation"""
  print "Commands:\n\t",  "\n\t".join(sorted(COMMANDS.keys()))
  print "\nHelp:", "\n\thelp", "\n\thelp <command>"

def print_usage(command=None):
  """Print one help message or list of available commands"""
  if command != None:
    if COMMANDS.has_key(command):
      print (COMMANDS[command].__doc__ or
             "No documentation provided for <%s>" % command)
    else:
      print "<%s> is not a valid command" % command
  else:
    print "Usage:"
    print "accumulo-slider --app <name>[ --appconf <confdir> --user <username>] <command>"
    print "  The option --appconf creates a conf dir that can be reused;"
    print "  on subsequent calls to accumulo-slider, --app can be left off if"
    print "  --appconf is specified.  If --appconf is not specified, a"
    print "  temporary conf dir is created each time accumulo-slider is run."
    print_commands()

def unknown_command(*args):
  print "Unknown command: [accumulo-slider %s]" % ' '.join(sys.argv[1:])
  print_usage()

COMMANDS = {"shell": shell, "tool": tool, "admin": admin, "classpath": classpath,
            "info": info, "version": version, "jar": jar,  "classname": classname,
            "quicklinks" : quicklinks, "proxies": proxies, "getconf": get_all_conf,
            "rfile-info": rfileinfo, "login-info": logininfo, "create-token": createtoken,
            "install": install, "help": print_usage}

def parse_config_opts(args):
  curr = args[:]
  curr.reverse()
  global CMD_OPTS
  args_list = []
  while len(curr) > 0:
    token = curr.pop()
    if token == "--app":
      CMD_OPTS['app_name'] = curr.pop() if (len(curr) != 0) else None
    elif token == "--user":
      CMD_OPTS['user'] =  curr.pop() if (len(curr) != 0) else None
    elif token == "--appconf":
      CMD_OPTS['app_conf'] =  curr.pop() if (len(curr) != 0) else None
    else:
      args_list.append(token)
  return args_list

def main():
  args = parse_config_opts(sys.argv[1:])
  if len(args) < 1:
    print_usage()
    sys.exit(-1)
  COMMAND = args[0]
  ARGS = args[1:]
  try:
    (COMMANDS.get(COMMAND, unknown_command))(*ARGS)
  finally:
    global CMD_OPTS
    if os.path.exists(TMP_DIR):
      print "Cleaning up tmp dir " + TMP_DIR
      shutil.rmtree(TMP_DIR)

if __name__ == "__main__":
  main()
