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
import subprocess

SLIDER_DIR = os.getenv('SLIDER_HOME', None)
if SLIDER_DIR == None or (not os.path.exists(SLIDER_DIR)):
  print "Unable to find SLIDER_HOME. Please configure SLIDER_HOME before running accumulo-slider"
  sys.exit(1)
SLIDER_CMD = os.path.join(SLIDER_DIR, 'bin', 'slider.py')

CMD_OPTS = {}

def call(cmd):
  print "Running: " + " ".join(cmd)
  retcode = subprocess.call(cmd)
  if retcode != 0:
    raise Exception("return code from running %s was %d" % (cmd[0], retcode))

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

def get_all_conf():
  """Syntax: [accumulo-slider --app appname --appconf confdir getconf]
  Downloads configuration for an accumulo instance to a specified conf dir,
  overwriting if the files already exist
  """
  if not 'app_conf' in CMD_OPTS.keys():
    print_usage()
    sys.exit(1)
  confdir = CMD_OPTS['app_conf']

  if not 'app_name' in CMD_OPTS.keys():
    print_usage()
    sys.exit(1)

  client_file = os.path.join(confdir, 'client.conf')
  site_file = os.path.join(confdir, 'accumulo-site.xml')
  env_file = os.path.join(confdir, 'accumulo-env.sh')

  if os.path.exists(client_file):
    os.remove(client_file)
  if os.path.exists(site_file):
    os.remove(site_file)
  if os.path.exists(env_file):
    os.remove(env_file)

  get_conf("client", "properties", client_file)
  get_conf("accumulo-site", "xml", site_file)
  get_conf("accumulo-env", "env", env_file)

def get_conf(confname, fileformat, destfile):
  if os.path.exists(destfile):
    raise Exception("conf file %s was removed but still exists" % (destfile))

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
    print "accumulo-slider --app <name> [--user <username>] quicklinks"
    print "accumulo-slider --app <name> [--user <username>] proxies"
    print "accumulo-slider --app <name> --appconf <confdir> [--user <username>] getconf"
    print_commands()

def unknown_command(*args):
  print "Unknown command: [accumulo-slider %s]" % ' '.join(sys.argv[1:])
  print_usage()

COMMANDS = {"quicklinks" : quicklinks, "proxies": proxies, "getconf": get_all_conf,
            "help": print_usage}

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
  (COMMANDS.get(COMMAND, unknown_command))(*ARGS)

if __name__ == "__main__":
  main()
