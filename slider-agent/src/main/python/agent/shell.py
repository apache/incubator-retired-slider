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

import logging
import os
import signal
import subprocess
import threading
import platform
from process_utils import get_flat_process_tree, kill_pids, wait_for_entire_process_tree_death, \
  get_processes_running, get_command_by_pid

if platform.system() != "Windows":
  try:
    import pwd
  except ImportError:
    import winpwd as pwd

global serverTracker
serverTracker = {}
logger = logging.getLogger()

shellRunner = None
threadLocal = threading.local()
tempFiles = [] 
def noteTempFile(filename):
  tempFiles.append(filename)

def getTempFiles():
  return tempFiles

class _dict_to_object:
  def __init__(self, entries):
    self.__dict__.update(entries)
  def __getitem__(self, item):
    return self.__dict__[item]
# windows specific code
def _kill_process_with_children_windows(parent_pid):
  shellRunner().run(["taskkill", "/T", "/PID", "{0}".format(parent_pid)])


class shellRunnerWindows:
  # Run any command
  def run(self, script, user=None):
    logger.warn("user argument ignored on windows")
    code = 0
    if not isinstance(script, list):
      cmd = " "
      cmd = cmd.join(script)
    else:
      cmd = script
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=False)
    out, err = p.communicate()
    code = p.wait()
    logger.debug("Exitcode for %s is %d" % (cmd, code))
    return {'exitCode': code, 'output': out, 'error': err}

  def runPowershell(self, file=None, script_block=None, args=[]):
    logger.warn("user argument ignored on windows")
    code = 0
    cmd = None
    if file:
      cmd = ['powershell', '-WindowStyle', 'Hidden', '-File', file] + args
    elif script_block:
      cmd = ['powershell', '-WindowStyle', 'Hidden', '-Command', script_block] + args
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=False)
    out, err = p.communicate()
    code = p.wait()
    logger.debug("Exitcode for %s is %d" % (cmd, code))
    return _dict_to_object({'exitCode': code, 'output': out, 'error': err})

#linux specific code
def _kill_process_with_children_linux(parent_pid):
    """
    Kills process tree starting from a given pid.
    :param parent_pid: head of tree
    :param graceful_kill_delays: map <command name, custom delay between SIGTERM and SIGKILL>
    :return:
    """
  
    pids = get_flat_process_tree(parent_pid)
    logger.info("Process tree: %s" % ','.join(pids))
    try:
      kill_pids(pids, signal.SIGTERM)
    except Exception, e:
      logger.warn("Failed to kill PID %d" % parent_pid)
      logger.warn("Reported error: " + repr(e))
  
    wait_for_entire_process_tree_death(pids)
  
    try:
      running_processes = get_processes_running(pids)
      if running_processes:
        process_names = map(lambda x: get_command_by_pid(x),  running_processes)
        logger.warn("These PIDs %s did not die after SIGTERM, sending SIGKILL. Exact commands to be killed:\n %s" %
                    (", ".join(running_processes), "\n".join(process_names)))
        kill_pids(running_processes, signal.SIGKILL)
    except Exception, e:
      logger.error("Failed to send SIGKILL to PID %d. Process exited?" % parent_pid)
      logger.error("Reported error: " + repr(e))


def _changeUid():
  try:
    os.setuid(threadLocal.uid)
  except Exception:
    logger.warn("can not switch user for running command.")


class shellRunnerLinux:
  # Run any command
  def run(self, script, user=None):
    try:
      if user != None:
        user = pwd.getpwnam(user)[2]
      else:
        user = os.getuid()
      threadLocal.uid = user
    except Exception:
      logger.warn("can not switch user for RUN_COMMAND.")
    code = 0
    cmd = " "
    cmd = cmd.join(script)
    p = subprocess.Popen(cmd, preexec_fn=_changeUid, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=True, close_fds=True)
    out, err = p.communicate()
    code = p.wait()
    logger.debug("Exitcode for %s is %d" % (cmd, code))
    return {'exitCode': code, 'output': out, 'error': err}


def kill_process_with_children(parent_pid):
  if platform.system() == "Windows":
    _kill_process_with_children_windows(parent_pid)
  else:
    _kill_process_with_children_linux(parent_pid)


if platform.system() == "Windows":
  shellRunner = shellRunnerWindows
else:
  shellRunner = shellRunnerLinux