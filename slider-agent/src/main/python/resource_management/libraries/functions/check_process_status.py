#!/usr/bin/env python
"""
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

Slider Agent

"""

from resource_management.core.exceptions import ComponentIsNotRunning
from resource_management.core.logger import Logger
__all__ = ["check_process_status"]

import os
import subprocess
import platform

IS_WINDOWS = platform.system() == "Windows"


def check_process_status(pid_file):
  """
  Function checks whether process is running.
  Process is considered running, if pid file exists, and process with
  a pid, mentioned in pid file is running
  If process is not running, will throw ComponentIsNotRunning exception

  @param pid_file: path to service pid file
  """
  if not pid_file or not os.path.isfile(pid_file):
    if not pid_file:
      Logger.warning("pid_file is not valid")
    else:
      Logger.info("pid file does not exist {0}".format(pid_file))
    raise ComponentIsNotRunning()

  with open(pid_file, "r") as f:
    try:
      pid = int(f.read())
    except:
      Logger.debug("Pid file {0} does not exist".format(pid_file))
      raise ComponentIsNotRunning()

    if IS_WINDOWS:
      not_running = True
      try:
        ps = subprocess.Popen(r'tasklist.exe /NH /FI "PID eq %d"' % (pid),
                              shell=True, stdout=subprocess.PIPE)
        output = ps.stdout.read()
        ps.stdout.close()
        ps.wait()
        not_running = str(pid) not in output
      except OSError, e:
        Logger.debug("Error {0}".format(str(e)))
        Logger.info("Process with pid {0} is not running. Stale pid file"
                     " at {1}".format(pid, pid_file))
      if not_running:
        raise ComponentIsNotRunning()
    else:
      try:
        # Kill will not actually kill the process
        # From the doc:
        # If sig is 0, then no signal is sent, but error checking is still
        # performed; this can be used to check for the existence of a
        # process ID or process group ID.
        os.kill(pid, 0)
      except OSError:
        Logger.info("Process with pid {0} is not running. Stale pid file"
                  " at {1}".format(pid, pid_file))
        raise ComponentIsNotRunning()
  pass
