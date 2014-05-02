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
import json
import pprint
import sys
from AgentConfig import AgentConfig
from AgentException import AgentException
from PythonExecutor import PythonExecutor
import hostname


logger = logging.getLogger()


class CustomServiceOrchestrator():
  """
  Executes a command for custom service. stdout and stderr are written to
  tmpoutfile and to tmperrfile respectively.
  """

  SCRIPT_TYPE_PYTHON = "PYTHON"
  COMMAND_NAME_STATUS = "STATUS"
  LIVE_STATUS = "STARTED"
  DEAD_STATUS = "INSTALLED"

  def __init__(self, config, controller):
    self.config = config
    self.tmp_dir = config.getResolvedPath(AgentConfig.APP_TASK_DIR)
    self.python_executor = PythonExecutor(self.tmp_dir, config)
    self.status_commands_stdout = os.path.join(self.tmp_dir,
                                               'status_command_stdout.txt')
    self.status_commands_stderr = os.path.join(self.tmp_dir,
                                               'status_command_stderr.txt')
    self.public_fqdn = hostname.public_hostname()
    self.applied_configs = {}
    # Clean up old status command files if any
    try:
      os.unlink(self.status_commands_stdout)
      os.unlink(self.status_commands_stderr)
    except OSError:
      pass # Ignore fail
    self.base_dir = os.path.join(
      config.getResolvedPath(AgentConfig.APP_PACKAGE_DIR), "package")


  def runCommand(self, command, tmpoutfile, tmperrfile,
                 override_output_files=True, store_config=False):
    try:
      script_type = command['commandParams']['script_type']
      script = command['commandParams']['script']
      timeout = int(command['commandParams']['command_timeout'])
      task_id = command['taskId']
      command_name = command['roleCommand']

      script_path = self.resolve_script_path(self.base_dir, script, script_type)
      script_tuple = (script_path, self.base_dir)

      tmpstrucoutfile = os.path.join(self.tmp_dir,
                                     "structured-out-{0}.json".format(task_id))
      if script_type.upper() != self.SCRIPT_TYPE_PYTHON:
      # We don't support anything else yet
        message = "Unknown script type {0}".format(script_type)
        raise AgentException(message)
        # Execute command using proper interpreter
      json_path = self.dump_command_to_json(command, store_config)
      py_file_list = [script_tuple]
      # filter None values
      filtered_py_file_list = [i for i in py_file_list if i]

      # Executing hooks and script
      ret = None
      for py_file, current_base_dir in filtered_py_file_list:
        script_params = [command_name, json_path, current_base_dir]
        python_paths = [os.path.join(self.config.getWorkRootPath(),
                                     "infra/agent/slider-agent/jinja2"),
                        os.path.join(self.config.getWorkRootPath(),
                                     "infra/agent/slider-agent")]
        environment_vars = [("PYTHONPATH", ":".join(python_paths))]
        ret = self.python_executor.run_file(py_file, script_params,
                                            tmpoutfile, tmperrfile, timeout,
                                            tmpstrucoutfile,
                                            override_output_files,
                                            environment_vars)
        # Next run_file() invocations should always append to current output
        override_output_files = False
        if ret['exitcode'] != 0:
          break

      if not ret: # Something went wrong
        raise AgentException("No script has been executed")

    except Exception: # We do not want to let agent fail completely
      exc_type, exc_obj, exc_tb = sys.exc_info()
      message = "Caught an exception while executing " \
                "command: {0}: {1}".format(exc_type, exc_obj)
      logger.exception(message)
      ret = {
        'stdout': message,
        'stderr': message,
        'structuredOut': '{}',
        'exitcode': 1,
      }

    return ret


  def resolve_script_path(self, base_dir, script, script_type):
    """
    Encapsulates logic of script location determination.
    """
    path = os.path.join(base_dir, script)
    if not os.path.exists(path):
      message = "Script {0} does not exist".format(path)
      raise AgentException(message)
    return path

  def requestComponentStatus(self, command):
    """
     Component status is determined by exit code, returned by runCommand().
     Exit code 0 means that component is running and any other exit code means that
     component is not running
    """
    override_output_files = True # by default, we override status command output
    if logger.level == logging.DEBUG:
      override_output_files = False

    if command['roleCommand'] == "GET_CONFIG":
      logger.info("Requesting applied config ...")
      return {
        'configurations': self.applied_configs
      }

    else:
      res = self.runCommand(command, self.status_commands_stdout,
                            self.status_commands_stderr,
                            override_output_files=override_output_files)
      if res['exitcode'] == 0:
        res['exitcode'] = CustomServiceOrchestrator.LIVE_STATUS
      else:
        res['exitcode'] = CustomServiceOrchestrator.DEAD_STATUS

      return res
    pass

  def dump_command_to_json(self, command, store_config=False):
    """
    Converts command to json file and returns file path
    """
    # Perform few modifications to stay compatible with the way in which
    # site.pp files are generated by manifestGenerator.py
    public_fqdn = self.public_fqdn
    command['public_hostname'] = public_fqdn
    # Now, dump the json file
    command_type = command['commandType']
    from ActionQueue import ActionQueue  # To avoid cyclic dependency

    if command_type == ActionQueue.STATUS_COMMAND:
      # These files are frequently created, thats why we don't
      # store them all, but only the latest one
      file_path = os.path.join(self.tmp_dir, "status_command.json")
    else:
      task_id = command['taskId']
      file_path = os.path.join(self.tmp_dir, "command-{0}.json".format(task_id))
      # Json may contain passwords, that's why we need proper permissions
    if os.path.isfile(file_path):
      os.unlink(file_path)

    self.finalize_command(command, store_config)

    with os.fdopen(os.open(file_path, os.O_WRONLY | os.O_CREAT,
                           0600), 'w') as f:
      content = json.dumps(command, sort_keys=False, indent=4)
      f.write(content)
    return file_path

  """
  patch content
  ${AGENT_WORK_ROOT} -> AgentConfig.getWorkRootPath()
  ${AGENT_LOG_ROOT} -> AgentConfig.getLogPath()
  """

  def finalize_command(self, command, store_config):

    if 'configurations' in command:
      for key in command['configurations']:
        if len(command['configurations'][key]) > 0:
          for k, value in command['configurations'][key].items():
            if value and len(value) > 0 and isinstance(value, basestring) > 0:
              value = value.replace("${AGENT_WORK_ROOT}",
                                    self.config.getWorkRootPath())
              value = value.replace("${AGENT_LOG_ROOT}",
                                    self.config.getLogPath())
              command['configurations'][key][k] = value
              pass
            pass
          pass
        pass
      pass

    if store_config:
      logger.info("Storing applied config: " + pprint.pformat(command['configurations']))
      self.applied_configs = command['configurations']

  pass


