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
import random
import sys
import socket
import posixpath
import platform
import copy
from AgentConfig import AgentConfig
from AgentException import AgentException
from PythonExecutor import PythonExecutor
import hostname
import Constants

MAX_ATTEMPTS = 5

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

  def __init__(self, config, controller, agentToggleLogger):
    self.config = config
    self.controller = controller
    self.tmp_dir = config.getResolvedPath(AgentConfig.APP_TASK_DIR)
    self.python_executor = PythonExecutor(self.tmp_dir, config, agentToggleLogger)
    self.status_commands_stdout = os.path.realpath(posixpath.join(self.tmp_dir,
                                                                  'status_command_stdout.txt'))
    self.status_commands_stderr = os.path.realpath(posixpath.join(self.tmp_dir,
                                                                  'status_command_stderr.txt'))
    self.public_fqdn = hostname.public_hostname()
    self.stored_command = {}
    self.allocated_ports = {}
    self.log_folders = {}
    # Clean up old status command files if any
    try:
      os.unlink(self.status_commands_stdout)
      os.unlink(self.status_commands_stderr)
    except OSError:
      pass # Ignore fail
    self.base_dir = os.path.realpath(posixpath.join(
      config.getResolvedPath(AgentConfig.APP_PACKAGE_DIR), "package"))


  def runCommand(self, command, tmpoutfile, tmperrfile,
                 override_output_files=True, store_command=False):
    allocated_ports = {}
    try:
      py_file_list = []
      json_path = None

      script_type = command['commandParams']['script_type']
      task_id = command['taskId']
      command_name = command['roleCommand']
      # transform upgrade specific command names
      if command_name == 'UPGRADE':
          command_name = 'PRE_UPGRADE'
      if command_name == 'UPGRADE_STOP':
        command_name = 'STOP'

      tmpstrucoutfile = os.path.realpath(posixpath.join(self.tmp_dir,
                                                        "structured-out-{0}.json".format(task_id)))
      if script_type.upper() == self.SCRIPT_TYPE_PYTHON:
        script = command['commandParams']['script']
        timeout = int(command['commandParams']['command_timeout'])
        script_path = ''
        if 'package' in command:
            add_on_dir_str = (self.config.getWorkRootPath()
                              + "/"
                              + AgentConfig.ADDON_PKG_ROOT_DIR
                              + "/application.addon."
                              + command['package']
                             )
            command['commandParams']['addonPackageRoot'] = add_on_dir_str
            add_on_base_dir = os.path.realpath(posixpath.join(add_on_dir_str, "package"))
            logger.info("Add on package: %s, add on base dir: %s" 
                        % (command['package'], str(add_on_base_dir)))
            script_path = self.resolve_script_path(add_on_base_dir, script, script_type)
        else:
            self.base_dir = os.path.realpath(posixpath.join(
                              self.config.getResolvedPath(AgentConfig.APP_PACKAGE_DIR),
                              "package"))
            logger.debug("Base dir: " + str(self.base_dir))
            script_path = self.resolve_script_path(self.base_dir, script, script_type)
        script_tuple = (script_path, self.base_dir)
        py_file_list = [script_tuple]

        json_path = self.dump_command_to_json(command, allocated_ports, store_command)
      elif script_type.upper() == "SHELL":
        timeout = int(command['commandParams']['command_timeout'])

        json_path = self.dump_command_to_json(command, allocated_ports, store_command)
        script_path = os.path.realpath(posixpath.join(self.config.getWorkRootPath(),
                                        "infra", "agent", "slider-agent", "scripts",
                                        "shell_cmd", "basic_installer.py"))
        script_tuple = (script_path, self.base_dir)
        py_file_list = [script_tuple]
      else:
        # We don't support anything else yet
        message = "Unknown script type {0}".format(script_type)
        raise AgentException(message)

      # filter None values
      filtered_py_file_list = [i for i in py_file_list if i]
      logger_level = logging.getLevelName(logger.level)

      # Executing hooks and script
      ret = None
      for py_file, current_base_dir in filtered_py_file_list:
        script_params = [command_name, json_path, current_base_dir]
        python_paths = [os.path.realpath(posixpath.join(self.config.getWorkRootPath(),
                                                        "infra", "agent", "slider-agent", "jinja2")),
                        os.path.realpath(posixpath.join(self.config.getWorkRootPath(),
                                                        "infra", "agent", "slider-agent"))]
        if platform.system() != "Windows":
          environment_vars = [("PYTHONPATH", ":".join(python_paths))]
        else:
          environment_vars = [("PYTHONPATH", ";".join(python_paths))]

        ret = self.python_executor.run_file(py_file, script_params,
                                            tmpoutfile, tmperrfile, timeout,
                                            tmpstrucoutfile,
                                            logger_level,
                                            override_output_files,
                                            environment_vars)
        # Next run_file() invocations should always append to current output
        override_output_files = False
        if ret[Constants.EXIT_CODE] != 0:
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
        Constants.EXIT_CODE: 1,
      }

    if Constants.EXIT_CODE in ret and ret[Constants.EXIT_CODE] == 0:
      ret[Constants.ALLOCATED_PORTS] = copy.deepcopy(allocated_ports)
      ## Generally all ports are allocated at once but just in case
      self.allocated_ports.update(allocated_ports)

    # Irrespective of the outcome report the folder paths
    if command_name == 'INSTALL':
      self.log_folders = {
        Constants.AGENT_LOG_ROOT: self.config.getLogPath(),
        Constants.AGENT_WORK_ROOT: self.config.getWorkRootPath()
      }
      ret[Constants.FOLDERS] = copy.deepcopy(self.log_folders)
    return ret


  def resolve_script_path(self, base_dir, script, script_type):
    """
    Encapsulates logic of script location determination.
    """
    path = os.path.realpath(posixpath.join(base_dir, script))
    if not os.path.exists(path):
      message = "Script {0} does not exist".format(path)
      raise AgentException(message)
    return path

  def getConfig(self, command):
    if 'configurations' in self.stored_command:
      if 'commandParams' in command and 'config_type' in command['commandParams']:
        config_type = command['commandParams']['config_type']
        logger.info("Requesting applied config for type {0}".format(config_type))
        if config_type in self.stored_command['configurations']:
          return {
            'configurations': {config_type: self.stored_command['configurations'][config_type]}
          }
        else:
          return {
            'configurations': {}
          }
        pass
      else:
        logger.info("Requesting all applied config.")
        return {
          'configurations': self.stored_command['configurations']
        }
      pass
    else:
      return {
        'configurations': {}
      }
    pass

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
     return self.getConfig(command)

    else:
      res = self.runCommand(command, self.status_commands_stdout,
                            self.status_commands_stderr,
                            override_output_files=override_output_files)
      if res[Constants.EXIT_CODE] == 0:
        res[Constants.EXIT_CODE] = CustomServiceOrchestrator.LIVE_STATUS
      else:
        res[Constants.EXIT_CODE] = CustomServiceOrchestrator.DEAD_STATUS

      return res
    pass

  def dump_command_to_json(self, command, allocated_ports, store_command=False):
    """
    Converts command to json file and returns file path
    """
    # Perform few modifications to stay compatible with the way in which
    # site.pp files are generated by manifestGenerator.py
    command['public_hostname'] = self.public_fqdn
    if 'hostname' in command:
      command['appmaster_hostname'] = command['hostname']
    command['hostname'] = self.public_fqdn

    # Now, dump the json file
    command_type = command['commandType']
    from ActionQueue import ActionQueue  # To avoid cyclic dependency

    if command_type == ActionQueue.STATUS_COMMAND:
      # These files are frequently created, thats why we don't
      # store them all, but only the latest one
      file_path = os.path.realpath(posixpath.join(self.tmp_dir, "status_command.json"))
    else:
      task_id = command['taskId']
      file_path = os.path.realpath(posixpath.join(self.tmp_dir, "command-{0}.json".format(task_id)))
      # Json may contain passwords, that's why we need proper permissions
    if os.path.isfile(file_path) and os.path.exists(file_path):
      os.unlink(file_path)

    self.finalize_command(command, store_command, allocated_ports)
    self.finalize_exec_command(command)

    with os.fdopen(os.open(file_path, os.O_WRONLY | os.O_CREAT,
                           0644), 'w') as f:
      content = json.dumps(command, sort_keys=False, indent=4)
      f.write(content)
    return file_path


  """
  patch content
  ${AGENT_WORK_ROOT} -> AgentConfig.getWorkRootPath()
  ${AGENT_LOG_ROOT} -> AgentConfig.getLogPath()
  ALLOCATED_PORT is a hint to allocate port. It works as follows:
  Its of the form {component_name.ALLOCATED_PORT}[{DEFAULT_default_port}][{PER_CONTAINER}]
  Either a port gets allocated or if not then just set the value to "0"
  """
  def finalize_command(self, command, store_command, allocated_ports):
    component = command['componentName']
    allocated_for_this_component_format = "${{{0}.ALLOCATED_PORT}}"
    allocated_for_any = ".ALLOCATED_PORT}"

    port_allocation_req = allocated_for_this_component_format.format(component)
    allowed_ports = self.get_allowed_ports(command)
    if 'configurations' in command:
      for key in command['configurations']:
        if len(command['configurations'][key]) > 0:
          for k, value in command['configurations'][key].items():
            if value and len(value) > 0 and isinstance(value, basestring) > 0:
              value = value.replace("${AGENT_WORK_ROOT}",
                                    self.config.getWorkRootPath())
              value = value.replace("${AGENT_LOG_ROOT}",
                                    self.config.getLogPath())
              if port_allocation_req in value:
                value = self.allocate_ports(value, port_allocation_req, allowed_ports)
                allocated_ports[key + "." + k] = value
              elif allocated_for_any in value:
                ## All unallocated ports should be set to 0
                logger.info("Assigning port 0 " + "for " + value)
                value = self.set_all_unallocated_ports(value)
              command['configurations'][key][k] = value
              pass
            pass
          pass
        pass
      pass

    if store_command:
      logger.info("Storing applied config: " + pprint.pformat(command['configurations']))
      self.stored_command = command

  pass

  """
  configurations/global/exec_cmd should be resolved based on the rest of the config
  {$conf:@//site/global/xmx_val} ==> command['configurations']['global']['xmx_val']
  """
  def finalize_exec_command(self, command):
    variable_format = "{{$conf:@//site/{0}/{1}}}"
    if 'configurations' in command:
      if 'global' in command['configurations'] and 'exec_cmd' in command['configurations']['global']:
        exec_cmd = command['configurations']['global']['exec_cmd']
        for key in command['configurations']:
          if len(command['configurations'][key]) > 0:
            for k, value in command['configurations'][key].items():
              replaced_key = variable_format.format(key, k)
              exec_cmd = exec_cmd.replace(replaced_key, value)
              pass
            pass
          pass
        command['configurations']['global']['exec_cmd'] = exec_cmd
      pass
    pass


  """
  All unallocated ports should be set to 0
  Look for "${SOME_COMPONENT_NAME.ALLOCATED_PORT}"
        or "${component.ALLOCATED_PORT}{DEFAULT_port}"
        or "${component.ALLOCATED_PORT}{DEFAULT_port}{PER_CONTAINER}"
  """

  def set_all_unallocated_ports(self, value):
    pattern_start = "${"
    sub_section_start = "}{"
    pattern_end = "}"
    index = value.find(pattern_start)
    while index != -1:
      replace_index_start = index
      replace_index_end = value.find(pattern_end, replace_index_start)
      next_pattern_start = value.find(sub_section_start, replace_index_start)
      while next_pattern_start == replace_index_end:
        replace_index_end = value.find(pattern_end, replace_index_end + 1)
        next_pattern_start = value.find(sub_section_start, next_pattern_start + 1)
        pass

      value = value[:replace_index_start] + "0" + value[replace_index_end + 1:]

      # look for the next
      index = value.find(pattern_start)

    return value
    pass

  """
  Port allocation can asks for multiple dynamic ports
  port_req_pattern is of type ${component_name.ALLOCATED_PORT}
    append {DEFAULT_ and find the default value
    append {PER_CONTAINER} if it exists
  """
  def allocate_ports(self, value, port_req_pattern, allowed_ports=None):
    default_port_pattern = "{DEFAULT_"
    do_not_propagate_pattern = "{PER_CONTAINER}"
    index = value.find(port_req_pattern)
    while index != -1:
      replaced_pattern = port_req_pattern
      def_port = None
      if index == value.find(port_req_pattern + default_port_pattern):
        replaced_pattern = port_req_pattern + default_port_pattern
        start_index = index + len(replaced_pattern)
        end_index = value.find("}", start_index)
        def_port_str = value[start_index:end_index]
        def_port = int(def_port_str)
        # default value of 0 means allocate any dynamic port
        if def_port == 0:
          def_port = None

        replaced_pattern = replaced_pattern + def_port_str + "}"
        pass
      if index == value.find(replaced_pattern + do_not_propagate_pattern):
        replaced_pattern = replaced_pattern + do_not_propagate_pattern
        pass
      port = self.allocate_port(def_port, allowed_ports)
      value = value.replace(replaced_pattern, str(port), 1)
      logger.info("Allocated port " + str(port) + " for " + replaced_pattern)
      index = value.find(port_req_pattern)
      pass
    return value
    pass


  def allocate_port(self, default_port=None, allowed_ports=None):
    if default_port != None:
      if self.is_port_available(default_port):
        return default_port

    port_list = [0] * MAX_ATTEMPTS
    if allowed_ports != None:
      port_list = allowed_ports

    i = 0
    port = -1
    itor = iter(port_list)
    while i < min(len(port_list), MAX_ATTEMPTS):
      try:
        sock = socket.socket()
        sock.bind(('', itor.next()))
        port = sock.getsockname()[1]
      except Exception, err:
        logger.info("Encountered error while trying to open socket - " + str(err))
      finally:
        sock.close()
      i = i + 1
      pass
    logger.info("Allocated dynamic port: " + str(port))
    return port

  def is_port_available(self, port):
    try:
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      sock.settimeout(0.2)
      sock.connect(('127.0.0.1', port))
      sock.close()
    except:
      return True
    return False


  def get_allowed_ports(self, command):
      allowed_ports = None
      global_config = command['configurations'].get('global')
      if global_config != None:
          allowed_ports_value = global_config.get("slider.allowed.ports")
          if allowed_ports_value:
              allowed_ports = self.get_allowed_port_list(allowed_ports_value)

      return allowed_ports


  def get_allowed_port_list(self, allowedPortsOptionValue,
                            num_values=MAX_ATTEMPTS):
    selection = set()
    invalid = set()
    # tokens are comma seperated values
    tokens = [x.strip() for x in allowedPortsOptionValue.split(',')]
    for i in tokens:
      try:
        selection.add(int(i))
      except:
        # should be a range
        try:
          token = [int(k.strip()) for k in i.split('-')]
          if len(token) > 1:
            token.sort()
            first = token[0]
            last = token[len(token)-1]
            for x in range(first, last+1):
              selection.add(x)
        except:
          # not an int and not a range...
          invalid.add(i)
    selection = random.sample(selection, min (len(selection), num_values))
    # Report invalid tokens before returning valid selection
    logger.info("Allowed port values: " + str(selection))
    logger.warning("Invalid port range values: " + str(invalid))
    return selection


