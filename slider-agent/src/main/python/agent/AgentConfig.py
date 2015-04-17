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

import ConfigParser
import StringIO
import os
import logging
import posixpath

logger = logging.getLogger()

config = ConfigParser.RawConfigParser()
content = """

[server]
hostname=localhost
port=8440
secured_port=8441
zk_quorum=localhost:2181
zk_reg_path=/register/org-apache-slider/cl1
check_path=/ws/v1/slider/agents/
register_path=/ws/v1/slider/agents/{name}/register
heartbeat_path=/ws/v1/slider/agents/{name}/heartbeat

[agent]
app_pkg_dir=app/definition
app_install_dir=app/install
app_run_dir=app/run
app_dbg_cmd=
debug_mode_enabled=true

app_task_dir=.
app_log_dir=.
app_tmp_dir=app/tmp

log_dir=.
run_dir=infra/run
version_file=infra/version

log_level=INFO

[python]

[command]
max_retries=2
sleep_between_retries=1
auto_restart=5,5

[security]
keysdir=security/keys
server_crt=ca.crt
passphrase_env_var_name=SLIDER_PASSPHRASE

[heartbeat]
state_interval=6
log_lines_count=300

"""
s = StringIO.StringIO(content)
config.readfp(s)


class AgentConfig:
  SERVER_SECTION = "server"
  AGENT_SECTION = "agent"
  PYTHON_SECTION = "python"
  COMMAND_SECTION = "command"
  SECURITY_SECTION = "security"
  HEARTBEAT_SECTION = "heartbeat"

  # the location of the app package
  APP_PACKAGE_DIR = "app_pkg_dir"
  ADDON_PKG_ROOT_DIR = "addon/definition"
  # the location where the app component is installed
  APP_INSTALL_DIR = "app_install_dir"
  # the location to store component instance PID directories
  APP_RUN_DIR = "app_run_dir"
  # debug hint for agents
  APP_DBG_CMD = "app_dbg_cmd"
  # allow agent to operate in debug mode
  DEBUG_MODE_ENABLED = "debug_mode_enabled"

  # run time dir for command executions
  APP_TASK_DIR = "app_task_dir"
  # application log directory
  APP_LOG_DIR = "app_log_dir"
  # application tmp directory
  APP_TMP_DIR = "app_tmp_dir"

  # agent log directory
  LOG_DIR = "log_dir"
  # agent run directory
  RUN_DIR = "run_dir"
  # agent version file
  VERSION_FILE = "version_file"

  AUTO_RESTART = "auto_restart"

  FOLDER_MAPPING = {
    APP_PACKAGE_DIR: "WORK",
    APP_INSTALL_DIR: "WORK",
    APP_RUN_DIR: "WORK",
    APP_TMP_DIR: "WORK",
    RUN_DIR: "WORK",
    VERSION_FILE: "WORK",
    APP_TASK_DIR: "LOG",
    APP_LOG_DIR: "LOG",
    LOG_DIR: "LOG"
  }

  def __init__(self, workroot, logroot, label="agent"):
    self.workroot = workroot
    self.logroot = logroot
    self.label = label

  def getWorkRootPath(self):
    return self.workroot

  def getLogPath(self):
    return self.logroot

  def getLabel(self):
    return self.label

  def getResolvedPath(self, name):
    global config

    relativePath = config.get(AgentConfig.AGENT_SECTION, name)
    if not os.path.isabs(relativePath):
      root_folder_to_use = self.workroot
      if name in AgentConfig.FOLDER_MAPPING and AgentConfig.FOLDER_MAPPING[
        name] == "LOG":
        root_folder_to_use = self.logroot
      return posixpath.join(root_folder_to_use, relativePath)
    else:
      return relativePath

  def get(self, category, name):
    global config
    return config.get(category, name)

  def isDebugEnabled(self):
    global config
    enabled = config.get(AgentConfig.AGENT_SECTION, AgentConfig.DEBUG_MODE_ENABLED)
    return enabled == "true";

  def debugCommand(self):
    global config
    command = config.get(AgentConfig.AGENT_SECTION, AgentConfig.APP_DBG_CMD)
    if command == None:
      return ""
    return command

  # return max, window - max failures within window minutes
  def getErrorWindow(self):
    window = config.get(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART)
    if window != None:
      parts = window.split(',')
      if len(parts) == 2:
        if parts[0].isdigit() and parts[1].isdigit():
          return (int(parts[0]), int(parts[1]))
      pass
    return (0, 0)

  def set(self, category, name, value):
    global config
    return config.set(category, name, value)

  def setConfig(self, configFile):
    global config
    config.read(configFile)


def main():
  print config


if __name__ == "__main__":
  main()
