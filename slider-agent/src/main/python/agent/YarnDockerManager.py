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
import subprocess
import Constants
import time
import traceback
from resource_management import *

logger = logging.getLogger()

class YarnDockerManager(Script):
  stored_status_command = ''
  stored_command = ''
  container_id = ''

  def __init__(self, tmpdir, workroot, customServiceOrchestrator):
    self.tmpdir = tmpdir
    self.workroot = workroot
    self.customServiceOrchestrator = customServiceOrchestrator

  def execute_command(self, command, store_command=False):
    returncode = ''
    out = ''
    err = ''
    
    if store_command:
      logger.info("Storing applied config: " + str(command['configurations']))
      self.stored_command = command
    status_command_str = self.extract_config_from_command(command, 'docker.status_command')
    if status_command_str:
      self.stored_status_command = status_command_str
    logger.info("status command" + self.stored_status_command)
    if 'hostLevelParams' in command:
      if 'container_id' in command['hostLevelParams']:
        self.container_id = command['hostLevelParams']['container_id']

    if command['roleCommand'] == 'INSTALL':
      with Environment(self.workroot) as env:
        self.install_container(command, env)
      returncode = 0; out = ''; err = ''
    if command['roleCommand'] == 'START':
      returncode, out, err = self.start_container(command)    
    # need check
    return {Constants.EXIT_CODE:returncode, 'stdout':out, 'stderr':err}


  def extract_config_files_from_command(self, command):
    if 'containers' in command and len(command['containers']) > 0:
      if 'configFiles' in command['containers'][0]:
        return command['containers'][0]['configFiles']
    return []

  def extract_config_file_properties_from_command(self, command, file):
    if 'configurations' in command:
      if 'dictionaryName' in file and file['dictionaryName'] in command['configurations']:
        properties = {}
        for k,v in command['configurations'][file['dictionaryName']].iteritems():
          properties[k] = format(v, **os.environ)
        return properties
    return {}

  def extract_config_from_command(self, command, field):
    value = ''
    if 'configurations' in command:
      if 'docker' in command['configurations']:
        if field in command['configurations']['docker']:
          logger.info(field + ': ' + str( command['configurations']['docker'][field]))
          value = command['configurations']['docker'][field]
    return value


  # will evolve into a class hierarch, linux and windows
  def execute_command_on_linux(self, docker_command, blocking, stdoutFile=None, stderrFile=None):

    logger.info("command str: " + docker_command)
    logger.info("command env: " + str(os.environ))
    if stdoutFile != None or stderrFile != None:
      proc = subprocess.Popen(docker_command, 
                              stdout = stdoutFile, 
                              stderr = stderrFile, universal_newlines = True, shell=True)
    else:
      proc = subprocess.Popen(docker_command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)

    returncode = 0
    out = ''
    err = ''
    if blocking is True:
      out, err = proc.communicate()
      returncode = proc.returncode
    else:
      time.sleep(5)
      if proc.returncode is not None:
        # this means the command has already returned
        returncode = proc.returncode
        out, err = proc.communicate()
    logger.info("returncode: " + str(returncode) + " out: " + str(out) + " err: " + str(err))
    return returncode, out, err

  def enqueue_output(out, queue):
    for line in iter(out.readline, b''):
        queue.put(line)
    out.close()

  def get_config_file_global(self, command, file, property, useEnv=True):
    keyName = file['dictionaryName'] + '.' + property
    if keyName in command['configurations']['global']:
      if useEnv:
        return format(command['configurations']['global'][keyName], **os.environ)
      else:
        return command['configurations']['global'][keyName]
    return None

  def install_container(self, command, env):
    try:
      configFiles = self.extract_config_files_from_command(command)
      for configFile in configFiles:
        properties = self.extract_config_file_properties_from_command(command, configFile)
        fileName = configFile['fileName']
        dir = self.get_config_file_global(command, configFile, 'destDir')
        if dir is None:
          dir = self.workroot
        logger.info("creating config file " + str(configFile) + " in directory "+str(dir))
        Directory(dir, recursive = True)
        if configFile['type'] == 'properties':
          PropertiesFile(fileName,
                         dir=dir,
                         properties=properties)
        elif configFile['type'] == 'env':
          content = self.get_config_file_global(command, configFile, 'content', useEnv=False)
          if content is not None:
            File(os.path.join(dir, fileName),
                 content=InlineTemplate(content, **properties))
        elif configFile['type'] == 'template':
          templateFile = self.get_config_file_global(command, configFile, 'templateFile')
          if templateFile is not None:
            with open(templateFile,"r") as fp:
              fileContent = fp.read()
            File(os.path.join(dir, fileName),
                 content=InlineTemplate(fileContent, **properties))
        elif configFile['type'] == 'xml':
          XmlConfig(fileName,
                    conf_dir=dir,
                    configurations=properties)
    except:
      traceback.print_exc()

  def start_container(self, command):
    #extracting param needed by docker run from the command passed from AM
    startCommand = self.extract_config_from_command(command, 'docker.startCommand')
    #adding redirecting stdout stderr to file
    outfilename = Constants.APPLICATION_STD_OUTPUT_LOG_FILE_PREFIX + \
                    self.container_id + Constants.APPLICATION_STD_OUTPUT_LOG_FILE_FILE_TYPE
          
    errfilename = Constants.APPLICATION_STD_ERROR_LOG_FILE_PREFIX + \
                    self.container_id + Constants.APPLICATION_STD_ERROR_LOG_FILE_FILE_TYPE

    stdoutFile = open(outfilename, 'w')
    stderrFile = open(errfilename, 'w')
    returncode,out,err = self.execute_command_on_linux(startCommand, False,  
                                                       stdoutFile, stderrFile)
    return returncode,out,err

  def query_status(self, command):
    if command['roleCommand'] == "GET_CONFIG":
      return self.getConfig(command)
    else:
      returncode = ''
      out = ''
      err = ''
      status_command_str = self.extract_config_from_command(command, 'docker.status_command')
      if status_command_str:
        self.stored_status_command = status_command_str
        logger.info("in query_status, got stored status command" + self.stored_status_command)
      if self.stored_status_command:
        logger.info("stored status command to run: " + self.stored_status_command)
        returncode, out, err = self.execute_command_on_linux(self.stored_status_command, True)
      logger.info("status of the app in docker container: " + str(returncode) + ";" + str(out) + ";" + str(err))
      
      return {Constants.EXIT_CODE:returncode, 'stdout':out, 'stderr':err}

  def getConfig(self, command):
    logger.info("get config command: " + str(command))
    config = {}
    
    if 'configurations' in self.stored_command:
      if 'commandParams' in command and 'config_type' in command['commandParams']:
        config_type = command['commandParams']['config_type']
        logger.info("Requesting applied config for type {0}".format(config_type))
        if config_type in self.stored_command['configurations']:
          logger.info("get config result: " + self.stored_command['configurations'][config_type])
          config = {
            'configurations': {config_type: self.stored_command['configurations'][config_type]}
          }
        else:
          config = {
            'configurations': {}
          }
        pass
      else:
        logger.info("Requesting all applied config." + str(self.stored_command['configurations']))
        config = {
          'configurations': self.stored_command['configurations']
        }
      pass
    else:
      config = {
        'configurations': {}
      }
    
    #query the ip and hostname of the docker container where the agent is running
    ip_query_command = "ip addr show dev eth0 | grep \'inet \' | awk \'{print $2}\' | cut -d / -f 1"
    proc = subprocess.Popen(ip_query_command, stdout = subprocess.PIPE, shell=True)
    ip, err = proc.communicate()
    if err is not None:
        logger.error("error when retrieving ip: " + err)
    
    hostname_query_command = "hostname"
    proc = subprocess.Popen(hostname_query_command, stdout = subprocess.PIPE, shell=True)
    hostname, err = proc.communicate()
    if err is not None:
        logger.error("error when retrieving hostname: " + err)
    
    config['ip'] = ip.rstrip()
    config['hostname'] = hostname.rstrip()
    
    logger.info('response from getconfig: ' + str(config))
    return config
    
