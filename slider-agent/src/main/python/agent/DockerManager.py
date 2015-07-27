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
from AgentConfig import AgentConfig
import Constants

logger = logging.getLogger()

class DockerManager():
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
      self.stored_status_command = status_command_str.split(" ")
    logger.info("status command" + str(self.stored_status_command))
    if command['hostLevelParams']:
        if command['hostLevelParams']['container_id']:
            self.container_id = command['hostLevelParams']['container_id']
        
    if command['roleCommand'] == 'INSTALL':
      returncode, out, err = self.pull_image(command)
      logger.info("docker pull result: " + str(returncode) + ";")
    if command['roleCommand'] == 'START':
      returncode, out, err = self.start_container(command)    
    # need check
    return {Constants.EXIT_CODE:returncode, 'stdout':out, 'stderr':err}
        
  def pull_image(self, command):
    logger.info(str( command['configurations']))
    command_path = self.extract_config_from_command(command, 'docker.command_path')
    imageName = self.extract_config_from_command(command, 'docker.image_name')
    
    docker_command = [command_path, 'pull', imageName]
    logger.info("docker pull command: " + str(docker_command))
    return self.execute_command_on_linux(docker_command)
    

  def extract_config_from_command(self, command, field):
    value = ''
    if 'configurations' in command:
      if 'docker' in command['configurations']:
        if field in command['configurations']['docker']:
          logger.info(field + ': ' + str( command['configurations']['docker'][field]))
          value = command['configurations']['docker'][field]
    return value


  # will evolve into a class hierarch, linux and windows
  def execute_command_on_linux(self, docker_command):
    command_str = ''
    for itr in docker_command:
        command_str = command_str + ' ' + itr

    logger.info("command str: " + command_str)
    proc = subprocess.Popen(command_str, stdout = subprocess.PIPE, shell=True)
    out, err = proc.communicate()
    logger.info("docker command output: " + str(out) + " err: " + str(err))
    return proc.returncode, out, err


  def start_container(self, command):
    #extracting param needed by docker run from the command passed from AM
    command_path = self.extract_config_from_command(command, 'docker.command_path')
    imageName = self.extract_config_from_command(command, 'docker.image_name')
    options = self.extract_config_from_command(command, 'docker.options')
    containerPort = self.extract_config_from_command(command, 'docker.containerPort')
    mounting_directory = self.extract_config_from_command(command, 'docker.mounting_directory')
    memory_usage = self.extract_config_from_command(command, "docker.memory_usage")
    additional_param = self.extract_config_from_command(command, 'docker.additional_param')
    input_file_local_path = self.extract_config_from_command(command, 'docker.input_file.local_path')
    input_file_mount_path = self.extract_config_from_command(command, 'docker.input_file.mount_path')
    
    docker_command = [command_path, "run"]
    
    #docker_command.append("--net=host")
    
    if options:
      docker_command = self.add_docker_run_options_to_command(docker_command, options)
    if containerPort:
      logger.info("container port is not null")
      self.add_port_binding_to_command(docker_command, command, containerPort)
    if mounting_directory:
      self.add_mnted_dir_to_command(docker_command, "/docker_use", mounting_directory)
    if input_file_local_path:
      self.add_mnted_dir_to_command(docker_command, "/inputDir", input_file_mount_path)
    if memory_usage:
      self.add_resource_restriction(docker_command, memory_usage)
    self.add_container_name_to_command(docker_command, command)
    docker_command.append(imageName)
    if additional_param:
      docker_command = self.add_additional_param_to_command(docker_command, additional_param)
    logger.info("docker run command: " + str(docker_command))
    return self.execute_command_on_linux(docker_command)

  def add_docker_run_options_to_command(self, docker_command, options):
    return docker_command + options.split(" ")

  def add_port_binding_to_command(self, docker_command, command, containerPort):
    docker_command.append("-p")
    hostPort = self.extract_config_from_command(command, 'docker.hostPort')
    
    if not hostPort:
      #this is the list of allowed port range specified in appConfig
      allowedPorts = self.customServiceOrchestrator.get_allowed_ports(command)
      #if the user specify hostPort in appConfig, then we use it, otherwise allocate it
      allocated_for_this_component_format = "${{{0}.ALLOCATED_PORT}}"
      component = command['componentName']
      port_allocation_req = allocated_for_this_component_format.format(component)
      hostPort = self.customServiceOrchestrator.allocate_ports(port_allocation_req, port_allocation_req, allowedPorts)
    docker_command.append(hostPort+":"+containerPort)
    
  def add_mnted_dir_to_command(self, docker_command, host_dir, container_dir):
    docker_command.append("-v")
    tmp_mount_dir = self.workroot + host_dir
    docker_command.append(tmp_mount_dir+":"+container_dir)

  def add_container_name_to_command(self, docker_command, command):
    docker_command.append("--name")
    docker_command.append(self.get_container_id(command))
    
  def add_additional_param_to_command(self, docker_command, additional_param):
    return docker_command + additional_param.split(" ")

  def get_container_id(self, command):
    # will make this more resilient to changes
    logger.info("container id is: " + self.container_id)
    return self.container_id

  def add_resource_restriction(self, docker_command, memory_usage):
    docker_command.append("-m")
    docker_command.append(memory_usage)

  def query_status(self, command):
    if command['roleCommand'] == "GET_CONFIG":
      return self.getConfig(command)
    else:
      returncode = ''
      out = ''
      err = ''
      status_command_str = self.extract_config_from_command(command, 'docker.status_command')
      if status_command_str:
        self.stored_status_command = status_command_str.split(" ")
        logger.info("in query_status, got stored status command" + str(self.stored_status_command))
      if self.stored_status_command:
        logger.info("stored status command to run: " + str(self.stored_status_command))
        returncode, out, err = self.execute_command_on_linux(self.stored_status_command)
      logger.info("status of the app in docker container: " + str(returncode) + ";" + str(out) + ";" + str(err))
      return {Constants.EXIT_CODE:returncode, 'stdout':out, 'stderr':err}

  def getConfig(self, command):
    logger.info("get config command: " + str(command))
    if 'configurations' in self.stored_command:
      if 'commandParams' in command and 'config_type' in command['commandParams']:
        config_type = command['commandParams']['config_type']
        logger.info("Requesting applied config for type {0}".format(config_type))
        if config_type in self.stored_command['configurations']:
          logger.info("get config result: " + self.stored_command['configurations'][config_type])
          return {
            'configurations': {config_type: self.stored_command['configurations'][config_type]}
          }
        else:
          return {
            'configurations': {}
          }
        pass
      else:
        logger.info("Requesting all applied config." + str(self.stored_command['configurations']))
        return {
          'configurations': self.stored_command['configurations']
        }
      pass
    else:
      return {
        'configurations': {}
      }
    pass

  def stop_container(self):
    docker_command = ["/usr/bin/docker", "stop"]
    docker_command.append(self.get_container_id(docker_command))
    logger.info("docker stop: " + str(docker_command))
    code, out, err = self.execute_command_on_linux(docker_command)
    logger.info("output: " + str(out))
    
        
