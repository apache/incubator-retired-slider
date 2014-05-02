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
import Queue

import logging
import traceback
import threading
import pprint
import os
import time

from shell import shellRunner
from AgentConfig import AgentConfig
from CommandStatusDict import CommandStatusDict
from CustomServiceOrchestrator import CustomServiceOrchestrator


logger = logging.getLogger()
installScriptHash = -1


class ActionQueue(threading.Thread):
  """ Action Queue for the agent. We pick one command at a time from the queue
  and execute it
  """

  STATUS_COMMAND = 'STATUS_COMMAND'
  EXECUTION_COMMAND = 'EXECUTION_COMMAND'

  IN_PROGRESS_STATUS = 'IN_PROGRESS'
  COMPLETED_STATUS = 'COMPLETED'
  FAILED_STATUS = 'FAILED'

  STORE_APPLIED_CONFIG = 'record_config'

  def __init__(self, config, controller):
    super(ActionQueue, self).__init__()
    self.commandQueue = Queue.Queue()
    self.commandStatuses = CommandStatusDict(callback_action=
    self.status_update_callback)
    self.config = config
    self.controller = controller
    self.sh = shellRunner()
    self._stop = threading.Event()
    self.tmpdir = config.getResolvedPath(AgentConfig.APP_TASK_DIR)
    self.customServiceOrchestrator = CustomServiceOrchestrator(config,
                                                               controller)


  def stop(self):
    self._stop.set()

  def stopped(self):
    return self._stop.isSet()

  def put(self, commands):
    for command in commands:
      logger.info("Adding " + command['commandType'] + " for service " + \
                  command['serviceName'] + " of cluster " + \
                  command['clusterName'] + " to the queue.")
      logger.debug(pprint.pformat(command))
      self.commandQueue.put(command)

  def empty(self):
    return self.commandQueue.empty()


  def run(self):
    while not self.stopped():
      time.sleep(2)
      command = self.commandQueue.get() # Will block if queue is empty
      self.process_command(command)
    logger.info("ActionQueue stopped.")


  def process_command(self, command):
    logger.debug("Took an element of Queue: " + pprint.pformat(command))
    # make sure we log failures
    try:
      if command['commandType'] == self.EXECUTION_COMMAND:
        self.execute_command(command)
      elif command['commandType'] == self.STATUS_COMMAND:
        self.execute_status_command(command)
      else:
        logger.error("Unrecognized command " + pprint.pformat(command))
    except Exception, err:
      # Should not happen
      traceback.print_exc()
      logger.warn(err)


  def execute_command(self, command):
    '''
    Executes commands of type  EXECUTION_COMMAND
    '''
    clusterName = command['clusterName']
    commandId = command['commandId']

    message = "Executing command with id = {commandId} for role = {role} of " \
              "cluster {cluster}".format(
      commandId=str(commandId), role=command['role'],
      cluster=clusterName)
    logger.info(message)
    logger.debug(pprint.pformat(command))

    taskId = command['taskId']
    # Preparing 'IN_PROGRESS' report
    in_progress_status = self.commandStatuses.generate_report_template(command)
    in_progress_status.update({
      'tmpout': self.tmpdir + os.sep + 'output-' + str(taskId) + '.txt',
      'tmperr': self.tmpdir + os.sep + 'errors-' + str(taskId) + '.txt',
      'structuredOut': self.tmpdir + os.sep + 'structured-out-' + str(
        taskId) + '.json',
      'status': self.IN_PROGRESS_STATUS
    })
    self.commandStatuses.put_command_status(command, in_progress_status)
    store_config = False
    if ActionQueue.STORE_APPLIED_CONFIG in command['commandParams']:
      store_config = 'true' == command['commandParams'][ActionQueue.STORE_APPLIED_CONFIG]

    # running command
    commandresult = self.customServiceOrchestrator.runCommand(command,
                                                              in_progress_status[
                                                                'tmpout'],
                                                              in_progress_status[
                                                                'tmperr'],
                                                              True,
                                                              store_config)
    # dumping results
    status = self.COMPLETED_STATUS
    if commandresult['exitcode'] != 0:
      status = self.FAILED_STATUS
    roleResult = self.commandStatuses.generate_report_template(command)
    roleResult.update({
      'stdout': commandresult['stdout'],
      'stderr': commandresult['stderr'],
      'exitCode': commandresult['exitcode'],
      'status': status,
    })
    if roleResult['stdout'] == '':
      roleResult['stdout'] = 'None'
    if roleResult['stderr'] == '':
      roleResult['stderr'] = 'None'

    if 'structuredOut' in commandresult:
      roleResult['structuredOut'] = str(commandresult['structuredOut'])
    else:
      roleResult['structuredOut'] = ''
      # let server know that configuration tags were applied
    if status == self.COMPLETED_STATUS:
      if command.has_key('configurationTags'):
        roleResult['configurationTags'] = command['configurationTags']
    self.commandStatuses.put_command_status(command, roleResult)

  # Store action result to agent response queue
  def result(self):
    return self.commandStatuses.generate_report()

  def execute_status_command(self, command):
    '''
    Executes commands of type STATUS_COMMAND
    '''
    try:
      cluster = command['clusterName']
      service = command['serviceName']
      component = command['componentName']
      reportResult = True
      if 'auto_generated' in command:
        reportResult = not command['auto_generated']

      component_status = self.customServiceOrchestrator.requestComponentStatus(command)

      result = {"componentName": component,
                "msg": "",
                "clusterName": cluster,
                "serviceName": service,
                "reportResult": reportResult,
                "roleCommand": command['roleCommand']
      }

      if 'configurations' in component_status:
        result['configurations'] = component_status['configurations']
      if 'exitcode' in component_status:
        result['status'] = component_status['exitcode']
        logger.debug("Got live status for component " + component + \
                     " of service " + str(service) + \
                     " of cluster " + str(cluster))
        logger.debug(pprint.pformat(result))

      if result is not None:
        self.commandStatuses.put_command_status(command, result, reportResult)
    except Exception, err:
      traceback.print_exc()
      logger.warn(err)
    pass


  def status_update_callback(self):
    """
    Actions that are executed every time when command status changes
    """
    self.controller.heartbeat_wait_event.set()
