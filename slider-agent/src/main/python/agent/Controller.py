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
import signal
import json
import sys
import os
import time
import threading
import urllib2
import pprint
import math
from random import randint

from AgentConfig import AgentConfig
from AgentToggleLogger import AgentToggleLogger
from Heartbeat import Heartbeat
from Register import Register
from ActionQueue import ActionQueue
from NetUtil import NetUtil
from Registry import Registry
import ssl
import ProcessHelper
import Constants
import security


logger = logging.getLogger()

AGENT_AUTO_RESTART_EXIT_CODE = 77
HEART_BEAT_RETRY_THRESHOLD = 2

WS_AGENT_CONTEXT_ROOT = '/ws'
SLIDER_PATH_AGENTS = WS_AGENT_CONTEXT_ROOT + '/v1/slider/agents/'
SLIDER_REL_PATH_REGISTER = '/register'
SLIDER_REL_PATH_HEARTBEAT = '/heartbeat'

class State:
  INIT, INSTALLING, INSTALLED, STARTING, STARTED, FAILED, UPGRADING, UPGRADED, STOPPING, STOPPED, TERMINATING = range(11)


class Controller(threading.Thread):
  def __init__(self, config, range=30):
    threading.Thread.__init__(self)
    logger.debug('Initializing Controller RPC thread.')
    self.lock = threading.Lock()
    self.safeMode = True
    self.credential = None
    self.config = config
    self.label = config.getLabel()
    self.hostname = config.get(AgentConfig.SERVER_SECTION, 'hostname')
    self.secured_port = config.get(AgentConfig.SERVER_SECTION, 'secured_port')
    self.server_url = 'https://' + self.hostname + ':' + self.secured_port
    self.registerUrl = self.server_url + SLIDER_PATH_AGENTS + self.label + SLIDER_REL_PATH_REGISTER
    self.heartbeatUrl = self.server_url + SLIDER_PATH_AGENTS + self.label + SLIDER_REL_PATH_HEARTBEAT
    self.netutil = NetUtil()
    self.responseId = -1
    self.repeatRegistration = False
    self.isRegistered = False
    self.cachedconnect = None
    self.range = range
    self.hasMappedComponents = True
    # Event is used for synchronizing heartbeat iterations (to make possible
    # manual wait() interruption between heartbeats )
    self.heartbeat_wait_event = threading.Event()
    # List of callbacks that are called at agent registration
    self.registration_listeners = []
    self.componentExpectedState = State.INIT
    self.componentActualState = State.INIT
    self.statusCommand = None
    self.failureCount = 0
    self.heartBeatRetryCount = 0
    self.autoRestartFailures = 0
    self.autoRestartTrackingSince = 0
    self.terminateAgent = False
    self.stopCommand = None
    self.appGracefulStopQueued = False
    self.appGracefulStopTriggered = False
    self.tags = ""
    self.appRoot = None
    self.appVersion = None


  def __del__(self):
    logger.info("Server connection disconnected.")
    pass

  def processDebugCommandForRegister(self):
    self.processDebugCommand(Constants.DO_NOT_REGISTER)
    pass

  def processDebugCommandForHeartbeat(self):
    self.processDebugCommand(Constants.DO_NOT_HEARTBEAT)
    pass

  def processDebugCommand(self, command):
    if self.config.isDebugEnabled() and self.config.debugCommand() == command:
      ## Test support - sleep for 10 minutes
      logger.info("Received debug command: "
                  + self.config.debugCommand() + " Sleeping for 10 minutes")
      time.sleep(60*10)
      pass
    pass

  def registerWithServer(self):
    id = -1
    ret = {}

    self.processDebugCommandForRegister()

    while not self.isRegistered:
      try:
        data = json.dumps(self.register.build(
          self.componentActualState,
          self.componentExpectedState,
          self.actionQueue.customServiceOrchestrator.allocated_ports,
          self.actionQueue.customServiceOrchestrator.log_folders,
          self.appVersion,
          self.tags,
          id))
        logger.info("Registering with the server at " + self.registerUrl +
                    " with data " + pprint.pformat(data))
        response = self.sendRequest(self.registerUrl, data)
        regResp = json.loads(response)
        exitstatus = 0
        # exitstatus is a code of error which was raised on server side.
        # exitstatus = 0 (OK - Default)
        # exitstatus = 1 (Registration failed because
        #                different version of agent and server)
        if 'exitstatus' in regResp.keys():
          exitstatus = int(regResp['exitstatus'])

        # log - message, which will be printed to agents  log
        if 'log' in regResp.keys():
          log = regResp['log']

        # container may be associated with tags
        if 'tags' in regResp.keys():
          self.tags = regResp['tags']

        if exitstatus == 1:
          logger.error(log)
          self.isRegistered = False
          self.repeatRegistration = False
          return regResp
        logger.info("Registered with the server with " + pprint.pformat(regResp))
        print("Registered with the server")
        self.responseId = int(regResp['responseId'])
        self.isRegistered = True
        if 'statusCommands' in regResp.keys():
          logger.info("Got status commands on registration " + pprint.pformat(
            regResp['statusCommands']))
          self.addToQueue(regResp['statusCommands'])
          pass
        else:
          self.hasMappedComponents = False
        pass
      except ssl.SSLError:
        self.repeatRegistration = False
        self.isRegistered = False
        return
      except Exception, err:
        # try a reconnect only after a certain amount of random time
        delay = randint(0, self.range)
        logger.info("Unable to connect to: " + self.registerUrl, exc_info=True)
        """ Sleeping for {0} seconds and then retrying again """.format(delay)
        time.sleep(delay)
        pass
      pass
    return regResp


  def addToQueue(self, commands):
    """Add to the queue for running the commands """
    """ Put the required actions into the Queue """
    if not commands:
      logger.debug("No commands from the server : " + pprint.pformat(commands))
    else:
      """Only add to the queue if not empty list """
      self.actionQueue.put(commands)
    pass

  # For testing purposes
  DEBUG_HEARTBEAT_RETRIES = 0
  DEBUG_SUCCESSFULL_HEARTBEATS = 0
  DEBUG_STOP_HEARTBEATING = False
  MAX_FAILURE_COUNT_TO_STOP = 2

  def shouldStopAgent(self):
    '''
    Stop the agent if:
      - Component has failed after start
      - AM sent terminate agent command
    '''
    shouldStopAgent = False
    if (self.componentActualState == State.FAILED) \
      and (self.componentExpectedState == State.STARTED) \
      and (self.failureCount >= Controller.MAX_FAILURE_COUNT_TO_STOP):
      logger.info("Component instance has failed, stopping the agent ...")
      shouldStopAgent = True
    if (self.componentActualState == State.STOPPED):
      logger.info("Component instance has stopped, stopping the agent ...")
      shouldStopAgent = True
    if self.terminateAgent:
      logger.info("Terminate agent command received from AM, stopping the agent ...")
      shouldStopAgent = True
    return shouldStopAgent

  def isAppGracefullyStopped(self):
    '''
    If an app graceful stop command was queued then it is considered stopped if:
      - app stop was triggered

    Note: We should enhance this method by checking if the app is stopped
          successfully and if not, then take alternate measures (like kill
          processes). For now if stop is triggered it is considered stopped.
    '''
    isAppStopped = False
    if self.appGracefulStopTriggered:
      isAppStopped = True
    return isAppStopped

  def stopApp(self):
    '''
    Stop the app if:
      - the app is currently in STARTED state and
        a valid stop command is provided
    '''
    if (self.componentActualState == State.STARTED) and (not self.stopCommand == None):
      # Try to do graceful stop
      self.addToQueue([self.stopCommand])
      self.appGracefulStopQueued = True
      logger.info("Attempting to gracefully stop the application ...")

  def storeAppRootAndVersion(self, command):
    '''
    Store app root and version for upgrade:
    '''
    if self.appRoot is None:
      if 'app_root' in command['configurations']['global']:
        self.appRoot = command['configurations']['global']['app_root']
    if self.appVersion is None:
      if 'app_version' in command['configurations']['global']:
        self.appVersion = command['configurations']['global']['app_version']

  def heartbeatWithServer(self):
    self.DEBUG_HEARTBEAT_RETRIES = 0
    self.DEBUG_SUCCESSFULL_HEARTBEATS = 0
    retry = False
    certVerifFailed = False

    self.processDebugCommandForHeartbeat()

    while not self.DEBUG_STOP_HEARTBEATING:

      commandResult = {}
      try:
        if self.appGracefulStopQueued and not self.isAppGracefullyStopped():
          # Continue to wait until app is stopped
          logger.info("Graceful stop in progress..")
          time.sleep(1)
          continue
        if self.shouldStopAgent():
          ProcessHelper.stopAgent()

        if not retry:
          data = json.dumps(
            self.heartbeat.build(commandResult,
                                 self.responseId, self.hasMappedComponents))
          self.updateStateBasedOnResult(commandResult)
          logger.debug("Sending request: " + data)
          pass
        else:
          self.DEBUG_HEARTBEAT_RETRIES += 1
        response = self.sendRequest(self.heartbeatUrl, data)
        logger.debug('Got server response: ' + pprint.pformat(response))
        response = json.loads(response)

        serverId = int(response['responseId'])

        if 'restartAgent' in response.keys():
          restartAgent = response['restartAgent']
          if restartAgent:
            logger.error("Got restartAgent command")
            self.restartAgent()
        if 'terminateAgent' in response.keys():
          self.terminateAgent = response['terminateAgent']
          if self.terminateAgent:
            logger.error("Got terminateAgent command")
            self.stopApp()
            # Continue will add some wait time
            continue

        restartEnabled = False
        if 'restartEnabled' in response:
          restartEnabled = response['restartEnabled']
          if restartEnabled:
            logger.debug("Component auto-restart is enabled.")

        if 'hasMappedComponents' in response.keys():
          self.hasMappedComponents = response['hasMappedComponents'] != False

        if 'registrationCommand' in response.keys():
          # check if the registration command is None. If none skip
          if response['registrationCommand'] is not None:
            logger.info(
              "RegistrationCommand received - repeat agent registration")
            self.isRegistered = False
            self.repeatRegistration = True
            return

        if serverId != self.responseId + 1:
          logger.error("Error in responseId sequence expected " + str(self.responseId + 1)
                       + " but got " + str(serverId) + " - restarting")
          self.restartAgent()
        else:
          self.responseId = serverId

        commandSentFromAM = False
        if 'executionCommands' in response.keys():
          self.updateStateBasedOnCommand(response['executionCommands'])
          self.addToQueue(response['executionCommands'])
          commandSentFromAM = True
          pass
        if 'statusCommands' in response.keys() and len(response['statusCommands']) > 0:
          self.addToQueue(response['statusCommands'])
          commandSentFromAM = True
          pass

        if not commandSentFromAM:
          logger.info("No commands sent from the Server.")
          pass

        # Add a start command
        if self.componentActualState == State.FAILED and \
                self.componentExpectedState == State.STARTED and restartEnabled:
          stored_command = self.actionQueue.customServiceOrchestrator.stored_command
          if len(stored_command) > 0:
            auto_start_command = self.create_start_command(stored_command)
            if auto_start_command and self.shouldAutoRestart():
              logger.info("Automatically adding a start command.")
              logger.debug("Auto start command: " + pprint.pformat(auto_start_command))
              self.updateStateBasedOnCommand([auto_start_command], False)
              self.addToQueue([auto_start_command])
          pass

        # Add a status command
        if (self.componentActualState != State.STARTING and \
                self.componentExpectedState == State.STARTED) and \
            not self.statusCommand == None:
          self.addToQueue([self.statusCommand])

        if retry:
          print("Reconnected to the server")
          logger.info("Reconnected to the server")
        retry = False
        certVerifFailed = False
        self.DEBUG_SUCCESSFULL_HEARTBEATS += 1
        self.DEBUG_HEARTBEAT_RETRIES = 0
        self.heartbeat_wait_event.clear()
      except ssl.SSLError:
        self.repeatRegistration = False
        self.isRegistered = False
        return
      except Exception, err:
        #randomize the heartbeat
        delay = randint(0, self.range)
        time.sleep(delay)
        if "code" in err:
          logger.error(err.code)
        else:
          logger.error(
            "Unable to connect to: " + self.heartbeatUrl + " due to " + str(
              err))
          logger.debug("Details: " + str(err), exc_info=True)
          if not retry:
            print("Connection to the server was lost. Reconnecting...")
          if 'certificate verify failed' in str(err) and not certVerifFailed:
            print(
              "Server certificate verify failed. Did you regenerate server certificate?")
            certVerifFailed = True
        self.heartBeatRetryCount += 1
        logger.error(
          "Heartbeat retry count = %d" % (self.heartBeatRetryCount))
        # Re-read zk registry in case AM was restarted and came up with new 
        # host/port, but do this only after heartbeat retry attempts crosses
        # threshold
        if self.heartBeatRetryCount > HEART_BEAT_RETRY_THRESHOLD:
          self.isRegistered = False
          self.repeatRegistration = True
          self.heartBeatRetryCount = 0
          self.cachedconnect = None # Previous connection is broken now
          zk_quorum = self.config.get(AgentConfig.SERVER_SECTION, Constants.ZK_QUORUM)
          zk_reg_path = self.config.get(AgentConfig.SERVER_SECTION, Constants.ZK_REG_PATH)
          registry = Registry(zk_quorum, zk_reg_path)
          amHost, amUnsecuredPort, amSecuredPort = registry.readAMHostPort()
          self.hostname = amHost
          self.secured_port = amSecuredPort
          self.config.set(AgentConfig.SERVER_SECTION, "hostname", self.hostname)
          self.config.set(AgentConfig.SERVER_SECTION, "secured_port", self.secured_port)
          self.server_url = 'https://' + self.hostname + ':' + self.secured_port
          self.registerUrl = self.server_url + SLIDER_PATH_AGENTS + self.label + SLIDER_REL_PATH_REGISTER
          self.heartbeatUrl = self.server_url + SLIDER_PATH_AGENTS + self.label + SLIDER_REL_PATH_HEARTBEAT
          return
        self.cachedconnect = None # Previous connection is broken now
        retry = True
      finally:
        # Sleep for some time
        timeout = self.netutil.HEARTBEAT_IDDLE_INTERVAL_SEC \
                  - self.netutil.MINIMUM_INTERVAL_BETWEEN_HEARTBEATS
        self.heartbeat_wait_event.wait(timeout=timeout)
        # Sleep a bit more to allow STATUS_COMMAND results to be collected
        # and sent in one heartbeat. Also avoid server overload with heartbeats
        time.sleep(self.netutil.MINIMUM_INTERVAL_BETWEEN_HEARTBEATS)
    pass
    logger.info("Controller stopped heart-beating.")


  def create_start_command(self, stored_command):
    taskId = int(stored_command['taskId'])
    taskId = taskId + 1
    stored_command['taskId'] = taskId
    stored_command['commandId'] = "{0}-1".format(taskId)
    stored_command[Constants.AUTO_GENERATED] = True
    return stored_command
    pass


  def updateStateBasedOnCommand(self, commands, createStatus=True):
    # A STOP command is paired with the START command to provide agents the
    # capability to gracefully stop the app if possible. The STOP command needs
    # to be stored since the AM might not be able to provide it since it could
    # have lost the container state for whatever reasons. The STOP command has
    # no other role to play in the Agent state transition so it is removed from
    # the commands list.
    index = 0
    deleteIndex = 0
    delete = False
    '''
    Do not break for START command, since we might get a STOP command
    (used during failure scenarios to gracefully attempt stop)
    '''
    for command in commands:
      if "package" in command and command["package"] != "MASTER":
        # we do not update component state upon add on package command
        continue

      if command["roleCommand"] == "START":
        self.componentExpectedState = State.STARTED
        self.componentActualState = State.STARTING
        self.failureCount = 0
        if createStatus:
          self.statusCommand = self.createStatusCommand(command)

      # The STOP command index is stored to be deleted
      if command["roleCommand"] == "STOP":
        logger.info("Got stop command = %s", (command))
        self.stopCommand = command
        '''
        If app is already running then stopApp() will initiate graceful stop
        '''
        self.stopApp()
        delete = True
        deleteIndex = index
        if self.componentActualState == State.STARTED:
          self.componentExpectedState = State.STOPPED
          self.componentActualState = State.STOPPING
          self.failureCount = 0

      if command["roleCommand"] == "INSTALL":
        self.componentExpectedState = State.INSTALLED
        self.componentActualState = State.INSTALLING
        self.failureCount = 0
        '''
        Store the app root of this container at this point. It will be needed
        during upgrade (if performed).
        '''
        self.storeAppRootAndVersion(command)
        logger.info("Stored appRoot = %s", (self.appRoot))
        logger.info("Stored appVersion = %s", (self.appVersion))
        break;

      if command["roleCommand"] == "UPGRADE":
        self.componentExpectedState = State.UPGRADED
        self.componentActualState = State.UPGRADING
        self.failureCount = 0
        command['configurations']['global']['app_root'] = self.appRoot
        command['configurations']['global']['app_version'] = self.appVersion
        break;

      if command["roleCommand"] == "UPGRADE_STOP":
        self.componentExpectedState = State.STOPPED
        self.componentActualState = State.STOPPING
        self.failureCount = 0
        command['configurations']['global']['app_root'] = self.appRoot
        command['configurations']['global']['app_version'] = self.appVersion
        break;

      if command["roleCommand"] == "TERMINATE":
        self.componentExpectedState = State.TERMINATING
        self.componentActualState = State.TERMINATING
        self.failureCount = 0
        command['configurations']['global']['app_root'] = self.appRoot
        command['configurations']['global']['app_version'] = self.appVersion
        break;

      index += 1
    logger.debug("Current state " + str(self.componentActualState) + 
                " expected " + str(self.componentExpectedState))

    # Delete the STOP command
    if delete:
      del commands[deleteIndex]

  def updateStateBasedOnResult(self, commandResult):
    if len(commandResult) > 0:
      if "commandStatus" in commandResult:
        if commandResult["commandStatus"] == ActionQueue.COMPLETED_STATUS:
          self.componentActualState = self.componentExpectedState
          self.logStates()
          pass
        pass

        if commandResult["commandStatus"] == ActionQueue.FAILED_STATUS:
          self.componentActualState = State.FAILED
          self.failureCount += 1
          self.logStates()
          pass

      if "healthStatus" in commandResult:
        if commandResult["healthStatus"] == "INSTALLED":
          # Mark it FAILED as its a failure remedied by auto-start or container restart
          self.componentActualState = State.FAILED
          self.failureCount += 1
          self.logStates()
        if (commandResult["healthStatus"] == "STARTED") and (self.componentActualState != State.STARTED):
          self.componentActualState = State.STARTED
          self.failureCount = 0
          self.logStates()
          pass
        if (commandResult["healthStatus"] == "UPGRADED") and (self.componentActualState != State.UPGRADED):
          self.componentActualState = State.UPGRADED
          self.failureCount = 0
          self.logStates()
          pass
        if (commandResult["healthStatus"] == "STOPPED") and (self.componentActualState != State.STOPPED):
          self.componentActualState = State.STOPPED
          self.failureCount = 0
          self.logStates()
          pass
        pass
      pass

  def logStates(self):
    logger.info("Component states (result): Expected: " + str(self.componentExpectedState) + \
                " and Actual: " + str(self.componentActualState))
    pass

  def createStatusCommand(self, command):
    statusCommand = {}
    statusCommand["clusterName"] = command["clusterName"]
    statusCommand["commandParams"] = command["commandParams"]
    statusCommand["commandType"] = "STATUS_COMMAND"
    statusCommand["roleCommand"] = "STATUS"
    statusCommand["componentName"] = command["role"]
    statusCommand["configurations"] = {}
    statusCommand["configurations"]["global"] = command["configurations"]["global"]
    statusCommand["hostLevelParams"] = command["hostLevelParams"]
    statusCommand["serviceName"] = command["serviceName"]
    statusCommand["taskId"] = "status"
    statusCommand[Constants.AUTO_GENERATED] = True
    logger.info("Status command: " + pprint.pformat(statusCommand))
    return statusCommand
    pass


  def run(self):
    self.agentToggleLogger = AgentToggleLogger("info")
    self.actionQueue = ActionQueue(self.config, controller=self, agentToggleLogger=self.agentToggleLogger)
    self.actionQueue.start()
    self.register = Register(self.config)
    self.heartbeat = Heartbeat(self.actionQueue, self.config, self.agentToggleLogger)

    opener = urllib2.build_opener()
    urllib2.install_opener(opener)

    while True:
      self.repeatRegistration = False
      self.registerAndHeartbeat()
      if not self.repeatRegistration:
        break
    logger.info("Controller stopped.")
    pass

  def registerAndHeartbeat(self):
    registerResponse = self.registerWithServer()
    message = registerResponse['response']
    logger.info("Response from server = " + message)
    if self.isRegistered:
      # Process callbacks
      for callback in self.registration_listeners:
        callback()
      time.sleep(self.netutil.HEARTBEAT_IDDLE_INTERVAL_SEC)
      self.heartbeatWithServer()
    logger.info("Controller stopped heartbeating.")

  def restartAgent(self):
    os._exit(AGENT_AUTO_RESTART_EXIT_CODE)
    pass

  def sendRequest(self, url, data):
    response = None
    try:
        if self.cachedconnect is None: # Lazy initialization
            self.cachedconnect = security.CachedHTTPSConnection(self.config)
        req = urllib2.Request(url, data, {'Content-Type': 'application/json'})
        response = self.cachedconnect.request(req)
        return response
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logger.error("Exception raised", exc_info=(exc_type, exc_value, exc_traceback))
        if response is None:
            err_msg = 'Request failed! Data: ' + str(data)
            logger.warn(err_msg)
            return {'exitstatus': 1, 'log': err_msg}
        else:
            err_msg = ('Response parsing failed! Request data: ' + str(data)
                       + '; Response: ' + str(response))
            logger.warn(err_msg)
            return {'exitstatus': 1, 'log': err_msg}


  # Basic window that only counts failures till the window duration expires
  def shouldAutoRestart(self):
    max, window = self.config.getErrorWindow()
    if max <= 0 or window <= 0:
      return True

    seconds_now = time.time()
    if self.autoRestartTrackingSince == 0:
      self.autoRestartTrackingSince = seconds_now
      self.autoRestartFailures = 1
      return True

    self.autoRestartFailures += 1
    minutes = math.floor((seconds_now - self.autoRestartTrackingSince) / 60)
    if self.autoRestartFailures > max:
      logger.info("Auto restart not allowed due to " + str(self.autoRestartFailures) + " failures in " + str(minutes) +
                  " minutes. Max restarts allowed is " + str(max) + " in " + str(window) + " minutes.")
      return False

    if minutes > window:
      logger.info("Resetting window as number of minutes passed is " + str(minutes))
      self.autoRestartTrackingSince = seconds_now
      self.autoRestartFailures = 1
      return True
    return True

    pass


def main(argv=None):
  # Allow Ctrl-C
  signal.signal(signal.SIGINT, signal.SIG_DFL)

  logger.setLevel(logging.INFO)
  formatter = logging.Formatter("%(asctime)s %(filename)s:%(lineno)d - \
    %(message)s")
  stream_handler = logging.StreamHandler()
  stream_handler.setFormatter(formatter)
  logger.addHandler(stream_handler)

  logger.info('Starting Server RPC Thread: %s' % ' '.join(sys.argv))

  config = AgentConfig()
  collector = Controller(config)
  collector.start()
  collector.run()


if __name__ == '__main__':
  main()
