#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

import StringIO
import ssl
import unittest, threading
from agent import Controller, ActionQueue
from agent import hostname
import sys
import time
from Controller import AGENT_AUTO_RESTART_EXIT_CODE
from Controller import State
from AgentConfig import AgentConfig
from mock.mock import patch, MagicMock, call, Mock
import logging
from threading import Event
from AgentToggleLogger import AgentToggleLogger

class TestController(unittest.TestCase):

  logger = logging.getLogger()

  @patch("threading.Thread")
  @patch("threading.Lock")
  @patch.object(Controller, "NetUtil")
  @patch.object(hostname, "hostname")
  def setUp(self, hostname_method, NetUtil_mock, lockMock, threadMock):

    #Controller.logger = MagicMock()
    lockMock.return_value = MagicMock()
    NetUtil_mock.return_value = MagicMock()
    hostname_method.return_value = "test_hostname"


    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = "something"

    self.controller = Controller.Controller(config)
    self.controller.netutil.MINIMUM_INTERVAL_BETWEEN_HEARTBEATS = 0.1
    self.controller.netutil.HEARTBEAT_NOT_IDDLE_INTERVAL_SEC = 0.1
    self.agentToggleLogger = AgentToggleLogger("info")
    self.controller.actionQueue = ActionQueue.ActionQueue(config, self.controller, self.agentToggleLogger)


  @patch("json.dumps")
  @patch("time.sleep")
  @patch("pprint.pformat")
  @patch.object(Controller, "randint")
  def test_registerWithServer(self, randintMock, pformatMock, sleepMock,
                              dumpsMock):

    out = StringIO.StringIO()
    sys.stdout = out

    register = MagicMock()
    self.controller.register = register

    self.controller.sendRequest = MagicMock()

    dumpsMock.return_value = "request"
    self.controller.sendRequest.return_value = '{"log":"Error text", "exitstatus":"1"}'

    self.assertEqual({u'exitstatus': u'1', u'log': u'Error text'}, self.controller.registerWithServer())

    self.controller.sendRequest.return_value = '{"responseId":1}'
    self.assertEqual({"responseId":1}, self.controller.registerWithServer())

    self.controller.sendRequest.return_value = '{"responseId":1, "statusCommands": "commands", "log":"", "exitstatus":"0"}'
    self.controller.isRegistered = False
    self.assertEqual({'exitstatus': '0', 'responseId': 1, 'log': '', 'statusCommands': 'commands'}, self.controller.registerWithServer())

    calls = []

    def side_effect(*args):
      if len(calls) == 0:
        calls.append(1)
        raise Exception("test")
      return "request"

    self.controller.sendRequest.return_value = '{"responseId":1}'

    dumpsMock.side_effect = side_effect
    self.controller.isRegistered = False
    self.assertEqual({"responseId":1}, self.controller.registerWithServer())
    self.assertTrue(randintMock.called)
    self.assertTrue(sleepMock.called)

    sys.stdout = sys.__stdout__

    self.controller.sendRequest = Controller.Controller.sendRequest


  @patch("pprint.pformat")
  def test_addToQueue(self, pformatMock):

    actionQueue = MagicMock()
    self.controller.actionQueue = actionQueue
    self.controller.addToQueue(None)
    self.assertFalse(actionQueue.put.called)
    self.controller.addToQueue("cmd")
    self.assertTrue(actionQueue.put.called)


  @patch("urllib2.build_opener")
  @patch("urllib2.install_opener")
  @patch.object(Controller, "ActionQueue")
  def test_run(self, ActionQueue_mock, installMock, buildMock):
    aq = MagicMock()
    ActionQueue_mock.return_value = aq

    buildMock.return_value = "opener"
    registerAndHeartbeat  = MagicMock("registerAndHeartbeat")
    calls = []
    def side_effect():
      if len(calls) == 0:
        self.controller.repeatRegistration = True
      calls.append(1)
    registerAndHeartbeat.side_effect = side_effect
    self.controller.registerAndHeartbeat = registerAndHeartbeat

    # repeat registration
    self.controller.run()

    self.assertTrue(buildMock.called)
    installMock.called_once_with("opener")
    self.assertEqual(2, registerAndHeartbeat.call_count)

    # one call, +1
    registerAndHeartbeat.side_effect = None
    self.controller.run()
    self.assertEqual(3, registerAndHeartbeat.call_count)

    # Action queue should be started during calls
    self.assertTrue(ActionQueue_mock.called)
    self.assertTrue(aq.start.called)


  @patch("urllib2.build_opener")
  @patch("urllib2.install_opener")
  @patch.object(ActionQueue.ActionQueue, "start")
  def test_repeatRegistration(self,
                              start_mock, installMock, buildMock):

    registerAndHeartbeat = MagicMock(name="registerAndHeartbeat")

    self.controller.registerAndHeartbeat = registerAndHeartbeat
    self.controller.run()
    self.assertTrue(installMock.called)
    self.assertTrue(buildMock.called)
    self.assertTrue(start_mock.called)
    self.controller.registerAndHeartbeat.assert_called_once_with()

    calls = []
    def switchBool():
      if len(calls) == 0:
        self.controller.repeatRegistration = True
        calls.append(1)
      self.controller.repeatRegistration = False

    registerAndHeartbeat.side_effect = switchBool
    self.controller.run()
    self.assertEqual(2, registerAndHeartbeat.call_count)

    self.controller.registerAndHeartbeat = \
      Controller.Controller.registerAndHeartbeat


  @patch("time.sleep")
  def test_registerAndHeartbeatWithException(self, sleepMock):

    registerWithServer = MagicMock(name="registerWithServer")
    registerWithServer.return_value = {"response":"resp"}
    self.controller.registerWithServer = registerWithServer
    heartbeatWithServer = MagicMock(name="heartbeatWithServer")
    self.controller.heartbeatWithServer = heartbeatWithServer

    Controller.Controller.__sendRequest__ = MagicMock(side_effect=Exception())

    self.controller.isRegistered = True
    self.controller.registerAndHeartbeat()
    registerWithServer.assert_called_once_with()
    heartbeatWithServer.assert_called_once_with()

    self.controller.registerWithServer =\
    Controller.Controller.registerWithServer
    self.controller.heartbeatWithServer =\
    Controller.Controller.registerWithServer

  @patch("time.sleep")
  def test_registerAndHeartbeat(self, sleepMock):

    registerWithServer = MagicMock(name="registerWithServer")
    registerWithServer.return_value = {"response":"resp"}
    self.controller.registerWithServer = registerWithServer
    heartbeatWithServer = MagicMock(name="heartbeatWithServer")
    self.controller.heartbeatWithServer = heartbeatWithServer

    listener1 = MagicMock()
    listener2 = MagicMock()
    self.controller.registration_listeners.append(listener1)
    self.controller.registration_listeners.append(listener2)
    self.controller.isRegistered = True
    self.controller.registerAndHeartbeat()
    registerWithServer.assert_called_once_with()
    heartbeatWithServer.assert_called_once_with()
    self.assertTrue(listener1.called)
    self.assertTrue(listener2.called)

    self.controller.registerWithServer = \
      Controller.Controller.registerWithServer
    self.controller.heartbeatWithServer = \
      Controller.Controller.registerWithServer


  @patch("time.sleep")
  def test_registerAndHeartbeat_check_registration_listener(self, sleepMock):
    registerWithServer = MagicMock(name="registerWithServer")
    registerWithServer.return_value = {"response":"resp"}
    self.controller.registerWithServer = registerWithServer
    heartbeatWithServer = MagicMock(name="heartbeatWithServer")
    self.controller.heartbeatWithServer = heartbeatWithServer

    self.controller.isRegistered = True
    self.controller.registerAndHeartbeat()
    registerWithServer.assert_called_once_with()
    heartbeatWithServer.assert_called_once_with()

    self.controller.registerWithServer = \
      Controller.Controller.registerWithServer
    self.controller.heartbeatWithServer = \
      Controller.Controller.registerWithServer


  @patch("os._exit")
  def test_restartAgent(self, os_exit_mock):

    self.controller.restartAgent()
    self.assertTrue(os_exit_mock.called)
    self.assertTrue(os_exit_mock.call_args[0][0] == AGENT_AUTO_RESTART_EXIT_CODE)


  @patch("time.time")
  def test_failure_window(self, mock_time):
    config = AgentConfig("", "")
    original_config = config.get(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART)
    config.set(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART, '2,1')
    ## The behavior of side_effect is different when you run tests in command line and when you do it through IDE
    ## So few extra items are there in the list
    mock_time.side_effect = [200, 500, 500]
    controller5 = Controller.Controller(config)

    try:
      self.assertTrue(controller5.shouldAutoRestart())
      self.assertTrue(controller5.shouldAutoRestart())
    finally:
      config.set(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART, original_config)


  @patch("time.time")
  def test_failure_window(self, mock_time):
    config = AgentConfig("", "")
    original_config = config.get(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART)
    config.set(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART, '3,1')
    ## The behavior of side_effect is different when you run tests in command line and when you do it through IDE
    ## So few extra items are there in the list
    mock_time.side_effect = [200, 210, 220, 230, 240, 250]
    controller5 = Controller.Controller(config)

    try:
      self.assertTrue(controller5.shouldAutoRestart())
      self.assertTrue(controller5.shouldAutoRestart())
      self.assertTrue(controller5.shouldAutoRestart())
      self.assertFalse(controller5.shouldAutoRestart())
    finally:
      config.set(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART, original_config)


  def test_failure_window2(self):
    config = MagicMock()
    config.getErrorWindow.return_value = (0, 0)
    controller = Controller.Controller(config)

    self.assertTrue(controller.shouldAutoRestart())

    config.getErrorWindow.return_value = (0, 1)
    self.assertTrue(controller.shouldAutoRestart())

    config.getErrorWindow.return_value = (1, 0)
    self.assertTrue(controller.shouldAutoRestart())

    config.getErrorWindow.return_value = (-1, -1)
    self.assertTrue(controller.shouldAutoRestart())

    config.getErrorWindow.return_value = (1, 1)
    self.assertTrue(controller.shouldAutoRestart())

    #second failure within a minute
    self.assertFalse(controller.shouldAutoRestart())

    #do not reset unless window expires
    self.assertFalse(controller.shouldAutoRestart())


  @patch("urllib2.urlopen")
  def test_sendRequest(self, requestMock):

    conMock = MagicMock()
    conMock.read.return_value = "response"
    url = "url"
    data = "data"
    requestMock.return_value = conMock

    expected = {'exitstatus': 1, 'log': 'Request failed! Data: ' + data}

    self.assertEqual(expected, self.controller.sendRequest(url, data))
    requestMock.called_once_with(url, data,
      {'Content-Type': 'application/json'})


  @patch.object(threading._Event, "wait")
  @patch("time.sleep")
  @patch("json.loads")
  @patch("json.dumps")
  def test_heartbeatWithServer(self, dumpsMock, loadsMock, sleepMock, event_mock):
    original_value = self.controller.config
    self.controller.config = AgentConfig("", "")
    out = StringIO.StringIO()
    sys.stdout = out

    hearbeat = MagicMock()
    self.controller.heartbeat = hearbeat

    dumpsMock.return_value = "data"

    sendRequest = MagicMock(name="sendRequest")
    self.controller.sendRequest = sendRequest

    self.controller.responseId = 1
    response = {"responseId":"2", "restartAgent": False}
    loadsMock.return_value = response

    def one_heartbeat(*args, **kwargs):
      self.controller.DEBUG_STOP_HEARTBEATING = True
      return "data"

    sendRequest.side_effect = one_heartbeat

    actionQueue = MagicMock()
    actionQueue.isIdle.return_value = True

    # one successful request, after stop
    self.controller.actionQueue = actionQueue
    self.controller.heartbeatWithServer()
    self.assertTrue(sendRequest.called)

    calls = []
    def retry(*args, **kwargs):
      if len(calls) == 0:
        calls.append(1)
        response["responseId"] = "3"
        raise Exception()
      if len(calls) > 0:
        self.controller.DEBUG_STOP_HEARTBEATING = True
      return "data"

    # exception, retry, successful and stop
    sendRequest.side_effect = retry
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    self.assertEqual(1, self.controller.DEBUG_SUCCESSFULL_HEARTBEATS)

    # retry registration
    response["registrationCommand"] = {"command": "register"}
    sendRequest.side_effect = one_heartbeat
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    self.assertTrue(self.controller.repeatRegistration)

    # components are not mapped
    response["registrationCommand"] = {"command": "register"}
    response["hasMappedComponents"] = False
    sendRequest.side_effect = one_heartbeat
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    self.assertFalse(self.controller.hasMappedComponents)

    # components are mapped
    response["hasMappedComponents"] = True
    sendRequest.side_effect = one_heartbeat
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    self.assertTrue(self.controller.hasMappedComponents)

    # components are mapped
    del response["hasMappedComponents"]
    sendRequest.side_effect = one_heartbeat
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    self.assertTrue(self.controller.hasMappedComponents)

    # wrong responseId => restart
    response = {"responseId":"2", "restartAgent": False}
    loadsMock.return_value = response

    restartAgent = MagicMock(name="restartAgent")
    self.controller.restartAgent = restartAgent
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    restartAgent.assert_called_once_with()

    # executionCommands
    self.controller.responseId = 1
    addToQueue = MagicMock(name="addToQueue")
    self.controller.addToQueue = addToQueue
    response["executionCommands"] = "executionCommands"
    self.controller.statusCommand = ["statusCommand"]
    updateStateBasedOnCommand = MagicMock(name="updateStateBasedOnCommand")
    self.controller.updateStateBasedOnCommand = updateStateBasedOnCommand
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    addToQueue.assert_has_calls([call("executionCommands")])
    updateStateBasedOnCommand.assert_has_calls([call("executionCommands")])

    # just status command when state = STARTED
    self.controller.responseId = 1
    response = {"responseId":"2", "restartAgent": False}
    loadsMock.return_value = response
    addToQueue = MagicMock(name="addToQueue")
    self.controller.addToQueue = addToQueue
    self.controller.statusCommand = "statusCommand"
    self.controller.componentActualState = State.STARTED
    self.controller.componentExpectedState = State.STARTED
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    addToQueue.assert_has_calls([call(["statusCommand"])])

    # just status command when state = FAILED
    self.controller.responseId = 1
    response = {"responseId":"2", "restartAgent": False}
    loadsMock.return_value = response
    addToQueue = MagicMock(name="addToQueue")
    self.controller.addToQueue = addToQueue
    self.controller.statusCommand = "statusCommand"
    self.controller.componentActualState = State.FAILED
    self.controller.componentExpectedState = State.STARTED
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    addToQueue.assert_has_calls([call(["statusCommand"])])

    # no status command when state = STARTING
    self.controller.responseId = 1
    response = {"responseId":"2", "restartAgent": False}
    loadsMock.return_value = response
    addToQueue = MagicMock(name="addToQueue")
    self.controller.addToQueue = addToQueue
    self.controller.statusCommand = "statusCommand"
    self.controller.componentActualState = State.STARTING
    self.controller.componentExpectedState = State.STARTED
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    addToQueue.assert_has_calls([])

    # statusCommands
    response["statusCommands"] = "statusCommands"
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    addToQueue.assert_has_calls([call("statusCommands")])

    # restartAgent command
    self.controller.responseId = 1
    self.controller.DEBUG_STOP_HEARTBEATING = False
    response["restartAgent"] = True
    restartAgent = MagicMock(name="restartAgent")
    self.controller.restartAgent = restartAgent
    self.controller.heartbeatWithServer()

    restartAgent.assert_called_once_with()

    # actionQueue not idle
    self.controller.responseId = 1
    self.controller.DEBUG_STOP_HEARTBEATING = False
    actionQueue.isIdle.return_value = False
    response["restartAgent"] = False
    self.controller.heartbeatWithServer()

    sleepMock.assert_called_with(
      self.controller.netutil.MINIMUM_INTERVAL_BETWEEN_HEARTBEATS)

    sys.stdout = sys.__stdout__
    self.controller.sendRequest = Controller.Controller.sendRequest
    self.controller.addToQueue = Controller.Controller.addToQueue

    self.controller.config = original_value
    pass

  @patch.object(threading._Event, "wait")
  @patch("time.sleep")
  @patch("json.loads")
  @patch("json.dumps")
  def test_heartbeatWithServerStopAgent(self, dumpsMock, loadsMock, sleepMock, event_mock):
    original_value = self.controller.config
    self.controller.config = AgentConfig("", "")
    out = StringIO.StringIO()
    sys.stdout = out

    hearbeat = MagicMock()
    self.controller.heartbeat = hearbeat

    dumpsMock.return_value = "data"

    sendRequest = MagicMock(name="sendRequest")
    self.controller.sendRequest = sendRequest

    self.controller.responseId = 1
    response = {"responseId":"2", "restartAgent": False}
    loadsMock.return_value = response

    def one_heartbeat(*args, **kwargs):
      self.controller.DEBUG_STOP_HEARTBEATING = True
      return "data"

    sendRequest.side_effect = one_heartbeat

    actionQueue = MagicMock()
    actionQueue.isIdle.return_value = True

    # one successful request, after stop
    self.controller.actionQueue = actionQueue
    self.controller.heartbeatWithServer()
    self.assertTrue(sendRequest.called)

    calls = []
    def retry(*args, **kwargs):
      if len(calls) == 0:
        calls.append(1)
        response["responseId"] = "3"
        raise Exception()
      if len(calls) > 0:
        self.controller.DEBUG_STOP_HEARTBEATING = True
      return "data"

    # exception, retry, successful and stop
    sendRequest.side_effect = retry
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    self.assertEqual(1, self.controller.DEBUG_SUCCESSFULL_HEARTBEATS)

    original_stopApp = self.controller.stopApp

    # terminateAgent command - test 1
    self.controller.responseId = 1
    self.controller.DEBUG_STOP_HEARTBEATING = False
    response = {"responseId":"2", "terminateAgent": True}
    loadsMock.return_value = response
    stopApp = MagicMock(name="stopApp")
    self.controller.stopApp = stopApp
    self.controller.heartbeatWithServer()
    stopApp.assert_called_once_with()
    
    # reset for next test
    self.controller.terminateAgent = False

    # terminateAgent command - test 2
    self.controller.responseId = 1
    self.controller.DEBUG_STOP_HEARTBEATING = False
    response = {"responseId":"2", "terminateAgent": True}
    loadsMock.return_value = response
    self.controller.stopApp = original_stopApp
    stopCommand = {"roleCommand": "STOP"}
    self.controller.stopCommand = stopCommand
    addToQueue = MagicMock(name="addToQueue")
    self.controller.addToQueue = addToQueue
    self.controller.componentActualState = State.STARTED
    self.controller.heartbeatWithServer()
    self.assertTrue(self.controller.terminateAgent)
    self.assertTrue(self.controller.appGracefulStopQueued)
    addToQueue.assert_has_calls([call([stopCommand])])

    # reset for next test
    self.controller.terminateAgent = False
    self.controller.appGracefulStopQueued = False

    # terminateAgent command - test 3
    self.controller.responseId = 1
    self.controller.DEBUG_STOP_HEARTBEATING = False
    # set stopCommand to None and let it get set by updateStateBasedOnCommand
    self.controller.stopCommand = None
    # in this heartbeat don't send terminateAgent signal
    response = {"responseId":"2", "terminateAgent": False}
    stopCommand = {"roleCommand": "STOP"}
    response["executionCommands"] = [stopCommand]
    loadsMock.return_value = response
    # terminateAgent is False - make STOP in commands set the stopCommand
    self.controller.heartbeatWithServer()
    self.assertFalse(self.controller.terminateAgent)
    assert not self.controller.stopCommand == None

    # Now STOP execution command stops the agent completely so terminateAgent
    # flag test is moved to test_heartbeatWithServerTerminateAgent
 
    sleepMock.assert_called_with(
      self.controller.netutil.MINIMUM_INTERVAL_BETWEEN_HEARTBEATS)

    sys.stdout = sys.__stdout__
    self.controller.sendRequest = Controller.Controller.sendRequest
    self.controller.addToQueue = Controller.Controller.addToQueue

    self.controller.config = original_value
    pass

  @patch.object(threading._Event, "wait")
  @patch("time.sleep")
  @patch("json.loads")
  @patch("json.dumps")
  def test_heartbeatWithServerTerminateAgent(self, dumpsMock, loadsMock, sleepMock, event_mock):
    original_value = self.controller.config
    self.controller.config = AgentConfig("", "")
    out = StringIO.StringIO()
    sys.stdout = out

    hearbeat = MagicMock()
    self.controller.heartbeat = hearbeat

    dumpsMock.return_value = "data"

    sendRequest = MagicMock(name="sendRequest")
    self.controller.sendRequest = sendRequest

    self.controller.responseId = 1
    response = {"responseId":"2", "restartAgent": False}
    loadsMock.return_value = response

    def one_heartbeat(*args, **kwargs):
      self.controller.DEBUG_STOP_HEARTBEATING = True
      return "data"

    sendRequest.side_effect = one_heartbeat

    actionQueue = MagicMock()
    actionQueue.isIdle.return_value = True

    # one successful request, after stop
    self.controller.actionQueue = actionQueue
    self.controller.heartbeatWithServer()
    self.assertTrue(sendRequest.called)

    calls = []
    def retry(*args, **kwargs):
      if len(calls) == 0:
        calls.append(1)
        response["responseId"] = "3"
        raise Exception()
      if len(calls) > 0:
        self.controller.DEBUG_STOP_HEARTBEATING = True
      return "data"

    # exception, retry, successful and stop
    sendRequest.side_effect = retry
    self.controller.DEBUG_STOP_HEARTBEATING = False
    self.controller.heartbeatWithServer()

    self.assertEqual(1, self.controller.DEBUG_SUCCESSFULL_HEARTBEATS)

    original_stopApp = self.controller.stopApp

    # terminateAgent command - test 1
    self.controller.responseId = 1
    self.controller.DEBUG_STOP_HEARTBEATING = False
    response = {"responseId":"2", "terminateAgent": True}
    loadsMock.return_value = response
    stopApp = MagicMock(name="stopApp")
    self.controller.stopApp = stopApp
    self.controller.heartbeatWithServer()
    stopApp.assert_called_once_with()
    
    # reset for next test
    self.controller.terminateAgent = False

    # terminateAgent command - test 2
    self.controller.responseId = 1
    self.controller.DEBUG_STOP_HEARTBEATING = False
    response = {"responseId":"2", "terminateAgent": True}
    loadsMock.return_value = response
    self.controller.stopApp = original_stopApp
    stopCommand = {"roleCommand": "STOP"}
    self.controller.stopCommand = stopCommand
    addToQueue = MagicMock(name="addToQueue")
    self.controller.addToQueue = addToQueue
    self.controller.componentActualState = State.STARTED
    self.controller.heartbeatWithServer()
    self.assertTrue(self.controller.terminateAgent)
    self.assertTrue(self.controller.appGracefulStopQueued)
    addToQueue.assert_has_calls([call([stopCommand])])

    # reset for next test
    self.controller.terminateAgent = False
    self.controller.appGracefulStopQueued = False

    # terminateAgent command - test 3
    self.controller.responseId = 2
    self.controller.DEBUG_STOP_HEARTBEATING = False
    response = {"responseId":"3", "terminateAgent": True}
    loadsMock.return_value = response
    addToQueue = MagicMock(name="addToQueue")
    self.controller.addToQueue = addToQueue
    self.controller.stopApp = original_stopApp
    self.controller.componentActualState = State.STARTED
    self.controller.heartbeatWithServer()
    self.assertTrue(self.controller.terminateAgent)
    self.assertTrue(self.controller.appGracefulStopQueued)
    addToQueue.assert_has_calls([call([stopCommand])])
    self.controller.terminateAgent = False

    sleepMock.assert_called_with(
      self.controller.netutil.MINIMUM_INTERVAL_BETWEEN_HEARTBEATS)

    sys.stdout = sys.__stdout__
    self.controller.sendRequest = Controller.Controller.sendRequest
    self.controller.addToQueue = Controller.Controller.addToQueue

    self.controller.config = original_value
    pass

  @patch.object(Controller.Controller, "createStatusCommand")
  def test_updateStateBasedOnResult(self, mock_createStatusCommand):
    commands = []
    commands.append({
      u'roleCommand': u'INSTALL',
      "configurations": {
        "global": {
          "app_root": "/dummy/app/root",
          "app_version": "1.0.0"
        }
      }
    })
    self.controller.updateStateBasedOnCommand(commands)

    commandResult = {"commandStatus": "COMPLETED"}
    self.controller.updateStateBasedOnResult(commandResult)
    self.assertEqual(State.INSTALLED, self.controller.componentActualState)
    self.assertEqual(State.INSTALLED, self.controller.componentExpectedState)

    commands = []
    commands.append({u'roleCommand': u'START'})
    self.controller.updateStateBasedOnCommand(commands)

    commandResult = {"commandStatus": "COMPLETED"}
    self.controller.updateStateBasedOnResult(commandResult)
    self.assertEqual(State.STARTED, self.controller.componentActualState)
    self.assertEqual(State.STARTED, self.controller.componentExpectedState)

    commandResult = {"healthStatus": "STARTED"}
    self.controller.updateStateBasedOnResult(commandResult)
    self.assertEqual(State.STARTED, self.controller.componentActualState)
    self.assertEqual(State.STARTED, self.controller.componentExpectedState)

    commandResult = {"healthStatus": "INSTALLED"}
    self.controller.updateStateBasedOnResult(commandResult)
    self.assertEqual(State.FAILED, self.controller.componentActualState)
    self.assertEqual(State.STARTED, self.controller.componentExpectedState)
    self.assertEqual(1, self.controller.failureCount)

    commandResult = {"healthStatus": "INSTALLED"}
    self.controller.updateStateBasedOnResult(commandResult)
    self.assertEqual(State.FAILED, self.controller.componentActualState)
    self.assertEqual(State.STARTED, self.controller.componentExpectedState)
    self.assertEqual(2, self.controller.failureCount)

    self.assertTrue(mock_createStatusCommand.called)


  def test_updateStateBasedOnCommand(self):
    commands = []
    self.controller.updateStateBasedOnCommand(commands)
    self.assertEqual(State.INIT, self.controller.componentActualState)
    self.assertEqual(State.INIT, self.controller.componentExpectedState)

    commands.append({
      u'roleCommand': u'INSTALL',
      "configurations": {
        "global": {
          "app_root": "/dummy/app/root",
          "app_version": "1.0.0"
        }
      }
    })
    self.controller.updateStateBasedOnCommand(commands)
    self.assertEqual(State.INSTALLING, self.controller.componentActualState)
    self.assertEqual(State.INSTALLED, self.controller.componentExpectedState)

    commands = []
    commands.append({
      u'roleCommand': u'START',
      "clusterName": "c1",
      "commandParams": ["cp"],
      "role": "HBASE_MASTER",
      "configurations": {"global": {"a": "b"}, "abc-site": {"c": "d"}},
      "hostLevelParams": [],
      "serviceName": "HBASE"
    })
    self.controller.updateStateBasedOnCommand(commands)
    self.assertEqual(State.STARTING, self.controller.componentActualState)
    self.assertEqual(State.STARTED, self.controller.componentExpectedState)

    self.assertEqual(self.controller.statusCommand["clusterName"], "c1")
    self.assertEqual(self.controller.statusCommand["commandParams"], ["cp"])
    self.assertEqual(self.controller.statusCommand["commandType"], "STATUS_COMMAND")
    self.assertEqual(self.controller.statusCommand["roleCommand"], "STATUS")
    self.assertEqual(self.controller.statusCommand["componentName"], "HBASE_MASTER")
    self.assertEqual(self.controller.statusCommand["configurations"], {"global": {"a": "b"}})
    self.assertEqual(self.controller.statusCommand["hostLevelParams"], [])
    self.assertEqual(self.controller.statusCommand["serviceName"], "HBASE")
    self.assertEqual(self.controller.statusCommand["taskId"], "status")
    self.assertEqual(self.controller.statusCommand["auto_generated"], True)
    self.assertEqual(len(self.controller.statusCommand), 10)

    commands = []
    self.controller.updateStateBasedOnCommand(commands)
    self.assertEqual(State.STARTING, self.controller.componentActualState)
    self.assertEqual(State.STARTED, self.controller.componentExpectedState)

  @patch("pprint.pformat")
  @patch("time.sleep")
  @patch("json.loads")
  @patch("json.dumps")
  def test_certSigningFailed(self, dumpsMock, loadsMock, sleepMock, pformatMock):
    register = MagicMock()
    self.controller.register = register

    dumpsMock.return_value = "request"
    response = {"responseId":1,}
    loadsMock.return_value = response

    self.controller.sendRequest = Mock(side_effect=ssl.SSLError())

    self.controller.repeatRegistration=True
    self.controller.registerWithServer()

    #Conroller thread and the agent stop if the repeatRegistration flag is False
    self.assertFalse(self.controller.repeatRegistration)

  @patch("time.sleep")
  def test_debugSetupForRegister(self, sleepMock):
    original_value = self.controller.config
    self.controller.config = AgentConfig("", "")
    self.controller.config.set(AgentConfig.AGENT_SECTION, AgentConfig.DEBUG_MODE_ENABLED, "true")
    self.controller.processDebugCommandForRegister()
    self.controller.processDebugCommandForHeartbeat()
    assert not sleepMock.called, 'sleep should not have been called'

    self.controller.config.set(AgentConfig.AGENT_SECTION, AgentConfig.APP_DBG_CMD, "DO_NOT_RERISTER")
    self.controller.config.set(AgentConfig.AGENT_SECTION, AgentConfig.APP_DBG_CMD, "DO_NOT_HEARTBEET")
    self.controller.processDebugCommandForRegister()
    self.controller.processDebugCommandForHeartbeat()
    assert not sleepMock.called, 'sleep should not have been called'

    self.controller.config.set(AgentConfig.AGENT_SECTION, AgentConfig.APP_DBG_CMD, "DO_NOT_REGISTER")
    self.controller.processDebugCommandForRegister()
    assert sleepMock.called, 'sleep should have been called'

    self.controller.processDebugCommandForHeartbeat()
    assert sleepMock.call_count == 1, 'sleep should have been called once'

    self.controller.config.set(AgentConfig.AGENT_SECTION, AgentConfig.APP_DBG_CMD, "DO_NOT_HEARTBEAT")
    self.controller.processDebugCommandForHeartbeat()
    assert sleepMock.call_count == 2, 'sleep should have been called twice'

    self.controller.config = original_value
    pass

  def test_create_start_command(self):
    stored_command = {
      'commandType': 'EXECUTION_COMMAND',
      'role': u'HBASE_MASTER',
      "componentName": "HBASE_MASTER",
      'roleCommand': u'INSTALL',
      'commandId': '1-1',
      'taskId': 3,
      'clusterName': u'cc',
      'serviceName': u'HBASE',
      'configurations': {'global': {}},
      'configurationTags': {'global': {'tag': 'v1'}},
      'auto_generated': False,
      'roleParams': {'auto_restart':'false'},
      'commandParams': {'script_type': 'PYTHON',
                        'script': 'scripts/abc.py',
                        'command_timeout': '600'}
    }

    expected = {
      'commandType': 'EXECUTION_COMMAND',
      'role': u'HBASE_MASTER',
      "componentName": "HBASE_MASTER",
      'roleCommand': u'INSTALL',
      'commandId': '4-1',
      'taskId': 4,
      'clusterName': u'cc',
      'serviceName': u'HBASE',
      'configurations': {'global': {}},
      'configurationTags': {'global': {'tag': 'v1'}},
      'auto_generated': False,
      'roleParams': {'auto_restart':'false'},
      'commandParams': {'script_type': 'PYTHON',
                        'script': 'scripts/abc.py',
                        'command_timeout': '600'},
      'auto_generated': True
    }

    modified_command = self.controller.create_start_command(stored_command)
    self.assertEqual.__self__.maxDiff = None
    self.assertEqual(modified_command, expected)

  @patch.object(Controller.Controller, "createStatusCommand")
  @patch.object(threading._Event, "wait")
  @patch("time.sleep")
  @patch("json.loads")
  @patch("json.dumps")
  def test_auto_start(self, dumpsMock, loadsMock, timeMock, waitMock, mock_createStatusCommand):
    original_value = self.controller.config
    self.controller.config = AgentConfig("", "")
    out = StringIO.StringIO()
    sys.stdout = out

    heartbeat = MagicMock()
    self.controller.heartbeat = heartbeat

    dumpsMock.return_value = "data"

    sendRequest = MagicMock(name="sendRequest")
    self.controller.sendRequest = sendRequest

    self.controller.responseId = 1
    response1 = {"responseId": "2", "restartAgent": False, "restartEnabled": True}
    response2 = {"responseId": "2", "restartAgent": False, "restartEnabled": False}
    loadsMock.side_effect = [response1, response2, response1]

    def one_heartbeat(*args, **kwargs):
      self.controller.DEBUG_STOP_HEARTBEATING = True
      return "data"

    sendRequest.side_effect = one_heartbeat

    actionQueue = MagicMock()
    actionQueue.isIdle.return_value = True

    # one successful request, after stop
    self.controller.actionQueue = actionQueue
    self.controller.componentActualState = State.FAILED
    self.controller.componentExpectedState = State.STARTED
    self.assertTrue(self.controller.componentActualState, State.FAILED)
    self.controller.actionQueue.customServiceOrchestrator.stored_command = {
      'commandType': 'EXECUTION_COMMAND',
      'role': u'HBASE',
      'roleCommand': u'START',
      'commandId': '7-1',
      'taskId': 7,
      "componentName": "HBASE_MASTER",
      'clusterName': u'cc',
      'serviceName': u'HDFS'
    }
    addToQueue = MagicMock(name="addToQueue")
    self.controller.addToQueue = addToQueue

    self.controller.heartbeatWithServer()
    self.assertTrue(sendRequest.called)

    self.assertTrue(self.controller.componentActualState, State.STARTING)
    self.assertTrue(self.controller.componentExpectedState, State.STARTED)
    self.assertEquals(self.controller.failureCount, 0)
    self.assertFalse(mock_createStatusCommand.called)
    addToQueue.assert_has_calls([call([{
      'commandType': 'EXECUTION_COMMAND',
      'clusterName': u'cc',
      'serviceName': u'HDFS',
      'role': u'HBASE',
      'taskId': 8,
      'roleCommand': u'START',
      'componentName': 'HBASE_MASTER',
      'commandId': '8-1',
      'auto_generated': True}])])
    self.controller.config = original_value

    # restartEnabled = False
    self.controller.componentActualState = State.FAILED
    self.controller.heartbeatWithServer()

    self.assertTrue(sendRequest.called)
    self.assertTrue(self.controller.componentActualState, State.FAILED)
    self.assertTrue(self.controller.componentExpectedState, State.STARTED)

    # restartEnabled = True
    self.controller.componentActualState = State.INSTALLED
    self.controller.componentExpectedState = State.INSTALLED
    self.controller.heartbeatWithServer()

    self.assertTrue(sendRequest.called)
    self.assertTrue(self.controller.componentActualState, State.INSTALLED)
    self.assertTrue(self.controller.componentExpectedState, State.INSTALLED)
    pass


if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
  unittest.main()




