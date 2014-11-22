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

from Queue import Queue
from unittest import TestCase
import unittest
from ActionQueue import ActionQueue
from AgentConfig import AgentConfig
import os
import errno
import time
import pprint
import tempfile
import threading
import StringIO
import sys
import logging
from threading import Thread
from mock.mock import patch, MagicMock, call
from CustomServiceOrchestrator import CustomServiceOrchestrator
from PythonExecutor import PythonExecutor
from CommandStatusDict import CommandStatusDict
from AgentToggleLogger import AgentToggleLogger
import platform
IS_WINDOWS = platform.system() == "Windows"


class TestActionQueue(TestCase):
  def setUp(self):
    out = StringIO.StringIO()
    sys.stdout = out
    # save original open() method for later use
    self.original_open = open
    self.agentToggleLogger = AgentToggleLogger("info")


  def tearDown(self):
    sys.stdout = sys.__stdout__

  datanode_install_command = {
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
    'commandParams': {'script_type': 'PYTHON',
                      'script': 'scripts/abc.py',
                      'command_timeout': '600'}
  }

  hbase_install_command = {
    'commandType': 'EXECUTION_COMMAND',
    'role': u'HBASE',
    'roleCommand': u'INSTALL',
    'commandId': '1-1',
    'taskId': 7,
    "componentName": "HBASE_MASTER",
    'clusterName': u'cc',
    'serviceName': u'HDFS',
  }

  status_command = {
    "serviceName": 'ACCUMULO',
    "commandType": "STATUS_COMMAND",
    "clusterName": "c1",
    "componentName": "ACCUMULO_MASTER",
    'configurations': {},
    'roleCommand': "STATUS"
  }

  status_command_with_config = {
    "serviceName": 'ACCUMULO',
    "commandType": "STATUS_COMMAND",
    "clusterName": "c1",
    "componentName": "ACCUMULO_MASTER",
    'configurations': {},
    "commandParams": {"retrieve_config": "false"},
    'roleCommand': "GET_CONFIG"
  }

  @patch.object(ActionQueue, "process_command")
  @patch.object(Queue, "get")
  @patch.object(CustomServiceOrchestrator, "__init__")
  def test_ActionQueueStartStop(self, CustomServiceOrchestrator_mock,
                                get_mock, process_command_mock):
    CustomServiceOrchestrator_mock.return_value = None
    dummy_controller = MagicMock()
    config = MagicMock()
    actionQueue = ActionQueue(config, dummy_controller, self.agentToggleLogger)
    actionQueue.start()
    time.sleep(3)
    actionQueue.stop()
    actionQueue.join()
    self.assertEqual(actionQueue.stopped(), True, 'Action queue is not stopped.')
    self.assertTrue(process_command_mock.call_count > 1)


  @patch("traceback.print_exc")
  @patch.object(ActionQueue, "execute_command")
  def test_process_command(self,
                           execute_command_mock, print_exc_mock):
    dummy_controller = MagicMock()
    actionQueue = ActionQueue(AgentConfig("", ""), dummy_controller, self.agentToggleLogger)
    execution_command = {
      'commandType': ActionQueue.EXECUTION_COMMAND,
    }
    wrong_command = {
      'commandType': "SOME_WRONG_COMMAND",
    }
    # Try wrong command
    actionQueue.process_command(wrong_command)
    self.assertFalse(execute_command_mock.called)
    self.assertFalse(print_exc_mock.called)

    execute_command_mock.reset_mock()
    print_exc_mock.reset_mock()
    # Try normal execution
    actionQueue.process_command(execution_command)
    self.assertTrue(execute_command_mock.called)
    self.assertFalse(print_exc_mock.called)

    execute_command_mock.reset_mock()
    print_exc_mock.reset_mock()

    execute_command_mock.reset_mock()
    print_exc_mock.reset_mock()

    # Try exception to check proper logging
    def side_effect(self):
      raise Exception("TerribleException")

    execute_command_mock.side_effect = side_effect
    actionQueue.process_command(execution_command)
    self.assertTrue(print_exc_mock.called)

    print_exc_mock.reset_mock()

    actionQueue.process_command(execution_command)
    self.assertTrue(print_exc_mock.called)


  @patch.object(ActionQueue, "status_update_callback")
  @patch.object(CustomServiceOrchestrator, "requestComponentStatus")
  @patch.object(ActionQueue, "execute_command")
  @patch.object(CustomServiceOrchestrator, "__init__")
  def test_execute_status_command(self, CustomServiceOrchestrator_mock,
                                  execute_command_mock,
                                  requestComponentStatus_mock,
                                  status_update_callback):
    CustomServiceOrchestrator_mock.return_value = None
    dummy_controller = MagicMock()
    actionQueue = ActionQueue(AgentConfig("", ""), dummy_controller, self.agentToggleLogger)

    requestComponentStatus_mock.return_value = {'exitcode': 'dummy report'}
    actionQueue.execute_status_command(self.status_command)
    report = actionQueue.result()
    expected = 'dummy report'
    self.assertEqual(len(report['componentStatus']), 1)
    self.assertEqual(report['componentStatus'][0]["status"], expected)
    self.assertEqual(report['componentStatus'][0]["componentName"], "ACCUMULO_MASTER")
    self.assertEqual(report['componentStatus'][0]["serviceName"], "ACCUMULO")
    self.assertEqual(report['componentStatus'][0]["clusterName"], "c1")
    self.assertTrue(requestComponentStatus_mock.called)

  @patch.object(ActionQueue, "status_update_callback")
  @patch.object(CustomServiceOrchestrator, "runCommand")
  @patch.object(ActionQueue, "execute_command")
  @patch('CustomServiceOrchestrator.CustomServiceOrchestrator', autospec=True)
  def test_execute_status_command_expect_config(self, CustomServiceOrchestrator_mock,
                                                execute_command_mock,
                                                runCommand_mock,
                                                status_update_callback):
    csoMocks = [MagicMock()]
    CustomServiceOrchestrator_mock.side_effect = csoMocks
    csoMocks[0].status_commands_stdout = None
    csoMocks[0].status_commands_stderr = None
    dummy_controller = MagicMock()
    actionQueue = ActionQueue(AgentConfig("", ""), dummy_controller, self.agentToggleLogger)

    runCommand_mock.return_value = {'configurations': {}}
    actionQueue.execute_status_command(self.status_command_with_config)
    report = actionQueue.result()
    self.assertEqual(len(report['componentStatus']), 1)
    self.assertEqual(report['componentStatus'][0]["componentName"], "ACCUMULO_MASTER")
    self.assertEqual(report['componentStatus'][0]["serviceName"], "ACCUMULO")
    self.assertEqual(report['componentStatus'][0]["clusterName"], "c1")
    self.assertEqual(report['componentStatus'][0]["configurations"], {})
    self.assertFalse(runCommand_mock.called)


  @patch("traceback.print_exc")
  @patch.object(ActionQueue, "execute_command")
  @patch.object(ActionQueue, "execute_status_command")
  def test_process_command2(self, execute_status_command_mock,
                           execute_command_mock, print_exc_mock):
    dummy_controller = MagicMock()
    actionQueue = ActionQueue(AgentConfig("", ""), dummy_controller, self.agentToggleLogger)
    execution_command = {
      'commandType': ActionQueue.EXECUTION_COMMAND,
    }
    status_command = {
      'commandType': ActionQueue.STATUS_COMMAND,
    }
    wrong_command = {
      'commandType': "SOME_WRONG_COMMAND",
    }
    # Try wrong command
    actionQueue.process_command(wrong_command)
    self.assertFalse(execute_command_mock.called)
    self.assertFalse(execute_status_command_mock.called)
    self.assertFalse(print_exc_mock.called)

    execute_command_mock.reset_mock()
    execute_status_command_mock.reset_mock()
    print_exc_mock.reset_mock()
    # Try normal execution
    actionQueue.process_command(execution_command)
    self.assertTrue(execute_command_mock.called)
    self.assertFalse(execute_status_command_mock.called)
    self.assertFalse(print_exc_mock.called)

    execute_command_mock.reset_mock()
    execute_status_command_mock.reset_mock()
    print_exc_mock.reset_mock()

    actionQueue.process_command(status_command)
    self.assertFalse(execute_command_mock.called)
    self.assertTrue(execute_status_command_mock.called)
    self.assertFalse(print_exc_mock.called)

    execute_command_mock.reset_mock()
    execute_status_command_mock.reset_mock()
    print_exc_mock.reset_mock()

    # Try exception to check proper logging
    def side_effect(self):
      raise Exception("TerribleException")

    execute_command_mock.side_effect = side_effect
    actionQueue.process_command(execution_command)
    self.assertTrue(print_exc_mock.called)

    print_exc_mock.reset_mock()

    execute_status_command_mock.side_effect = side_effect
    actionQueue.process_command(execution_command)
    self.assertTrue(print_exc_mock.called)


  @patch.object(CustomServiceOrchestrator, "resolve_script_path")
  @patch("json.load")
  @patch("__builtin__.open")
  @patch.object(ActionQueue, "status_update_callback")
  def test_execute_command(self, status_update_callback_mock, open_mock, json_load_mock,
                           resolve_script_path_mock):

    self.assertEqual.__self__.maxDiff = None
    tempdir = tempfile.gettempdir()
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempdir
    config.getLogPath.return_value = tempdir

    # Make file read calls visible
    def open_side_effect(file, mode):
      if mode == 'r':
        file_mock = MagicMock()
        file_mock.read.return_value = "Read from " + str(file)
        return file_mock
      else:
        return self.original_open(file, mode)

    open_mock.side_effect = open_side_effect
    json_load_mock.return_value = ''
    resolve_script_path_mock.return_value = "abc.py"

    dummy_controller = MagicMock()
    actionQueue = ActionQueue(config, dummy_controller, self.agentToggleLogger)
    unfreeze_flag = threading.Event()
    python_execution_result_dict = {
      'stdout': 'out',
      'stderr': 'stderr',
      'structuredOut': ''
    }

    def side_effect(py_file, script_params,
                    tmpoutfile, tmperrfile, timeout,
                    tmpstrucoutfile,
                    loglevel,
                    override_output_files,
                    environment_vars):
      unfreeze_flag.wait()
      return python_execution_result_dict

    def patched_aq_execute_command(command):
      # We have to perform patching for separate thread in the same thread
      with patch.object(PythonExecutor, "run_file") as runCommand_mock:
        runCommand_mock.side_effect = side_effect
        actionQueue.execute_command(command)
        ### Test install/start/stop command ###

        ## Test successful execution with configuration tags

    python_execution_result_dict['status'] = 'COMPLETE'
    python_execution_result_dict['exitcode'] = 0
    # We call method in a separate thread
    execution_thread = Thread(target=patched_aq_execute_command,
                              args=(self.datanode_install_command, ))
    execution_thread.start()
    #  check in progress report
    # wait until ready
    while True:
      time.sleep(0.1)
      report = actionQueue.result()
      if len(report['reports']) != 0:
        break
    expected = {'status': 'IN_PROGRESS',
                'stderr': 'Read from {0}/errors-3.txt'.format(tempdir),
                'stdout': 'Read from {0}/output-3.txt'.format(tempdir),
                'structuredOut': '',
                'clusterName': u'cc',
                'roleCommand': u'INSTALL',
                'serviceName': u'HBASE',
                'role': u'HBASE_MASTER',
                'actionId': '1-1',
                'taskId': 3,
                'exitcode': 777,
                'reportResult': True}
    if IS_WINDOWS:
      expected = {'status': 'IN_PROGRESS',
                  'stderr': 'Read from {0}\\errors-3.txt'.format(tempdir),
                  'stdout': 'Read from {0}\\output-3.txt'.format(tempdir),
                  'structuredOut': '',
                  'clusterName': u'cc',
                  'roleCommand': u'INSTALL',
                  'serviceName': u'HBASE',
                  'role': u'HBASE_MASTER',
                  'actionId': '1-1',
                  'taskId': 3,
                  'exitcode': 777,
                  'reportResult': True}
    self.assertEqual(report['reports'][0], expected)
    # Continue command execution
    unfreeze_flag.set()
    # wait until ready
    while report['reports'][0]['status'] == 'IN_PROGRESS':
      time.sleep(0.1)
      report = actionQueue.result()
      # check report
    configname = os.path.join(tempdir, 'command-3.json')
    expected = {'status': 'COMPLETED',
                'stderr': 'stderr',
                'stdout': 'out',
                'clusterName': u'cc',
                'configurationTags': {'global': {'tag': 'v1'}},
                'roleCommand': u'INSTALL',
                'serviceName': u'HBASE',
                'role': u'HBASE_MASTER',
                'actionId': '1-1',
                'taskId': 3,
                'structuredOut': '',
                'exitcode': 0,
                'allocatedPorts': {},
                'folders': {'AGENT_LOG_ROOT': tempdir, 'AGENT_WORK_ROOT': tempdir},
                'reportResult': True}
    self.assertEqual(len(report['reports']), 1)
    self.assertEqual(report['reports'][0], expected)
    self.assertTrue(os.path.isfile(configname))
    # Check that we had 2 status update calls ( IN_PROGRESS and COMPLETE)
    self.assertEqual(status_update_callback_mock.call_count, 2)
    os.remove(configname)

    # now should not have reports (read complete/failed reports are deleted)
    report = actionQueue.result()
    self.assertEqual(len(report['reports']), 0)

    ## Test failed execution
    python_execution_result_dict['status'] = 'FAILED'
    python_execution_result_dict['exitcode'] = 13
    # We call method in a separate thread
    execution_thread = Thread(target=patched_aq_execute_command,
                              args=(self.datanode_install_command, ))
    execution_thread.start()
    unfreeze_flag.set()
    #  check in progress report
    # wait until ready
    report = actionQueue.result()
    while len(report['reports']) == 0 or \
            report['reports'][0]['status'] == 'IN_PROGRESS':
      time.sleep(0.1)
      report = actionQueue.result()
      # check report
    expected = {'status': 'FAILED',
                'stderr': 'stderr',
                'stdout': 'out',
                'clusterName': u'cc',
                'roleCommand': u'INSTALL',
                'serviceName': u'HBASE',
                'role': u'HBASE_MASTER',
                'actionId': '1-1',
                'taskId': 3,
                'structuredOut': '',
                'exitcode': 13,
                'reportResult': True}
    self.assertEqual(len(report['reports']), 1)
    self.assertEqual(report['reports'][0], expected)

    # now should not have reports (read complete/failed reports are deleted)
    report = actionQueue.result()
    self.assertEqual(len(report['reports']), 0)


if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
  unittest.main()

