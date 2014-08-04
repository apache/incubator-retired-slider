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
import os
import pprint

from unittest import TestCase
import unittest
import threading
import tempfile
import time
import logging
from threading import Thread

from PythonExecutor import PythonExecutor
from CustomServiceOrchestrator import CustomServiceOrchestrator
from mock.mock import MagicMock, patch
import StringIO
import sys
from socket import socket


class TestCustomServiceOrchestrator(TestCase):
  def setUp(self):
    # disable stdout
    out = StringIO.StringIO()
    sys.stdout = out
    # generate sample config
    tmpdir = tempfile.gettempdir()


  @patch("hostname.public_hostname")
  @patch("os.path.isfile")
  @patch("os.unlink")
  def test_dump_command_to_json(self, unlink_mock,
                                isfile_mock, hostname_mock):
    hostname_mock.return_value = "test.hst"
    command = {
      'commandType': 'EXECUTION_COMMAND',
      'componentName': 'NAMENODE',
      'role': u'DATANODE',
      'roleCommand': u'INSTALL',
      'commandId': '1-1',
      'taskId': 3,
      'clusterName': u'cc',
      'serviceName': u'HDFS',
      'configurations': {'global': {}},
      'configurationTags': {'global': {'tag': 'v1'}},
      'clusterHostInfo': {'namenode_host': ['1'],
                          'slave_hosts': ['0', '1'],
                          'all_hosts': ['h1.hortonworks.com', 'h2.hortonworks.com'],
                          'all_ping_ports': ['8670:0,1']}
    }

    tempdir = tempfile.gettempdir()
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempdir
    config.getLogPath.return_value = tempdir

    dummy_controller = MagicMock()
    orchestrator = CustomServiceOrchestrator(config, dummy_controller)
    isfile_mock.return_value = True
    # Test dumping EXECUTION_COMMAND
    json_file = orchestrator.dump_command_to_json(command, {})
    self.assertTrue(os.path.exists(json_file))
    self.assertTrue(os.path.getsize(json_file) > 0)
    self.assertEqual(oct(os.stat(json_file).st_mode & 0777), '0600')
    self.assertTrue(json_file.endswith("command-3.json"))
    os.unlink(json_file)
    # Test dumping STATUS_COMMAND
    command['commandType'] = 'STATUS_COMMAND'
    json_file = orchestrator.dump_command_to_json(command, {})
    self.assertTrue(os.path.exists(json_file))
    self.assertTrue(os.path.getsize(json_file) > 0)
    self.assertEqual(oct(os.stat(json_file).st_mode & 0777), '0600')
    self.assertTrue(json_file.endswith("status_command.json"))
    os.unlink(json_file)
    # Testing side effect of dump_command_to_json
    self.assertEquals(command['public_hostname'], "test.hst")
    self.assertTrue(unlink_mock.called)


  @patch.object(CustomServiceOrchestrator, "resolve_script_path")
  @patch.object(CustomServiceOrchestrator, "dump_command_to_json")
  @patch.object(PythonExecutor, "run_file")
  def test_runCommand(self,
                      run_file_mock, dump_command_to_json_mock,
                      resolve_script_path_mock):
    command = {
      'role': 'REGION_SERVER',
      'hostLevelParams': {
        'stack_name': 'HDP',
        'stack_version': '2.0.7',
        'jdk_location': 'some_location'
      },
      'commandParams': {
        'script_type': 'PYTHON',
        'script': 'scripts/hbase_regionserver.py',
        'command_timeout': '600',
        'service_package_folder': 'HBASE'
      },
      'taskId': '3',
      'roleCommand': 'INSTALL'
    }

    tempdir = tempfile.gettempdir()
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempdir
    config.getLogPath.return_value = tempdir

    resolve_script_path_mock.return_value = "/basedir/scriptpath"
    dummy_controller = MagicMock()
    orchestrator = CustomServiceOrchestrator(config, dummy_controller)
    # normal run case
    run_file_mock.return_value = {
      'stdout': 'sss',
      'stderr': 'eee',
      'exitcode': 0
    }
    ret = orchestrator.runCommand(command, "out.txt", "err.txt")
    self.assertEqual(ret['exitcode'], 0)
    self.assertTrue(run_file_mock.called)
    self.assertEqual(run_file_mock.call_count, 1)

    run_file_mock.reset_mock()

    # Case when we force another command
    run_file_mock.return_value = {
      'stdout': 'sss',
      'stderr': 'eee',
      'exitcode': 0,
    }
    ret = orchestrator.runCommand(command, "out.txt", "err.txt")
    ## Check that override_output_files was true only during first call
    self.assertEquals(run_file_mock.call_args_list[0][0][7], True)

    run_file_mock.reset_mock()
    # Case when we force another command
    run_file_mock.return_value = {
      'stdout': 'sss',
      'stderr': 'eee',
      'exitcode': 1
    }
    ret = orchestrator.runCommand(command, "out.txt", "err.txt")
    self.assertFalse('allocated_ports' in ret)

    run_file_mock.reset_mock()

    # unknown script type case
    command['commandParams']['script_type'] = "PUPPET"
    ret = orchestrator.runCommand(command, "out.txt", "err.txt")
    self.assertEqual(ret['exitcode'], 1)
    self.assertFalse(run_file_mock.called)
    self.assertTrue("Unknown script type" in ret['stdout'])

    #By default returns empty dictionary
    self.assertEqual(ret['structuredOut'], '{}')
    pass


  @patch.object(CustomServiceOrchestrator, "allocate_ports")
  @patch.object(CustomServiceOrchestrator, "resolve_script_path")
  @patch.object(PythonExecutor, "run_file")
  def test_runCommand_get_port(self,
                               run_file_mock,
                               resolve_script_path_mock,
                               allocate_ports_mock):
    command = {
      'role': 'HBASE_REGIONSERVER',
      'hostLevelParams': {
        'stack_name': 'HDP',
        'stack_version': '2.0.7',
        'jdk_location': 'some_location'
      },
      'commandParams': {
        'script_type': 'PYTHON',
        'script': 'scripts/hbase_regionserver.py',
        'command_timeout': '600',
        'service_package_folder': 'HBASE'
      },
      'taskId': '3',
      'roleCommand': 'INSTALL',
      'commandType': 'EXECUTE',
      'componentName': 'HBASE_REGIONSERVER',
      'configurations': {'a': {'a.port': '${HBASE_REGIONSERVER.ALLOCATED_PORT}'}}
    }

    tempdir = tempfile.gettempdir()
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempdir
    config.getLogPath.return_value = tempdir

    allocate_ports_mock.return_value = str(10233)

    resolve_script_path_mock.return_value = "/basedir/scriptpath"
    dummy_controller = MagicMock()
    orchestrator = CustomServiceOrchestrator(config, dummy_controller)
    # normal run case
    run_file_mock.return_value = {
      'stdout': 'sss',
      'stderr': 'eee',
      'exitcode': 0
    }
    ret = orchestrator.runCommand(command, "out.txt", "err.txt")
    self.assertEqual(ret['exitcode'], 0)
    self.assertEqual(ret['allocated_ports'], {'a.a.port': '10233'})
    self.assertTrue(run_file_mock.called)
    self.assertEqual(run_file_mock.call_count, 1)


  @patch.object(socket, "close")
  @patch.object(socket, "connect")
  def test_allocate_port_def(self, socket_connect_mock, socket_close_mock):
    e = OSError()
    socket_connect_mock.side_effect = e
    tempdir = tempfile.gettempdir()
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempdir
    config.getLogPath.return_value = tempdir

    dummy_controller = MagicMock()
    orchestrator = CustomServiceOrchestrator(config, dummy_controller)
    ret = orchestrator.allocate_port(10)
    self.assertEqual(ret, 10)

  @patch.object(socket, "getsockname")
  @patch.object(socket, "bind")
  @patch.object(socket, "close")
  @patch.object(socket, "connect")
  def test_allocate_port_new(self, socket_connect_mock, socket_close_mock,
                         socket_bind_mock, socket_getsockname_mock):
    tempdir = tempfile.gettempdir()
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempdir
    config.getLogPath.return_value = tempdir

    dummy_controller = MagicMock()
    orchestrator = CustomServiceOrchestrator(config, dummy_controller)
    socket_getsockname_mock.return_value = [100, 101]
    ret = orchestrator.allocate_port(10)
    self.assertEqual(ret, 101)

  @patch.object(socket, "getsockname")
  @patch.object(socket, "bind")
  def test_allocate_port_no_def(self, socket_bind_mock, socket_getsockname_mock):
    tempdir = tempfile.gettempdir()
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempdir
    config.getLogPath.return_value = tempdir

    dummy_controller = MagicMock()
    orchestrator = CustomServiceOrchestrator(config, dummy_controller)
    socket_getsockname_mock.return_value = [100, 102]
    ret = orchestrator.allocate_port()
    self.assertEqual(ret, 102)


  @patch.object(CustomServiceOrchestrator, "is_port_available")
  @patch.object(CustomServiceOrchestrator, "allocate_port")
  def test_allocate_port_combinations(self, allocate_port_mock, is_port_available_mock):
    tempdir = tempfile.gettempdir()
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempdir
    config.getLogPath.return_value = tempdir

    dummy_controller = MagicMock()
    orchestrator = CustomServiceOrchestrator(config, dummy_controller)

    is_port_available_mock.return_value = False
    allocate_port_mock.side_effect = [101, 102, 103, 104, 105, 106]
    ret = orchestrator.allocate_ports("1000", "${A.ALLOCATED_PORT}")
    self.assertEqual(ret, "1000")
    ret = orchestrator.allocate_ports("${A.ALLOCATED_PORT}", "${A.ALLOCATED_PORT}")
    self.assertEqual(ret, "101")
    ret = orchestrator.allocate_ports("${A.ALLOCATED_PORT},${A.ALLOCATED_PORT}", "${A.ALLOCATED_PORT}")
    self.assertEqual(ret, "102,103")
    ret = orchestrator.allocate_ports("${A.ALLOCATED_PORT}{DEFAULT_0}", "${A.ALLOCATED_PORT}")
    self.assertEqual(ret, "104")
    ret = orchestrator.allocate_ports("${A.ALLOCATED_PORT}{DEFAULT_0}{DO_NOT_PROPAGATE}", "${A.ALLOCATED_PORT}")
    self.assertEqual(ret, "105")
    ret = orchestrator.allocate_ports("${A.ALLOCATED_PORT}{DO_NOT_PROPAGATE}", "${A.ALLOCATED_PORT}")
    self.assertEqual(ret, "106")


  @patch.object(CustomServiceOrchestrator, "is_port_available")
  def test_allocate_port_combinations2(self, is_port_available_mock):
    tempdir = tempfile.gettempdir()
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempdir
    config.getLogPath.return_value = tempdir

    dummy_controller = MagicMock()
    orchestrator = CustomServiceOrchestrator(config, dummy_controller)

    is_port_available_mock.return_value = True
    ret = orchestrator.allocate_ports("${A.ALLOCATED_PORT}{DEFAULT_1005}", "${A.ALLOCATED_PORT}")
    self.assertEqual(ret, "1005")

    ret = orchestrator.allocate_ports("${A.ALLOCATED_PORT}{DEFAULT_1005}-${A.ALLOCATED_PORT}{DEFAULT_1006}",
                                      "${A.ALLOCATED_PORT}")
    self.assertEqual(ret, "1005-1006")

    ret = orchestrator.allocate_ports("${A.ALLOCATED_PORT}{DEFAULT_1006}{DO_NOT_PROPAGATE}",
                                      "${A.ALLOCATED_PORT}")
    self.assertEqual(ret, "1006")

  @patch("hostname.public_hostname")
  @patch("os.path.isfile")
  @patch("os.unlink")
  @patch.object(CustomServiceOrchestrator, "resolve_script_path")
  @patch.object(PythonExecutor, "run_file")
  def test_runCommand_with_config(self,
                                  run_file_mock,
                                  resolve_script_path_mock, unlink_mock,
                                  isfile_mock, hostname_mock):
    hostname_mock.return_value = "test.hst"
    isfile_mock.return_value = True
    command = {
      'role': 'REGION_SERVER',
      'componentName': 'REGION_SERVER',
      'hostLevelParams': {
        'stack_name': 'HDP',
        'stack_version': '2.0.7',
        'jdk_location': 'some_location'
      },
      'commandParams': {
        'script_type': 'PYTHON',
        'script': 'scripts/hbase_regionserver.py',
        'command_timeout': '600',
        'service_package_folder': 'HBASE'
      },
      'configurations': {
        "hbase-site": {
          "hbase.log": "${AGENT_LOG_ROOT}",
          "hbase.number": "10485760"},
        "hbase-log4j": {"a": "b"}
      },
      'taskId': '3',
      'roleCommand': 'INSTALL',
      'commandType': 'EXECUTION_COMMAND',
      'commandId': '1-1'
    }

    command_get = {
      'roleCommand': 'GET_CONFIG',
      'commandType': 'STATUS_COMMAND'
    }

    command_get_specific = {
      'roleCommand': 'GET_CONFIG',
      'commandType': 'STATUS_COMMAND',
      'commandParams': {
        'config_type': 'hbase-site'
      }
    }

    tempdir = tempfile.gettempdir()
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempdir
    config.getLogPath.return_value = tempdir

    resolve_script_path_mock.return_value = "/basedir/scriptpath"
    dummy_controller = MagicMock()
    orchestrator = CustomServiceOrchestrator(config, dummy_controller)
    # normal run case
    run_file_mock.return_value = {
      'stdout': 'sss',
      'stderr': 'eee',
      'exitcode': 0,
    }

    expected = {
      'hbase-site': {
        'hbase.log': tempdir, 'hbase.number': '10485760'},
      'hbase-log4j': {'a': 'b'}}

    expected_specific = {
      'hbase-site': {
        'hbase.log': tempdir, 'hbase.number': '10485760'},
    }

    ret = orchestrator.runCommand(command, "out.txt", "err.txt", True, True)
    self.assertEqual(ret['exitcode'], 0)
    self.assertTrue(run_file_mock.called)
    self.assertEqual(orchestrator.applied_configs, expected)

    ret = orchestrator.requestComponentStatus(command_get)
    self.assertEqual(ret['configurations'], expected)

    ret = orchestrator.requestComponentStatus(command_get_specific)
    self.assertEqual(ret['configurations'], expected_specific)
    pass

  @patch.object(CustomServiceOrchestrator, "runCommand")
  def test_requestComponentStatus(self, runCommand_mock):
    status_command = {
      "serviceName": 'HDFS',
      "commandType": "STATUS_COMMAND",
      "clusterName": "",
      "componentName": "DATANODE",
      'configurations': {},
      'roleCommand': "STATUS"
    }
    dummy_controller = MagicMock()

    tempdir = tempfile.gettempdir()
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempdir
    config.getLogPath.return_value = tempdir

    orchestrator = CustomServiceOrchestrator(config, dummy_controller)
    # Test alive case
    runCommand_mock.return_value = {
      "exitcode": 0
    }
    status = orchestrator.requestComponentStatus(status_command)
    self.assertEqual(CustomServiceOrchestrator.LIVE_STATUS, status['exitcode'])

    # Test dead case
    runCommand_mock.return_value = {
      "exitcode": 1
    }
    status = orchestrator.requestComponentStatus(status_command)
    self.assertEqual(CustomServiceOrchestrator.DEAD_STATUS, status['exitcode'])

  @patch.object(CustomServiceOrchestrator, "allocate_ports")
  def test_finalize_command(self, mock_allocate_ports):
    dummy_controller = MagicMock()
    tempdir = tempfile.gettempdir()
    tempWorkDir = tempdir + "W"
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempWorkDir
    config.getLogPath.return_value = tempdir
    mock_allocate_ports.return_value = "10023"

    orchestrator = CustomServiceOrchestrator(config, dummy_controller)
    command = {}
    command['componentName'] = "HBASE_MASTER"
    command['configurations'] = {}
    command['configurations']['hbase-site'] = {}
    command['configurations']['hbase-site']['a'] = 'b'
    command['configurations']['hbase-site']['work_root'] = "${AGENT_WORK_ROOT}"
    command['configurations']['hbase-site']['log_root'] = "${AGENT_LOG_ROOT}/log"
    command['configurations']['hbase-site']['blog_root'] = "/b/${AGENT_LOG_ROOT}/log"
    command['configurations']['oozie-site'] = {}
    command['configurations']['oozie-site']['log_root'] = "${AGENT_LOG_ROOT}"
    command['configurations']['oozie-site']['a_port'] = "${HBASE_MASTER.ALLOCATED_PORT}"

    allocated_ports = {}
    orchestrator.finalize_command(command, False, allocated_ports)
    self.assertEqual(command['configurations']['hbase-site']['work_root'], tempWorkDir)
    self.assertEqual(command['configurations']['oozie-site']['log_root'], tempdir)
    self.assertEqual(command['configurations']['oozie-site']['a_port'], "10023")
    self.assertEqual(orchestrator.applied_configs, {})
    self.assertEqual(len(allocated_ports), 1)
    self.assertTrue('oozie-site.a_port' in allocated_ports)
    self.assertEqual(allocated_ports['oozie-site.a_port'], '10023')

    command['configurations']['hbase-site']['work_root'] = "${AGENT_WORK_ROOT}"
    command['configurations']['hbase-site']['log_root'] = "${AGENT_LOG_ROOT}/log"
    command['configurations']['hbase-site']['blog_root'] = "/b/${AGENT_LOG_ROOT}/log"
    command['configurations']['oozie-site']['log_root'] = "${AGENT_LOG_ROOT}"
    command['configurations']['oozie-site']['b_port'] = "${HBASE_REGIONSERVER.ALLOCATED_PORT}"

    orchestrator.finalize_command(command, True, {})
    self.assertEqual(command['configurations']['hbase-site']['log_root'], tempdir + "/log")
    self.assertEqual(command['configurations']['hbase-site']['blog_root'], "/b/" + tempdir + "/log")
    self.assertEqual(command['configurations']['oozie-site']['b_port'], "0")
    self.assertEqual(orchestrator.applied_configs, command['configurations'])


  def test_port_allocation(self):
    dummy_controller = MagicMock()
    tempdir = tempfile.gettempdir()
    tempWorkDir = tempdir + "W"
    config = MagicMock()
    config.get.return_value = "something"
    config.getResolvedPath.return_value = tempdir
    config.getWorkRootPath.return_value = tempWorkDir
    config.getLogPath.return_value = tempdir

    orchestrator = CustomServiceOrchestrator(config, dummy_controller)
    port = orchestrator.allocate_port()
    self.assertFalse(port == -1)
    self.assertTrue(port > 0)

  def tearDown(self):
    # enable stdout
    sys.stdout = sys.__stdout__


if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
  unittest.main()

