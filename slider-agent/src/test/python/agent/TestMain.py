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
import StringIO
import sys

import NetUtil, security
from mock.mock import MagicMock, patch, ANY
import unittest
import ProcessHelper, main
import logging
import signal
from AgentConfig import AgentConfig
import ConfigParser
import os
import tempfile
from Controller import Controller
from Registry import Registry
from optparse import OptionParser
import platform

IS_WINDOWS = platform.system() == "Windows"

logger = logging.getLogger()

class TestMain(unittest.TestCase):
  def setUp(self):
    # disable stdout
    out = StringIO.StringIO()
    sys.stdout = out


  def tearDown(self):
    # enable stdout
    sys.stdout = sys.__stdout__

  @patch("os._exit")
  @patch("os.getpid")
  @patch.object(ProcessHelper, "stopAgent")
  def test_signal_handler(self, stopAgent_mock, os_getpid_mock, os_exit_mock):
    # testing exit of children
    main.agentPid = 4444
    os_getpid_mock.return_value = 5555
    main.signal_handler("signum", "frame")
    self.assertTrue(os_exit_mock.called)

    os_exit_mock.reset_mock()

    # testing exit of main process
    os_getpid_mock.return_value = main.agentPid
    main.signal_handler("signum", "frame")
    self.assertFalse(os_exit_mock.called)
    self.assertTrue(stopAgent_mock.called)


  @patch.object(main.logger, "addHandler")
  @patch.object(main.logger, "setLevel")
  @patch("logging.handlers.RotatingFileHandler")
  def test_setup_logging(self, rfh_mock, setLevel_mock, addHandler_mock):
    # Testing silent mode
    _, tmpoutfile = tempfile.mkstemp()
    main.setup_logging(False, tmpoutfile)
    self.assertTrue(addHandler_mock.called)
    setLevel_mock.assert_called_with(logging.INFO)

    addHandler_mock.reset_mock()
    setLevel_mock.reset_mock()

    # Testing verbose mode
    main.setup_logging(True, tmpoutfile)
    self.assertTrue(addHandler_mock.called)
    setLevel_mock.assert_called_with(logging.DEBUG)


  @patch.object(main.logger, "setLevel")
  @patch("logging.basicConfig")
  def test_update_log_level(self, basicConfig_mock, setLevel_mock):
    config = AgentConfig("", "")
    _, tmpoutfile = tempfile.mkstemp()

    # Testing with default setup (config file does not contain loglevel entry)
    # Log level should not be changed
    config.set('agent', 'log_level', None)
    main.update_log_level(config, tmpoutfile)
    self.assertFalse(setLevel_mock.called)

    setLevel_mock.reset_mock()

    # Testing debug mode
    config.set('agent', 'log_level', 'DEBUG')
    main.update_log_level(config, tmpoutfile)
    setLevel_mock.assert_called_with(logging.DEBUG)
    setLevel_mock.reset_mock()

    # Testing any other mode
    config.set('agent', 'log_level', 'INFO')
    main.update_log_level(config, tmpoutfile)
    setLevel_mock.assert_called_with(logging.INFO)

    setLevel_mock.reset_mock()

    config.set('agent', 'log_level', 'WRONG')
    main.update_log_level(config, tmpoutfile)
    setLevel_mock.assert_called_with(logging.INFO)

  if not IS_WINDOWS:
    @patch("signal.signal")
    def test_bind_signal_handlers(self, signal_mock):
      main.bind_signal_handlers()
      # Check if on SIGINT/SIGTERM agent is configured to terminate
      signal_mock.assert_any_call(signal.SIGINT, main.signal_handler)
      signal_mock.assert_any_call(signal.SIGTERM, main.signal_handler)
      # Check if on SIGUSR1 agent is configured to fall into debug
      signal_mock.assert_any_call(signal.SIGUSR1, main.debug)


  @patch("os.path.exists")
  @patch("ConfigParser.RawConfigParser.read")
  def test_resolve_config(self, read_mock, exists_mock):
    config = AgentConfig("", "")
    # Trying case if conf file exists
    exists_mock.return_value = True
    main.update_config_from_file(config)
    self.assertTrue(read_mock.called)

    exists_mock.reset_mock()
    read_mock.reset_mock()

    # Trying case if conf file does not exist
    exists_mock.return_value = False
    main.update_config_from_file(config)
    self.assertFalse(read_mock.called)


  @patch("os.remove")
  @patch("sys.exit")
  @patch("os.path.isfile")
  @patch("os.path.isdir")
  def test_perform_prestart_checks(self, isdir_mock, isfile_mock,
                                   exit_mock, remove_mock):
    main.config = AgentConfig("", "")

    # Trying case if there is another instance running
    isfile_mock.return_value = True
    isdir_mock.return_value = True
    main.perform_prestart_checks(main.config)
    self.assertFalse(exit_mock.called)
    self.assertTrue(remove_mock.called)

    isfile_mock.reset_mock()
    isdir_mock.reset_mock()
    exit_mock.reset_mock()

    # Trying case if agent prefix dir does not exist
    isfile_mock.return_value = False
    isdir_mock.return_value = False
    main.perform_prestart_checks(main.config)
    self.assertTrue(exit_mock.called)

    isfile_mock.reset_mock()
    isdir_mock.reset_mock()
    exit_mock.reset_mock()

    # Trying normal case
    isfile_mock.return_value = False
    isdir_mock.return_value = True
    main.perform_prestart_checks(main.config)
    self.assertFalse(exit_mock.called)

  if not IS_WINDOWS:
    @patch("time.sleep")
    @patch("os.kill")
    @patch("os._exit")
    @patch("os.path.exists")
    def test_daemonize_and_stop(self, exists_mock, _exit_mock, kill_mock,
                              sleep_mock):
      oldpid = ProcessHelper.pidfile
      pid = str(os.getpid())
      _, tmpoutfile = tempfile.mkstemp()
      ProcessHelper.pidfile = tmpoutfile

      # Test daemonization
      main.write_pid()
      saved = open(ProcessHelper.pidfile, 'r').read()
      self.assertEqual(pid, saved)

      # Reuse pid file when testing agent stop
      # Testing normal exit
      exists_mock.return_value = False
      main.stop_agent()
      kill_mock.assert_called_with(int(pid), signal.SIGTERM)

      # Restore
      kill_mock.reset_mock()
      _exit_mock.reset_mock()

      # Testing exit when failed to remove pid file
      exists_mock.return_value = True
      main.stop_agent()
      kill_mock.assert_any_call(int(pid), signal.SIGTERM)
      kill_mock.assert_any_call(int(pid), signal.SIGKILL)
      _exit_mock.assert_called_with(1)

      # Restore
      ProcessHelper.pidfile = oldpid
      os.remove(tmpoutfile)

  @patch.object(Registry, "readAMHostPort")
  @patch.object(main, "setup_logging")
  @patch.object(main, "bind_signal_handlers")
  @patch.object(main, "update_config_from_file")
  @patch.object(main, "perform_prestart_checks")
  @patch.object(main, "write_pid")
  @patch.object(main, "update_log_level")
  @patch.object(NetUtil.NetUtil, "try_to_connect")
  @patch.object(Controller, "__init__")
  @patch.object(Controller, "start")
  @patch.object(Controller, "join")
  @patch("optparse.OptionParser.parse_args")
  @patch.object(Controller, "is_alive")
  def test_main(self, isAlive_mock, parse_args_mock, join_mock, start_mock,
                Controller_init_mock, try_to_connect_mock,
                update_log_level_mock, write_pid_mock,
                perform_prestart_checks_mock,
                update_config_from_file_mock,
                bind_signal_handlers_mock, setup_logging_mock,
                readAMHostPort_mock):
    Controller_init_mock.return_value = None
    isAlive_mock.return_value = False
    options = MagicMock()
    parse_args_mock.return_value = (options, MagicMock)
    readAMHostPort_mock.return_value = ("host1", 101, 100)

    tmpdir = tempfile.gettempdir()

    #testing call without command-line arguments
    os.environ["AGENT_WORK_ROOT"] = os.path.join(tmpdir, "work")
    os.environ["AGENT_LOG_ROOT"] = ",".join([os.path.join(tmpdir, "log"),os.path.join(tmpdir, "log2")])
    try_to_connect_mock.return_value = 1
    main.main()

    self.assertTrue(setup_logging_mock.called)
    if not IS_WINDOWS:
      self.assertTrue(bind_signal_handlers_mock.called)
    self.assertTrue(update_config_from_file_mock.called)
    self.assertTrue(perform_prestart_checks_mock.called)
    self.assertTrue(write_pid_mock.called)
    self.assertTrue(update_log_level_mock.called)
    self.assertTrue(options.log_folder == os.path.join(tmpdir, "log"))
    try_to_connect_mock.assert_called_once_with('https://host1:101/ws/v1/slider/agents/', 3, ANY)
    self.assertTrue(start_mock.called)

  class AgentOptions:
      def __init__(self, label, zk_quorum, zk_reg_path, verbose, debug):
          self.label = label
          self.zk_quorum = zk_quorum
          self.zk_reg_path = zk_reg_path
          self.verbose = verbose
          self.debug = debug

  @patch.object(Registry, "readAMHostPort")
  @patch("time.sleep")
  @patch.object(main, "setup_logging")
  @patch.object(main, "bind_signal_handlers")
  @patch.object(main, "stop_agent")
  @patch.object(main, "update_config_from_file")
  @patch.object(main, "perform_prestart_checks")
  @patch.object(main, "write_pid")
  @patch.object(main, "update_log_level")
  @patch.object(NetUtil.NetUtil, "try_to_connect")
  @patch.object(AgentConfig, 'set')
  @patch.object(Controller, "__init__")
  @patch.object(Controller, "start")
  @patch.object(Controller, "join")
  @patch.object(Controller, "is_alive")
  @patch("optparse.OptionParser.parse_args")
  def test_main2(self, parse_args_mock, isAlive_mock, join_mock, start_mock,
                Controller_init_mock, AgentConfig_set_mock,
                try_to_connect_mock,
                update_log_level_mock, write_pid_mock,
                perform_prestart_checks_mock,
                update_config_from_file_mock, stop_mock,
                bind_signal_handlers_mock, setup_logging_mock,
                time_sleep_mock, readAMHostPort_mock):
      Controller_init_mock.return_value = None
      isAlive_mock.return_value = False
      parse_args_mock.return_value = (
          TestMain.AgentOptions("agent", "host1:2181", "/registry/org-apache-slider/cl1", True, ""), [])
      tmpdir = tempfile.gettempdir()
      time_sleep_mock.return_value = None
      readAMHostPort_mock.return_value = (None, None, None)

      #testing call without command-line arguments
      os.environ["AGENT_WORK_ROOT"] = os.path.join(tmpdir, "work")
      os.environ["AGENT_LOG_ROOT"] = os.path.join(tmpdir, "log")
      main.main()
      self.assertTrue(AgentConfig_set_mock.call_count == 3)
      self.assertTrue(readAMHostPort_mock.call_count == 10)
      AgentConfig_set_mock.assert_any_call("server", "zk_quorum", "host1:2181")
      AgentConfig_set_mock.assert_any_call("server", "zk_reg_path", "/registry/org-apache-slider/cl1")


  def test_config1(self):
    config = AgentConfig("", "")
    (max, window) = config.getErrorWindow()
    self.assertEqual(max, 5)
    self.assertEqual(window, 5)

    config.set(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART, '')
    (max, window) = config.getErrorWindow()
    self.assertEqual(max, 0)
    self.assertEqual(window, 0)

    config.set(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART, '33')
    (max, window) = config.getErrorWindow()
    self.assertEqual(max, 0)
    self.assertEqual(window, 0)

    config.set(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART, '-4,-6')
    (max, window) = config.getErrorWindow()
    self.assertEqual(max, 0)
    self.assertEqual(window, 0)

    config.set(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART, 'wd,er')
    (max, window) = config.getErrorWindow()
    self.assertEqual(max, 0)
    self.assertEqual(window, 0)

    config.set(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART, '2,20')
    (max, window) = config.getErrorWindow()
    self.assertEqual(max, 2)
    self.assertEqual(window, 20)

    config.set(AgentConfig.COMMAND_SECTION, AgentConfig.AUTO_RESTART, ' 2, 30')
    (max, window) = config.getErrorWindow()
    self.assertEqual(max, 0)
    self.assertEqual(window, 0)


if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
  unittest.main()