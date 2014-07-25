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

import os
import unittest
import tempfile
from mock.mock import patch, MagicMock, call
from agent import shell
from sys import platform as _platform
import subprocess, time
import sys
import platform

class TestShell(unittest.TestCase):
  unsupported_for_test = []

  def linux_distribution(self):
    PYTHON_VER = sys.version_info[0] * 10 + sys.version_info[1]

    if PYTHON_VER < 26:
      linux_dist = platform.dist()
    else:
      linux_dist = platform.linux_distribution()

    return linux_dist


  def test_kill_process_with_children(self):
    dist = self.linux_distribution()
    operatingSystem = dist[0].lower()
    if operatingSystem in self.unsupported_for_test:
      return

    if _platform == "linux" or _platform == "linux2": # Test is Linux-specific
      gracefull_kill_delay_old = shell.gracefull_kill_delay
      shell.gracefull_kill_delay = 0.1
      sleep_cmd = "sleep 10"
      test_cmd = """ (({0}) & ({0} & {0})) """.format(sleep_cmd)
      # Starting process tree (multiple process groups)
      test_process = subprocess.Popen(test_cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
      time.sleep(0.3) # Delay to allow subprocess to start
      # Check if processes are running
      ps_cmd = """ps aux """
      ps_process = subprocess.Popen(ps_cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
      (out, err) = ps_process.communicate()
      self.assertTrue(sleep_cmd in out)
      # Kill test process
      shell.kill_process_with_children(test_process.pid)
      test_process.communicate()
      # Now test process should not be running
      ps_process = subprocess.Popen(ps_cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
      (out, err) = ps_process.communicate()
      self.assertFalse(sleep_cmd in out)
      shell.gracefull_kill_delay = gracefull_kill_delay_old
    else:
      # Do not run under other systems
      pass
