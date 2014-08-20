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
import sys
from CommandStatusDict import CommandStatusDict
from mock.mock import patch, MagicMock, call, Mock
import logging
from threading import Event

class TestCommandStatusDict(unittest.TestCase):

  logger = logging.getLogger()

  auto_hbase_install_command = {
    'commandType': 'EXECUTION_COMMAND',
    'role': u'HBASE',
    'roleCommand': u'INSTALL',
    'commandId': '1-1',
    'taskId': 7,
    "componentName": "HBASE_MASTER",
    'clusterName': u'cc',
    'serviceName': u'HDFS',
    'auto_generated': True
  }

  @patch("__builtin__.open")
  def test_generate_progress_report(self, open_mock):
    csd = CommandStatusDict(None)
    report = {}
    report['tmpout'] = None
    report['tmperr'] = None
    report['structuredOut'] = None

    # Make file read calls visible
    def open_side_effect(file, mode):
      if mode == 'r':
        file_mock = MagicMock()
        file_mock.read.return_value = "Read from " + str(file)
        return file_mock
      else:
        return self.original_open(file, mode)

    open_mock.side_effect = open_side_effect

    inprogress = csd.generate_in_progress_report(self.auto_hbase_install_command, report)
    expected = {
      'status': 'IN_PROGRESS',
      'stderr': 'Read from None',
      'stdout': 'Read from None',
      'clusterName': u'cc',
      'structuredOut': '{}',
      'reportResult': False,
      'roleCommand': u'INSTALL',
      'serviceName': u'HDFS',
      'role': u'HBASE',
      'actionId': '1-1',
      'taskId': 7,
      'exitcode': 777}
    self.assertEqual(inprogress, expected)

    self.auto_hbase_install_command['auto_generated'] = False
    inprogress = csd.generate_in_progress_report(self.auto_hbase_install_command, report)
    expected['reportResult'] = True
    self.assertEqual(inprogress, expected)
    pass

if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)
  unittest.main()




