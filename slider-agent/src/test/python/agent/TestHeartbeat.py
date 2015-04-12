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

from unittest import TestCase
import unittest
from Heartbeat import Heartbeat
from ActionQueue import ActionQueue
from AgentConfig import AgentConfig
import socket
import os
import time
from mock.mock import patch, MagicMock, call
import StringIO
import sys
import logging
from Controller import State
from AgentToggleLogger import AgentToggleLogger


class TestHeartbeat(TestCase):
  def setUp(self):
    # disable stdout
    out = StringIO.StringIO()
    sys.stdout = out
    self.agentToggleLogger = AgentToggleLogger("info")


  def tearDown(self):
    # enable stdout
    sys.stdout = sys.__stdout__


  def test_build(self):
    config = AgentConfig("", "")
    config.set('agent', 'prefix', 'tmp')
    dummy_controller = MagicMock()
    actionQueue = ActionQueue(config, dummy_controller, self.agentToggleLogger)
    heartbeat = Heartbeat(actionQueue, config, self.agentToggleLogger)
    result = heartbeat.build({}, 100)
    print "Heartbeat: " + str(result)
    self.assertEquals(result['hostname'] != '', True,
                      "hostname should not be empty")
    self.assertEquals(result['responseId'], 100)
    self.assertEquals('componentStatus' not in result, True,
                      "Heartbeat should contain componentStatus")
    self.assertEquals(result['reports'] is not None, True,
                      "Heartbeat should contain reports")
    self.assertEquals(result['timestamp'] >= 1353679373880L, True)
    self.assertEquals(len(result['nodeStatus']), 2)
    self.assertEquals(result['nodeStatus']['cause'], "NONE")
    self.assertEquals(result['nodeStatus']['status'], "HEALTHY")
    # result may or may NOT have an agentEnv structure in it
    self.assertEquals((len(result) is 6) or (len(result) is 7), True)
    self.assertEquals(not heartbeat.reports, True,
                      "Heartbeat should not contain task in progress")


  @patch.object(ActionQueue, "result")
  def test_build_long_result(self, result_mock):
    config = AgentConfig("", "")
    config.set('agent', 'prefix', 'tmp')
    dummy_controller = MagicMock()
    actionQueue = ActionQueue(config, dummy_controller, self.agentToggleLogger)
    result_mock.return_value = {
      'reports': [{'status': 'IN_PROGRESS',
                   'stderr': 'Read from /tmp/errors-3.txt',
                   'stdout': 'Read from /tmp/output-3.txt',
                   'clusterName': u'cc',
                   'roleCommand': u'INSTALL',
                   'serviceName': u'HDFS',
                   'role': u'DATANODE',
                   'actionId': '1-1',
                   'taskId': 3,
                   'exitcode': 777,
                   'reportResult' : True},

                  {'status': 'COMPLETED',
                   'stderr': 'stderr',
                   'stdout': 'out',
                   'clusterName': 'clusterName',
                   'roleCommand': 'UPGRADE',
                   'serviceName': 'serviceName',
                   'role': 'role',
                   'actionId': 17,
                   'taskId': 'taskId',
                   'exitcode': 0,
                   'reportResult' : True},

                  {'status': 'FAILED',
                   'stderr': 'stderr',
                   'stdout': 'out',
                   'clusterName': u'cc',
                   'roleCommand': u'INSTALL',
                   'serviceName': u'HDFS',
                   'role': u'DATANODE',
                   'actionId': '1-1',
                   'taskId': 3,
                   'exitcode': 13,
                   'reportResult' : True},

                  {'status': 'COMPLETED',
                   'stderr': 'stderr',
                   'stdout': 'out',
                   'clusterName': u'cc',
                   'configurationTags': {'global': {'tag': 'v1'}},
                   'roleCommand': u'INSTALL',
                   'serviceName': u'HDFS',
                   'role': u'DATANODE',
                   'actionId': '1-1',
                   'taskId': 3,
                   'exitcode': 0,
                   'reportResult' : True},

                  {'status': 'COMPLETED',
                   'stderr': 'stderr',
                   'stdout': 'out',
                   'clusterName': u'cc',
                   'configurationTags': {'global': {'tag': 'v1'}},
                   'roleCommand': u'INSTALL',
                   'serviceName': u'HDFS',
                   'role': u'DATANODE',
                   'actionId': '1-1',
                   'taskId': 3,
                   'exitcode': 0,
                   'reportResult' : False}
      ],
      'componentStatus': [
        {'status': 'HEALTHY', 'componentName': 'DATANODE', 'reportResult' : True},
        {'status': 'UNHEALTHY', 'componentName': 'NAMENODE', 'reportResult' : True},
        {'status': 'UNHEALTHY', 'componentName': 'HBASE_MASTER', 'reportResult' : False},
      ],
    }
    heartbeat = Heartbeat(actionQueue, config, self.agentToggleLogger)
    # State.STARTED results in agentState to be set to 4 (enum order)
    hb = heartbeat.build({}, 10)
    hb['hostname'] = 'hostname'
    hb['timestamp'] = 'timestamp'
    hb['fqdn'] = 'fqdn'
    expected = {'package': '', 'nodeStatus':
                  {'status': 'HEALTHY',
                   'cause': 'NONE'},
                'timestamp': 'timestamp', 'hostname': 'hostname', 'fqdn': 'fqdn',
                'responseId': 10, 'reports': [
      {'status': 'IN_PROGRESS', 'roleCommand': u'INSTALL',
       'serviceName': u'HDFS', 'role': u'DATANODE', 'actionId': '1-1',
       'stderr': 'Read from /tmp/errors-3.txt',
       'stdout': 'Read from /tmp/output-3.txt', 'clusterName': u'cc',
       'taskId': 3, 'exitcode': 777},
      {'status': 'COMPLETED', 'roleCommand': 'UPGRADE',
       'serviceName': 'serviceName', 'role': 'role', 'actionId': 17,
       'stderr': 'stderr', 'stdout': 'out', 'clusterName': 'clusterName',
       'taskId': 'taskId', 'exitcode': 0},
      {'status': 'FAILED', 'roleCommand': u'INSTALL', 'serviceName': u'HDFS',
       'role': u'DATANODE', 'actionId': '1-1', 'stderr': 'stderr',
       'stdout': 'out', 'clusterName': u'cc', 'taskId': 3, 'exitcode': 13},
      {'status': 'COMPLETED', 'stdout': 'out',
       'configurationTags': {'global': {'tag': 'v1'}}, 'taskId': 3,
       'exitcode': 0, 'roleCommand': u'INSTALL', 'clusterName': u'cc',
       'serviceName': u'HDFS', 'role': u'DATANODE', 'actionId': '1-1',
       'stderr': 'stderr'}],  'componentStatus': [
      {'status': 'HEALTHY', 'componentName': 'DATANODE'},
      {'status': 'UNHEALTHY', 'componentName': 'NAMENODE'}]}
    self.assertEqual.__self__.maxDiff = None
    self.assertEquals(hb, expected)

  @patch.object(ActionQueue, "result")
  def test_build_result2(self, result_mock):
    config = AgentConfig("", "")
    config.set('agent', 'prefix', 'tmp')
    dummy_controller = MagicMock()
    actionQueue = ActionQueue(config, dummy_controller, self.agentToggleLogger)
    result_mock.return_value = {
      'reports': [{'status': 'IN_PROGRESS',
                   'stderr': 'Read from /tmp/errors-3.txt',
                   'stdout': 'Read from /tmp/output-3.txt',
                   'clusterName': u'cc',
                   'roleCommand': u'INSTALL',
                   'serviceName': u'HDFS',
                   'role': u'DATANODE',
                   'actionId': '1-1',
                   'taskId': 3,
                   'exitcode': 777,
                   'reportResult' : False}
      ],
      'componentStatus': []
      }
    heartbeat = Heartbeat(actionQueue, config, self.agentToggleLogger)

    commandResult = {}
    hb = heartbeat.build(commandResult, 10)
    hb['hostname'] = 'hostname'
    hb['timestamp'] = 'timestamp'
    hb['fqdn'] = 'fqdn'
    expected = {'package': '', 'nodeStatus':
                  {'status': 'HEALTHY',
                   'cause': 'NONE'},
                'timestamp': 'timestamp', 'hostname': 'hostname', 'fqdn': 'fqdn',
                'responseId': 10, 'reports': []}
    self.assertEqual.__self__.maxDiff = None
    self.assertEquals(hb, expected)
    self.assertEquals(commandResult, {'commandStatus': 'IN_PROGRESS'})

  @patch.object(ActionQueue, "result")
  def test_build_result3(self, result_mock):
    config = AgentConfig("", "")
    config.set('agent', 'prefix', 'tmp')
    dummy_controller = MagicMock()
    actionQueue = ActionQueue(config, dummy_controller, self.agentToggleLogger)
    result_mock.return_value = {
      'reports': [{'status': 'COMPLETED',
                   'stderr': 'Read from /tmp/errors-3.txt',
                   'stdout': 'Read from /tmp/output-3.txt',
                   'clusterName': u'cc',
                   'roleCommand': u'INSTALL',
                   'serviceName': u'HDFS',
                   'role': u'DATANODE',
                   'actionId': '1-1',
                   'taskId': 3,
                   'exitcode': 777,
                   'reportResult' : False}
      ],
      'componentStatus': []
    }
    heartbeat = Heartbeat(actionQueue, config, self.agentToggleLogger)

    commandResult = {}
    hb = heartbeat.build(commandResult, 10)
    hb['hostname'] = 'hostname'
    hb['timestamp'] = 'timestamp'
    hb['fqdn'] = 'fqdn'
    expected = {'package': '', 'nodeStatus':
                  {'status': 'HEALTHY',
                   'cause': 'NONE'},
                'timestamp': 'timestamp', 'hostname': 'hostname', 'fqdn': 'fqdn',
                'responseId': 10, 'reports': []}
    self.assertEqual.__self__.maxDiff = None
    self.assertEquals(hb, expected)
    self.assertEquals(commandResult, {'commandStatus': 'COMPLETED'})



if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
  unittest.main()
