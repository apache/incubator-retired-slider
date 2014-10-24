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

import unittest
from AgentToggleLogger import AgentToggleLogger
import logging

class TestAgentToggleLogger(unittest.TestCase):
  def setUp(self):
    self.agentToggleLogger = AgentToggleLogger("info")

  def test_adjustLogLevel(self):
    from ActionQueue import ActionQueue
    # log level is set to info here (during initialization)
    # run an execution command first
    self.agentToggleLogger.adjustLogLevelAtStart(ActionQueue.EXECUTION_COMMAND)
    assert self.agentToggleLogger.logLevel == "info"
    self.agentToggleLogger.adjustLogLevelAtEnd(ActionQueue.EXECUTION_COMMAND)
    assert self.agentToggleLogger.logLevel == "info"

    # run a status command now
    self.agentToggleLogger.adjustLogLevelAtStart(ActionQueue.STATUS_COMMAND)
    assert self.agentToggleLogger.logLevel == "info"
    self.agentToggleLogger.adjustLogLevelAtEnd(ActionQueue.STATUS_COMMAND)
    assert self.agentToggleLogger.logLevel == "debug"

    # run a status command again
    self.agentToggleLogger.adjustLogLevelAtStart(ActionQueue.STATUS_COMMAND)
    assert self.agentToggleLogger.logLevel == "debug"
    self.agentToggleLogger.adjustLogLevelAtEnd(ActionQueue.STATUS_COMMAND)
    assert self.agentToggleLogger.logLevel == "debug"

    # now an execution command shows up
    self.agentToggleLogger.adjustLogLevelAtStart(ActionQueue.EXECUTION_COMMAND)
    assert self.agentToggleLogger.logLevel == "info"
    self.agentToggleLogger.adjustLogLevelAtEnd(ActionQueue.EXECUTION_COMMAND)
    assert self.agentToggleLogger.logLevel == "info"


if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)
  unittest.main()

