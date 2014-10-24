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

logger = logging.getLogger()

'''
Create a singleton instance of this class for every loop that either
writes to or reads from the action queue, takes action based on the
commandType and dumps logs along the way. It's target is to keep the
verbosity level of agent logs to zero during non-interesting heartbeats
like STATUS_COMMAND, and to ensure that it starts logging at info level
again, the moment a non-STATUS_COMMAND shows up during its path into or
out of the action queue.
'''
class AgentToggleLogger:
  def __init__(self, logLevel="info"):
    self.logLevel = logLevel

  def log(self, message, *args, **kwargs):
    if self.logLevel == "info":
      logger.info(message, *args, **kwargs)
    else:
      logger.debug(message, *args, **kwargs)

  '''
  The methods adjustLogLevelAtStart and adjustLogLevelAtEnd work hand
  in hand to do the following :
  - STATUS related info should be logged at least once before the agent
    enters into the STATUS loop
  - If a non STATUS command shows up in the queue the logger switches
    to info level
  - STATUS will be logged at least once every time the log level toggles
    back to info level when a non STATUS command shows up
  '''

  # Call this method at the start of the loop over action queue,
  # right after reading from or writing to the queue
  def adjustLogLevelAtStart(self, commandType):
    from ActionQueue import ActionQueue
    if self.logLevel != "info" and commandType != ActionQueue.STATUS_COMMAND:
      self.logLevel = "info"

  # Call this method as the last statement in the loop over action queue
  def adjustLogLevelAtEnd(self, commandType):
    from ActionQueue import ActionQueue
    if commandType == ActionQueue.STATUS_COMMAND:
      self.logLevel = "debug"
    else:
      self.logLevel = "info"

