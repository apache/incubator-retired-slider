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

import json
import logging
import time
from pprint import pformat
import hostname

from ActionQueue import ActionQueue
import AgentConfig


logger = logging.getLogger()

class Heartbeat:
  def __init__(self, actionQueue, config=None, agentToggleLogger=None):
    self.actionQueue = actionQueue
    self.config = config
    self.reports = []
    self.agentToggleLogger = agentToggleLogger

  def build(self, commandResult, id='-1',
            componentsMapped=False):
    timestamp = int(time.time() * 1000)
    queueResult = self.actionQueue.result()
    self.agentToggleLogger.log("Queue result: " + pformat(queueResult))

    nodeStatus = {"status": "HEALTHY",
                  "cause": "NONE"}
    if not self.actionQueue.componentPackage == '':
      logger.info("Add package to heartbeat: "
                  + self.actionQueue.componentPackage)
    heartbeat = {'responseId': int(id),
                 'timestamp': timestamp,
                 'hostname': self.config.getLabel(),
                 'nodeStatus': nodeStatus,
                 'package': self.actionQueue.componentPackage,
                 'fqdn': hostname.public_hostname()
    }

    commandsInProgress = False
    if not self.actionQueue.commandQueue.empty():
      commandsInProgress = True
    if len(queueResult) != 0:
      heartbeat['reports'] = []
      for report in queueResult['reports']:
        if report['reportResult']:
          del report['reportResult']
          heartbeat['reports'].append(report)
        else:
          # dropping the result but only recording the status
          commandResult["commandStatus"] = report["status"]
          pass
      if len(heartbeat['reports']) > 0:
        # There may be IN_PROGRESS tasks
        commandsInProgress = True
      pass

    # For first request/heartbeat assume no components are mapped
    if int(id) == 0:
      componentsMapped = False

    # create a report of command results (there is only one component at any point)
    if len(heartbeat['reports']) > 0:
      for report in heartbeat['reports']:
        commandResult["commandStatus"] = report["status"]
      pass

    # Process component status - it can have internal or external STATUS requests and CONFIG requests
    if queueResult['componentStatus']:
      componentStatuses = []
      for componentStatus in queueResult['componentStatus']:
        if componentStatus['reportResult']:
          del componentStatus['reportResult']
          componentStatuses.append(componentStatus)
        else:
          commandResult["healthStatus"] = componentStatus["status"]
        pass
      if len(componentStatuses) > 0:
        heartbeat['componentStatus'] = componentStatuses

    self.agentToggleLogger.log(
                 "Sending heartbeat with response id: " + str(id) + " and "
                 "timestamp: " + str(timestamp) +
                 ". Command(s) in progress: " + repr(commandsInProgress) +
                 ". Components mapped: " + repr(componentsMapped))
    logger.debug("Heartbeat : " + pformat(heartbeat))

    return heartbeat


def main(argv=None):
  actionQueue = ActionQueue(AgentConfig.getConfig())
  heartbeat = Heartbeat(actionQueue)
  print json.dumps(heartbeat.build('3', 3))


if __name__ == '__main__':
  main()
