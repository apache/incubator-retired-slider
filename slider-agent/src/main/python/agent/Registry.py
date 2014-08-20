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
from kazoo.client import KazooClient

logger = logging.getLogger()

class Registry:
  def __init__(self, zk_quorum, zk_reg_path):
    self.zk_quorum = zk_quorum
    self.zk_reg_path = zk_reg_path

  def readAMHostPort(self):
    amHost = ""
    amSecuredPort = ""
    amUnsecuredPort = ""
    zk = None
    try:
      zk = KazooClient(hosts=self.zk_quorum, read_only=True)
      zk.start()
      data, stat = zk.get(self.zk_reg_path)
      logger.debug("Registry Data: %s" % (data.decode("utf-8")))
      sliderRegistry = json.loads(data)
      amUrl = sliderRegistry["payload"]["internalView"]["endpoints"]["org.apache.slider.agents.secure"]["address"]
      amHost = amUrl.split("/")[2].split(":")[0]
      amSecuredPort = amUrl.split(":")[2].split("/")[0]

      amUnsecureUrl = sliderRegistry["payload"]["internalView"]["endpoints"]["org.apache.slider.agents.oneway"]["address"]
      amUnsecuredPort = amUnsecureUrl.split(":")[2].split("/")[0]

      # the port needs to be utf-8 encoded
      amSecuredPort = amSecuredPort.encode('utf8', 'ignore')
      amUnsecuredPort = amUnsecuredPort.encode('utf8', 'ignore')
    except Exception:
      # log and let empty strings be returned
      logger.error("Could not connect to zk registry at %s in quorum %s" % 
                   (self.zk_reg_path, self.zk_quorum))
      pass
    finally:
      if not zk == None:
        zk.stop()
        zk.close()
    logger.info("AM Host = %s, AM Secured Port = %s" % (amHost, amSecuredPort))
    return amHost, amUnsecuredPort, amSecuredPort
