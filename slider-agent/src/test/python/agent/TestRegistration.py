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
import os
import errno
import tempfile
from mock.mock import patch
from mock.mock import MagicMock
from agent.Register import Register
from agent.AgentConfig import AgentConfig

class TestRegistration(TestCase):

  def test_registration_build(self):
    tmpdir = tempfile.gettempdir()
    ver_dir = os.path.join(tmpdir, "infra")
    config = AgentConfig(tmpdir, ver_dir)
    config.set('agent', 'prefix', tmpdir)
    config.set('agent', 'current_ping_port', '33777')
    try:
      os.mkdir(ver_dir)
    except OSError as exception:
      if exception.errno != errno.EEXIST:
        raise
    pass
    ver_file = os.path.join(ver_dir, "version")
    with open(ver_file, "w") as text_file:
      text_file.write("1.3.0")

    register = Register(config)
    data = register.build(1)
    #print ("Register: " + pprint.pformat(data))
    self.assertEquals(data['hostname'] != "", True, "hostname should not be empty")
    self.assertEquals(data['publicHostname'] != "", True, "publicHostname should not be empty")
    self.assertEquals(data['responseId'], 1)
    self.assertEquals(data['timestamp'] > 1353678475465L, True, "timestamp should not be empty")
    self.assertEquals(data['agentVersion'], '1.3.0', "agentVersion should not be empty")
    self.assertEquals(len(data), 5)

    self.assertEquals(os.path.join(tmpdir, "app/definition"), config.getResolvedPath("app_pkg_dir"))
    self.assertEquals(os.path.join(tmpdir, "app/install"), config.getResolvedPath("app_install_dir"))
    self.assertEquals(os.path.join(ver_dir, "app/log"), config.getResolvedPath("app_log_dir"))
    self.assertEquals(os.path.join(ver_dir, "infra/log"), config.getResolvedPath("log_dir"))
    self.assertEquals(os.path.join(ver_dir, "app/command-log"), config.getResolvedPath("app_task_dir"))

    os.remove(ver_file)
    os.removedirs(ver_dir)

if __name__ == "__main__":
  unittest.main()