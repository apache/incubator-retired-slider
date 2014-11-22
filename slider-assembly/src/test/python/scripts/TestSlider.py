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

from mock import MagicMock, patch, ANY
import unittest
import logging
import slider
import os
import platform

IS_WINDOWS = platform.system() == "Windows"

logger = logging.getLogger()

class TestSlider(unittest.TestCase):

  @patch("os.environ.get")
  @patch.object(slider, "confDir")
  @patch.object(slider, "libDir")
  @patch.object(slider, "executeEnvSh")
  @patch("os.path.exists")
  @patch.object(slider, "java")
  def test_main(self, java_mock, exists_mock, executeEnvSh_mock, libDir_mock, confDir_mock, os_env_get_mock):
    sys.argv = ["slider", "list"]
    exists_mock.return_value = True
    libDir_mock.return_value = "/dir/libdir"
    confDir_mock.return_value = "/dir/confdir"
    os_env_get_mock.return_value = "env_val"
    slider.main()
    self.assertTrue(java_mock.called)
    if IS_WINDOWS:
      java_mock.assert_called_with(
        'org.apache.slider.Slider',
        ['list'],
        '/dir/libdir\\*;/dir/confdir;env_val;env_val',
        ['-Dslider.confdir=/dir/confdir', '-Dslider.libdir=/dir/libdir', 'env_val'])
    else:
      java_mock.assert_called_with(
        'org.apache.slider.Slider',
        ['list'],
        '/dir/libdir/*:/dir/confdir:env_val:env_val',
        ['-Dslider.confdir=/dir/confdir', '-Dslider.libdir=/dir/libdir', 'env_val'])
    pass


if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
  unittest.main()