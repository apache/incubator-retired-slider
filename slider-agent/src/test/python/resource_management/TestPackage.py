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
from mock.mock import patch, MagicMock

from resource_management.core import Environment, Fail
from resource_management.core.system import System
from resource_management.core.resources import Package

from resource_management.core.providers import get_class_path, PROVIDERS

class TestPackage(TestCase):
  
  def test_default_provider(self):
    self.assertEquals(PROVIDERS['default']['Tarball'], get_class_path('something unknown', 'Tarball'))

