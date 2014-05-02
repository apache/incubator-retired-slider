#!/usr/bin/env python
"""
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

Slider Agent

"""

from __future__ import with_statement

from resource_management.core import shell
from resource_management.core.providers import Provider
from resource_management.core.logger import Logger

TAR_CMD = "tar -xvf %s -C %s"
ZIP_CMD = "unzip %s -d %s"

class TarballProvider(Provider):
  def action_install(self):
    package_name = self.resource.package_name
    location = self.resource.location
    if not self._check_existence(package_name, location):
      cmd = TAR_CMD % (package_name, location)
      if package_name.lower().endswith("zip"):
        cmd = ZIP_CMD % (package_name, location)
      Logger.info("Installing tarball %s at %s (%s)" % (package_name, location, cmd))
      shell.checked_call(cmd)

  def _check_existence(self, name, location):
    return False

