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

"""

import sys
import os
from datetime import datetime
from resource_management import *
from resource_management.core.base import Fail


class CommandLogger(Script):
  def install(self, env):
    self.install_packages(env)

  def configure(self, env):
    import params

    env.set_params(params)

  def start(self, env):
    import params

    env.set_params(params)
    self.configure(env)
    self.rename_file(env)
    self.ensure_file(env)
    self.check_and_log(env, "Starting instance.")

  def stop(self, env):
    import params

    env.set_params(params)
    self.check_and_log(env, "Stopping instance.")
    self.rename_file(env)

  def status(self, env):
    Logger.info("Returning status as live.")

  def check_and_log(self, env, message):
    import params

    file_location = params.file_location
    datetime_format = params.datetime_format
    if not os.path.isfile(file_location) or not os.access(file_location,
                                                          os.W_OK):
      raise Fail("File does not exist or not writable. %s" % file_location)
    with open(file_location, "a") as logfile:
      logfile.write("Time: " + datetime.utcnow().strftime(datetime_format) + "\n")
      logfile.write("Log: " + message + "\n")
      logfile.write("---------------\n")

  def rename_file(self, env):
    import params

    file_location = params.file_location
    if os.path.isfile(file_location) and \
      os.access(file_location, os.W_OK):
      new_file_name = \
        file_location + "." + datetime.utcnow().strftime("%d_%m_%y_%I_%M_%S")
      os.rename(file_location, new_file_name)

  def ensure_file(self, env):
    import params

    file_location = params.file_location
    TemplateConfig( file_location,
                    template_tag = None
    )

  def pre_upgrade(self, env):
    import params
    env.set_params(params)
    Logger.info("Pre upgrade checks.")

  def post_upgrade(self, env):
    import params
    env.set_params(params)
    Logger.info("Post upgrade checks.")

if __name__ == "__main__":
  CommandLogger().execute()
