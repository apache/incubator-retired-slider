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
import logging.handlers
from optparse import OptionParser
import sys
import os
import ConfigParser
import errno
import shutil
import shlex
import subprocess
import time
import fnmatch
from SliderTesterConfig import SliderTesterConfig

logger = logging.getLogger()
formatstr = "%(levelname)s %(asctime)s %(filename)s:%(lineno)d - %(message)s"


class SliderTester:
  SLIDER_CREATE_CMD_FORMAT = '{0} create {1} --image {2}{3} --option agent.conf {2}{4}  --template {5} --resources {6}  --option application.def {2}{7}'
  HDFS_LS = "hdfs dfs -ls"
  HDFS_CHOWN = "hdfs dfs -chown"
  HDFS_MKDIR = "hdfs dfs -mkdir"
  HDFS_RMR = "hdfs dfs -rm -r"

  def __init__(self, config):
    self.config = config
    self.slider_user = self.config.get(SliderTesterConfig.TEST_SECTION, SliderTesterConfig.APP_USER)
    self.hdfs_user = self.config.get(SliderTesterConfig.TEST_SECTION, SliderTesterConfig.HDFS_ROOT_USER)
    self.jdk_path = self.config.get(SliderTesterConfig.SLIDER_SECTION, SliderTesterConfig.JDK_PATH)

  def run_slider_command(self, cmd, user=None):
    if self.jdk_path and len(self.jdk_path) > 0:
      cmd = "PATH=$PATH:{0}&&{1}".format(self.jdk_path, cmd)

    return self.run_os_command(cmd, user)

  def run_os_command(self, cmd, user=None):
    FORMAT_WITH_SU = 'su -l {0} -c "{1}"'
    if user:
      cmd = FORMAT_WITH_SU.format(user, cmd)

    logger.info('Executing command: ' + str(cmd))
    if type(cmd) == str:
      cmd = shlex.split(cmd)

    process = subprocess.Popen(cmd,
                               stdout=subprocess.PIPE,
                               stdin=subprocess.PIPE,
                               stderr=subprocess.PIPE
    )
    (stdoutdata, stderrdata) = process.communicate()

    logger.debug("Out: " + stdoutdata)
    logger.debug("Err: " + stderrdata)
    logger.debug("Process ret code: " + str(process.returncode))
    return process.returncode, stdoutdata, stderrdata

  def ensure_path_exists(self, path):
    try:
      os.makedirs(path)
    except OSError as exception:
      if exception.errno != errno.EEXIST:
        raise
      pass
    pass

  def ensure_ownership(self, path, user):
    CMD = "chown -R {0} {1}"
    self.run_os_command(CMD.format(user, path))
    pass

  def deploy_and_validate_slider(self):
    UNTAR = "tar -xvf {0} -C {1}"
    self.slider_package = self.config.get(SliderTesterConfig.SLIDER_SECTION, SliderTesterConfig.PACKAGE)
    if not os.path.isfile(self.slider_package):
      raise Exception("Invalid Slider package: " + self.slider_package)

    self.app_package = self.config.get(SliderTesterConfig.APP_SECTION, SliderTesterConfig.PACKAGE)
    if not os.path.isfile(self.app_package):
      raise Exception("Invalid App package: " + self.app_package)

    self.test_root = self.config.get(SliderTesterConfig.TEST_SECTION, SliderTesterConfig.TEST_ROOT)
    if os.path.exists(self.test_root):
      if os.path.isfile(self.test_root):
        os.remove(self.test_root)
      else:
        shutil.rmtree(self.test_root)
    pass

    self.slider_root = os.path.join(self.test_root, "slider")
    self.ensure_path_exists(self.slider_root)
    self.ensure_ownership(self.slider_root, self.slider_user)

    self.run_os_command(UNTAR.format(self.slider_package, self.slider_root), self.slider_user)

    self.app_root = os.path.join(self.test_root, "expandedapp")
    self.ensure_path_exists(self.app_root)
    self.ensure_ownership(self.app_root, self.slider_user)

    self.run_os_command(UNTAR.format(self.app_package, self.app_root), self.slider_user)

    self.app_resources = self.findfile("resources.json", self.app_root)
    self.app_conf = self.findfile("appConfig.json", self.app_root)

    self.slider_exec = self.findfile("slider", self.slider_root)

    (retcode, out, err) = self.run_slider_command(" ".join([self.slider_exec, "version"]), self.slider_user)
    if retcode != 0:
      raise Exception("Could not execute version check using " + self.slider_exec)

    if not "Compiled against Hadoop" in out:
      raise Exception("Could not execute version check using " + self.slider_exec)

    self.agent_conf = self.findfile("agent.ini", self.slider_root)
    self.agent_tarball = self.findfile("slider-agent*.tar.gz", self.slider_root)

    pass

  def configure_slider_client(self):
    slider_conf_file = self.findfile("slider-client.xml", self.slider_root)

    data = None
    with open(slider_conf_file, 'r') as orig:
      data = orig.readlines()

    # add/replace a set of well known configs

    name_value_to_add = {
      SliderTesterConfig.SLIDER_ZK_QUORUM:
        self.config.get(SliderTesterConfig.CLUSTER_SECTION, SliderTesterConfig.SLIDER_ZK_QUORUM),
      SliderTesterConfig.YARN_RESOURCEMANAGER_ADDRESS:
        self.config.get(SliderTesterConfig.CLUSTER_SECTION, SliderTesterConfig.YARN_RESOURCEMANAGER_ADDRESS),
      SliderTesterConfig.YARN_RESOURCEMANAGER_SCHEDULER_ADDRESS:
        self.config.get(SliderTesterConfig.CLUSTER_SECTION, SliderTesterConfig.YARN_RESOURCEMANAGER_SCHEDULER_ADDRESS),
      SliderTesterConfig.FS_DEFAULTFS:
        self.config.get(SliderTesterConfig.CLUSTER_SECTION, SliderTesterConfig.FS_DEFAULTFS),
      SliderTesterConfig.YARN_APP_CP:
        self.config.get(SliderTesterConfig.CLUSTER_SECTION, SliderTesterConfig.YARN_APP_CP),
    }
    output = []
    for line in data:
      output.append(line)
      if "<configuration>" in line:
        output.append(os.linesep)
        logger.info("Adding additional configuations ...")
        for (k, v) in name_value_to_add.items():
          output.append("  <property>" + os.linesep)
          output.append("    <name>{0}</name>".format(k) + os.linesep)
          output.append("    <value>{0}</value>".format(v) + os.linesep)
          output.append("  </property>" + os.linesep)
          output.append(os.linesep)
        pass
      pass

    with open(slider_conf_file, 'w') as modified:
      modified.writelines(output)
    pass

  def create_hdfs_folders(self):
    self.hdfs_work_root = self.config.get(SliderTesterConfig.TEST_SECTION, SliderTesterConfig.HDFS_ROOT_DIR)
    (ret, out, err) = self.run_os_command(" ".join([SliderTester.HDFS_LS, self.hdfs_work_root]), self.hdfs_user)
    if ret == 0:
      self.run_os_command(" ".join([SliderTester.HDFS_RMR, self.hdfs_work_root]), self.hdfs_user)

    self.run_os_command(" ".join([SliderTester.HDFS_MKDIR, self.hdfs_work_root]), self.hdfs_user)
    self.run_os_command(" ".join([SliderTester.HDFS_CHOWN, self.slider_user, self.hdfs_work_root]), self.hdfs_user)

    self.hdfs_user_root = os.path.join(
      self.config.get(SliderTesterConfig.TEST_SECTION, SliderTesterConfig.HDFS_USER_DIR), self.slider_user)

    (ret, out, err) = self.run_os_command(" ".join([SliderTester.HDFS_LS, self.hdfs_user_root]), self.hdfs_user)
    if ret != 0:
      self.run_os_command(" ".join([SliderTester.HDFS_MKDIR, self.hdfs_user_root]), self.hdfs_user)
      self.run_os_command(" ".join([SliderTester.HDFS_CHOWN, self.slider_user, self.hdfs_user_root]), self.hdfs_user)

    self.hdfs_agent_root = os.path.join(self.hdfs_work_root, "agent")
    self.hdfs_agent_conf_root = os.path.join(self.hdfs_agent_root, "conf")
    self.run_os_command(" ".join([SliderTester.HDFS_MKDIR, self.hdfs_agent_root]), self.slider_user)
    self.run_os_command(" ".join([SliderTester.HDFS_MKDIR, self.hdfs_agent_conf_root]), self.slider_user)

    self.run_os_command("hdfs dfs -copyFromLocal " + self.agent_tarball + " " + self.hdfs_agent_root, self.slider_user)
    self.run_os_command("hdfs dfs -copyFromLocal " + self.agent_conf + " " + self.hdfs_agent_conf_root,
                        self.slider_user)

    self.run_os_command("hdfs dfs -copyFromLocal " + self.app_package + " " + self.hdfs_work_root, self.slider_user)

    self.agent_tarball_hdfs = os.path.join(self.hdfs_agent_root, os.path.basename(self.agent_tarball))
    self.agent_conf_hdfs = os.path.join(self.hdfs_agent_conf_root, os.path.basename(self.agent_conf))
    self.app_package_hdfs = os.path.join(self.hdfs_work_root, os.path.basename(self.app_package))
    self.cluster_name = self.config.get(SliderTesterConfig.TEST_SECTION, SliderTesterConfig.CLUSTER_NAME)

    self.cluster_location = os.path.join(self.hdfs_user_root, ".slider/cluster", self.cluster_name)
    self.run_os_command(" ".join([SliderTester.HDFS_RMR, self.cluster_location]), self.hdfs_user)

  pass

  def create_cluster(self):
    self.fsdefault = self.config.get(SliderTesterConfig.CLUSTER_SECTION, SliderTesterConfig.FS_DEFAULTFS)

    cmd = SliderTester.SLIDER_CREATE_CMD_FORMAT.format(self.slider_exec,
                                                       self.cluster_name, self.fsdefault, self.agent_tarball_hdfs,
                                                       self.agent_conf_hdfs, self.app_conf, self.app_resources,
                                                       self.app_package_hdfs)

    (retcode, out, err) = self.run_slider_command(cmd, self.slider_user)
    if retcode != 0:
      raise Exception("Could not create cluster. Out: " + out + " Err: " + err)
    pass

  def verify_cluster(self):
    (retcode, out, err) = self.run_os_command(" ".join([SliderTester.HDFS_LS, self.cluster_location]), self.slider_user)
    if retcode != 0:
      raise Exception("Could not verify cluster. Out: " + out + " Err: " + err)
    pass

  def clean_up(self):
    (retcode, out, err) = self.run_slider_command(" ".join([self.slider_exec, "stop", self.cluster_name]),
                                                  self.slider_user)
    if retcode != 0:
      raise Exception("Could not clean cluster. Out: " + out + " Err: " + err)
    pass


  def findfile(self, filepattern, path):
    matches = []
    for root, dirnames, filenames in os.walk(path):
      for filename in fnmatch.filter(filenames, filepattern):
        matches.append(os.path.join(root, filename))
    if len(matches) > 0:
      return matches[0]
    else:
      return None
    pass


  def do_test_setup(self):
    self.cluster_type = self.config.get(SliderTesterConfig.TEST_SECTION, SliderTesterConfig.CLUSTER_TYPE)
    if self.cluster_type.lower() == "habse":
      self.hbase_setup()
      return

    if self.cluster_type.lower() == "storm":
      self.storm_setup()
      return

    if self.cluster_type.lower() == "storm":
      self.storm_setup()
      return
    pass


  def hbase_setup(self):
    (ret, out, err) = self.run_os_command(" ".join([SliderTester.HDFS_LS, "/app"]), self.hdfs_user)
    if ret != 0:
      self.run_os_command(" ".join([SliderTester.HDFS_MKDIR, "/app"]), self.hdfs_user)
    pass

    (ret, out, err) = self.run_os_command(" ".join([SliderTester.HDFS_LS, "/app/habse"]), self.hdfs_user)
    if ret == 0:
      self.run_os_command(" ".join([SliderTester.HDFS_RMR, "/app/hbase"]), self.hdfs_user)

    self.run_os_command(" ".join([SliderTester.HDFS_MKDIR, "/app/hbase"]), self.hdfs_user)
    self.run_os_command(" ".join([SliderTester.HDFS_CHOWN, self.slider_user, "/app/hbase"]), self.hdfs_user)
    pass

  pass


def resolve_config(configFile):
  try:
    config = SliderTesterConfig.config
    if os.path.exists(configFile):
      config.read(configFile)
      SliderTesterConfig.setConfig(config)
    else:
      raise Exception("No config found, use default")

  except Exception, err:
    logger.warn(err)
  return config


def main():
  parser = OptionParser()
  parser.add_option("-c", "--config", dest="config", help="SliderTester config file location", default=None)
  parser.add_option("-o", "--out", dest="outputfile", default="/tmp/slider-test.log", help="log file to store results.",
                    metavar="FILE")
  (options, args) = parser.parse_args()

  if os.path.isfile(options.outputfile):
    os.remove(options.outputfile)

  print "Logging at " + options.outputfile

  global logger
  logger = logging.getLogger('HostCleanup')
  handler = logging.FileHandler(options.outputfile)
  formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
  handler.setFormatter(formatter)
  logger.addHandler(handler)
  logging.basicConfig(level=logging.DEBUG)

  if options.config:
    resolve_config(options.config)

  s = SliderTester(SliderTesterConfig().getConfig())

  s.deploy_and_validate_slider()

  s.configure_slider_client()

  s.create_hdfs_folders()

  s.do_test_setup()

  s.create_cluster()

  time.sleep(120)
  s.verify_cluster()

  s.clean_up();

  # Copy slider and app package to TEST_ROOT/packages
  # Expand app package at TEST_ROOT/app
  # Expand slider package at TEST_ROOT/slider
  ## set jdk path
  ## check slider version

  # Create HDFS folders and ensure permission
  # Populate HDFS folders
  # Finalize resources and appConf json files
  # Call create
  # Validate existence of the app
  # Call start


if __name__ == "__main__":
  main()

