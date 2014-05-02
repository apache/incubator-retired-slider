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

import ConfigParser
import StringIO
import os

config = ConfigParser.RawConfigParser()
content = """

[slider]
package=/pkgs/slider-0.22.0-all.tar.gz
jdk.path=/usr/jdk64/jdk1.7.0_45/bin

[app]
package=/pkgs/hbase_v096.tar

[cluster]
yarn.application.classpath=/etc/hadoop/conf,/usr/lib/hadoop/*,/usr/lib/hadoop/lib/*,/usr/lib/hadoop-hdfs/*,/usr/lib/hadoop-hdfs/lib/*,/usr/lib/hadoop-yarn/*,/usr/lib/hadoop-yarn/lib/*,/usr/lib/hadoop-mapreduce/*,/usr/lib/hadoop-mapreduce/lib/*
slider.zookeeper.quorum=c6401.ambari.apache.org:2181
yarn.resourcemanager.address=c6401.ambari.apache.org:8050
yarn.resourcemanager.scheduler.address=c6401.ambari.apache.org:8030
fs.defaultFS=hdfs://c6401.ambari.apache.org:8020

[test]
app.user=yarn
hdfs.root.user=hdfs
hdfs.root.dir=/slidertst
hdfs.user.dir=/user
test.root=/test
cluster.name=tst1
cluster.type=hbase

[agent]
"""
s = StringIO.StringIO(content)
config.readfp(s)


class SliderTesterConfig:

  SLIDER_SECTION = "slider"
  APP_SECTION = "app"
  CLUSTER_SECTION = "cluster"
  TEST_SECTION = "test"
  AGENT_SECTION = "agent"

  PACKAGE = "package"
  SLIDER_ZK_QUORUM = "slider.zookeeper.quorum"
  YARN_RESOURCEMANAGER_ADDRESS = "yarn.resourcemanager.address"
  YARN_RESOURCEMANAGER_SCHEDULER_ADDRESS = "yarn.resourcemanager.scheduler.address"
  FS_DEFAULTFS = "fs.defaultFS"
  YARN_APP_CP="yarn.application.classpath"

  APP_USER = "app.user"
  HDFS_ROOT_USER = "hdfs.root.user"
  HDFS_ROOT_DIR = "hdfs.root.dir"
  HDFS_USER_DIR = "hdfs.user.dir"
  TEST_ROOT = "test.root"
  CLUSTER_NAME = "cluster.name"
  JDK_PATH="jdk.path"
  CLUSTER_TYPE="cluster.type"

  def getConfig(self):
    global config
    return config


def setConfig(customConfig):
  global config
  config = customConfig


def main():
  print config


if __name__ == "__main__":
  main()

