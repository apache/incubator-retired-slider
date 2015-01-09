/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.funtest.framework

import groovy.transform.CompileStatic
import org.apache.slider.common.SliderXMLConfKeysForTesting
import org.apache.slider.common.SliderKeys

/**
 * Properties unique to the functional tests
 */
@CompileStatic
public interface FuntestProperties extends SliderXMLConfKeysForTesting {

  /**
   * Maven Property of location of slider conf dir: {@value}
   */
  String SLIDER_CONF_DIR_PROP = "slider.conf.dir"

  /**
   * Maven Property of location of slider binary image dir: {@value}
   */
  String SLIDER_BIN_DIR_PROP = "slider.bin.dir"

  String KEY_SLIDER_TEST_NUM_WORKERS = "slider.test.cluster.size"
  int DEFAULT_SLIDER_NUM_WORKERS = 1

  String DEFAULT_SLIDER_ZK_HOSTS = "localhost:2181";

  /**
   * Time to sleep waiting for the AM to come back up
   */
  String KEY_AM_RESTART_SLEEP_TIME = "slider.test.am.restart.time"
  int DEFAULT_AM_RESTART_SLEEP_TIME = 30000

  String CLIENT_CONFIG_FILENAME = SliderKeys.SLIDER_CLIENT_XML
  
  String ENV_SLIDER_CONF_DIR = "SLIDER_CONF_DIR"
  String ENV_HADOOP_CONF_DIR = "HADOOP_CONF_DIR"
  String ENV_SLIDER_CLASSPATH_EXTRA = "SLIDER_CLASSPATH_EXTRA"
  
  String SCRIPT_NAME = "slider"
  String KEY_TEST_CONF_XML = "slider.test.conf.xml"
  String KEY_TEST_CONF_DIR = "slider.test.conf.dir"
  String BIN_SLIDER = "bin/slider"
  String BIN_SLIDER_PYTHON = "bin/slider.py"
  String AGENT_INI = "agent.ini"
  String AGENT_INI_IN_SLIDER_TAR = "agent/conf/" + AGENT_INI

  String AGENT_TAR_FILENAME = "slider-agent.tar.gz"
  String AGENT_SLIDER_GZ_IN_SLIDER_TAR = "agent/" + AGENT_TAR_FILENAME


  String KEY_TEST_INSTANCE_LAUNCH_TIME =
          "slider.test.instance.launch.wait.seconds";
  int DEFAULT_INSTANCE_LAUNCH_TIME_SECONDS = 60 * 3;

  String ENV_PREFIX = "env."
  String CORE_SITE_XML = "core-site.xml"
  String HDFS_SITE_XML = "hdfs-site.xml"
  String YARN_SITE_XML = "yarn-site.xml"

}
