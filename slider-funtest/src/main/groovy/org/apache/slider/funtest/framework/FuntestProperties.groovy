package org.apache.slider.funtest.framework

import groovy.transform.CompileStatic
import org.apache.slider.common.SliderXMLConfKeysForTesting
import org.apache.slider.common.SliderKeys

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

  String KEY_SLIDER_TEST_ZK_HOSTS = "slider.test.zkhosts";
  String DEFAULT_SLIDER_ZK_HOSTS = "localhost:2181";

  /**
   * Time to sleep waiting for the AM to come back up
   */
  String KEY_AM_RESTART_SLEEP_TIME = "slider.test.am.restart.time"
  int DEFAULT_AM_RESTART_SLEEP_TIME = 30000

  String KEY_SLIDER_FUNTESTS_ENABLED = "slider.funtest.enabled"

  String CLIENT_CONFIG_FILENAME = SliderKeys.CLIENT_RESOURCE
  
  String ENV_CONF_DIR = "SLIDER_CONF_DIR"
  String ENV_SLIDER_CLASSPATH_EXTRA = "SLIDER_CLASSPATH_EXTRA"

  String SCRIPT_NAME = "slider"
  static final String KEY_TEST_CONF_XML = "slider.test.conf.xml"
  static final String KEY_TEST_CONF_DIR = "slider.test.conf.dir"
}
