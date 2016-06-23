/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.client

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.ClientArgs
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.main.ServiceLauncherBaseTest
import org.apache.slider.core.persist.ConfTreeSerDeser
import org.apache.slider.core.persist.JsonSerDeser
import org.junit.Assert
import org.junit.Test

/**
 * Test bad argument handling
 */
//@CompileStatic
class TestReplaceTokens extends Assert {

  static final String PACKAGE = "/org/apache/slider/core/conf/examples/"
  static final String app_configuration = "app_configuration_tokenized.json"
  static final String app_configuration_processed =
    "app_configuration_processed.json"

  /**
   * help should print out help string and then succeed
   * @throws Throwable
   */
  @Test
  public void testHelp() throws Throwable {
    JsonSerDeser<ConfTree> confTreeJsonSerDeser =
      new JsonSerDeser<ConfTree>(ConfTree)
    def confTree = confTreeJsonSerDeser.fromResource(PACKAGE + app_configuration)
    SliderClient.replaceTokens(confTree, "testUser", "testCluster")
    assert confTree.global.get("site.fs.defaultFS") == "hdfs://testCluster:8020"
    assert confTree.global.get("site.fs.default.name") == "hdfs://testCluster:8020"
    assert confTree.global.get("site.hbase.user_name") == "testUser"
    assert confTree.global.get("site.hbase.another.user") == "testUser"


  }
  
}
