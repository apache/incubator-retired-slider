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

package org.apache.slider.providers.hbase.funtest

import org.apache.slider.funtest.categories.FunctionalTests
import org.apache.slider.funtest.framework.CommandTestBase
import org.apache.slider.funtest.framework.SliderShell
import org.apache.slider.common.params.Arguments
import org.apache.slider.providers.hbase.HBaseClientProvider
import org.apache.slider.providers.hbase.HBaseKeys
import org.junit.Before
import org.junit.BeforeClass

import static org.apache.slider.common.SliderXMLConfKeysForTesting.*

/**
 * Anything specific to HBase tests
 */
@org.junit.experimental.categories.Category(FunctionalTests)

abstract class HBaseCommandTestBase extends CommandTestBase {
  public static final boolean HBASE_TESTS_ENABLED
  public static final int HBASE_LAUNCH_WAIT_TIME
  
  static {
    HBASE_TESTS_ENABLED =
        SLIDER_CONFIG.getBoolean(KEY_TEST_HBASE_ENABLED, true)
    HBASE_LAUNCH_WAIT_TIME = SLIDER_CONFIG.getInt(
        KEY_TEST_HBASE_LAUNCH_TIME,
        DEFAULT_HBASE_LAUNCH_TIME_SECONDS)
  }

  @BeforeClass
  public static void extendClasspath() {
    addExtraJar(HBaseClientProvider)
  }

  @Before
  public void verifyPreconditions() {
    assumeHBaseTestsEnabled()
    getRequiredConfOption(SLIDER_CONFIG, KEY_TEST_HBASE_TAR)
    getRequiredConfOption(SLIDER_CONFIG, KEY_TEST_HBASE_APPCONF)
  }


  public void assumeHBaseTestsEnabled() {
    assumeFunctionalTestsEnabled()
    assume(HBASE_TESTS_ENABLED, "HBase tests disabled")
  }

  /**
   * Create an hbase cluster -this patches in the relevant attributes
   * for the HBase cluster
   * @param name name
   * @param masters no. of master nodes
   * @param workers no. of region servers
   * @param argsList list of arguments
   * @param clusterOps map of cluster options
   * @return the role map -for use when waiting for the cluster to go live
   */
  public Map<String, Integer> createHBaseCluster(
      String name,
      int masters,
      int workers,
      List<String> argsList,
      Map<String, String> clusterOps) {
    Map<String, Integer> roleMap = [
        (HBaseKeys.ROLE_MASTER): masters,
        (HBaseKeys.ROLE_WORKER): workers
    ]

    argsList << Arguments.ARG_IMAGE <<
    SLIDER_CONFIG.getTrimmed(KEY_TEST_HBASE_TAR)

    argsList << Arguments.ARG_CONFDIR <<
    SLIDER_CONFIG.getTrimmed(KEY_TEST_HBASE_APPCONF)

    argsList << Arguments.ARG_PROVIDER << HBaseKeys.PROVIDER_HBASE

    SliderShell shell = createSliderApplication(
        name,
        roleMap,
        argsList,
        true,
        clusterOps
    )
    return roleMap
  }


  public String getDescription() {
    return "Create an HBase"
  }

  public int getWorkerPortAssignment() {
    return 0
  }

  public int getMasterPortAssignment() {
    return 0
  }

}
