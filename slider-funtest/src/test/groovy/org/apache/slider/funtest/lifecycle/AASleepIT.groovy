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

package org.apache.slider.funtest.lifecycle

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.ResourcePaths
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class AASleepIT extends AgentCommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {


  static String CLUSTER = "test-aa-sleep"

  static String TEST_RESOURCE = ResourcePaths.SLEEP_RESOURCES
  static String TEST_METADATA = ResourcePaths.SLEEP_META
  public static final String SLEEP_100 = "SLEEP_100"
  public static final String SLEEP_LONG = "SLEEP_LONG"

  @Before
  public void prepareCluster() {
    setupCluster(CLUSTER)
  }

  @After
  public void destroyCluster() {
    cleanup(CLUSTER)
  }

  @Test
  public void testAASleepIt() throws Throwable {
    describe("Test Anti-Affinity Placement")
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();

    // TODO: Determine YARN cluster size via an API/CLI Call, maybe use labels too?
    int yarnClusterSize = 1;
    int desired = yarnClusterSize + 1
    SliderShell shell = createSliderApplicationMinPkg(CLUSTER,
        TEST_METADATA,
        TEST_RESOURCE,
        null,
        [ARG_RES_COMP_OPT, SLEEP_LONG, "$desired"],
        launchReportFile)

    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)

    //at this point the cluster should exist.
    assertPathExists(
        clusterFS,
        "Cluster parent directory does not exist",
        clusterpath.parent)

    assertPathExists(clusterFS, "Cluster directory does not exist", clusterpath)

    status(0, CLUSTER)
    expectLiveContainerCountReached(CLUSTER, SLEEP_100, desired -1 ,
        CONTAINER_LAUNCH_TIMEOUT)

    // sleep for some manual test
    describe("You may quickly perform manual tests against the application instance $CLUSTER")
    sleep(1000 * 30)

    //stop
    freeze(0, CLUSTER,
        [
            ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
            ARG_MESSAGE, "final-shutdown"
        ])

    assertInYarnState(appId, YarnApplicationState.FINISHED)
    destroy(0, CLUSTER)

    //cluster now missing
    exists(EXIT_UNKNOWN_INSTANCE, CLUSTER)
  }
}
