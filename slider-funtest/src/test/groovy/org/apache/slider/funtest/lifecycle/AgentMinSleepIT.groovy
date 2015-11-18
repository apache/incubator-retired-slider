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
public class AgentMinSleepIT extends AgentCommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {


  static String CLUSTER = "test-agent-sleep-100"

  static String TEST_RESOURCE = ResourcePaths.SLEEP_RESOURCES
  static String TEST_METADATA = ResourcePaths.SLEEP_META

  @Before
  public void prepareCluster() {
    setupCluster(CLUSTER)
  }

  @After
  public void destroyCluster() {
    cleanup(CLUSTER)
  }

  @Test
  public void testAgentMinSleepIt() throws Throwable {
    describe("Create a cluster using metainfo and resources only that executes sleep 100")
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();

    SliderShell shell = createSliderApplicationMinPkg(CLUSTER,
        TEST_METADATA,
        TEST_RESOURCE,
        null,
        [],
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
    expectLiveContainerCountReached(CLUSTER, "SLEEP_100", 1,
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
