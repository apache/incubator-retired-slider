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
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.ResourceKeys
import org.apache.slider.api.RoleKeys
import org.apache.slider.api.StatusKeys
import org.apache.slider.api.types.NodeEntryInformation
import org.apache.slider.api.types.NodeInformation
import org.apache.slider.api.types.NodeInformationList
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.core.launch.SerializedApplicationReport
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

  static String NAME = "test-aa-sleep"

  static String TEST_RESOURCE = ResourcePaths.SLEEP_RESOURCES
  static String TEST_METADATA = ResourcePaths.SLEEP_META
  public static final String SLEEP_100 = "SLEEP_100"
  public static final int SLEEP_LONG_PRIORITY = 3
  public static final String SLEEP_LONG_PRIORITY_S = Integer.toString(SLEEP_LONG_PRIORITY)

  public static final String SLEEP_LONG = "SLEEP_LONG"

  @Before
  public void prepareCluster() {
    setupCluster(NAME)
  }

  @After
  public void destroyCluster() {
    cleanup(NAME)
  }

  @Test
  public void testAASleepIt() throws Throwable {
    describe("Test Anti-Affinity Placement")

    describe "diagnostics"

    slider([ACTION_DIAGNOSTICS, ARG_VERBOSE, ARG_CLIENT, ARG_YARN, ARG_CREDENTIALS]).dumpOutput()

    describe "list nodes"

    def healthyNodes = listNodes("", true)
    def allNodes = listNodes("", false)

    def healthyNodeCount = healthyNodes.size()
    def allNodeCount = allNodes.size();
    def unhealthyNodeCount = allNodeCount - healthyNodeCount
    describe("Cluster nodes : ${healthyNodeCount}")
    def nodesPrettyJson = NodeInformationList.createSerializer().toJson(allNodes)
    log.info(nodesPrettyJson)
    if (unhealthyNodeCount > 0 ) {
      log.warn("$unhealthyNodeCount unhealthy nodes")
    }
    assert healthyNodeCount > 0

    File launchReportFile = createTempJsonFile();

    int desired = buildDesiredCount(healthyNodeCount)

    SliderShell shell = createSliderApplicationMinPkg(NAME,
      TEST_METADATA,
      TEST_RESOURCE,
      ResourcePaths.SLEEP_APPCONFIG,
      [
        ARG_RES_COMP_OPT, SLEEP_100, ResourceKeys.COMPONENT_INSTANCES, "0",
        ARG_RES_COMP_OPT, SLEEP_LONG, ResourceKeys.COMPONENT_INSTANCES, Integer.toString(desired),
        ARG_RES_COMP_OPT, SLEEP_LONG, ResourceKeys.COMPONENT_PRIORITY, SLEEP_LONG_PRIORITY_S
      ],
      launchReportFile)

    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)

    status(0, NAME)

    def expected = buildExpectedCount(desired)

    operations(NAME, loadAppReport(launchReportFile), desired, expected, healthyNodes)

    //stop
    freeze(0, NAME,
        [
            ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
            ARG_MESSAGE, "final-shutdown"
        ])

    assertInYarnState(appId, YarnApplicationState.FINISHED)
    destroy(0, NAME)

    //cluster now missing
    exists(EXIT_UNKNOWN_INSTANCE, NAME)
  }

  protected int buildExpectedCount(int desired) {
    desired - 1
  }

  protected int buildDesiredCount(int clustersize) {
    clustersize + 1
  }

  protected void operations(String name,
      SerializedApplicationReport appReport,
      int desired,
      int expected,
      NodeInformationList healthyNodes) {

    // now here await for the cluster size to grow: if it does, there's a problem
    // spin for a while and fail if the number ever goes above it.
    ClusterDescription cd = null
    (desired * 5).times {
      cd = assertContainersLive(NAME, SLEEP_LONG, expected)
      sleep(1000 * 10)
    }

    // here cluster is still 1 below expected
    def role = cd.getRole(SLEEP_LONG)
    assert "1" == role.get(RoleKeys.ROLE_PENDING_AA_INSTANCES)
    assert 1 == cd.statistics[SLEEP_LONG][StatusKeys.STATISTICS_CONTAINERS_ANTI_AFFINE_PENDING]
    // look through the nodes
    def currentNodes = listNodes(name)
    // assert that there is no entry of the sleep long priority on any node
    currentNodes.each { NodeInformation it ->
      def entry = it.entries[SLEEP_LONG]
      assert entry == null || entry.live <= 1
    }

    // now reduce the cluster size and assert that the size stays the same


  }
}
