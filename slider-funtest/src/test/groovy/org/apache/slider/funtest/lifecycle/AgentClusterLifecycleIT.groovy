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
import org.apache.slider.api.StatusKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class AgentClusterLifecycleIT extends AgentCommandTestBase
  implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {


  static String CLUSTER = "test-agent-cluster-lifecycle"

  static String APP_RESOURCE2 = "../slider-core/src/test/app_packages/test_command_log/resources_no_role.json"


  @Before
  public void prepareCluster() {
    setupCluster(CLUSTER)
  }

  @After
  public void destroyCluster() {
    cleanup(CLUSTER)
  }

  @Test
  public void testAgentClusterLifecycle() throws Throwable {

    describe "Walk a 0-role cluster through its lifecycle"

    // sanity check to verify the config is correct
    assert clusterFS.uri.scheme != "file"

    def clusterpath = buildClusterPath(CLUSTER)
    assert !clusterFS.exists(clusterpath)

    File launchReportFile = createTempJsonFile();
    SliderShell shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [],
        launchReportFile)

    logShell(shell)
    assert launchReportFile.exists()
    assert launchReportFile.size() > 0
    def launchReport = maybeLoadAppReport(launchReportFile)
    assert launchReport;

    def appId = ensureYarnApplicationIsUp(launchReportFile)

    //at this point the cluster should exist.
    assertPathExists(clusterFS, "Cluster parent directory does not exist", clusterpath.parent)

    assertPathExists(clusterFS, "Cluster directory does not exist", clusterpath)

    // assert it exists on the command line
    exists(0, CLUSTER)

    //destroy will fail in use
    destroy(EXIT_APPLICATION_IN_USE, CLUSTER)

    //start will fail as cluster is in use
    thaw(EXIT_APPLICATION_IN_USE, CLUSTER)

    //it's still there
    exists(0, CLUSTER)

    //listing the cluster will succeed
    list(0, [CLUSTER])

    list(0, [""])
    list(0, [CLUSTER, ARG_LIVE])
    list(0, [CLUSTER, ARG_STATE, "running"])
    list(0, [ARG_LIVE])
    list(0, [ARG_STATE, "running"])

    //simple status
    status(0, CLUSTER)

    //now status to a temp file
    File jsonStatus = File.createTempFile("tempfile", ".json")
    try {
      slider(0,
          [
              ACTION_STATUS, CLUSTER,
              ARG_OUTPUT, jsonStatus.canonicalPath
          ])

      assert jsonStatus.exists()
      ClusterDescription cd = ClusterDescription.fromFile(jsonStatus)

      assert CLUSTER == cd.name

      log.info(cd.toJsonString())

      //get a slider client against the cluster
      SliderClient sliderClient = bondToCluster(SLIDER_CONFIG, CLUSTER)
      ClusterDescription cd2 = sliderClient.clusterDescription
      assert CLUSTER == cd2.name

      log.info("Connected via Client {}", sliderClient.toString())

      //stop
      freeze(0, CLUSTER, [
          ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
          ARG_MESSAGE, "freeze-in-test-cluster-lifecycle"
      ])
      describe " >>> Cluster is now frozen."
      
      // should be in finished state, as this was a clean shutdown
      assertInYarnState(appId, YarnApplicationState.FINISHED)

      //cluster exists if you don't want it to be live
      exists(0, CLUSTER, false)
      //condition returns false if it is required to be live
      exists(EXIT_FALSE, CLUSTER, true)

      // list cluster state
      // it is known about
      list( 0, [CLUSTER])
      // it has finished
      list( 0, [CLUSTER, ARG_STATE, "FINISHED"])
      // it is not live
      list(-1, [CLUSTER, ARG_LIVE])
      // it is not running
      list(-1, [CLUSTER, ARG_STATE, "running"])

      // therefore, there is at least one cluster
      // that has finished
      list( 0, [ARG_STATE, "FINISHED"])

      def thawReport = createTempJsonFile()
      //start then stop the cluster
      thaw(CLUSTER,
          [
              ARG_WAIT, Integer.toString(THAW_WAIT_TIME),
              ARG_OUTPUT, thawReport.absolutePath,
          ])
      def thawedAppId = ensureYarnApplicationIsUp(thawReport)
     

      assertAppRunning(thawedAppId)

      exists(0, CLUSTER)
      describe " >>> Cluster is now thawed."
      list(0, [CLUSTER, ARG_LIVE])
      list(0, [CLUSTER, ARG_STATE, "running"])
      list(0, [ARG_LIVE])
      list(0, [ARG_STATE, "running"])
      list(0, [CLUSTER, ARG_STATE, "FINISHED"])

      freeze(0, CLUSTER,
          [
              ARG_FORCE,
              ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
              ARG_MESSAGE, "forced-freeze-in-test"
          ])

      describe " >>> Cluster is now force frozen - 2nd time."

      // new instance should be in killed state
      assertInYarnState(thawedAppId, YarnApplicationState.KILLED)
      //cluster is no longer live
      exists(0, CLUSTER, false)

      //condition returns false if it is required to be live
      exists(EXIT_FALSE, CLUSTER, true)

      //start with a restart count set to enable restart
      describe "the kill/restart phase may fail if yarn.resourcemanager.am.max-attempts is too low"

      def thawReport2 = createTempJsonFile()
      //start then stop the cluster
      thaw(CLUSTER,
          [
              ARG_WAIT, Integer.toString(THAW_WAIT_TIME),
              ARG_DEFINE, SliderXmlConfKeys.KEY_AM_RESTART_LIMIT + "=3",
              ARG_OUTPUT, thawReport2.absolutePath
          ])
      def thawedAppId2 = ensureYarnApplicationIsUp(thawReport2)
      describe " >>> Cluster is now thawed - 2nd time."


      describe " >>> Kill AM and wait for restart."
      ClusterDescription status = killAmAndWaitForRestart(sliderClient, CLUSTER)

      assertAppRunning(thawedAppId2)
      def restarted = status.getInfo(
          StatusKeys.INFO_CONTAINERS_AM_RESTART)
      assert restarted != null
      assert Integer.parseInt(restarted) == 0
      freeze(0, CLUSTER,
          [
              ARG_FORCE,
              ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
              ARG_MESSAGE, "final-shutdown"
          ])

      list(0, [CLUSTER, "--verbose", "--state", "FINISHED"]).dumpOutput()
      
      destroy(0, CLUSTER)

      //cluster now missing
      exists(EXIT_UNKNOWN_INSTANCE, CLUSTER)
      list(0, [])
      list(EXIT_UNKNOWN_INSTANCE, [CLUSTER])

    } finally {
      jsonStatus.delete()
    }
  }
}
