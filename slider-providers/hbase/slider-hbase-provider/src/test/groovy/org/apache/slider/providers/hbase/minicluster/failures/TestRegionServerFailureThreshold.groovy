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

package org.apache.slider.providers.hbase.minicluster.failures

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.slider.api.ResourceKeys
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.api.ClusterDescription
import org.apache.slider.core.exceptions.BadClusterStateException
import org.apache.slider.core.exceptions.ErrorStrings
import org.apache.slider.common.params.Arguments
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.junit.Test

import static org.apache.slider.providers.hbase.HBaseKeys.ROLE_WORKER

/**
 * test that if a container is killed too many times,
 * the AM stays down
 */
@CompileStatic
@Slf4j

class TestRegionServerFailureThreshold extends HBaseMiniClusterTestBase {

  @Test
  public void testRegionServerFailureThreshold() throws Throwable {
    failureThresholdTestRun("", true, 2, 5)
  }

  /**
   * Sets the failure threshold then runs the #of kill attempts
   * @param testName
   * @param toKill
   * @param threshold
   * @param killAttempts
   */
  private void failureThresholdTestRun(
      String testName,
      boolean toKill,
      int threshold,
      int killAttempts) {
    String action = toKill ? "kill" : "stop"
    int regionServerCount = 1
    String clustername = createMiniCluster(testName, configuration, 1, 1, 1, true, true)
    describe(
        "Create a single region service HBase instance" +
        "then $action the RS $killAttempts times with a threshold of $threshold");

    //now launch the cluster
    def globalThreshold = threshold - 1
    ServiceLauncher<SliderClient> launcher = createHBaseCluster(
        clustername,
        regionServerCount,
        [
            Arguments.ARG_RES_COMP_OPT,
            ROLE_WORKER,
            ResourceKeys.CONTAINER_FAILURE_THRESHOLD,
            Integer.toString(threshold),

            Arguments.ARG_RESOURCE_OPT, 
            ResourceKeys.CONTAINER_FAILURE_THRESHOLD,
            Integer.toString(globalThreshold)
        ],
        true,
        true)
    SliderClient client = launcher.service
    addToTeardown(client);
    def aggregateConf = client.loadPersistedClusterDescription(clustername)
    log.info aggregateConf.toString()

    def resourceOperations = aggregateConf.resourceOperations
    def failureOptValue = resourceOperations.globalOptions.getMandatoryOptionInt(
        ResourceKeys.CONTAINER_FAILURE_THRESHOLD)
    assert globalThreshold == failureOptValue
    def workerThreshold = resourceOperations.getComponentOptInt(ROLE_WORKER,
        ResourceKeys.CONTAINER_FAILURE_THRESHOLD, 0)
    assert threshold == workerThreshold
    ClusterDescription status = client.getClusterDescription(clustername)

    ClusterStatus clustat = basicHBaseClusterStartupSequence(client)
    ClusterStatus hbaseStat
    try {
      for (restarts in 1..killAttempts) {
        status = waitForWorkerInstanceCount(
            client,
            regionServerCount,
            hbaseClusterStartupToLiveTime)
        //get the hbase status
/*
        hbaseStat = waitForHBaseRegionServerCount(
            client,
            clustername,
            regionServerCount,
            HBASE_CLUSTER_STARTUP_TO_LIVE_TIME)

        log.info("Initial cluster status : ${hbaseStatusToString(hbaseStat)}");
*/
        describe("running processes")
        lsJavaProcesses()
        describe("about to " + action + " servers")
        if (toKill) {
          killAllRegionServers()
        } else {
          stopAllRegionServers()
        }

        //sleep a bit
        sleep(toKill ? 15000 : 25000);

        describe("waiting for recovery")

        //and expect a recovery 
        if (restarts <= threshold) {

          def restartTime = 1000
          status = waitForWorkerInstanceCount(
              client,
              regionServerCount,
              restartTime)
          hbaseStat = waitForHBaseRegionServerCount(
              client,
              clustername,
              regionServerCount,
              restartTime)
        } else {
          //expect the cluster to have failed
          try {
            def finalCD = client.getClusterDescription(clustername)
            describe( "failure threshold ignored")
            dumpClusterDescription("expected the cluster to have failed", finalCD)
            describe "stopping cluster"
            maybeStopCluster(
                client,
                "",
                "stopping cluster that isn't failing correctly")
            
            
            fail("AM had not failed after $restarts worker kills")
            
          } catch (BadClusterStateException e) {
            assertExceptionDetails(e,
                SliderExitCodes.EXIT_BAD_STATE,
                ErrorStrings.E_APPLICATION_NOT_RUNNING)
            //success
            break;
          }
        }
      }
    } catch (BadClusterStateException e) {
      assertExceptionDetails(e,
          SliderExitCodes.EXIT_BAD_STATE, 
          ErrorStrings.E_APPLICATION_NOT_RUNNING)
    }
    ApplicationReport report = client.applicationReport
    log.info(report.diagnostics)
    assert report.finalApplicationStatus == FinalApplicationStatus.FAILED
    assert report.diagnostics.contains(ErrorStrings.E_UNSTABLE_CLUSTER)

  }


}
