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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.StatusKeys
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.client.SliderClient
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class TestClusterLifecycle extends HBaseCommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes {


  static String CLUSTER = "test_cluster_lifecycle"


  @Before
  public void prepareCluster() {
    setupCluster(CLUSTER)
  }

  @After
  public void destroyCluster() {
    teardown(CLUSTER)
  }

  @Test
  public void testClusterLifecycle() throws Throwable {

    describe "Walk a 0-role cluster through its lifecycle"


    def clusterpath = buildClusterPath(CLUSTER)
    assert !clusterFS.exists(clusterpath)


    Map<String, Integer> roleMap = createHBaseCluster(CLUSTER,
                                         0,
                                         0,
                                         [],
                                         [:])
    

    //at this point the cluster should exist.
    assertPathExists(clusterFS,"Cluster parent directory does not exist", clusterpath.parent)
    
    assertPathExists(clusterFS,"Cluster directory does not exist", clusterpath)

    // assert it exists on the command line
    exists(0, CLUSTER)

    //destroy will fail in use

    destroy(EXIT_APPLICATION_IN_USE, CLUSTER)

    //thaw will fail as cluster is in use
    thaw(EXIT_APPLICATION_IN_USE, CLUSTER)

    //it's still there
    exists(0, CLUSTER)

    //listing the cluster will succeed
    list(0, CLUSTER)

    //simple status
    status(0, CLUSTER)

    //now status to a temp file
    File jsonStatus = File.createTempFile("tempfile", ".json")
    try {
      slider(0,
           [
               SliderActions.ACTION_STATUS, CLUSTER,
               ARG_OUTPUT, jsonStatus.canonicalPath
           ])

      assert jsonStatus.exists()
      ClusterDescription cd = ClusterDescription.fromFile(jsonStatus)

      assert CLUSTER == cd.name

      log.info(cd.toJsonString())

      getConf(0, CLUSTER)

      //get a slider client against the cluster
      SliderClient sliderClient = bondToCluster(SLIDER_CONFIG, CLUSTER)
      ClusterDescription cd2 = sliderClient.getClusterDescription()
      assert CLUSTER == cd2.name

      log.info("Connected via Client {}", sliderClient.toString())

      //freeze
      slider(0, [
          SliderActions.ACTION_FREEZE, CLUSTER,
          ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
          ARG_MESSAGE, "freeze-in-testHBaseCreateCluster"
      ])

      //cluster exists if you don't want it to be live
      exists(0, CLUSTER, false)
      // condition returns false if it is required to be live
      exists(EXIT_FALSE, CLUSTER, true)


      // thaw then freeze the cluster

      slider(0,
           [
               SliderActions.ACTION_THAW, CLUSTER,
               ARG_WAIT, Integer.toString(THAW_WAIT_TIME),
           ])
      exists(0, CLUSTER)
      slider(0, [
          SliderActions.ACTION_FREEZE, CLUSTER,
          ARG_FORCE,
          ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
          ARG_MESSAGE, "forced-freeze-in-test"
      ])

      //cluster is no longer live
      exists(0, CLUSTER, false)
      
      // condition returns false if it is required to be live
      exists(EXIT_FALSE, CLUSTER, true)

      // thaw with a restart count set to enable restart

      describe "the kill/restart phase may fail if yarn.resourcemanager.am.max-attempts is too low"
      slider(0,
           [
               SliderActions.ACTION_THAW, CLUSTER,
               ARG_WAIT, Integer.toString(THAW_WAIT_TIME),
               ARG_DEFINE, SliderXmlConfKeys.KEY_AM_RESTART_LIMIT + "=3"
           ])



      ClusterDescription status = killAmAndWaitForRestart(sliderClient, CLUSTER)

      def restarted = status.getInfo(
          StatusKeys.INFO_CONTAINERS_AM_RESTART)
      assert restarted != null
      assert Integer.parseInt(restarted) == 0
      freeze(CLUSTER)

      destroy(0, CLUSTER)

      //cluster now missing
      exists(EXIT_UNKNOWN_INSTANCE, CLUSTER)

    } finally {
      jsonStatus.delete()
    }


  }


}
