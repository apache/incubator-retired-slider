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

package org.apache.slider.agent.standalone

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.api.ClusterNode
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.ActionRegistryArgs
import org.apache.slider.common.params.ActionDestroyArgs
import org.apache.slider.common.tools.Duration
import org.apache.slider.core.build.InstanceBuilder
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.launch.LaunchedApplication
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.persist.LockAcquireFailedException
import org.junit.After
import org.junit.Test

@CompileStatic
@Slf4j
class TestStandaloneAgentAM  extends AgentMiniClusterTestBase {

  public static final String PORT_RANGE = "60000-60010"

  @After
  void fixclientname() {
    sliderClientClassName = DEFAULT_SLIDER_CLIENT
  }
  
  @Test
  public void testStandaloneAgentAM() throws Throwable {

    describe "create a standalone AM then perform actions on it"
    sliderClientClassName = ExtendedSliderClient.name
    //launch fake master
    String clustername = createMiniCluster("", configuration, 1, true)


    describe("Launching AM")
    ServiceLauncher<SliderClient> launcher =
        createStandaloneAM(clustername, true, false)
    SliderClient client = launcher.service
    addToTeardown(client);

    ApplicationReport report = waitForClusterLive(client)
    URI uri = new URI(report.originalTrackingUrl)
    assert uri.port in 60000..60010
    assert report.rpcPort in 60000..60010

    logReport(report)
    List<ApplicationReport> apps = client.applications;

    //get some of its status
    dumpClusterStatus(client, "standalone application status")
    List<ClusterNode> clusterNodes = client.listClusterNodesInRole(
        SliderKeys.COMPONENT_AM)
    assert clusterNodes.size() == 1

    ClusterNode masterNode = clusterNodes[0]
    log.info("Master node = ${masterNode}");

    List<ClusterNode> nodes
    String[] uuids = client.listNodeUUIDsByRole(SliderKeys.COMPONENT_AM)
    assert uuids.length == 1;
    nodes = client.listClusterNodes(uuids);
    assert nodes.size() == 1;
    describe "AM Node UUID=${uuids[0]}"

    nodes = listNodesInRole(client, SliderKeys.COMPONENT_AM)
    assert nodes.size() == 1;
    nodes = listNodesInRole(client, "")
    assert nodes.size() == 1;
    assert nodes[0].role == SliderKeys.COMPONENT_AM


    String username = client.username
    def serviceRegistryClient = client.yarnAppListClient
    describe("list of all applications")
    logApplications(apps)
    assert 1 == apps.size()
    def appReport = apps.head()
    assert appReport.host
    assert appReport.host.contains(".")
    assert appReport.originalTrackingUrl.contains(appReport.host)

    describe("apps of user $username")
    List<ApplicationReport> userInstances = serviceRegistryClient.listInstances()
    logApplications(userInstances)
    assert userInstances.size() == 1
    describe("named app $clustername")
    ApplicationReport instance = serviceRegistryClient.findInstance(clustername)
    logReport(instance)
    assert instance != null

    //switch to the slider ZK-based registry
    describe "service registry instance IDs"

    // iterate waiting for registry to come up
    List<String> instanceIds = []
    Duration duration = new Duration(10000)
    duration.start()

    while (!duration.limitExceeded && instanceIds.size() < 1) {
      instanceIds = client.listRegisteredSliderInstances()
      if (!instanceIds.size()) {
        sleep(500)
      }
    }

    log.info("number of instanceIds: ${instanceIds.size()}")
    assert instanceIds.size() >= 1
    instanceIds.each { String it -> log.info(it) }

    describe "Yarn registry"
    def yarnRegistry = client.registryOperations
    
    describe "teardown of cluster instance #1"
    //now kill that cluster
    assert 0 == clusterActionFreeze(client, clustername)
    //list it & See if it is still there
    ApplicationReport oldInstance = serviceRegistryClient.findInstance(
        clustername)
    assert oldInstance != null
    assert oldInstance.yarnApplicationState >= YarnApplicationState.FINISHED

    sleep(5000)
    //create another AM
    def newcluster = clustername + "2"
    launcher = createStandaloneAM(newcluster, true, true)
    client = launcher.service
    ApplicationId i2AppID = client.applicationId

    //expect 2 in the list
    userInstances = serviceRegistryClient.listInstances()
    logApplications(userInstances)
    assert userInstances.size() == 2

    //but when we look up an instance, we get the new App ID
    ApplicationReport instance2 = serviceRegistryClient.findInstance(
        newcluster)
    assert i2AppID == instance2.applicationId

    describe("attempting to create instance #3")
    //now try to create instance #3, and expect an in-use failure
    try {
      createStandaloneAM(newcluster, false, true)
      fail("expected a failure, got a standalone AM")
    } catch (SliderException e) {
      assertFailureClusterInUse(e);
    }

    // do a quick registry listing here expecting a usage failure.
    ActionRegistryArgs registryArgs = new ActionRegistryArgs()
    registryArgs.name = clustername;
    def exitCode = client.actionRegistry(registryArgs)
    assert LauncherExitCodes.EXIT_USAGE == exitCode 

    describe("Stopping instance #2")
    //now stop that cluster
    assert 0 == clusterActionFreeze(client, newcluster)

    logApplications(client.listSliderInstances(username))

    //verify it is down
    ApplicationReport reportFor = client.getApplicationReport(i2AppID)

    //downgrade this to a fail
//    Assume.assumeTrue(YarnApplicationState.FINISHED <= report.yarnApplicationState)
    assert YarnApplicationState.FINISHED <= reportFor.yarnApplicationState


    ApplicationReport instance3 = serviceRegistryClient.findInstance(
        newcluster)
    assert instance3.yarnApplicationState >= YarnApplicationState.FINISHED

    // destroy it
    ActionDestroyArgs args = new ActionDestroyArgs()
    args.force = true;
    client.actionDestroy(newcluster, args)
    
  }


  static class ExtendedSliderClient extends SliderClient {
    @Override
    protected void persistInstanceDefinition(boolean overwrite,
                                             Path appconfdir,
                                             InstanceBuilder builder)
    throws IOException, SliderException, LockAcquireFailedException {
      AggregateConf conf = builder.instanceDescription
      conf.appConfOperations.
          globalOptions[SliderKeys.KEY_ALLOWED_PORT_RANGE]= PORT_RANGE
      super.persistInstanceDefinition(overwrite, appconfdir, builder)
    }

    @Override
    LaunchedApplication launchApplication(String clustername,
                                          Path clusterDirectory,
                                          AggregateConf instanceDefinition,
                                          boolean debugAM)
    throws YarnException, IOException {
      instanceDefinition.appConfOperations.
          globalOptions[SliderKeys.KEY_ALLOWED_PORT_RANGE] =PORT_RANGE
      return super.launchApplication(clustername, clusterDirectory, instanceDefinition, debugAM)
    }
  }
}
