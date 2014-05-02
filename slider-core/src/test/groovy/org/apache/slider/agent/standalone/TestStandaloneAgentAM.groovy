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
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.api.ClusterNode
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderKeys
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.registry.info.ServiceInstanceData
import org.apache.slider.server.services.curator.CuratorServiceInstance
import org.apache.slider.server.services.curator.RegistryBinderService
import org.junit.Test

@CompileStatic
@Slf4j
class TestStandaloneAgentAM  extends AgentMiniClusterTestBase {
  @Test
  public void testStandaloneAgentAM() throws Throwable {


    describe "create a masterless AM then get the service and look it up via the AM"

    //launch fake master
    String clustername = "test_standalone_agent_am"
    createMiniCluster(clustername, configuration, 1, true)
    ServiceLauncher<SliderClient> launcher
    launcher = createMasterlessAM(clustername, 0, true, false)
    SliderClient client = launcher.service
    addToTeardown(client);

    ApplicationReport report = waitForClusterLive(client)
    logReport(report)
    List<ApplicationReport> apps = client.applications;

    //get some of its status
    dumpClusterStatus(client, "masterless application status")
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
    def serviceRegistryClient = client.YARNRegistryClient
    describe("list of all applications")
    logApplications(apps)
    describe("apps of user $username")
    List<ApplicationReport> userInstances = serviceRegistryClient.listInstances()
    logApplications(userInstances)
    assert userInstances.size() == 1
    describe("named app $clustername")
    ApplicationReport instance = serviceRegistryClient.findInstance(clustername)
    logReport(instance)
    assert instance != null

    //switch to the ZK-based registry

    describe "service registry names"
    RegistryBinderService<ServiceInstanceData> registry = client.registry
    def names = registry.queryForNames();
    dumpRegistryNames(names)
    describe "service registry instance IDs"

    def instanceIds = client.listRegistryInstanceIDs()

    log.info("number of instanceIds: ${instanceIds.size()}")
    instanceIds.each { String it -> log.info(it) }

    describe "service registry slider instances"
    List<CuratorServiceInstance<ServiceInstanceData>> instances = client.listRegistryInstances(
    )
    instances.each { CuratorServiceInstance<ServiceInstanceData> svc ->
      log.info svc.toString()
    }
    describe "end list service registry slider instances"

    describe "teardown of cluster instance #1"
    //now kill that cluster
    assert 0 == clusterActionFreeze(client, clustername)
    //list it & See if it is still there
    ApplicationReport oldInstance = serviceRegistryClient.findInstance(
        clustername)
    assert oldInstance != null
    assert oldInstance.yarnApplicationState >= YarnApplicationState.FINISHED

    //create another AM
    launcher = createMasterlessAM(clustername, 0, true, true)
    client = launcher.service
    ApplicationId i2AppID = client.applicationId

    //expect 2 in the list
    userInstances = serviceRegistryClient.listInstances()
    logApplications(userInstances)
    assert userInstances.size() == 2

    //but when we look up an instance, we get the new App ID
    ApplicationReport instance2 = serviceRegistryClient.findInstance(
        clustername)
    assert i2AppID == instance2.applicationId



    describe("attempting to create instance #3")
    //now try to create instance #3, and expect an in-use failure
    try {
      createMasterlessAM(clustername, 0, false, true)
      fail("expected a failure, got a masterless AM")
    } catch (SliderException e) {
      assertFailureClusterInUse(e);
    }

    describe("Stopping instance #2")

    //now stop that cluster
    assert 0 == clusterActionFreeze(client, clustername)

    logApplications(client.listSliderInstances(username))

    //verify it is down
    ApplicationReport reportFor = client.getApplicationReport(i2AppID)

    //downgrade this to a fail
//    Assume.assumeTrue(YarnApplicationState.FINISHED <= report.yarnApplicationState)
    assert YarnApplicationState.FINISHED <= reportFor.yarnApplicationState


    ApplicationReport instance3 = serviceRegistryClient.findInstance(
        clustername)
    assert instance3.yarnApplicationState >= YarnApplicationState.FINISHED


  }


}
