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

package org.apache.slider.agent.rest

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.WebResource
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.api.StateValues
import org.apache.slider.api.types.ComponentInformation
import org.apache.slider.api.types.ContainerInformation
import org.apache.slider.client.rest.SliderApplicationAPI
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.core.conf.ConfTreeOperations
import org.apache.slider.server.appmaster.web.rest.application.ApplicationResource
import org.apache.slider.test.SliderTestUtils

import javax.ws.rs.core.MediaType

import static org.apache.slider.api.ResourceKeys.COMPONENT_INSTANCES
import static org.apache.slider.api.StatusKeys.*
import static org.apache.slider.common.SliderKeys.COMPONENT_AM
import static org.apache.slider.server.appmaster.web.rest.RestPaths.*

/**
 * This class contains parts of tests that can be run
 * against a deployed AM: local or remote.
 * It uses Jersey ... and must be passed a client that is either secure
 * or not
 * 
 */
@CompileStatic
@Slf4j
class SliderRestClientTestDelegates extends SliderTestUtils {
  public static final String TEST_GLOBAL_OPTION = "test.global.option"
  public static final String TEST_GLOBAL_OPTION_PRESENT = "present"

  final String appmaster;
  final String application;
  final Client jersey;
  final SliderApplicationAPI appAPI;


  SliderRestClientTestDelegates(String appmaster, Client jersey) {
    this.jersey = jersey
    this.appmaster = appmaster
    application = appendToURL(appmaster, SLIDER_PATH_APPLICATION)
    WebResource amResource = jersey.resource(appmaster)
    amResource.type(MediaType.APPLICATION_JSON)
    appAPI = new SliderApplicationAPI(jersey, amResource)
  }


  public void testGetDesiredModel() throws Throwable {
      appAPI.getDesiredModel()  
      appAPI.getDesiredAppconf()  
      appAPI.getDesiredYarnResources()  
  }

  public void testGetResolvedModel() throws Throwable {
      appAPI.getResolvedModel()  
      appAPI.getResolvedAppconf()  
      appAPI.getResolvedYarnResources()  
  }

  
  public void testLiveResources() throws Throwable {
    describe "Live Resources"

    ConfTreeOperations tree = appAPI.getLiveYarnResources()

    log.info tree.toString()
    def liveAM = tree.getComponent(COMPONENT_AM)
    def desiredInstances = liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES);
    assert desiredInstances ==
           liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES_ACTUAL)

    assert 1 == liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES_STARTED)
    assert 0 == liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES_REQUESTING)
    assert 0 == liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES_FAILED)
    assert 0 == liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES_COMPLETED)
    assert 0 == liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES_RELEASING)
  }

  public void testLiveContainers() throws Throwable {
    describe "Application REST ${LIVE_CONTAINERS}"

    Map<String, ContainerInformation> containers = appAPI.enumContainers()
    assert containers.size() == 1
    log.info "${containers}"
    ContainerInformation amContainerInfo =
        (ContainerInformation) containers.values()[0]
    assert amContainerInfo.containerId

    def amContainerId = amContainerInfo.containerId
    assert containers[amContainerId]

    assert amContainerInfo.component == COMPONENT_AM
    assert amContainerInfo.createTime > 0
    assert amContainerInfo.exitCode == null
    assert amContainerInfo.output == null
    assert amContainerInfo.released == null
    assert amContainerInfo.state == StateValues.STATE_LIVE

    describe "containers"

    ContainerInformation amContainerInfo2 =
        appAPI.getContainer(amContainerId)
    assert amContainerInfo2.containerId == amContainerId

    // fetch missing
    try {
      def result = appAPI.getContainer("unknown")
      fail("expected an error, got $result")
    } catch (FileNotFoundException e) {
      // expected
    }


    describe "components"

    Map<String, ComponentInformation> components =
        appAPI.enumComponents()

    // two components
    assert components.size() >= 1
    log.info "${components}"

    ComponentInformation amComponentInfo =
        (ComponentInformation) components[COMPONENT_AM]

    ComponentInformation amFullInfo = appAPI.getComponent(COMPONENT_AM) 

    assert amFullInfo.containers.size() == 1
    assert amFullInfo.containers[0] == amContainerId

  }

 
  /**
   * Test the rest model. For this to work the cluster has to be configured
   * with the global option
   * @param appmaster
   */
  public void testRESTModel() {
    describe "model"

    assertPathServesList(appmaster,
        MODEL,
        ApplicationResource.MODEL_ENTRIES)

    def unresolvedConf = appAPI.getDesiredModel() 
//    log.info "Unresolved \n$unresolvedConf"
    def unresolvedAppConf = unresolvedConf.appConfOperations

    def sam = "slider-appmaster"
    assert unresolvedAppConf.getComponentOpt(sam,
        TEST_GLOBAL_OPTION, "") == ""
    def resolvedConf = appAPI.getResolvedModel() 
    assert resolvedConf.appConfOperations.getComponentOpt(
        sam, TEST_GLOBAL_OPTION, "") == TEST_GLOBAL_OPTION_PRESENT

    def unresolved = fetchTypeList(ConfTree, appmaster,
        [MODEL_DESIRED_APPCONF, MODEL_DESIRED_RESOURCES])
    assert unresolved[MODEL_DESIRED_APPCONF].components[sam]
    [TEST_GLOBAL_OPTION] == null


    
    def resolvedAppconf = appAPI.getResolvedAppconf() 
    assert resolvedAppconf.
               components[sam][TEST_GLOBAL_OPTION] == TEST_GLOBAL_OPTION_PRESENT
  }

  public void testPing() {
    // GET
    describe "pinging"
    
    appAPI.ping("hello")
  }


  /**
   * Test the stop command.
   * Important: once executed, the AM is no longer there.
   * This must be the last test in the sequence.
   */
/*

  public void testStop() {
    String target = appendToURL(appmaster, SLIDER_PATH_APPLICATION, ACTION_STOP)
    describe "Stop URL $target"
    URL targetUrl = new URL(target)
    def outcome = connectionOperations.execHttpOperation(
        HttpVerb.POST,
        targetUrl,
        new byte[0],
        MediaType.TEXT_PLAIN)
    log.info "Stopped: $outcome"

    // await the shutdown
    sleep(1000)
    
    // now a ping is expected to fail
    String ping = appendToURL(appmaster, SLIDER_PATH_APPLICATION, ACTION_PING)
    URL pingUrl = new URL(ping)

    repeatUntilSuccess("probe for missing registry entry",
        this.&probePingFailing, 30000, 500,
        [url: ping],
        true,
        "AM failed to shut down") {
      def pinged = jFetchType(ACTION_PING + "?body=hello",
          PingResource
      )
      fail("AM didn't shut down; Ping GET= $pinged")
    }
    
  }
*/

  public void testSuiteGetOperations() {

    testGetDesiredModel()
    testGetResolvedModel()
    testLiveResources()
    testLiveContainers();
    testRESTModel()
  }

  public void testSuiteComplexVerbs() {
    testPing();
  }
}
