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
import org.apache.slider.api.SliderApplicationApi
import org.apache.slider.api.StateValues
import org.apache.slider.api.types.ComponentInformation
import org.apache.slider.api.types.ContainerInformation
import org.apache.slider.client.rest.SliderApplicationApiRestClient
import org.apache.slider.core.conf.ConfTreeOperations
import org.apache.slider.test.Outcome

import javax.ws.rs.core.MediaType

import static org.apache.slider.api.ResourceKeys.COMPONENT_INSTANCES
import static org.apache.slider.api.StatusKeys.*
import static org.apache.slider.common.SliderKeys.COMPONENT_AM
import static org.apache.slider.server.appmaster.web.rest.RestPaths.LIVE_CONTAINERS
import static org.apache.slider.server.appmaster.web.rest.RestPaths.SLIDER_PATH_APPLICATION

/**
 * Uses the Slider Application API for the tests.
 * {@link SliderApplicationApiRestClient}
 */
@CompileStatic
@Slf4j
class RestAPIClientTestDelegates extends AbstractRestTestDelegate {

 
  final SliderApplicationApi appAPI;


  RestAPIClientTestDelegates(String appmaster, Client jersey,
      boolean enableComplexVerbs = true) {
    super(enableComplexVerbs)
    WebResource amResource = jersey.resource(appmaster)
    amResource.type(MediaType.APPLICATION_JSON)
    def appResource = amResource.path(SLIDER_PATH_APPLICATION);
    appAPI = new SliderApplicationApiRestClient(jersey, appResource)
  }


  public void testGetDesiredModel() throws Throwable {
      appAPI.getDesiredModel()  
      appAPI.getDesiredAppconf()  
      appAPI.getDesiredResources()  
  }

  public void testGetResolvedModel() throws Throwable {
      appAPI.getResolvedModel()  
      appAPI.getResolvedAppconf()  
      appAPI.getResolvedResources()  
  }

  
  public void testLiveResources() throws Throwable {
    describe "Live Resources"

    ConfTreeOperations tree = appAPI.getLiveResources()

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

    def unresolvedConf = appAPI.getDesiredModel() 
//    log.info "Unresolved \n$unresolvedConf"
    def unresolvedAppConf = unresolvedConf.appConfOperations

    def sam = "slider-appmaster"
    assert unresolvedAppConf.getComponentOpt(sam,
        TEST_GLOBAL_OPTION, "") == ""
    def resolvedConf = appAPI.getResolvedModel() 
    assert resolvedConf.appConfOperations.getComponentOpt(
        sam, TEST_GLOBAL_OPTION, "") == TEST_GLOBAL_OPTION_PRESENT

    def unresolved = appAPI.getDesiredModel() 
    assert null == 
           unresolved.resources.components[sam][TEST_GLOBAL_OPTION] 
    
    
    def resolvedAppconf = appAPI.getResolvedAppconf() 
    assert TEST_GLOBAL_OPTION_PRESENT==
           resolvedAppconf. components[sam][TEST_GLOBAL_OPTION]
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

  public void testStop() {

    appAPI.stop("stop")

    repeatUntilSuccess("probe for liveness",
        this.&probeForLivenessFailing, STOP_WAIT_TIME, STOP_PROBE_INTERVAL,
        [:],
        true,
        "AM failed to shut down") {
      appAPI.getApplicationLiveness()
    }
    
  }

  /**
   * Probe that spins until the liveness query fails
   * @param args argument map
   * @return the outcome
   */
  Outcome probeForLivenessFailing(Map args) {
    try {
      appAPI.getApplicationLiveness()
      return Outcome.Retry
    } catch (IOException e) {
      // expected
      return Outcome.Success
    }
  }

  public void testSuiteGetOperations() {
    testGetDesiredModel()
    testGetResolvedModel()
    testLiveResources()
    testLiveContainers();
    testRESTModel()
    testAppLiveness()
  }

  public void testSuiteComplexVerbs() {
    testPing();
  }
  
  public void testAppLiveness() {
    def liveness = appAPI.applicationLiveness
    describe "Liveness:\n$liveness"
    
    assert liveness.allRequestsSatisfied
    assert !liveness.requestsOutstanding
  }
}
