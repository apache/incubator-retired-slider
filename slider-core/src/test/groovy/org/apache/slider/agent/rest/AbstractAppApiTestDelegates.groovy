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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.api.SliderApplicationApi
import org.apache.slider.api.StateValues
import org.apache.slider.api.types.ComponentInformation
import org.apache.slider.api.types.ContainerInformation
import org.apache.slider.core.conf.ConfTreeOperations
import org.apache.slider.test.Outcome

import static org.apache.slider.api.ResourceKeys.*
import static org.apache.slider.api.StatusKeys.*
import static org.apache.slider.common.SliderKeys.COMPONENT_AM

@CompileStatic
@Slf4j
public abstract class AbstractAppApiTestDelegates extends AbstractRestTestDelegate {

  private SliderApplicationApi appAPI;

  AbstractAppApiTestDelegates(
      boolean enableComplexVerbs,
      SliderApplicationApi appAPI) {
    super(enableComplexVerbs)
    if (appAPI) {
      setAppAPI(appAPI)
    }
  }

  SliderApplicationApi getAppAPI() {
    return appAPI
  }

  protected void setAppAPI(SliderApplicationApi appAPI) {
    log.info("Setting API implementation to $appAPI")
    this.appAPI = appAPI
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
    describe "Live Resources via $appAPI"

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
    describe "Application /live/containers from $appAPI"

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
    assert amContainerInfo.output == null || !amContainerInfo.output.length
    assert amContainerInfo.released == null
    assert amContainerInfo.state == StateValues.STATE_LIVE

    describe "containers via $appAPI"

    ContainerInformation amContainerInfo2 =
        appAPI.getContainer(amContainerId)
    assert amContainerInfo2.containerId == amContainerId

    // fetch missing
    try {
      def result = appAPI.getContainer("unknown")
      fail("expected an error, got $result")
    } catch (FileNotFoundException expected) {
      // expected
    }


    describe "components via $appAPI"

    Map<String, ComponentInformation> components =
        appAPI.enumComponents()

    // two components
    assert components.size() >= 1
    log.info "${components}"

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
    describe "model via $appAPI"

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
    describe "pinging via $appAPI"
    
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

  public void testAppLiveness() {
    describe "Liveness: check via $appAPI"
    def liveness = appAPI.applicationLiveness
    describe "Liveness:\n$liveness"

    assert liveness.allRequestsSatisfied
    assert !liveness.requestsOutstanding
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
    } catch (IOException success) {
      // expected
      return Outcome.Success
    }
  }
  
  @Override
  public void testSuiteGetOperations() {
    testGetDesiredModel()
    testGetResolvedModel()
    testLiveResources()
    testLiveContainers();
    testRESTModel()
    testAppLiveness()
  }

  public void testFlexOperation() {
    // get current state
    def current = appAPI.getDesiredResources()

    // create a guaranteed unique field
    def uuid = UUID.randomUUID()
    def field = "yarn.test.flex.uuid"
    current.set(field, uuid)
    appAPI.putDesiredResources(current.confTree)
    repeatUntilSuccess("probe for resource PUT",
        this.&probeForResolveConfValues, 
        5000, 200,
        [
            "key": field,
            "val": uuid
        ],
        true,
        "Flex resources failed to propagate") {
      def resolved = appAPI.getResolvedResources()
      fail("Did not find field $field=$uuid in\n$resolved")
    }
  }

  /**
   * Probe that spins until the field has desired value
   * in the resolved resource
   * @param args argument map. key=field, val=value
   * @return the outcome
   */
  Outcome probeForResolveConfValues(Map args) {
    assert args["key"]
    assert args["val"]
    String key = args["key"]
    String val  = args["val"]
    def resolved = appAPI.getResolvedResources()
    return Outcome.fromBool(resolved.get(key)==val)
  }

  /**
   * Get the resolved value and push that out as the new state
   * 
   */
  public void testFlexToResolved() {
    def resolved = appAPI.getResolvedResources()
    appAPI.putDesiredResources(resolved.confTree)
  }
  
  @Override
  public void testSuiteComplexVerbs() {
    testPing();
    testFlexOperation();
  }

}
