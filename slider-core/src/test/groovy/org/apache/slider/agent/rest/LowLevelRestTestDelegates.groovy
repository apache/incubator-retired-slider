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
import org.apache.hadoop.yarn.webapp.NotFoundException
import org.apache.slider.api.StateValues
import org.apache.slider.api.types.ComponentInformation
import org.apache.slider.api.types.ContainerInformation
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.core.conf.ConfTreeOperations
import org.apache.slider.core.persist.ConfTreeSerDeser
import org.apache.slider.core.restclient.HttpOperationResponse
import org.apache.slider.core.restclient.HttpVerb
import org.apache.slider.core.restclient.UrlConnectionOperations
import org.apache.slider.server.appmaster.web.rest.application.ApplicationResource
import org.apache.slider.api.types.PingInformation
import org.apache.slider.test.Outcome

import javax.ws.rs.core.MediaType

import static org.apache.slider.api.ResourceKeys.COMPONENT_INSTANCES
import static org.apache.slider.api.StatusKeys.*
import static org.apache.slider.common.SliderKeys.COMPONENT_AM
import static org.apache.slider.server.appmaster.web.rest.RestPaths.*

/**
 * Low-level operations
 */
@CompileStatic
@Slf4j
class LowLevelRestTestDelegates extends AbstractRestTestDelegate {

  private final String appmaster;
  private final String application;
  // flag to indicate complex verbs are enabled

  LowLevelRestTestDelegates(String appmaster, boolean enableComplexVerbs = true) {
    super(enableComplexVerbs)
    this.appmaster = appmaster
    application = appendToURL(appmaster, SLIDER_PATH_APPLICATION)
  }


  public void testCodahaleOperations() throws Throwable {
    describe "Codahale operations  $this"
    getWebPage(appmaster)
    getWebPage(appmaster, SYSTEM_THREADS)
    getWebPage(appmaster, SYSTEM_HEALTHCHECK)
    getWebPage(appmaster, SYSTEM_PING)
    getWebPage(appmaster, SYSTEM_METRICS_JSON)
  }
  
  public void logCodahaleMetrics() {
    // query Coda Hale metrics
    log.info getWebPage(appmaster, SYSTEM_HEALTHCHECK)
    log.info getWebPage(appmaster, SYSTEM_METRICS)
  }


  public void testMimeTypes() throws Throwable {
    describe "Mime Types  $this"
    HttpOperationResponse response= executeGet(
        appendToURL(appmaster,
        SLIDER_PATH_APPLICATION, LIVE_RESOURCES))
    response.headers.each { key, val -> log.info("$key $val")}
    log.info "Content type: ${response.contentType}"
    assert response.contentType.contains(MediaType.APPLICATION_JSON_TYPE.toString())
  }

  
  public void testLiveResources() throws Throwable {
    describe "Live Resources  $this"
    ConfTreeOperations tree = fetchConfigTree(appmaster, LIVE_RESOURCES)

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
    describe "Application REST ${LIVE_CONTAINERS}  $this"

    Map<String, ContainerInformation> containers =
        fetchType(HashMap, appmaster, LIVE_CONTAINERS)
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

    describe "containers $this"

    ContainerInformation retrievedContainerInfo =
        fetchType(ContainerInformation, appmaster,
            LIVE_CONTAINERS + "/${amContainerId}")
    assert retrievedContainerInfo.containerId == amContainerId

    // fetch missing
    try {
      def result = fetchType(ContainerInformation, appmaster,
          LIVE_CONTAINERS + "/unknown")
      fail("expected an error, got $result")
    } catch (NotFoundException e) {
      // expected
    }


    describe "components  $this"

    Map<String, ComponentInformation> components =
        fetchType(HashMap, appmaster, LIVE_COMPONENTS)
    // two components
    assert components.size() >= 1
    log.info "${components}"

    ComponentInformation amComponentInfo =
        (ComponentInformation) components[COMPONENT_AM]

    ComponentInformation amFullInfo = fetchType(
        ComponentInformation,
        appmaster,
        LIVE_COMPONENTS + "/${COMPONENT_AM}")

    assert amFullInfo.containers.size() == 1
    assert amFullInfo.containers[0] == amContainerId

  }

  /**
   * Test the rest model. For this to work the cluster has to be configured
   * with the global option
   * @param appmaster
   */
  public void testRESTModel() {
    describe "model  $this"

    assertPathServesList(appmaster,
        MODEL,
        ApplicationResource.MODEL_ENTRIES)

    def unresolvedConf = fetchType(AggregateConf, appmaster, MODEL_DESIRED)
//    log.info "Unresolved \n$unresolvedConf"
    def unresolvedAppConf = unresolvedConf.appConfOperations

    def sam = "slider-appmaster"
    assert unresolvedAppConf.getComponentOpt(sam,
        TEST_GLOBAL_OPTION, "") == ""
    def resolvedConf = fetchType(AggregateConf, appmaster, MODEL_RESOLVED)
//    log.info "Resolved \n$resolvedConf"
    assert resolvedConf.appConfOperations.getComponentOpt(
        sam, TEST_GLOBAL_OPTION, "") == TEST_GLOBAL_OPTION_PRESENT

    def unresolved = fetchTypeList(ConfTree, appmaster,
        [MODEL_DESIRED_APPCONF, MODEL_DESIRED_RESOURCES])
    assert unresolved[MODEL_DESIRED_APPCONF].components[sam]
    [TEST_GLOBAL_OPTION] == null


    def resolved = fetchTypeList(ConfTree, appmaster,
        [MODEL_RESOLVED_APPCONF, MODEL_RESOLVED_RESOURCES])
    assert resolved[MODEL_RESOLVED_APPCONF].components[sam][TEST_GLOBAL_OPTION] ==
        TEST_GLOBAL_OPTION_PRESENT
  }

  
  /**
   * Test the various ping operations
   */
  public void testPing() {
    // GET

    String ping = applicationURL(ACTION_PING)
    describe "ping to AM URL $appmaster, ping URL $ping"
    def pinged = fetchType(PingInformation, appmaster, ACTION_PING + "?body=hello")
    log.info "Ping GET: $pinged"

    URL pingUrl = new URL(ping)
    def message = "hello"

    // HEAD
    pingAction(HttpVerb.HEAD, pingUrl, message)

    // Other verbs
    pingAction(HttpVerb.POST, pingUrl, message)
    pingAction(HttpVerb.PUT, pingUrl, message)
    pingAction(HttpVerb.DELETE, pingUrl, message)

  }

  /**
   * Create a URL under the application/ resources
   * @param subpath
   * @return
   */
  public String applicationURL(String subpath) {
    return appendToURL(appmaster, SLIDER_PATH_APPLICATION, subpath)
  }


  private HttpOperationResponse pingAction(
      HttpVerb verb,
      URL pingUrl,
      String payload) {
    return pingAction(connectionOperations, verb, pingUrl, payload)
  }

  private HttpOperationResponse pingAction(
      UrlConnectionOperations ops, HttpVerb verb, URL pingUrl, String payload) {
    def pinged
    def outcome = ops.execHttpOperation(
        verb,
        pingUrl,
        payload.bytes,
        MediaType.TEXT_PLAIN)
    byte[] bytes = outcome.data
    if (verb.hasResponseBody()) {
      assert bytes.length > 0, "0 bytes from ping $verb.verb"
      pinged = deser(PingInformation, bytes)
      log.info "Ping $verb.verb: $pinged"
      assert verb.verb == pinged.verb
    } else {
      assert bytes.length ==
             0, "${bytes.length} bytes of data from ping $verb.verb"
    }
    return outcome
  }

  /**
   * Test the stop command.
   * Important: once executed, the AM is no longer there.
   * This must be the last test in the sequence.
   */
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

    
    // now a ping is expected to fail
    String ping = appendToURL(appmaster, SLIDER_PATH_APPLICATION, ACTION_PING)

    repeatUntilSuccess("probe for missing registry entry",
        this.&probePingFailing, 30000, 500,
        [url: ping],
        true,
        "AM failed to shut down") {
      def pinged = fetchType(
          PingInformation,
          appmaster,
          ACTION_PING + "?body=hello")
      fail("AM didn't shut down; Ping GET= $pinged")
    }
    
  }

  /**
   * Probe that spins until the url specified by "url") refuses
   * connections
   * @param args argument map
   * @return the outcome
   */
  Outcome probePingFailing(Map args) {
    String ping = args["url"]
    URL pingUrl = new URL(ping)
    try {
      def response = pingAction(HttpVerb.HEAD, pingUrl, "should not be running")
      return Outcome.Retry
    } catch (IOException e) {
      // expected
      return Outcome.Success
    }
  }


  @Override
  public void testSuiteGetOperations() {

    testCodahaleOperations()
    testMimeTypes()
    testLiveResources()
    testLiveContainers();
    testRESTModel()
  }

  @Override
  public void testSuiteComplexVerbs() {
    testPing();
    testPutDesiredResources()
  }


  public void testPutDesiredResources() throws Throwable {
    describe "testPutDesiredResources $this"
    ConfTreeOperations current = fetchConfigTree(appmaster, MODEL_DESIRED_RESOURCES)
    ConfTreeSerDeser serDeser = new ConfTreeSerDeser()
    
    def uuid = UUID.randomUUID()
    def field = "yarn.test.flex.uuid"
    current.set(field, uuid)
    def outcome = connectionOperations.execHttpOperation(
        HttpVerb.PUT,
        new URL(applicationURL(MODEL_DESIRED_RESOURCES)),
        serDeser.toJson(current.confTree).bytes,
        MediaType.APPLICATION_JSON)
    repeatUntilSuccess("probe for resource PUT",
        this.&probeForResolveConfValues,
        5000, 200,
        [
            "key": field,
            "val": uuid
        ],
        true,
        "Flex resources failed to propagate") {
      def resolved = modelDesiredResolvedResources
      fail("Did not find field $field=$uuid in\n$resolved")
    }

    def resolved = modelDesiredResolvedResources
    log.info("Flexed cluster resources to $resolved")
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
    String val = args["val"]
    ConfTreeOperations resolved = modelDesiredResolvedResources

    return Outcome.fromBool(resolved.get(key) == val)
  }

  public ConfTreeOperations getModelDesiredResolvedResources() {
    return fetchConfigTree(
        appmaster,
        MODEL_RESOLVED_RESOURCES)
  }

  @Override
  public String toString() {
    return "- low level REST to " + application
  }
}
