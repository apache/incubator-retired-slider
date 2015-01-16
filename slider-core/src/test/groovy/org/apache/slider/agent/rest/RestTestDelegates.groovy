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
import org.apache.hadoop.yarn.webapp.NotFoundException
import org.apache.slider.api.StateValues
import org.apache.slider.api.types.SerializedComponentInformation
import org.apache.slider.api.types.SerializedContainerInformation
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.core.conf.ConfTreeOperations
import org.apache.slider.core.registry.docstore.PublishedConfiguration
import org.apache.slider.core.restclient.HttpOperationResponse
import org.apache.slider.core.restclient.HttpVerb
import org.apache.slider.core.restclient.UrlConnectionOperations
import org.apache.slider.server.appmaster.web.rest.RestPaths
import org.apache.slider.server.appmaster.web.rest.application.ApplicationResource
import org.apache.slider.server.appmaster.web.rest.application.resources.PingResource
import org.apache.slider.test.Outcome
import org.apache.slider.test.SliderTestUtils

import javax.ws.rs.core.MediaType

import static org.apache.slider.api.ResourceKeys.COMPONENT_INSTANCES
import static org.apache.slider.api.StatusKeys.*
import static org.apache.slider.common.SliderKeys.COMPONENT_AM
import static org.apache.slider.server.appmaster.web.rest.RestPaths.*

/**
 * This class contains parts of tests that can be run
 * against a deployed AM: local or remote
 */
@CompileStatic
@Slf4j
class RestTestDelegates extends SliderTestUtils {
  public static final String TEST_GLOBAL_OPTION = "test.global.option"
  public static final String TEST_GLOBAL_OPTION_PRESENT = "present"

  final String appmaster;
  final String application;

  RestTestDelegates(String appmaster) {
    this.appmaster = appmaster
    application = appendToURL(appmaster, RestPaths.SLIDER_PATH_APPLICATION)
  }

  
  public void testCodahaleOperations() throws Throwable {
    describe "Codahale operations"
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

  public void testLiveResources() throws Throwable {
    describe "Live Resources"
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


  public void testRestletOperations() throws Throwable {
    Client client = createJerseyClient()
    String path = appendToURL(application, LIVE_RESOURCES)
    WebResource webResource = client.resource(path)
    webResource.type(MediaType.APPLICATION_JSON)
               .get(ConfTree.class);
  }
  
  public void testLiveContainers() throws Throwable {
    describe "Application REST ${LIVE_CONTAINERS}"

    Map<String, SerializedContainerInformation> containers =
        fetchType(HashMap, appmaster, LIVE_CONTAINERS)
    assert containers.size() == 1
    log.info "${containers}"
    SerializedContainerInformation amContainerInfo =
        (SerializedContainerInformation) containers.values()[0]
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

    SerializedContainerInformation retrievedContainerInfo =
        fetchType(SerializedContainerInformation, appmaster,
            LIVE_CONTAINERS + "/${amContainerId}")
    assert retrievedContainerInfo.containerId == amContainerId

    // fetch missing
    try {
      def result = fetchType(SerializedContainerInformation, appmaster,
          LIVE_CONTAINERS + "/unknown")
      fail("expected an error, got $result")
    } catch (NotFoundException e) {
      // expected
    }


    describe "components"

    Map<String, SerializedComponentInformation> components =
        fetchType(HashMap, appmaster, LIVE_COMPONENTS)
    // two components
    assert components.size() == 1
    log.info "${components}"

    SerializedComponentInformation amComponentInfo =
        (SerializedComponentInformation) components[COMPONENT_AM]

    SerializedComponentInformation amFullInfo = fetchType(
        SerializedComponentInformation,
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
    describe "model"

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
    assert resolved[MODEL_RESOLVED_APPCONF].components[sam]
    [TEST_GLOBAL_OPTION] ==
    TEST_GLOBAL_OPTION_PRESENT
  }

  public void testPing() {
    // GET
    String ping = appendToURL(appmaster, SLIDER_PATH_APPLICATION, ACTION_PING)
    describe "ping to AM URL $appmaster, ping URL $ping"
    def pinged = fetchType(PingResource, appmaster, ACTION_PING + "?body=hello")
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
      pinged = deser(PingResource, bytes)
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
      def pinged = fetchType(
          PingResource,
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

}
