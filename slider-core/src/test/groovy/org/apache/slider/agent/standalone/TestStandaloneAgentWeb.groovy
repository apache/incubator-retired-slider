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
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.webapp.NotFoundException
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.api.StateValues
import org.apache.slider.api.types.SerializedComponentInformation
import org.apache.slider.api.types.SerializedContainerInformation
import org.apache.slider.common.params.Arguments
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.core.restclient.HttpOperationResponse
import org.apache.slider.core.restclient.HttpVerb
import org.apache.slider.server.appmaster.web.rest.application.ApplicationResource
import org.apache.slider.server.appmaster.web.rest.application.resources.PingResource

import javax.ws.rs.core.MediaType

import static org.apache.slider.api.ResourceKeys.*
import static org.apache.slider.api.StatusKeys.*
import org.apache.slider.client.SliderClient
import static org.apache.slider.common.SliderKeys.*;
import org.apache.slider.core.conf.ConfTreeOperations
import org.apache.slider.core.main.ServiceLauncher

import static org.apache.slider.server.appmaster.web.rest.RestPaths.*;
import org.junit.Test

import static org.apache.slider.server.appmaster.management.MetricsKeys.*

@CompileStatic
@Slf4j
class TestStandaloneAgentWeb extends AgentMiniClusterTestBase {
  
  public static final int WEB_STARTUP_TIME = 30000
  public static final String TEST_GLOBAL_OPTION = "test.global.option"
  public static final String TEST_GLOBAL_OPTION_PRESENT = "present"
  public static final byte[] NO_BYTES = new byte[0]

  @Test
  public void testStandaloneAgentWeb() throws Throwable {

    describe "create a standalone AM then perform actions on it"
    //launch fake master
    def conf = configuration
    conf.setBoolean(METRICS_LOGGING_ENABLED, true)
    conf.setInt(METRICS_LOGGING_LOG_INTERVAL, 1)
    String clustername = createMiniCluster("", conf, 1, true)


    ServiceLauncher<SliderClient> launcher =
        createStandaloneAMWithArgs(clustername,
            [Arguments.ARG_OPTION,
             TEST_GLOBAL_OPTION, TEST_GLOBAL_OPTION_PRESENT],
            true, false)
    SliderClient client = launcher.service
    addToTeardown(client);

    ApplicationReport report = waitForClusterLive(client)
    def realappmaster = report.originalTrackingUrl

    // set up url config to match
    initConnectionFactory(launcher.configuration)


    execHttpRequest(WEB_STARTUP_TIME) {
      GET(realappmaster)
    }
    
    execHttpRequest(WEB_STARTUP_TIME) {
      def metrics = GET(realappmaster, SYSTEM_METRICS)
      log.info metrics
    }
    
    sleep(5000)
    def appmaster = report.trackingUrl

    GET(appmaster)

    log.info GET(appmaster, SYSTEM_PING)
    log.info GET(appmaster, SYSTEM_THREADS)
    log.info GET(appmaster, SYSTEM_HEALTHCHECK)
    log.info GET(appmaster, SYSTEM_METRICS_JSON)
    
    describe "Codahale operations"
    // now switch to the Hadoop URL connection, with SPNEGO escalation
    getWebPage(appmaster)
    getWebPage(appmaster, SYSTEM_THREADS)
    getWebPage(appmaster, SYSTEM_HEALTHCHECK)
    getWebPage(appmaster, SYSTEM_METRICS_JSON)
    
    log.info getWebPage(realappmaster, SYSTEM_METRICS_JSON)

    // get the root page, including some checks for cache disabled
    getWebPage(appmaster, {
      HttpURLConnection conn ->
        assertConnectionNotCaching(conn)
    })

    // now some REST gets
    describe "Application REST ${LIVE_RESOURCES}"

    ConfTreeOperations tree = fetchConfigTree(conf, appmaster, LIVE_RESOURCES)

    log.info tree.toString()
    def liveAM = tree.getComponent(COMPONENT_AM)
    def desiredInstances = liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES);
    assert desiredInstances == liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES_ACTUAL)

    assert 1 == liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES_STARTED)
    assert 0 == liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES_REQUESTING)
    assert 0 == liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES_FAILED)
    assert 0 == liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES_COMPLETED)
    assert 0 == liveAM.getMandatoryOptionInt(COMPONENT_INSTANCES_RELEASING)

    describe "Application REST ${LIVE_CONTAINERS}"

    Map<String, SerializedContainerInformation> containers =
        fetchType(HashMap, appmaster, LIVE_CONTAINERS)
    assert containers.size() == 1
    log.info "${containers}"
    SerializedContainerInformation amContainerInfo = (SerializedContainerInformation) containers.values()[0]
    assert amContainerInfo.containerId

    def amContainerId = amContainerInfo.containerId
    assert containers[amContainerId]

    assert amContainerInfo.component == COMPONENT_AM
    assert amContainerInfo.createTime > 0
    assert amContainerInfo.exitCode == null
    assert amContainerInfo.output == null
    assert amContainerInfo.released == null
    assert amContainerInfo.state == StateValues.STATE_LIVE
   
    describe "base entry lists"

    assertPathServesList(appmaster, LIVE, ApplicationResource.LIVE_ENTRIES)
    
    describe "containers"

    SerializedContainerInformation retrievedContainerInfo =
        fetchType(SerializedContainerInformation, appmaster,
            LIVE_CONTAINERS +"/${amContainerId}")
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
        (SerializedComponentInformation)components[COMPONENT_AM]

    SerializedComponentInformation amFullInfo = fetchType(
        SerializedComponentInformation,
        appmaster,
        LIVE_COMPONENTS +"/${COMPONENT_AM}")

    assert amFullInfo.containers.size() == 1
    assert amFullInfo.containers[0] == amContainerId

    testRESTModel(appmaster)
    
    // PUT & POST &c must go direct for now
    String wsroot = appendToURL(realappmaster, SLIDER_CONTEXT_ROOT)
    testPing(realappmaster)

  }

  public void testRESTModel(String appmaster) {
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
    assert unresolved[MODEL_DESIRED_APPCONF].components[sam][TEST_GLOBAL_OPTION] == null


    def resolved = fetchTypeList(ConfTree, appmaster,
        [MODEL_RESOLVED_APPCONF, MODEL_RESOLVED_RESOURCES])
    assert resolved[MODEL_RESOLVED_APPCONF].components[sam][TEST_GLOBAL_OPTION] ==
           TEST_GLOBAL_OPTION_PRESENT
  }

  /**
   * Assert that a path resolves to an array list that contains
   * those entries (and only those entries) expected
   * @param appmaster AM ref
   * @param path path under AM
   * @param entries entries to assert the presence of
   */
  public void assertPathServesList(
      String appmaster,
      String path,
      List<String> entries) {
    def list = fetchType(ArrayList, appmaster, path)
    assert list.size() == entries.size()
    assert entries.containsAll(list)
  }

  public void testPing(String appmaster) {
    // GET
    String ping = appendToURL(appmaster, SLIDER_PATH_APPLICATION, ACTION_PING)
    describe "ping to AM URL $appmaster, ping URL $ping"
    def pinged = fetchType(PingResource, appmaster,  ACTION_PING +"?body=hello")
    log.info "Ping GET: $pinged"
    
    // POST
    URL pingUrl = new URL(ping)


    def message = "hello"
    pingAction(HttpVerb.POST, pingUrl, message)
    pingAction(HttpVerb.PUT, pingUrl, message)
    pingAction(HttpVerb.DELETE, pingUrl, message)
    pingAction(HttpVerb.HEAD, pingUrl, message)

  }

  public HttpOperationResponse pingAction(HttpVerb verb, URL pingUrl, String payload) {
    def pinged
    def outcome = connectionFactory.execHttpOperation(
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
      assert bytes.length == 0, "${bytes.length} bytes of data from ping $verb.verb"
    }
    return outcome
  }

}
