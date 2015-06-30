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

import com.google.common.base.Preconditions
import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.UniformInterfaceException
import com.sun.jersey.api.client.WebResource
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.webapp.NotFoundException
import org.apache.slider.api.StateValues
import org.apache.slider.api.types.ComponentInformation
import org.apache.slider.api.types.ContainerInformation
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.core.conf.ConfTreeOperations
import org.apache.slider.core.restclient.HttpVerb
import org.apache.slider.server.appmaster.web.rest.application.ApplicationResource
import org.apache.slider.api.types.PingInformation
import org.apache.slider.test.Outcome

import javax.net.ssl.SSLException
import javax.ws.rs.core.MediaType

import static org.apache.slider.api.ResourceKeys.COMPONENT_INSTANCES
import static org.apache.slider.api.StatusKeys.*
import static org.apache.slider.common.SliderKeys.COMPONENT_AM
import static org.apache.slider.server.appmaster.web.rest.RestPaths.*

/**
 * This class contains parts of tests that can be run
 * against a deployed AM: local or remote.
 * It uses Jersey WebResource... and must be passed a client
 * that is either secure or not
 * {@link WebResource}
 * 
 * 
 */
@CompileStatic
@Slf4j
class JerseyTestDelegates extends AbstractRestTestDelegate {

  final String appmaster;
  final String application;
  final Client jersey;
  final WebResource amResource
  final WebResource appResource
  

  JerseyTestDelegates(String appmaster, Client jersey,
      boolean enableComplexVerbs = true) {
    super(enableComplexVerbs)
    this.jersey = jersey
    this.appmaster = appmaster
    application = appendToURL(appmaster, SLIDER_PATH_APPLICATION)
    amResource = jersey.resource(appmaster)
    amResource.type(MediaType.APPLICATION_JSON)
    appResource = amResource.path(SLIDER_PATH_APPLICATION)
  }

  /**
   * <T> T get(Class<T> c)
   * Get operation against a path under the Application
   * @param subpath path
   * @return
   */
  public <T> T jGetApplicationResource(String subpath, Class<T> c) {
    return (T)jExec(HttpVerb.GET, subpath, c);
  }

  /**
   * <T> T get(Class<T> c)
   * Get operation against a path under the Application
   * @param subpath path
   * @return
   */
  public <T> T jExec(HttpVerb  method, String subpath, Class<T> c) {
    WebResource resource = applicationResource(subpath);
    jExec(method, resource, c);
  }

  public <T> T jExec(HttpVerb method, WebResource resource, Class<T> c) {
    try {
      Preconditions.checkArgument(c != null);
      resource.accept(MediaType.APPLICATION_JSON_TYPE);
      (T) resource.method(method.verb, c);
    } catch (UniformInterfaceException ex) {
      uprateFaults(method, resource, ex);
      // never reached as the uprate throws the new exception
      return null;
    }
  }

  /**
   * Create a resource under the application path
   * @param subpath
   * @return
   */
  public WebResource applicationResource(String subpath) {
    return appResource.path(subpath)
  }

  /**
   * Convert faults to exceptions; pass through 200 responses
   * @param method
   * @param webResource
   * @param ex
   * @return
   */
  public void uprateFaults(
      HttpVerb method,
      WebResource webResource,
      UniformInterfaceException ex) {
    uprateFaults(method.verb,
        webResource.URI.toString(),
        ex.response.status,
        ex.response.toString())
  }

  /**
   * <T> T get(Class<T> c)
   * Get operation against a path under the AM
   * @param path path
   * @return
   */
  public <T> T jGetAMResource(String path, Class<T> c) {
    assert c
    WebResource webResource = buildResource(path)
    (T)webResource.get(c)
  }

  /**
   * Get operation against a path under the AM
   * @param path path
   * @return the string value
   */
  public String jerseyGet(String path) {
    return jGetAMResource(path, String.class)
  }

  /**
   * Build a resource against a path under the AM API
   * @param path path
   * @return a resource for use
   */
  public WebResource buildResource(String path) {
    assert path
    String fullpath = SliderUtils.appendToURL(appmaster, path);
    WebResource webResource = jersey.resource(fullpath);
    webResource.type(MediaType.APPLICATION_JSON);
    log.info("HTTP operation against $fullpath");
    return webResource;
  }

  public void testJerseyGetConftree() throws Throwable {
    jGetApplicationResource(LIVE_RESOURCES, ConfTree.class);
  }
  
  public void testCodahaleOperations() throws Throwable {
    describe "Codahale operations"
    
    jerseyGet("/")
    jerseyGet(SYSTEM_THREADS)
    jerseyGet(SYSTEM_HEALTHCHECK)
    jerseyGet(SYSTEM_PING)
    jerseyGet(SYSTEM_METRICS_JSON)
  }
  
  public void logCodahaleMetrics() {
    // query Coda Hale metrics
    log.info jerseyGet(SYSTEM_HEALTHCHECK)
    log.info jerseyGet(SYSTEM_METRICS)
  }

  /**
   * Fetch a typed entry <i>under the application path</i>
   * @param subpath
   * @param clazz
   * @return
   */
  public <T> T jFetchType(
      String subpath, Class<T> clazz) {
    (T)jGetApplicationResource(subpath, clazz)
  }

  public ConfTreeOperations jGetConfigTree(
      String path) {
    ConfTree ctree = jGetApplicationResource(path, ConfTree)
    ConfTreeOperations tree = new ConfTreeOperations(ctree)
    return tree
  }


  public void testMimeTypes() throws Throwable {
    describe "Mime Types"

    WebResource resource = applicationResource(LIVE_RESOURCES)
    def response = resource.get(ClientResponse)
    response.headers.each {key, val -> log.info("$key: $val")}
    log.info response.toString()
    assert response.type.equals(MediaType.APPLICATION_JSON_TYPE)
  }
  
  
  public void testLiveResources() throws Throwable {
    describe "Live Resources"

    ConfTreeOperations tree = jGetConfigTree(LIVE_RESOURCES)

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

    Map<String, ContainerInformation> containers =
        jGetApplicationResource(LIVE_CONTAINERS, HashMap)
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

    ContainerInformation retrievedContainerInfo =
        jFetchType(
            LIVE_CONTAINERS + "/${amContainerId}",
            ContainerInformation
        )
    assert retrievedContainerInfo.containerId == amContainerId

    // fetch missing
    try {
      def result = jFetchType(
          LIVE_CONTAINERS + "/unknown",
          ContainerInformation
      )
      fail("expected an error, got $result")
    } catch (NotFoundException e) {
      // expected
    }


    describe "components"

    Map<String, ComponentInformation> components =
        jFetchType(LIVE_COMPONENTS, HashMap)
    // two components
    assert components.size() >= 1
    log.info "${components}"

    ComponentInformation amComponentInfo =
        (ComponentInformation) components[COMPONENT_AM]

    ComponentInformation amFullInfo = jFetchType(
        LIVE_COMPONENTS + "/${COMPONENT_AM}",
        ComponentInformation
    )

    assert amFullInfo.containers.size() == 1
    assert amFullInfo.containers[0] == amContainerId

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
    def list = jFetchType(path, ArrayList)
    assert list.size() == entries.size()
    assert entries.containsAll(list)
  }

  /**
   * Fetch a list of URLs, all of which must be of the same type
   * @param clazz class of resolved values
   * @param appmaster URL to app master
   * @param subpaths list of subpaths
   * @return a map of paths to values
   */
  public <T> Map<String, T> fetchTypeList(
      Class<T> clazz, String appmaster, List<String> subpaths
                                         ) {
    Map<String, T> results = [:]
    subpaths.each { String it ->
      results[it] = (jFetchType(it, clazz))
    }
    return results;
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

    def unresolvedConf = jFetchType(MODEL_DESIRED, AggregateConf)
//    log.info "Unresolved \n$unresolvedConf"
    def unresolvedAppConf = unresolvedConf.appConfOperations

    def sam = "slider-appmaster"
    assert unresolvedAppConf.getComponentOpt(sam,
        TEST_GLOBAL_OPTION, "") == ""
    def resolvedConf = jFetchType(MODEL_RESOLVED, AggregateConf)
//    log.info "Resolved \n$resolvedConf"
    assert resolvedConf.appConfOperations.getComponentOpt(
        sam, TEST_GLOBAL_OPTION, "") == TEST_GLOBAL_OPTION_PRESENT

    def unresolved = fetchTypeList(ConfTree, appmaster,
        [MODEL_DESIRED_APPCONF, MODEL_DESIRED_RESOURCES])
    assert unresolved[MODEL_DESIRED_APPCONF].components[sam]
    [TEST_GLOBAL_OPTION] == null


    def resolved = fetchTypeList(ConfTree, appmaster,
        [MODEL_RESOLVED_APPCONF, MODEL_RESOLVED_RESOURCES])
    assert resolved[MODEL_RESOLVED_APPCONF].
               components[sam][TEST_GLOBAL_OPTION] == TEST_GLOBAL_OPTION_PRESENT
  }

  public void testPing() {
    // GET
    describe "pinging"
    
    def pinged = jExec(HttpVerb.GET, ACTION_PING, PingInformation)
    log.info "Ping GET: $pinged"
    // HEAD
//    jExec(HttpVerb.HEAD, ACTION_PING, PingResource)
    jExec(HttpVerb.PUT, ACTION_PING, PingInformation)
    jExec(HttpVerb.DELETE, ACTION_PING, PingInformation)
    jExec(HttpVerb.POST, ACTION_PING, PingInformation)
    ping(HttpVerb.PUT, ACTION_PING, "ping-text")
    ping(HttpVerb.POST, ACTION_PING, "ping-text")
    ping(HttpVerb.DELETE, ACTION_PING, "ping-text")
  }

  /**
   * Execute a ping; assert that a response came back with the relevant
   * verb if the verb has a response body
   * @param method method to invoke
   * @param subpath ping path
   * @param payload payload
   * @return the resource if the verb has a response
   */
  private PingInformation ping(HttpVerb method, String subpath, Object payload) {
    def actionPing = applicationResource(ACTION_PING)
    def upload = method.hasUploadBody() ? payload : null
    if (method.hasResponseBody()) {
      def pinged = actionPing.method(method.verb, PingInformation, upload)
      assert method.verb == pinged.verb
      return pinged
    } else {
      actionPing.method(method.verb, upload)
      return null
    }
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

  /**
   * Probe that spins until the url specified by "url") refuses
   * connections
   * @param args argument map
   * @return the outcome
   */
/*
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
*/

  public void testSuiteGetOperations() {

    testCodahaleOperations()
    testMimeTypes()
    testJerseyGetConftree()
    testLiveResources()
    testLiveContainers();
    testRESTModel()
  }

  public void testSuiteComplexVerbs() {
    testPing();
  }

  /**
   * Probe callback for is the web service live; currently
   * checks the "/system/health" path.
   * Raising an SSL exception is considered a sign of liveness.
   * @param args args: ignored
   * @return the outcome
   */
  Outcome probeServiceLive(Map<String, String> args) {
    try {
      jerseyGet(SYSTEM_HEALTHCHECK)
      return Outcome.Success
    } catch (SSLException e) {
      // SSL exception => success
      return Outcome.Success
    } catch (IOException e) {
      def cause = e.getCause()
      if (cause && cause instanceof SSLException) {
        // nested SSL exception => success
        return Outcome.Success
      }
      // any other IOE is a retry
      return Outcome.Retry
    }
  }


}
