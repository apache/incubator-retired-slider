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

package org.apache.slider.test

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.config.ClientConfig
import com.sun.jersey.api.client.config.DefaultClientConfig
import com.sun.jersey.api.json.JSONConfiguration
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager
import org.apache.commons.httpclient.URI
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.registry.client.types.ServiceRecord
import org.apache.hadoop.service.ServiceStateException
import org.apache.hadoop.util.Shell
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.webapp.ForbiddenException
import org.apache.hadoop.yarn.webapp.NotFoundException
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.ClusterNode
import org.apache.slider.api.RoleKeys
import org.apache.slider.api.StateValues
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.tools.Duration
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.core.conf.ConfTreeOperations
import org.apache.slider.core.exceptions.BadClusterStateException
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.exceptions.WaitTimeoutException
import org.apache.slider.core.main.ServiceLaunchException
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.persist.JsonSerDeser
import org.apache.slider.core.registry.docstore.PublishedConfigSet
import org.apache.slider.core.restclient.HttpOperationResponse
import org.apache.slider.core.restclient.UgiJerseyBinding
import org.apache.slider.core.restclient.UrlConnectionOperations
import org.apache.slider.server.appmaster.web.HttpCacheHeaders
import org.apache.slider.server.appmaster.web.rest.RestPaths
import org.apache.slider.server.services.workflow.ForkedProcessService
import org.junit.Assert
import org.junit.Assume

import javax.ws.rs.core.HttpHeaders
import java.util.concurrent.TimeoutException

import static Arguments.ARG_OPTION

/**
 * Static utils for tests in this package and in other test projects.
 * 
 * It is designed to work with mini clusters as well as remote ones
 * 
 * This class is not final and may be extended for test cases.
 * 
 * Some of these methods are derived from the SwiftUtils and SwiftTestUtils
 * classes -replicated here so that they are available in Hadoop-2.0 code
 */
@Slf4j
@CompileStatic
class SliderTestUtils extends Assert {
  public static final String DEFAULT_SLIDER_CLIENT = SliderClient.class.name
  static String sliderClientClassName = DEFAULT_SLIDER_CLIENT

  public static void describe(String s) {
    log.info("");
    log.info("===============================");
    log.info(s);
    log.info("===============================");
    log.info("");
  }

  public static String prettyPrint(String json) {
    JsonOutput.prettyPrint(json)
  }

  public static void skip(String message) {
    log.warn("Skipping test: {}", message)
    Assume.assumeTrue(message, false);
  }

  public static void assume(boolean condition, String message) {
    if (!condition) {
      skip(message)
    }
  }

  /**
   * Skip a test if not running on Windows
   */
  public assumeWindows() {
    assume(Shell.WINDOWS, "not windows")
  }

  /**
   * Skip a test if running on Windows
   */
  public assumeNotWindows() {
    assume(!Shell.WINDOWS, "windows")
  }

  /**
   * Equality size for a list
   * @param left
   * @param right
   */
  public static void assertListEquals(List left, List right) {
    String lval = collectionToString(left)
    String rval = collectionToString(right)
    String text = "comparing $lval to $rval"
    assertEquals(text, left.size(), right.size())
    for (int i = 0; i < left.size(); i++) {
      assertEquals(text, left[i], right[i])
    }
  }

  /**
   * Assert a list has a given length
   * @param list list
   * @param size size to have
   */
  public static void assertListLength(List list, int size) {
    String lval = collectionToString(list)
    assertEquals(lval, size, list.size())
  }

  /**
   * Stringify a collection with [ ] at either end
   * @param collection collection
   * @return string value
   */
  public static String collectionToString(List collection) {
    return "[" + SliderUtils.join(collection, ", ", false) + "]"
  }

  /**
   * Assume that a string option is set and not equal to ""
   * @param conf configuration file
   * @param key key to look for
   */
  public static void assumeStringOptionSet(Configuration conf, String key) {
    if (!conf.getTrimmed(key)) {
      skip("Configuration key $key not set")
    }
  }

  /**
   * assert that a string option is set and not equal to ""
   * @param conf configuration file
   * @param key key to look for
   */
  public static void assertStringOptionSet(Configuration conf, String key) {
    getRequiredConfOption(conf, key)
  }

  /**
   * Assume that a boolean option is set and true.
   * Unset or false triggers a test skip
   * @param conf configuration file
   * @param key key to look for
   */
  public static void assumeBoolOptionTrue(Configuration conf, String key) {
    assumeBoolOption(conf, key, false)
  }

  /**
   * Assume that a boolean option is true.
   * False triggers a test skip
   * @param conf configuration file
   * @param key key to look for
   * @param defval default value if the property is not defined
   */
  public static void assumeBoolOption(
      Configuration conf, String key, boolean defval) {
    assume(conf.getBoolean(key, defval),
        "Configuration key $key is false")
  }

  /**
   * Get a required config option (trimmed, incidentally).
   * Test will fail if not set
   * @param conf configuration
   * @param key key
   * @return the string
   */
  public static String getRequiredConfOption(Configuration conf, String key) {
    String val = conf.getTrimmed(key)
    if (!val) {
      fail("Missing configuration option $key")
    }
    return val;
  }

  /**
   * Fails a test because required behavior has not been implemented.
   */
  public static void failNotImplemented() {
    fail("Not implemented")
  }

  /**
   * skip a test on windows
   */
  public static void skipOnWindows() {
    if (Shell.WINDOWS) {
      skip("Not supported on windows")
    }
  }

  /**
   * Assert that any needed libraries being present. On Unix none are needed;
   * on windows they must be present
   */
  public static void assertNativeLibrariesPresent() {
    String errorText = SliderUtils.checkForRequiredNativeLibraries()
    if (errorText != "") {
      fail(errorText)
    }
  }

  /**
   * Wait for the cluster live; fail if it isn't within the (standard) timeout
   * @param sliderClient client
   * @return the app report of the live cluster
   */
  public static ApplicationReport waitForClusterLive(
      SliderClient sliderClient,
      int goLiveTime) {
    ApplicationReport report = sliderClient.monitorAppToRunning(
        new Duration(goLiveTime));
    assertNotNull(
        "Cluster did not go live in the time $goLiveTime",
        report);
    return report;
  }

  protected static String[] toArray(List<Object> args) {
    String[] converted = new String[args.size()];
    for (int i = 0; i < args.size(); i++) {
      def elt = args.get(i)
      assert args.get(i) != null
      converted[i] = elt.toString();
    }
    return converted;
  }

  public static void waitWhileClusterLive(SliderClient client, int timeout) {
    Duration duration = new Duration(timeout);
    duration.start()
    while (client.actionExists(client.deployedClusterName, true) &&
           !duration.limitExceeded) {
      sleep(1000);
    }
    if (duration.limitExceeded) {
      fail("Cluster ${client.deployedClusterName} still live after $timeout ms")
    }
  }

  public static void waitUntilClusterLive(SliderClient client, int timeout) {
    Duration duration = new Duration(timeout);
    duration.start()
    while (0 != client.actionExists(client.deployedClusterName, true) &&
           !duration.limitExceeded) {
      sleep(1000);
    }
    if (duration.limitExceeded) {
      fail("Cluster ${client.deployedClusterName} not live after $timeout ms")
    }
  }

  /**
   * Spin waiting for the Slider role count to match expected
   * @param client client
   * @param role role to look for
   * @param desiredCount RS count
   * @param timeout timeout
   */
  public static ClusterDescription waitForRoleCount(
      SliderClient client,
      String role,
      int desiredCount,
      int timeout) {
    return waitForRoleCount(client, [(role): desiredCount], timeout)
  }

  /**
   * Spin waiting for the Slider role count to match expected
   * @param client client
   * @param roles map of roles to look for
   * @param desiredCount RS count
   * @param timeout timeout
   */
  public static ClusterDescription waitForRoleCount(
      SliderClient client,
      Map<String, Integer> roles,
      int timeout,
      String operation = "startup") {
    String clustername = client.deployedClusterName;
    ClusterDescription status = null
    Duration duration = new Duration(timeout);
    duration.start()
    boolean roleCountFound = false;
    while (!roleCountFound) {
      StringBuilder details = new StringBuilder()

      boolean timedOut = duration.limitExceeded
      try {
        status = client.getClusterDescription(clustername)
        roleCountFound = true;
        for (Map.Entry<String, Integer> entry : roles.entrySet()) {
          String role = entry.key
          int desiredCount = entry.value
          List<String> instances = status.instances[role]
          int instanceCount = instances != null ? instances.size() : 0;
          if (instanceCount != desiredCount) {
            roleCountFound = false;
          }
          details.append("[$role]: desired: $desiredCount;" +
                         " actual: $instanceCount ")

          // call out requested count, as this is a cause of problems on
          // overloaded functional test clusters
          def requested = status.roles[role][RoleKeys.ROLE_REQUESTED_INSTANCES]
          if (requested != "0") {
            details.append("requested: $requested ")
          }
        }
        if (roleCountFound) {
          //successful
          log.info("$operation: role count as desired: $details")
          break;
        }
      } catch (BadClusterStateException e) {
        // cluster not live yet; ignore or rethrow
        if (timedOut) {
          throw e;
        }
        details.append(e.toString());
      }
      if (timedOut) {
        duration.finish();
        describe("$operation: role count not met after $duration: $details")
        log.info(prettyPrint(status.toJsonString()))
        fail("$operation: role counts not met after $duration: " +
             details.toString() +
             " in \n$status ")
      }
      log.debug("Waiting: " + details)
      Thread.sleep(1000)
    }
    return status
  }

  /**
   * Wait for the hbase master to be live (or past it in the lifecycle)
   * @param clustername cluster
   * @param spintime time to wait
   * @return true if the cluster came out of the sleep time live 
   * @throws IOException
   * @throws SliderException
   */
  public static boolean spinForClusterStartup(
      SliderClient client,
      long spintime,
      String role)
  throws WaitTimeoutException, IOException, SliderException {
    int state = client.waitForRoleInstanceLive(role, spintime);
    return state == StateValues.STATE_LIVE;
  }

  public static ClusterDescription dumpClusterStatus(
      SliderClient client,
      String text) {
    ClusterDescription status = client.clusterDescription;
    dumpClusterDescription(text, status)
    return status;
  }

  public static List<ClusterNode> listNodesInRole(
      SliderClient client,
      String role) {
    return client.listClusterNodesInRole(role)
  }

  public static void dumpClusterDescription(
      String text,
      ClusterDescription status) {
    describe(text)
    log.info(prettyPrint(status.toJsonString()))
  }


  public static void dumpClusterDescription(String text, AggregateConf status) {
    describe(text)
    log.info(status.toString())
  }

  /**
   * Fetch the current site config from the Slider AM, from the 
   * <code>clientProperties</code> field of the ClusterDescription
   * @param client client
   * @param clustername name of the cluster
   * @return the site config
   */
  public static Configuration fetchClientSiteConfig(SliderClient client) {
    ClusterDescription status = client.clusterDescription;
    Configuration siteConf = new Configuration(false)
    status.clientProperties.each { String key, String val ->
      siteConf.set(key, val, "slider cluster");
    }
    return siteConf;
  }

  /**
   * Fetch a web page
   * @param url URL
   * @return the response body
   */

  public static String GET(URL url) {
    return fetchWebPageRaisedErrorCodes(url.toString())
  }

  public static String GET(URL url, String path) {
    return GET(url.toString(), path)
  }

  public static String GET(String base, String path) {
    String s = appendToURL(base, path)
    return GET(s)
  }

  def static String GET(String s) {
    return fetchWebPageRaisedErrorCodes(s)
  }

  public static String appendToURL(String base, String path) {
    return SliderUtils.appendToURL(base, path)
  }

  public static String appendToURL(String base, String... paths) {
    return SliderUtils.appendToURL(base, paths)
  }

  /**
   * Fetch a web page using HttpClient 3.1.
   * This <i>DOES NOT</i> work with secure connections.
   * @param url URL
   * @return the response body
   */

  public static String fetchWebPage(String url) {
    log.info("GET $url")
    def httpclient = new HttpClient(new MultiThreadedHttpConnectionManager());
    httpclient.httpConnectionManager.params.connectionTimeout = 10000;
    GetMethod get = new GetMethod(url);

    get.followRedirects = true;
    int resultCode
    try {
      resultCode = httpclient.executeMethod(get);
      if (resultCode != 200) {
        log.warn("Result code of $resultCode")
      }
    } catch (IOException e) {
      log.error("Failed on $url: $e", e)
      throw e;
    }
    String body = get.responseBodyAsString;
    return body;
  }

  /**
   * Fetches a web page asserting that the response code is between 200 and 400.
   * This <i>DOES NOT</i> work with secure connections.
   * <p>
   * 
   * Will error on 400 and 500 series response codes and let 200 and 300 through. 
   * @param url URL to get as string
   * @return body of response
   * @throws IOException Network IO problems or exit code >= 400 not specifically handled
   * @throws NotFoundException 404 received
   * @throws ForbiddenException 401 received
   */
  public static String fetchWebPageRaisedErrorCodes(String url) {
    assert null != url

    log.info("Fetching HTTP content at " + url);

    def client = new HttpClient(new MultiThreadedHttpConnectionManager());
    client.httpConnectionManager.params.connectionTimeout = 10000;
    GetMethod get = new GetMethod(url);
    URI destURI = get.getURI()
    assert destURI.port != 0
    assert destURI.host


    get.followRedirects = true;
    int resultCode
    try {
      resultCode = client.executeMethod(get);
    } catch (IOException e) {
      throw NetUtils.wrapException(url, destURI.port, "localhost", 0, e)
    }

    def body = get.responseBodyAsString

    uprateFaults("GET", url, resultCode, body)
    return body;
  }

  /**
   * Generate exceptions from error codes >= 400. Some are converted
   * into specific exceptions.
   * @param verb HTTP verb
   * @param url URL
   * @param resultCode result code
   * @param body any body
   * @throws NotFoundException 404 received
   * @throws ForbiddenException 401 received
   * @throws IOException any other exit code
   */
  public static void uprateFaults(
      String verb,
      String url,
      int resultCode,
      String body) {

    if (resultCode == 404) {
      throw new NotFoundException(url);
    }
    if (resultCode == 401) {
      throw new ForbiddenException(url);
    }
    if (!(resultCode >= 200 && resultCode < 400)) {
      String bodyDetails = (body == null ?
                            "(no body)"  :
                            "body length ${body?.length()}:\n:$body")
      String message = "$verb to $url failed with exit code $resultCode; $bodyDetails"

      log.error(message);
      throw new IOException(message);
    }
  }

  /**
   * Fetches a web page asserting that the response code is between 200 and 400.
   * Will error on 400 and 500 series response codes and let 200 and 300 through.
   * <p>
   * if security is enabled, this uses SPNEGO to auth
   * @param page
   * @return body of response
   */
  public static String getWebPage(String base, String path) {
    String s = appendToURL(base, path)
    return getWebPage(s)
  }

  /**
   * Execute any operation provided as a closure which returns a string, swallowing exceptions until
   * eventually they time out.
   * 
   * @param timeout
   * @param operation
   * @return
   */
  public static String execOperation(int timeout, Closure operation) {
    Duration duration = new Duration(timeout).start()
    Exception ex = new IOException("limit exceeded before starting");
    while (!duration.limitExceeded) {
      try {
        String result = operation();
        return result;
      } catch (Exception e) {
        ex = e;
        sleep(1000)
      }
    }
    // timeout
    throw ex;
  } 

  /**
  * Static factory for URL connections
   */
  static UrlConnectionOperations connectionOperations
  static UgiJerseyBinding jerseyBinding;

  
  /**
   * Static initializer of the connection operations
   * @param conf config
   */
  public static synchronized void initHttpTestSupport(Configuration conf) {
    connectionOperations = new UrlConnectionOperations(conf);
    jerseyBinding = new UgiJerseyBinding(connectionOperations)
  }

  /**
   * Check for the HTTP support being initialized
   */
  public static synchronized void assertHttpSupportInitialized() {
    assert connectionOperations 
    assert jerseyBinding 
  }
  
  /**
   * Create Jersey client with UGI integration
   * @return
   */
  public static Client createUGIJerseyClient() {
    assertHttpSupportInitialized()
    ClientConfig clientConfig = createJerseyClientConfig()
    return new Client(jerseyBinding.handler, clientConfig);
  }

  /**
   * Create Jersey client with URL handling by way
   * of the java.net classes. This DOES NOT have any SPNEGO
   * integration. If used to query a secure cluster via the
   * RM Proxy, it MUST fail.
   * @return a basic Jersey client
   */
  public static Client createBasicJerseyClient() {
    ClientConfig clientConfig = createJerseyClientConfig()
    return new Client(new URLConnectionClientHandler(),
        clientConfig);
  }


  /**
   * Create a jersey client config with the settings needed for tests
   * (e.g. POJO mappings)
   * @return a client config
   */
  public static ClientConfig createJerseyClientConfig() {
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.features[JSONConfiguration.FEATURE_POJO_MAPPING] = Boolean.TRUE;
    return clientConfig
  }

  /**
   * Fetches a web page asserting that the response code is between 200 and 400.
   * Will error on 400 and 500 series response codes and let 200 and 300 through.
   * 
   * if security is enabled, this uses SPNEGO to auth
   * <p>
   *   Relies on {@link #initHttpTestSupport(org.apache.hadoop.conf.Configuration)} 
   *   to have been called.
   *   
   * @param path path to page
   * @param connectionChecks optional closure to run against an open connection
   * @return body of response
   */
  public static String getWebPage(String path, Closure connectionChecks = null) {
    HttpOperationResponse outcome = executeGet(path)
    return new String(outcome.data);
  }

  public static HttpOperationResponse executeGet(String path) {
    assert path
    assertHttpSupportInitialized()

    log.info("Fetching HTTP content at $path");
    URL url = new URL(path)
    def outcome = connectionOperations.execGet(url)
    return outcome
  }

  /**
   * Assert that a connection is not caching by looking at the headers
   * @param conn connection to examine
   */
  public static void assertConnectionNotCaching(HttpURLConnection conn) {
    assert conn.expiration <= conn.date
    assert conn.getHeaderField(HttpHeaders.CACHE_CONTROL) ==
           HttpCacheHeaders.HTTP_HEADER_CACHE_CONTROL_NONE
    assert conn.getHeaderField(HttpCacheHeaders.HTTP_HEADER_PRAGMA) ==
           HttpCacheHeaders.HTTP_HEADER_CACHE_CONTROL_NONE
  }

/**
   * Assert that a service operation succeeded
   * @param service service
   */
  public static void assertSucceeded(ServiceLauncher service) {
    assert 0 == service.serviceExitCode;
  }

  public static void assertContainersLive(ClusterDescription clusterDescription,
      String component, int expected) {
    log.info("Asserting component $component expected count $expected}",)
    int actual = extractLiveContainerCount(clusterDescription, component)
    if (expected != actual) {
      log.warn("$component actual=$actual, expected $expected in \n$clusterDescription")
    }
    assert expected == actual
  }

  /**
   * Robust extraction of live container count
   * @param clusterDescription status
   * @param component component to resolve
   * @return the number of containers live.
   */
  public static int extractLiveContainerCount(
      ClusterDescription clusterDescription,
      String component) {
    def instances = clusterDescription?.instances?.get(component)
    int actual = instances != null ? instances.size() : 0
    return actual
  }
  /**
   * Exec a set of commands, wait a few seconds for it to finish.
   * @param status code
   * @param commands
   * @return the process
   */
  public static ForkedProcessService exec(int status, List<String> commands) {
    ForkedProcessService process = exec(commands)

    def exitCode = process.exitCode
    assert exitCode != null
    assert status == exitCode
    return process
  }
  /**
     * Exec a set of commands, wait a few seconds for it to finish.
     * @param commands
     * @return
     */
  public static ForkedProcessService exec(List<String> commands) {
    ForkedProcessService process;
    process = new ForkedProcessService(
        commands[0],
        [:],
        commands);
    process.init(new Configuration());
    process.start();
    int timeoutMillis = 5000
    if (!process.waitForServiceToStop(timeoutMillis)) {
      throw new TimeoutException(
          "Process did not stop in " + timeoutMillis + "mS");
    }
    process
  }

  /**
   * Does an application exist? Run the commands and if the
   * operation fails with a FileNotFoundException, then
   * this method returns false.
   * <p>
   *   Run something harmless like a -version command, something
   *   which must return 0
   *   
   * @param commands
   * @return true if the command sequence succeeded
   * false if they failed with no file
   * @throws Exception on any other failure cause
   */
  public static boolean doesAppExist(List<String> commands) {
    try {
      exec(0, commands)
      return true;
    } catch (ServiceStateException e) {
      if (!(e.cause instanceof FileNotFoundException)) {
        throw e;
      }
      return false;
    }
  }

  /**
   * Locate an executable on the path
   * @param exe executable name. If it is an absolute path which
   * exists then it will returned direct
   * @return the path to an exe or null for no match
   */
  public static File locateExecutable(String exe) {
    File exeNameAsPath = new File(exe).absoluteFile
    if (exeNameAsPath.exists()) {
      return exeNameAsPath
    }
    
    File exepath = null
    String path = extractPath()
    String[] dirs = path.split(System.getProperty("path.separator"));
    dirs.each { String dirname ->
      File dir = new File(dirname)

      File possible = new File(dir, exe)
      if (possible.exists()) {
        exepath = possible
      }
    }
    return exepath
  }

  /**
   * Lookup the PATH env var
   * @return the path or null
   */
  public static String extractPath() {
    return extractEnvVar("PATH")
  }
  
  /**
   * Find an environment variable. Uses case independent checking for
   * the benefit of windows.
   * Will fail if the var is not found.
   * @param var path variable <i>in upper case</i>
   * @return the env var
   */
  public static String extractEnvVar(String var) {
    String realkey = "";

    System.getenv().keySet().each { String it ->
      if (it.toUpperCase(Locale.ENGLISH).equals(var)) {
        realkey = it;
      }
    }

    if (!realkey) {
      fail("No environment variable $var found")
    }
    assert realkey
    def val = System.getenv(realkey)
    
    log.info("$realkey = $val")
    return val
  }

  /**
   * Execute a closure, assert it fails with a given exit code and text
   * @param exitCode exit code
   * @param text text (can be "")
   * @param action action
   * @return
   */
  def assertFailsWithException(int exitCode,
      String text,
      Closure action) {
    try {
      action()
      fail("Operation was expected to fail —but it succeeded")
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(e, exitCode, text)
    }
  }
  
  /**
   * Execute a closure, assert it fails with a given exit code and text
   * @param exitCode exit code
   * @param text text (can be "")
   * @param action action
   * @return
   */
  def assertFailsWithExceptionClass(Class clazz,
      String text,
      Closure action) {
    try {
      action()
      fail("Operation was expected to fail —but it succeeded")
    } catch (Exception e) {
      assertExceptionDetails(e, clazz, text)
    }
  }

  /**
   * Make an assertion about the exit code of an exception
   * @param ex exception
   * @param exitCode exit code
   * @param text error text to look for in the exception
   */
  static void assertExceptionDetails(
      ServiceLaunchException ex,
      int exitCode,
      String text = "") {
    if (exitCode != ex.exitCode) {
      def message = "Wrong exit code, expected $exitCode but got $ex.exitCode in $ex"
      log.warn(message, ex)
      throw new AssertionError(message, ex)
    }
    if (text) {
      if (!(ex.toString().contains(text))) {
        def message = "String match for \"${text}\"failed in $ex"
        log.warn(message, ex)
        throw new AssertionError(message, ex)
      }
    }
  }
  /**
   * Make an assertion about the exit code of an exception
   * @param ex exception
   * @param exitCode exit code
   * @param text error text to look for in the exception
   */
  static void assertExceptionDetails(
      Exception ex,
      Class clazz,
      String text = "") {
    if (ex.class != clazz) {
      throw ex;
    }
    if (text && !(ex.toString().contains(text))) {
      throw ex;
    }
  }

  /**
   * Launch the slider client with the specific args; no validation
   * of return code takes place
   * @param conf configuration
   * @param args arg list
   * @return the return code
   */
  protected static ServiceLauncher<SliderClient> execSliderCommand(
      Configuration conf,
      List args) {
    ServiceLauncher<SliderClient> serviceLauncher =
        new ServiceLauncher<SliderClient>(sliderClientClassName);

    log.debug("slider ${SliderUtils.join(args, " ", false)}")
    serviceLauncher.launchService(conf,
        toArray(args),
        false);
    return serviceLauncher
  }

  public static ServiceLauncher launch(Class serviceClass,
      Configuration conf,
      List<Object> args) throws
      Throwable {
    ServiceLauncher serviceLauncher =
        new ServiceLauncher(serviceClass.name);

    def joinedArgs = SliderUtils.join(args, " ", false)
    log.debug("slider ${joinedArgs}")

    serviceLauncher.launchService(conf,
        toArray(args),
        false);
    return serviceLauncher;
  }

  public static Throwable launchExpectingException(Class serviceClass,
      Configuration conf,
      String expectedText,
      List args)
  throws Throwable {
    try {
      ServiceLauncher launch = launch(serviceClass, conf, args);
      throw new AssertionError("Expected an exception with text containing $expectedText "+
               " -but the service completed with exit code ${launch.serviceExitCode}");
    } catch (AssertionError error) {
      throw error;
    } catch (Throwable thrown) {
      if (expectedText && !thrown.toString().contains(expectedText)) {
        //not the right exception -rethrow
        log.warn("Caught Exception did not contain expected text" +
                 "\"" + expectedText + "\"")
        throw thrown;
      }
      return thrown;
    }
  }


  public static ServiceLauncher<SliderClient> launchClientAgainstRM(
      String address,
      List args,
      Configuration conf) {
    assert address != null
    log.info("Connecting to rm at ${address}")
    if (!args.contains(Arguments.ARG_MANAGER)) {
      args += [Arguments.ARG_MANAGER, address]
    }
    ServiceLauncher<SliderClient> launcher = execSliderCommand(conf, args)
    return launcher
  }

  /**
   * Add a configuration parameter as a cluster configuration option
   * @param extraArgs extra arguments
   * @param conf config
   * @param option option
   */
  public static void addClusterConfigOption(
      List<String> extraArgs,
      YarnConfiguration conf,
      String option) {

    conf.getTrimmed(option);
    extraArgs << ARG_OPTION << option << getRequiredConfOption(conf, option)

  }

  /**
   * Assert that a path refers to a directory
   * @param fs filesystem
   * @param path path of the directory
   * @throws IOException on File IO problems
   */
  public static void assertIsDirectory(HadoopFS fs,
      Path path) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(path);
    assertIsDirectory(fileStatus);
  }

  /**
   * Assert that a path refers to a directory
   * @param fileStatus stats to check
   */
  public static void assertIsDirectory(FileStatus fileStatus) {
    assertTrue("Should be a dir -but isn't: " + fileStatus,
        fileStatus.isDirectory());
  }

  /**
   * Assert that a path exists -but make no assertions as to the
   * type of that entry
   *
   * @param fileSystem filesystem to examine
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws IOException IO problems
   */
  public static void assertPathExists(
      HadoopFS fileSystem,
      String message,
      Path path) throws IOException {
    if (!fileSystem.exists(path)) {
      //failure, report it
      fail(
          message + ": not found \"" + path + "\" in " + path.getParent() +
          "-" +
          ls(fileSystem, path.getParent()));
    }
  }

  /**
   * Assert that a path does not exist
   *
   * @param fileSystem filesystem to examine
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws IOException IO problems
   */
  public static void assertPathDoesNotExist(
      HadoopFS fileSystem,
      String message,
      Path path) throws IOException {
    try {
      FileStatus status = fileSystem.getFileStatus(path);
      // a status back implies there is a file here
      fail(message + ": unexpectedly found " + path + " as  " + status);
    } catch (FileNotFoundException expected) {
      //this is expected

    }
  }

  /**
   * Assert that a FileSystem.listStatus on a dir finds the subdir/child entry
   * @param fs filesystem
   * @param dir directory to scan
   * @param subdir full path to look for
   * @throws IOException IO probles
   */
  public static void assertListStatusFinds(HadoopFS fs,
      Path dir,
      Path subdir) throws IOException {
    FileStatus[] stats = fs.listStatus(dir);
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    for (FileStatus stat : stats) {
      builder.append(stat.toString()).append('\n');
      if (stat.getPath().equals(subdir)) {
        found = true;
      }
    }
    assertTrue("Path " + subdir
        + " not found in directory " + dir + ":" + builder,
        found);
  }

  /**
   * List a a path to string
   * @param fileSystem filesystem
   * @param path directory
   * @return a listing of the filestatuses of elements in the directory, one
   * to a line, precedeed by the full path of the directory
   * @throws IOException connectivity problems
   */
  public static String ls(HadoopFS fileSystem, Path path)
  throws
      IOException {
    if (path == null) {
      //surfaces when someone calls getParent() on something at the top of the path
      return "/";
    }
    FileStatus[] stats;
    String pathtext = "ls " + path;
    try {
      stats = fileSystem.listStatus(path);
    } catch (FileNotFoundException e) {
      return pathtext + " -file not found";
    } catch (IOException e) {
      return pathtext + " -failed: " + e;
    }
    return pathtext + fileStatsToString(stats, "\n");
  }

  /**
   * Take an array of filestats and convert to a string (prefixed w/ a [01] counter
   * @param stats array of stats
   * @param separator separator after every entry
   * @return a stringified set
   */
  public static String fileStatsToString(FileStatus[] stats, String separator) {
    StringBuilder buf = new StringBuilder(stats.length * 128);
    for (int i = 0; i < stats.length; i++) {
      buf.append(String.format("[%02d] %s", i, stats[i])).append(separator);
    }
    return buf.toString();
  }

  public static void waitWhileClusterLive(SliderClient sliderClient) {
    waitWhileClusterLive(sliderClient, 30000)
  }

  public static void dumpRegistryInstances(
      Map<String, ServiceRecord> instances) {
    describe "service registry slider instances"
    instances.each { Map.Entry<String, ServiceRecord> it ->
      log.info(" $it.key : $it.value")
    }
    describe "end list service registry slider instances"
  }


  public static void dumpRegistryInstanceIDs(List<String> instanceIds) {
    describe "service registry instance IDs"
    dumpCollection(instanceIds)
  }

  public static void dumpRegistryServiceTypes(Collection<String> entries) {
    describe "service registry types"
    dumpCollection(entries)
  }

  def static void dumpCollection(Collection entries) {
    log.info("number of entries: ${entries.size()}")
    entries.each { log.info(it.toString()) }
  }

  def static void dumpArray(Object[] entries) {
    log.info("number of entries: ${entries.length}")
    entries.each { log.info(it.toString()) }
  }

  public static void dumpMap(Map map) {
    map.entrySet().each { Map.Entry it ->
      log.info("\"${it.key.toString()}\": \"${it.value.toString()}\"")
    }
  }

  /**
   * Get a time option in seconds if set, otherwise the default value (also in seconds).
   * This operation picks up the time value as a system property if set -that
   * value overrides anything in the test file
   * @param conf
   * @param key
   * @param defVal
   * @return
   */
  public static int getTimeOptionMillis(
      Configuration conf,
      String key,
      int defValMillis) {
    int val = conf.getInt(key, 0)
    val = Integer.getInteger(key, val)
    int time = 1000 * val
    if (time == 0) {
      time = defValMillis
    }
    return time;
  }

  def dumpConfigurationSet(PublishedConfigSet confSet) {
    confSet.keys().each { String key ->
      def config = confSet.get(key)
      log.info "$key -- ${config.description}"
    }
  }

  /**
   * Convert a file to a URI suitable for use in an argument
   * @param file file
   * @return a URI string valid on all platforms
   */
  public String toURIArg(File file) {
    file.absoluteFile.toURI().toString()
  }

  /**
   * Assert a file exists; fails with a listing of the parent dir
   * @param text text for front of message
   * @param file file to look for
   * @throws FileNotFoundException
   */
  public void assertFileExists(String text, File file) {
    if (!file.exists()) {
      def parent = file.parentFile
      def files = parent.list()
      StringBuilder builder = new StringBuilder()
      builder.append("${parent.absolutePath}:\n")
      files.each { String name -> builder.append("  $name\n") }
      throw new FileNotFoundException("$text: $file not found in $builder")
    }
  }

  /**
   * Repeat a probe until it succeeds, if it does not execute a failure
   * closure then raise an exception with the supplied message
   * @param probe probe
   * @param timeout time in millis before giving up
   * @param sleepDur sleep between failing attempts
   * @param args map of arguments to the probe
   * @param failIfUnsuccessful if the probe fails after all the attempts
   * —should it raise an exception
   * @param failureMessage message to include in exception raised
   * @param failureHandler closure to invoke prior to the failure being raised
   */
  protected void repeatUntilSuccess(
      String action,
      Closure probe,
      int timeout,
      int sleepDur,
      Map args,
      boolean failIfUnsuccessful,
      String failureMessage,
      Closure failureHandler) {
    log.debug("Probe $action timelimit $timeout")
    if (timeout < 1000) {
      fail("Timeout $timeout too low: milliseconds are expected, not seconds")
    }
    int attemptCount = 1
    boolean succeeded = false;
    boolean completed = false;
    Duration duration = new Duration(timeout)
    duration.start();
    while (!completed) {
      Outcome outcome = (Outcome) probe(args)
      if (outcome.equals(Outcome.Success)) {
        // success
        log.debug("Success after $attemptCount attempt(s)")
        succeeded = true;
        completed = true;
      } else if (outcome.equals(Outcome.Retry)) {
        // failed but retry possible
        attemptCount++;
        completed = duration.limitExceeded
        if (!completed) {
          log.debug("Attempt $attemptCount failed")
          sleep(sleepDur)
        }
      } else if (outcome.equals(Outcome.Fail)) {
        // fast fail
        log.debug("Fast fail of probe")
        completed = true;
      }
    }
    if (!succeeded) {
      if (duration.limitExceeded) {
        log.info("probe timed out after $timeout and $attemptCount attempts")
      }
      if (failureHandler) {
        failureHandler()
      }
      if (failIfUnsuccessful) {
        fail(failureMessage)
      }
    }
  }

  /**
   * Get a web page and deserialize the supplied JSON into
   * an instance of the specific class.
   * @param clazz class to deserialize to
   * @param appmaster URL to base AM
   * @param subpath subpath under AM
   * @return the parsed data type
   */
  public <T> T fetchType(
      Class<T> clazz, String appmaster, String subpath) {

    def json = getWebPage(
        appmaster,
        RestPaths.SLIDER_PATH_APPLICATION + subpath)
    return (T) deser(clazz, json);
  }

  public <T> T deser(Class<T> clazz, String json) {
    JsonSerDeser serDeser = new JsonSerDeser(clazz)
    T ctree = (T) serDeser.fromJson(json)
    return ctree
  }

  public <T> T deser(Class<T> clazz, byte[] data) {
    JsonSerDeser serDeser = new JsonSerDeser(clazz)
    T ctree = (T) serDeser.fromBytes(data)
    return ctree
  }
  
  public ConfTreeOperations fetchConfigTree(
      String appmaster, String subpath) {
    ConfTree ctree = fetchType(ConfTree, appmaster, subpath)
    ConfTreeOperations tree = new ConfTreeOperations(ctree)
    return tree
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
    Map < String, T > results = [:]
    subpaths.each { String it ->
      results[it] = (fetchType(clazz, appmaster, it))
    }
    return results;
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
}
