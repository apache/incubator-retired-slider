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

package org.apache.slider.funtest.framework

import groovy.transform.CompileStatic
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ExitUtil
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.api.ClusterDescription
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.common.params.Arguments
import org.apache.slider.client.SliderClient
import org.apache.slider.test.SliderTestUtils
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.rules.Timeout
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import static org.apache.slider.common.SliderExitCodes.*
import static FuntestProperties.*
import static Arguments.*
import static org.apache.slider.common.params.SliderActions.*
import static org.apache.slider.common.SliderXMLConfKeysForTesting.*

@CompileStatic
abstract class CommandTestBase extends SliderTestUtils {
  private static final Logger log =
      LoggerFactory.getLogger(CommandTestBase.class);
  
  public static final String SLIDER_CONF_DIR = sysprop(SLIDER_CONF_DIR_PROP)
  public static final String SLIDER_BIN_DIR = sysprop(SLIDER_BIN_DIR_PROP)
  public static final File SLIDER_BIN_DIRECTORY = new File(
      SLIDER_BIN_DIR).canonicalFile
  public static final File SLIDER_SCRIPT = new File(
      SLIDER_BIN_DIRECTORY,
      "bin/slider").canonicalFile
  public static final File SLIDER_CONF_DIRECTORY = new File(SLIDER_CONF_DIR).canonicalFile
  public static final File SLIDER_CONF_XML = new File(SLIDER_CONF_DIRECTORY,
      CLIENT_CONFIG_FILENAME).canonicalFile

  public static final YarnConfiguration SLIDER_CONFIG
  public static final int THAW_WAIT_TIME
  public static final int FREEZE_WAIT_TIME

  public static final int ACCUMULO_LAUNCH_WAIT_TIME
  public static final int SLIDER_TEST_TIMEOUT
  public static final boolean ACCUMULO_TESTS_ENABLED

  public static final boolean FUNTESTS_ENABLED
  public static final boolean AGENTTESTS_ENABLED


  static {
    SLIDER_CONFIG = new ConfLoader().loadSliderConf(SLIDER_CONF_XML); 
    THAW_WAIT_TIME = getTimeOptionMillis(SLIDER_CONFIG, 
        KEY_TEST_THAW_WAIT_TIME,
        1000 * DEFAULT_THAW_WAIT_TIME_SECONDS)
    FREEZE_WAIT_TIME = getTimeOptionMillis(SLIDER_CONFIG,
        KEY_TEST_FREEZE_WAIT_TIME,
        1000 * DEFAULT_TEST_FREEZE_WAIT_TIME_SECONDS)
    SLIDER_TEST_TIMEOUT = getTimeOptionMillis(SLIDER_CONFIG,
        KEY_TEST_TIMEOUT,
        1000 * DEFAULT_TEST_TIMEOUT_SECONDS)
    ACCUMULO_LAUNCH_WAIT_TIME = getTimeOptionMillis(SLIDER_CONFIG,
        KEY_ACCUMULO_LAUNCH_TIME,
        1000 * DEFAULT_ACCUMULO_LAUNCH_TIME_SECONDS)
    FUNTESTS_ENABLED =
        SLIDER_CONFIG.getBoolean(KEY_SLIDER_FUNTESTS_ENABLED, true)
    ACCUMULO_TESTS_ENABLED =
        SLIDER_CONFIG.getBoolean(KEY_TEST_ACCUMULO_ENABLED, false)

    AGENTTESTS_ENABLED =
        SLIDER_CONFIG.getBoolean(KEY_TEST_AGENT_ENABLED, true)
 }

  @Rule
  public final Timeout testTimeout = new Timeout(SLIDER_TEST_TIMEOUT);


  @BeforeClass
  public static void setupTestBase() {
    Configuration conf = loadSliderConf();
    if (SliderUtils.maybeInitSecurity(conf)) {
      log.debug("Security enabled")
      SliderUtils.forceLogin()
    } else {
      log.info "Security off, making cluster dirs broadly accessible"
    }
    SliderShell.confDir = SLIDER_CONF_DIRECTORY
    SliderShell.script = SLIDER_SCRIPT
    log.info("Test using ${HadoopFS.getDefaultUri(SLIDER_CONFIG)} " +
             "and YARN RM @ ${SLIDER_CONFIG.get(YarnConfiguration.RM_ADDRESS)}")
    
    // now patch the settings with the path of the conf direcotry
    
  }

  /**
   * give our thread a name
   */
  @Before
  public void nameThread() {
    Thread.currentThread().name = "JUnit"
  }

  /**
   * Add a jar to the slider classpath
   * @param clazz
   */
  public static void addExtraJar(Class clazz) {
    def jar = SliderUtils.findContainingJarOrFail(clazz)

    def path = jar.absolutePath
    if (!SliderShell.slider_classpath_extra.contains(path)) {
      SliderShell.slider_classpath_extra << path
    }
  }

  public static String sysprop(String key) {
    def property = System.getProperty(key)
    if (!property) {
      throw new RuntimeException("Undefined property $key")
    }
    return property
  }
  
  /**
   * Exec any slider command
   * @param conf
   * @param commands
   * @return the shell
   */
  public static SliderShell slider(List<String> commands) {
    SliderShell shell = new SliderShell(commands)
    shell.execute()
    return shell
  }

  /**
   * Execute an operation, state the expected error code
   * @param exitCode exit code
   * @param commands commands
   * @return
   */
  public static SliderShell slider(int exitCode, List<String> commands) {
    return SliderShell.run(commands, exitCode)
  }

  /**
   * Load the client XML file
   * @return
   */
  public static Configuration loadSliderConf() {
    Configuration conf = (new ConfLoader()).loadSliderConf(SLIDER_CONF_XML)
    return conf
  }

  public static HadoopFS getClusterFS() {
    return HadoopFS.get(SLIDER_CONFIG)
  }

  static SliderShell destroy(String name) {
    slider([
        ACTION_DESTROY, name
    ])
  }

  static SliderShell destroy(int result, String name) {
    slider(result, [
        ACTION_DESTROY, name
    ])
  }

  static SliderShell exists(String name, boolean live = true) {

    List<String> args = [
        ACTION_EXISTS, name
    ]
    if (live) {
      args << Arguments.ARG_LIVE
    }
    slider(args)
  }

  static SliderShell exists(int result, String name, boolean live = true) {
    List<String> args = [
        ACTION_EXISTS, name
    ]
    if (live) {
      args << ARG_LIVE
    }
    slider(result, args)
  }

  static SliderShell freeze(String name) {
    slider([
        ACTION_FREEZE, name
    ])
  }

  static SliderShell getConf(String name) {
    slider([
        ACTION_GETCONF, name
    ])
  }

  static SliderShell getConf(int result, String name) {
    slider(result,
         [
             ACTION_GETCONF, name
         ])
  }

  static SliderShell killContainer(String name, String containerID) {
    slider(0,
         [
             ACTION_KILL_CONTAINER,
             name,
             containerID
         ])
  }
  
  static SliderShell freezeForce(String name) {
    slider([
        ACTION_FREEZE, ARG_FORCE, name
    ])
  }

  static SliderShell list(String name) {
    List<String> cmd = [
        ACTION_LIST
    ]
    if (name != null) {
      cmd << name
    }
    slider(cmd)
  }

  static SliderShell list(int result, String name) {
    List<String> cmd = [
        ACTION_LIST
    ]
    if (name != null) {
      cmd << name
    }
    slider(result, cmd)
  }

  static SliderShell status(String name) {
    slider([
        ACTION_STATUS, name
    ])
  }

  static SliderShell status(int result, String name) {
    slider(result,
         [
             ACTION_STATUS, name
         ])
  }

  static SliderShell thaw(String name) {
    slider([
        ACTION_THAW, name
    ])
  }

  static SliderShell thaw(int result, String name) {
    slider(result,
         [
             ACTION_THAW, name
         ])
  }

  /**
   * Ensure that a cluster has been destroyed
   * @param name
   */
  static void ensureClusterDestroyed(String name) {
    def froze = freezeForce(name)

    def result = froze.ret
    if (result != 0 && result != EXIT_UNKNOWN_INSTANCE) {
      froze.assertExitCode(0)
    }
    destroy(0, name)
  }

  /**
   * If the functional tests are enabled, set up the cluster
   * 
   * @param cluster
   */
  static void setupCluster(String cluster) {
    if (FUNTESTS_ENABLED) {
      ensureClusterDestroyed(cluster)
    }
  }

  /**
   * Teardown operation -freezes cluster, and may destroy it
   * though for testing it is best if it is retained
   * @param name cluster name
   */
  static void teardown(String name) {
    if (FUNTESTS_ENABLED) {
      freezeForce(name)
    }
  }

  /**
   * Assert the exit code is that the cluster is unknown
   * @param shell shell
   */
  public static void assertSuccess(SliderShell shell) {
    assertExitCode(shell, 0)
  }
  /**
   * Assert the exit code is that the cluster is unknown
   * @param shell shell
   */
  public static void assertUnknownCluster(SliderShell shell) {
    assertExitCode(shell, EXIT_UNKNOWN_INSTANCE)
  }

  /**
   * Assert a shell exited with a given error code
   * if not the output is printed and an assertion is raised
   * @param shell shell
   * @param errorCode expected error code
   */
  public static void assertExitCode(SliderShell shell, int errorCode) {
    shell.assertExitCode(errorCode)
  }

  /**
   * Create a connection to the cluster by execing the status command
   *
   * @param clustername
   * @return
   */
  SliderClient bondToCluster(Configuration conf, String clustername) {

    String address = getRequiredConfOption(conf, YarnConfiguration.RM_ADDRESS)

    ServiceLauncher<SliderClient> launcher = launchClientAgainstRM(
        address,
        ["exists", clustername],
        conf)

    int exitCode = launcher.serviceExitCode
    if (exitCode) {
      throw new ExitUtil.ExitException(exitCode, "exit code = $exitCode")
    }
    SliderClient sliderClient = launcher.service
    sliderClient.deployedClusterName = clustername
    return sliderClient;
  }

  /**
   * Create or build a slider cluster (the action is set by the first verb)
   * @param action operation to invoke: ACTION_CREATE or ACTION_BUILD
   * @param clustername cluster name
   * @param roles map of rolename to count
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @param clusterOps map of key=value cluster options to set with the --option arg
   * @return shell which will have executed the command.
   */
  public SliderShell createOrBuildSliderCluster(
      String action,
      String clustername,
      Map<String, Integer> roles,
      List<String> extraArgs,
      boolean blockUntilRunning,
      Map<String, String> clusterOps) {
    assert action != null
    assert clustername != null



    List<String> roleList = [];
    roles.each { String role, Integer val ->
      log.info("Role $role := $val")
      roleList << ARG_COMPONENT << role << Integer.toString(val)
    }

    List<String> argsList = [action, clustername]

    argsList << ARG_ZKHOSTS <<
      SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.REGISTRY_ZK_QUORUM)
    

    if (blockUntilRunning) {
      argsList << ARG_WAIT << Integer.toString(THAW_WAIT_TIME)
    }

    argsList += roleList;

    //now inject any cluster options
    clusterOps.each { String opt, String val ->
      argsList << ARG_OPTION << opt.toString() << val.toString();
    }

    if (extraArgs != null) {
      argsList += extraArgs;
    }
    slider(0, argsList)
  }

  /**
   * Create a slider cluster
   * @param clustername cluster name
   * @param roles map of rolename to count
   * @param extraArgs list of extra args to add to the creation command
   * @param blockUntilRunning block until the AM is running
   * @param clusterOps map of key=value cluster options to set with the --option arg
   * @return launcher which will have executed the command.
   */
  public SliderShell createSliderApplication(
      String clustername,
      Map<String, Integer> roles,
      List<String> extraArgs,
      boolean blockUntilRunning,
      Map<String, String> clusterOps) {
    return createOrBuildSliderCluster(
        ACTION_CREATE,
        clustername,
        roles,
        extraArgs,
        blockUntilRunning,
        clusterOps)
  }

  public Path buildClusterPath(String clustername) {
    return new Path(clusterFS.homeDirectory, "${SliderKeys.SLIDER_BASE_DIRECTORY}/cluster/${clustername}")
  }


  public ClusterDescription killAmAndWaitForRestart(
      SliderClient sliderClient, String cluster) {
    
    assert cluster
    slider(0, [
        ACTION_AM_SUICIDE, cluster,
        ARG_EXITCODE, "1",
        ARG_WAIT, "1000",
        ARG_MESSAGE, "suicide"
    ])



    def sleeptime = SLIDER_CONFIG.getInt( KEY_AM_RESTART_SLEEP_TIME,
                                        DEFAULT_AM_RESTART_SLEEP_TIME)
    sleep(sleeptime)
    ClusterDescription status

    try {
      // am should have restarted it by now
      // cluster is live
      exists(0, cluster, true)

      status = sliderClient.clusterDescription
    } catch (SliderException e) {
      if (e.exitCode == EXIT_BAD_STATE) {
        log.error(
            "Property $YarnConfiguration.RM_AM_MAX_ATTEMPTS may be too low")
      }
      throw e;
    }
    return status
  }

  /**
   * if tests are not enabled: skip them  
   */
  public static void assumeFunctionalTestsEnabled() {
    assume(FUNTESTS_ENABLED, "Functional tests disabled")
  }

  public static void assumeAccumuloTestsEnabled() {
    assumeFunctionalTestsEnabled()
    assume(ACCUMULO_TESTS_ENABLED, "Accumulo tests disabled")
  }

  public void assumeAgentTestsEnabled() {
    assumeFunctionalTestsEnabled()
    assume(AGENTTESTS_ENABLED, "Agent tests disabled")
  }

}
