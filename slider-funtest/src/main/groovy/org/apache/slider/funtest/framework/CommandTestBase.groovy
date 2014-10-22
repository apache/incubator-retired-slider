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
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.registry.client.api.RegistryConstants
import org.apache.hadoop.util.ExitUtil
import org.apache.hadoop.util.Shell
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.tools.ConfigHelper
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.api.ClusterDescription
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.client.SliderClient
import org.apache.slider.test.SliderTestUtils
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.rules.Timeout
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import static org.apache.slider.common.SliderExitCodes.*
import static org.apache.slider.funtest.framework.FuntestProperties.*
import static org.apache.slider.common.params.Arguments.*
import static org.apache.slider.common.params.SliderActions.*
import static org.apache.slider.common.SliderXMLConfKeysForTesting.*

@CompileStatic
abstract class CommandTestBase extends SliderTestUtils {
  private static final Logger log =
      LoggerFactory.getLogger(CommandTestBase.class);

  public static final String SLIDER_CONF_DIR = sysprop(SLIDER_CONF_DIR_PROP)
  public static final String SLIDER_TAR_DIR = sysprop(SLIDER_BIN_DIR_PROP)
  public static final File SLIDER_TAR_DIRECTORY = new File(
      SLIDER_TAR_DIR).canonicalFile
  public static final File SLIDER_SCRIPT = new File(
      SLIDER_TAR_DIRECTORY,
      BIN_SLIDER).canonicalFile
  public static final File SLIDER_SCRIPT_PYTHON = new File(
      SLIDER_TAR_DIRECTORY,
      BIN_SLIDER_PYTHON).canonicalFile
  public static final File SLIDER_CONF_DIRECTORY = new File(
      SLIDER_CONF_DIR).canonicalFile
  public static final File SLIDER_CONF_XML = new File(SLIDER_CONF_DIRECTORY,
      CLIENT_CONFIG_FILENAME).canonicalFile
  public static final YarnConfiguration SLIDER_CONFIG
  public static final int THAW_WAIT_TIME
  public static final int FREEZE_WAIT_TIME

  public static final int SLIDER_TEST_TIMEOUT

  public static final String YARN_RAM_REQUEST

  /**
   * Keytab for secure cluster
   */
  public static final String TEST_AM_KEYTAB
  static File keytabFile

  /**
   * shell-escaped ~ symbol. On windows this does
   * not need to be escaped
   */
  public static final String TILDE
  
  /*
  Static initializer for test configurations. If this code throws exceptions
  (which it may) the class will not be instantiable.
   */
  static {
    new HdfsConfiguration()
    ConfigHelper.registerDeprecatedConfigItems();
    SLIDER_CONFIG = ConfLoader.loadSliderConf(SLIDER_CONF_XML, true);
    THAW_WAIT_TIME = getTimeOptionMillis(SLIDER_CONFIG,
        KEY_TEST_THAW_WAIT_TIME,
        1000 * DEFAULT_THAW_WAIT_TIME_SECONDS)
    FREEZE_WAIT_TIME = getTimeOptionMillis(SLIDER_CONFIG,
        KEY_TEST_FREEZE_WAIT_TIME,
        1000 * DEFAULT_TEST_FREEZE_WAIT_TIME_SECONDS)
    SLIDER_TEST_TIMEOUT = getTimeOptionMillis(SLIDER_CONFIG,
        KEY_TEST_TIMEOUT,
        1000 * DEFAULT_TEST_TIMEOUT_SECONDS)

    YARN_RAM_REQUEST = SLIDER_CONFIG.getTrimmed(
        KEY_TEST_YARN_RAM_REQUEST,
        DEFAULT_YARN_RAM_REQUEST)

    TEST_AM_KEYTAB = SLIDER_CONFIG.getTrimmed(
        KEY_TEST_AM_KEYTAB)
    
    

    TILDE = Shell.WINDOWS? "~" : "\\~" 
  }

  @Rule
  public final Timeout testTimeout = new Timeout(SLIDER_TEST_TIMEOUT);


  @BeforeClass
  public static void setupTestBase() {
    Configuration conf = loadSliderConf();

    SliderShell.confDir = SLIDER_CONF_DIRECTORY
    
    // choose python script if on windows or the launch key recommends it
    // 
    boolean python = SLIDER_CONFIG.getBoolean(KEY_LAUNCH_PYTHON, false)
    SliderShell.scriptFile =
        (SliderShell.windows || python) ? SLIDER_SCRIPT_PYTHON : SLIDER_SCRIPT
    
    //set the property of the configuration directory
    def path = SLIDER_CONF_DIRECTORY.absolutePath
    SLIDER_CONFIG.set(ENV_SLIDER_CONF_DIR, path)
    // locate any hadoop conf dir
    def hadoopConf = SLIDER_CONFIG.getTrimmed(ENV_HADOOP_CONF_DIR)
    if (hadoopConf) {
      File hadoopConfDir = new File(hadoopConf).canonicalFile
      // propagate the value to the client config
      SliderShell.setEnv(ENV_HADOOP_CONF_DIR, hadoopConfDir.absolutePath)
    }

    if (SliderUtils.maybeInitSecurity(conf)) {
      log.debug("Security enabled")
      SliderUtils.forceLogin()
      // now look for the security key
/*
      if (!TEST_AM_KEYTAB) {
        fail("Security keytab is not defined in $KEY_TEST_AM_KEYTAB")
      }
      keytabFile = new File(TEST_AM_KEYTAB)
      if (!keytabFile.exists()) {
        throw new FileNotFoundException("Security keytab ${keytabFile} " +
                    " defined in $KEY_TEST_AM_KEYTAB")
      }
*/

    } else {
      log.info "Security is off"
    }
    
    log.info("Test using ${HadoopFS.getDefaultUri(SLIDER_CONFIG)} " +
             "and YARN RM @ ${SLIDER_CONFIG.get(YarnConfiguration.RM_ADDRESS)}")
  }

  /**
   * give the test thread a name
   */
  @Before
  public void nameThread() {
    Thread.currentThread().name = "JUnit"
  }

  /**
   * Add a configuration file at a given path
   * @param dir directory containing the file
   * @param filename filename
   * @return true if the file was found
   * @throws IOException loading problems (other than a missing file)
   */
  public static boolean maybeAddConfFile(File dir, String filename) throws IOException {
    File confFile = new File(dir, filename)
    if (confFile.isFile()) {
      ConfigHelper.addConfigurationFile(SLIDER_CONFIG, confFile, true)
      log.debug("Loaded $confFile")
      return true;
    } else {
      log.debug("Did not find $confFile —skipping load")
      return false;
    }
  }
  
  /**
   * Add a jar to the slider classpath by looking up a class and determining
   * its containing JAR
   * @param clazz class inside the JAR
   */
  public static void addExtraJar(Class clazz) {
    def jar = SliderUtils.findContainingJarOrFail(clazz)

    def path = jar.absolutePath
    if (!SliderShell.slider_classpath_extra.contains(path)) {
      SliderShell.slider_classpath_extra << path
    }
  }

  /**
   * Resolve a system property, throwing an exception if it is not present
   * @param key property name
   * @return the value
   * @throws RuntimeException if the property is not set
   */
  public static String sysprop(String key) {
    def property = System.getProperty(key)
    if (!property) {
      throw new RuntimeException("Undefined property $key")
    }
    return property
  }

  /**
   * Print to system out
   * @param string
   */
  static void println(String s) {
    System.out.println(s)
  }
  
  /**
   * Print a newline to system out
   * @param string
   */
  static void println() {
    System.out.println()
  }
  
  /**
   * Exec any slider command
   * @param conf
   * @param commands
   * @return the shell
   */
  public static SliderShell slider(Collection<String> commands) {
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
  public static SliderShell slider(int exitCode, Collection<String> commands) {
    return SliderShell.run(exitCode, commands)
  }

  /**
   * Load the client XML file
   * @return
   */
  public static Configuration loadSliderConf() {
    Configuration conf = ConfLoader.loadSliderConf(SLIDER_CONF_XML, true)
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
      args << ARG_LIVE
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


  static SliderShell freeze(
      int exitCode,
      String name,
      Collection<String> args) {
    slider(exitCode, [ACTION_FREEZE, name] + args)
  }

  /**
   * Stop cluster: no exit code checking
   * @param name
   * @param args
   * @return
   */
  static SliderShell freeze(String name, Collection<String> args) {
    slider([ACTION_FREEZE, name] + args)
  }

  static SliderShell freezeForce(String name) {
    freeze(name, [ARG_FORCE])
  }

  static SliderShell killContainer(String name, String containerID) {
    slider(0,
        [
            ACTION_KILL_CONTAINER,
            name,
            containerID
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

  static SliderShell thaw(String name, Collection<String> args) {
    slider(0, [ACTION_THAW, name] + args)
  }

  static SliderShell resolve(int result, Collection<String> commands) {
    slider(result,
        [ACTION_RESOLVE] + commands
    )
  }

  static SliderShell registry(int result, Collection<String> commands) {
    slider(result,
        [ACTION_REGISTRY] + commands
    )
  }

  static SliderShell registry(Collection<String> commands) {
    slider(0,
        [ACTION_REGISTRY] + commands
    )
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
    ensureClusterDestroyed(cluster)
  }

  /**
   * Teardown operation -freezes cluster, and may destroy it
   * though for testing it is best if it is retained
   * @param name cluster name
   */
  static void teardown(String name) {
    freezeForce(name)
  }

  /**
   * Assert the exit code is that the cluster is 0
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

    List<String> argsList = [action, clustername]

    argsList << ARG_ZKHOSTS <<
    SLIDER_CONFIG.getTrimmed(RegistryConstants.KEY_REGISTRY_ZK_QUORUM)


    if (blockUntilRunning) {
      argsList << ARG_WAIT << Integer.toString(THAW_WAIT_TIME)
    }

    List<String> roleList = [];
    roles.each { String role, Integer val ->
      log.info("Role $role := $val")
      roleList << ARG_COMPONENT << role << Integer.toString(val)
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

  /**
   * Create a templated slider app
   * @param name name
   * @param appTemplate application template
   * @param resourceTemplate resource template
   * @return the shell
   */
  public SliderShell createTemplatedSliderApplication(
      String name,
      String appTemplate,
      String resourceTemplate,
      List<String> extraArgs=[]) {
    List<String> commands = [
        ACTION_CREATE, name,
        ARG_TEMPLATE, appTemplate,
        ARG_RESOURCES, resourceTemplate
    ]
    
    maybeAddCommandOption(commands,
        [ARG_COMP_OPT, SliderKeys.COMPONENT_AM, SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME],
        SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME));
    maybeAddCommandOption(commands,
        [ARG_COMP_OPT, SliderKeys.COMPONENT_AM,  SliderXmlConfKeys.KEY_HDFS_KEYTAB_DIR ],
        SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.KEY_HDFS_KEYTAB_DIR));
    maybeAddCommandOption(commands,
        [ARG_COMP_OPT, SliderKeys.COMPONENT_AM, SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH],
        SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH));
    maybeAddCommandOption(commands,
        [ARG_COMP_OPT, SliderKeys.COMPONENT_AM, SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL],
        SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL));
    commands.addAll(extraArgs)
    SliderShell shell = slider(LauncherExitCodes.EXIT_SUCCESS, commands)
    return shell
  }

  /**
   * If the option is not null/empty, add the command and the option
   * @param args arg list being built up
   * @param command command to add option
   * @param option option to probe and use
   * @return the (possibly extended) list
   */
  public List<String> maybeAddCommandOption(
      List<String> args, List<String> commands, String option) {
    if ( SliderUtils.isSet(option)) {
      args.addAll(commands)
      args << option
    }
    return args
  }

  public Path buildClusterPath(String clustername) {
    return new Path(
        clusterFS.homeDirectory,
        "${SliderKeys.SLIDER_BASE_DIRECTORY}/cluster/${clustername}")
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


    def sleeptime = SLIDER_CONFIG.getInt(KEY_AM_RESTART_SLEEP_TIME,
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

  protected void ensureApplicationIsUp(String clusterName) {
    repeatUntilTrue(this.&isApplicationUp,
        SLIDER_CONFIG.getInt(KEY_TEST_INSTANCE_LAUNCH_TIME,
            DEFAULT_INSTANCE_LAUNCH_TIME_SECONDS),
        1000,
        ['arg1': clusterName],
        true,
        'Application did not start, aborting test.')
  }

  protected boolean isApplicationUp(Map<String, String> args) {
    String applicationName = args['arg1'];
    return isApplicationInState(YarnApplicationState.RUNNING, applicationName);
  }

  protected boolean isApplicationUp(String applicationName) {
    return isApplicationInState(YarnApplicationState.RUNNING, applicationName);
  }

  /**
   * 
   * @param yarnState
   * @param applicationName
   * @return
   */
  public static boolean isApplicationInState(YarnApplicationState yarnState, String applicationName) {
    SliderShell shell = slider(
      [ACTION_EXISTS, applicationName, ARG_STATE, yarnState.toString()])

    return shell.ret == 0
  }

  protected void repeatUntilTrue(Closure c, int maxAttempts, int sleepDur, Map args,
                                 boolean failIfUnsuccessful = false, String message = "") {
    int attemptCount = 0
    while (attemptCount < maxAttempts) {
      if (c(args)) {
        break
      };
      attemptCount++;

      if (failIfUnsuccessful) {
        assert attemptCount != maxAttempts, message
      }

      sleep(sleepDur)
    }
  }

}
