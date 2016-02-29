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
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.registry.client.api.RegistryConstants
import org.apache.hadoop.util.ExitUtil
import org.apache.hadoop.util.Shell
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.api.StatusKeys
import org.apache.slider.api.types.NodeInformationList
import org.apache.slider.client.SliderClient
import org.apache.slider.common.Constants
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.api.ClusterDescription
import org.apache.slider.common.tools.ConfigHelper
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.launch.SerializedApplicationReport
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.persist.ApplicationReportSerDeser
import org.apache.slider.core.persist.JsonSerDeser
import org.apache.slider.test.SliderTestUtils
import org.apache.slider.test.Outcome;

import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.rules.Timeout
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.net.ssl.SSLException

import static org.apache.slider.common.SliderExitCodes.*
import static org.apache.slider.core.main.LauncherExitCodes.*
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
  public static int THAW_WAIT_TIME
  public static final int FREEZE_WAIT_TIME

  public static final int SLIDER_TEST_TIMEOUT

  public static final String YARN_RAM_REQUEST

  /**
   * Keytab for secure cluster
   */
  public static final String TEST_AM_KEYTAB

  /**
   * shell-escaped ~ symbol. On windows this does
   * not need to be escaped
   */
  public static final String TILDE
  public static final int CONTAINER_LAUNCH_TIMEOUT = 90000
  public static final int PROBE_SLEEP_TIME = 2000
  public static final int REGISTRY_STARTUP_TIMEOUT = 90000
  public static final String E_LAUNCH_FAIL = 'Application did not start'

  public static final WINDOWS_CLUSTER
  /*
  Static initializer for test configurations. If this code throws exceptions
  (which it may) the class will not be instantiable.
   */
  static {
    ConfigHelper.injectSliderXMLResource()
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
    WINDOWS_CLUSTER = SLIDER_CONFIG.getBoolean(KEY_TEST_WINDOWS_CLUSTER, Shell.WINDOWS)
  }

  @Rule
  public final Timeout testTimeout = new Timeout(SLIDER_TEST_TIMEOUT);


  @BeforeClass
  public static void setupTestBase() {
    Configuration conf = loadSliderConf();

    SliderShell.confDir = SLIDER_CONF_DIRECTORY
    
    // choose python script if on windows
    // 
    SliderShell.scriptFile =
        SliderShell.windows ? SLIDER_SCRIPT_PYTHON : SLIDER_SCRIPT
    
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

  public static void logShell(SliderShell shell) {
    shell.dumpOutput();
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
   * used enough in setting properties it's worth pulling out
   * @param key sysprop/conf definition
   * @param val value
   * @return the concatenated string
   */
  static String define(String key, String val) {
    key + "=" + val
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
        ACTION_DESTROY, name, ARG_FORCE
    ])
  }

  static SliderShell destroy(int result, String name) {
    slider(result, [
        ACTION_DESTROY, name, ARG_FORCE
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
    freeze(name, [ARG_FORCE, ARG_WAIT, "10000"])
  }

  /**
   * Non-forced stop, wait some seconds
   * @param name
   * @return
   */
  static SliderShell stop(String name) {
    freeze(name, [ARG_WAIT, "10000"])
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

  static SliderShell lookup(int result, String id, File out) {
    assert id
    def commands = [ACTION_LOOKUP, ARG_ID, id]
    if (out) {
      commands += [ARG_OUTPUT, out.absolutePath]
    }
    slider(result, commands)
  }
  
  static SliderShell lookup(String id, File out) {
    assert id
    def commands = [ACTION_LOOKUP, ARG_ID, id]
    if (out) {
      commands += [ARG_OUTPUT, out.absolutePath]
    }
    slider(commands)
  }

  static SliderShell list(int result, Collection<String> commands =[]) {
    slider(result, [ACTION_LIST] + commands )
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
    slider(0, [ACTION_REGISTRY] + commands)
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
    describe "setting up $cluster"
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
   * Assert that the stdout/stderr streams of the shell contain the string
   * to look for.
   * If the assertion does not hold, the output is logged before
   * the assertion is thrown
   * @param shell
   * @param lookThisUp
   * @param n number of times (default = 1)
   */
  public static void assertOutputContains(
      SliderShell shell,
      String lookThisUp,
      int n = 1) {
    if (!shell.outputContains(lookThisUp)) {
      log.error("Missing $lookThisUp from:")
      shell.dumpOutput()
      fail("Missing $lookThisUp from:\n$shell.out\n$shell.err" )
    }
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
   * Create a slider app using the alternate packaging capability
   * <p>
   * If the extraArgs list does not contain a --wait parm then a wait
   * duration of THAW_WAIT_TIME will be added to the launch args.
   * @param name name
   * @param metaInfo application metaInfo
   * @param appTemplate application template
   * @param resourceTemplate resource template
   * @param extraArgs list of extra arguments to the command
   * @param launchReportFile optional file to save the AM launch report to
   * @return the shell
   */
  public SliderShell createSliderApplicationMinPkg(
      String name,
      String metaInfo,
      String resourceTemplate,
      String appTemplate,
      List<String> extraArgs = [],
      File launchReportFile = null) {

    if (!launchReportFile) {
      launchReportFile = createTempJsonFile()
    }
    // delete any previous copy of the file
    launchReportFile.delete();

    List<String> commands = [
        ACTION_CREATE, name,
        ARG_METAINFO, metaInfo,
        ARG_OUTPUT, launchReportFile.absolutePath
    ]

    if (StringUtils.isNotBlank(appTemplate)) {
      commands << ARG_TEMPLATE << appTemplate
    }
    if (StringUtils.isNotBlank(resourceTemplate)) {
      commands << ARG_RESOURCES << resourceTemplate
    }
    if (!extraArgs.contains(ARG_WAIT)) {
      commands << ARG_WAIT << Integer.toString(THAW_WAIT_TIME)
    }

    maybeAddCommandOption(commands,
        [ARG_COMP_OPT, SliderKeys.COMPONENT_AM, SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME],
        SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME));
    maybeAddCommandOption(commands,
        [ARG_COMP_OPT, SliderKeys.COMPONENT_AM, SliderXmlConfKeys.KEY_HDFS_KEYTAB_DIR],
        SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.KEY_HDFS_KEYTAB_DIR));
    maybeAddCommandOption(commands,
        [ARG_COMP_OPT, SliderKeys.COMPONENT_AM, SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH],
        SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH));
    maybeAddCommandOption(commands,
        [ARG_COMP_OPT, SliderKeys.COMPONENT_AM, SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL],
        SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL));
    commands.addAll(extraArgs)
    SliderShell shell = new SliderShell(commands)
    if (0 != shell.execute()) {
      // app has failed.

      // grab the app report of the last known instance of this app
      // which may not be there if it was a config failure; may be out of date
      // from a previous run
      log.error("Launch failed with exit code ${shell.ret}")
      shell.dumpOutput()

      // now grab that app report if it is there
      def appReport = maybeLookupFromLaunchReport(launchReportFile)
      String extraText = ""
      if (appReport) {
        log.error("Application report:\n$appReport")
        extraText = appReport.diagnostics
      }

      fail("Application Launch Failure, exit code  ${shell.ret}\n${extraText}")
    }
    return shell
  }

  /**
   * Create a templated slider app.
   * <p>
   * If the extraArgs list does not contain a --wait parm then a wait 
   * duration of THAW_WAIT_TIME will be added to the launch args.
   * @param name name
   * @param appTemplate application template
   * @param resourceTemplate resource template
   * @param extraArgs list of extra arguments to the command
   * @param launchReportFile optional file to save the AM launch report to
   * @return the shell
   */
  public SliderShell createTemplatedSliderApplication(
      String name,
      String appTemplate,
      String resourceTemplate,
      List<String> extraArgs = [],
      File launchReportFile = null) {

    if (!launchReportFile) {
      launchReportFile = createTempJsonFile()
    }
    // delete any previous copy of the file
    launchReportFile.delete();
    
    List<String> commands = [
        ACTION_CREATE, name,
        ARG_TEMPLATE, appTemplate,
        ARG_RESOURCES, resourceTemplate,
        ARG_OUTPUT, launchReportFile.absolutePath
    ]
    
    if (!extraArgs.contains(ARG_WAIT)) {
      commands << ARG_WAIT << Integer.toString(THAW_WAIT_TIME)
    }

    maybeAddCommandOption(commands,
        [ARG_COMP_OPT, SliderKeys.COMPONENT_AM, SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME],
        SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME));
    maybeAddCommandOption(commands,
        [ARG_COMP_OPT, SliderKeys.COMPONENT_AM, SliderXmlConfKeys.KEY_HDFS_KEYTAB_DIR],
        SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.KEY_HDFS_KEYTAB_DIR));
    maybeAddCommandOption(commands,
        [ARG_COMP_OPT, SliderKeys.COMPONENT_AM, SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH],
        SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH));
    maybeAddCommandOption(commands,
        [ARG_COMP_OPT, SliderKeys.COMPONENT_AM, SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL],
        SLIDER_CONFIG.getTrimmed(SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL));

    commands << ARG_COMP_OPT << SliderKeys.COMPONENT_AM <<
        "env." + Constants.HADOOP_JAAS_DEBUG << "true";
    commands.addAll(extraArgs)
    SliderShell shell = new SliderShell(commands)
    if (0 != shell.execute()) {
      // app has failed.

      // grab the app report of the last known instance of this app
      // which may not be there if it was a config failure; may be out of date
      // from a previous run
      log.error("Launch failed with exit code ${shell.ret}")
      shell.dumpOutput()

      // now grab that app report if it is there
      def appReport = maybeLookupFromLaunchReport(launchReportFile)
      String extraText = ""
      if (appReport) {
        log.error("Application report:\n$appReport")
        extraText = appReport.diagnostics
      }

      fail("Application Launch Failure, exit code  ${shell.ret}\n${extraText}")
    }
    return shell
  }

  /**
   * If the option is not null/empty, add the command and the option
   * @param args arg list being built up
   * @param command command to add option
   * @param option option to probe and use
   * @return the (possibly extended) list
   */
  public static List<String> maybeAddCommandOption(
      List<String> args, List<String> commands, String option) {
    if ( SliderUtils.isSet(option)) {
      args.addAll(commands)
      args << option
    }
    return args
  }

  public static SerializedApplicationReport maybeLoadAppReport(File reportFile) {
    if (reportFile.exists() && reportFile.length()> 0) {
      ApplicationReportSerDeser serDeser = new ApplicationReportSerDeser()
      def report = serDeser.fromFile(reportFile)
      return report
    }
    return null;
  }

  public static SerializedApplicationReport loadAppReport(File reportFile) {
    if (reportFile.exists() && reportFile.length() > 0) {
      ApplicationReportSerDeser serDeser = new ApplicationReportSerDeser()
      def report = serDeser.fromFile(reportFile)
      return report
    }else {
      throw new FileNotFoundException(reportFile.absolutePath)
    }
  }

  public static SerializedApplicationReport maybeLookupFromLaunchReport(File launchReport) {
    def report = maybeLoadAppReport(launchReport)
    if (report) {
      return lookupApplication(report.applicationId)
    } else {
      return null
    }
  }

  /**
   * Lookup an application, return null if loading failed
   * @param id application ID
   * @return an application report or null
   */
  public static SerializedApplicationReport lookupApplication(String id) {
    File reportFile = createTempJsonFile();
    try {
      def shell = lookup(id, reportFile)
      if (shell.ret == 0) {
        return maybeLoadAppReport(reportFile)
      } else {
        log.warn("Lookup operation failed with ${shell.ret}")
        shell.dumpOutput()
        return null
      }
    } finally {
      reportFile.delete()
    }
  }

  /**
   * Lookup an application, return null if loading failed
   * @param id application ID
   * @return an application report or null
   */
  public static NodeInformationList listNodes(String name = "", boolean healthy = false, String label = "") {
    File reportFile = createTempJsonFile();
    try {
      def shell = nodes(name, reportFile, healthy, label)
      if (log.isDebugEnabled()) {
        shell.dumpOutput()
      }
      JsonSerDeser<NodeInformationList> serDeser = NodeInformationList.createSerializer();
      serDeser.fromFile(reportFile)
    } finally {
      reportFile.delete()
    }
  }

  /**
   * List cluster nodes
   * @param name of cluster or null
   * @param out output file (or null)
   * @param healthy list healthy nodes only
   * @param label label to filter on
   * @return output
   */
  static SliderShell nodes(String name, File out = null, boolean healthy = false, String label = "") {
    def commands = [ACTION_NODES]
    if (label) {
      commands += [ ARG_LABEL, label]
    }
    if (name) {
      commands << name
    }
    if (out) {
      commands += [ARG_OUTPUT, out.absolutePath]
    }
    if (healthy) {
      commands << ARG_HEALTHY
    }
    slider(0, commands)
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

    sleep(5000)
    ensureApplicationIsUp(cluster)
    return sliderClient.clusterDescription
  }

  /**
   * Kill an AM and await restrt
   * @param sliderClient
   * @param application
   * @param appId
   * @return
   */
  public void killAmAndWaitForRestart(String application, String appId) {

    assert application
    slider(0, [
        ACTION_AM_SUICIDE, application,
        ARG_EXITCODE, "1",
        ARG_WAIT, "500",
        ARG_MESSAGE, "suicide"
    ])

    // app goes live
    log.info "awaiting app to enter RUNNING state"
    ensureYarnApplicationIsUp(appId)
  }

  /**
   * Spinning operation to perform a registry call
   * @param application application
   * @param target target URL For diagnostics
   */
  protected void ensureRegistryCallSucceeds(String application, URL target) {
    def failureText = "Application registry is not accessible at $target after $REGISTRY_STARTUP_TIMEOUT ms"
    repeatUntilSuccess("registry",
        this.&isRegistryAccessible,
        REGISTRY_STARTUP_TIMEOUT,
        PROBE_SLEEP_TIME,
        [application: application],
        true,
        failureText) {
      describe failureText
      exists(application, true).dumpOutput()
      if (target != null) {
        // GET the text of the target if provided
        def body = target.text
        def errorText = "Registry probe to $target failed with response:\n$body"
        log.error(errorText)
        fail(errorText)
      }

      // trigger a failure on registry lookup
      registry(0, [ARG_NAME, application, ARG_LISTEXP])
    }
  }

  /**
   * wait for an application to come up
   * @param application
   */
  protected void ensureApplicationIsUp(String application) {
    repeatUntilSuccess("await application up",
        this.&isApplicationRunning,
        instanceLaunchTime,
        PROBE_SLEEP_TIME,
        [application: application],
        true,
        E_LAUNCH_FAIL) {
      describe "final state of app that tests say is not up"
      exists(application, true).dumpOutput()
    }
  }

  /**
   * Wait for an application to move out of a specific state. Don't fail this
   * test if the application is never found to move of the state. State
   * transitions sometimes happen so fast that short lived transient states
   * might not caught during probes. Hence just exit success, after timeout.
   * 
   * @param application
   * @param yarnState
   */
  protected void ensureApplicationNotInState(String application,
    YarnApplicationState yarnState) {
    repeatUntilSuccess("await application to be not in state " + yarnState,
        this.&isApplicationNotInState,
        10000,
        0,
        [
          application: application,
          yarnState: yarnState
        ],
        false,
        "") {
      describe "final state of app for not in state check"
      exists(application).dumpOutput()
    }
  }

  /**
   * Is the registry accessible for an application?
   * @param args argument map containing <code>"application"</code>
   * @return probe outcome
   */
  protected Outcome isRegistryAccessible(Map<String, String> args) {
    String applicationName = args['application'];
    SliderShell shell = slider(
        [
            ACTION_REGISTRY,
            ARG_NAME,
            applicationName,
            ARG_LISTEXP
        ])
    if (EXIT_SUCCESS != shell.execute()) {
      logShell(shell)
    }
    return Outcome.fromBool(EXIT_SUCCESS == shell.execute())
  }

  /**
   * Probe for an application running; uses <code>exists</code> operation
   * @param args argument map containing <code>"application"</code>
   * @return
   */
  protected Outcome isApplicationRunning(Map<String, String> args) {
    String applicationName = args['application'];
    return Outcome.fromBool(isApplicationUp(applicationName))
  }

  /**
   * Use <code>exists</code> operation to probe for an application being up
   * @param applicationName app name
   * @return true if it s running
   */
  protected boolean isApplicationUp(String applicationName) {
    return isApplicationInState(
        applicationName,
        YarnApplicationState.RUNNING
    );
  }

  /**
   * Probe for an application to be in a state other than the specified state;
   * uses <code>exists</code> operation
   * @param args argument map containing <code>"application"</code> and
   *   <code>state</code>
   * @return
   */
  protected Outcome isApplicationNotInState(Map<String, String> args) {
    String applicationName = args['application'];
    YarnApplicationState yarnState = YarnApplicationState
      .valueOf(args['yarnState']);
    return Outcome.fromBool(!isApplicationInState(applicationName, yarnState))
  }

  /**
   * is an application in a desired yarn state. Uses the <code>exists</code>
   * CLI operation
   * @param yarnState
   * @param applicationName
   * @return
   */
  public static boolean isApplicationInState(
      String applicationName,
      YarnApplicationState yarnState) {
    SliderShell shell = slider(
      [ACTION_EXISTS, applicationName, ARG_STATE, yarnState.toString()])
    return shell.ret == 0
  }

  /**
   * Probe callback for is the the app running or not
   * @param args map where 'applicationId' must m
   * @return
   */

  protected static Outcome isYarnApplicationRunning(Map<String, String> args) {
    String applicationId = args['applicationId'];
    return isYarnApplicationInState(applicationId,
        YarnApplicationState.RUNNING, true)
  }

  /**
   * Probe callback for is the the app running or not
   * @param args map where 'applicationId' must m
   * @return
   */
  protected static Outcome isYarnApplicationInExactState(Map<String, String> args) {
    String applicationId = args['applicationId'];
    String state = args['state']
    def desired = YarnApplicationState.valueOf(state)
    return isYarnApplicationInState(applicationId, desired, false)
  }

  /**
   * is a yarn application in a desired yarn state 
   * @param yarnState
   * @param applicationName
   * @return an outcome indicating whether the app is at the state, on its way
   * or has gone past
   */
  public static Outcome isYarnApplicationRunning(
      String applicationId) {
    return isYarnApplicationInState(applicationId,
        YarnApplicationState.RUNNING, true)
  }

  /**
   * Probe for a YARN application being in a given state
   * @param applicationId app id
   * @param yarnStat desired state
   * @return success for a match, retry if state below desired, and fail if
   * above it
   */
  public static Outcome isYarnApplicationInState(
      String applicationId, YarnApplicationState yarnState, boolean failfast) {
    YarnApplicationState appState = lookupYarnAppState(applicationId)
    if (yarnState == appState) {
      return Outcome.Success;
    }

    if (failfast && appState.ordinal() > yarnState.ordinal()) {
      log.debug("App state $appState past desired state $yarnState: failing")
      // app has passed beyond hope
      return Outcome.Fail
    }
    return Outcome.Retry
  }

  /**
   * Look up the YARN application by ID, get its application record
   * @param applicationId the application ID
   * @return the application state
   */
  public static YarnApplicationState lookupYarnAppState(String applicationId) {
    def sar = lookupApplication(applicationId)
    assert sar != null;
    YarnApplicationState appState = YarnApplicationState.valueOf(sar.state)
    return appState
  }

  /**
   * Assert an application is in a given state; fail if not
   * @param applicationId appId
   * @param expectedState expected state
   */
  public static void assertInYarnState(String applicationId,
      YarnApplicationState expectedState) {
    def applicationReport = lookupApplication(applicationId)
    assert expectedState.toString() == applicationReport.state 
  }

  /**
   * Wait for the YARN app to come up. This will fail fast
   * @param launchReportFile launch time file containing app id
   * @return the app ID
   */
  protected String ensureYarnApplicationIsUp(File launchReportFile) {
    def id = loadAppReport(launchReportFile).applicationId
    ensureYarnApplicationIsUp(id)
    return id;
  }
 
  /**
   * Wait for the YARN app to come up. This will fail fast
   * @param applicationId
   */
  protected void ensureYarnApplicationIsUp(String applicationId) {
    repeatUntilSuccess("await yarn application Running",
        this.&isYarnApplicationRunning,
        instanceLaunchTime,
        PROBE_SLEEP_TIME,
        [applicationId: applicationId],
        true,
        E_LAUNCH_FAIL) {
      describe "final state of application"
      def sar = lookupApplication(applicationId)

      def message = E_LAUNCH_FAIL + "\n$sar"
      log.error(message)
      fail(message)
    }
  }

  /**
   * Wait for the YARN app to come up. This will fail fast
   * @param applicationId
   */
  protected void awaitYarnApplicationAccepted(String applicationId) {
    repeatUntilSuccess("Await Yarn App Accepted",
        this.&isYarnApplicationInExactState,
        instanceLaunchTime,
        1000,
        [applicationId: applicationId,
         state: YarnApplicationState.ACCEPTED.toString()],
        true,
        "application never reached accepted state") {
      describe "app did not enter accepted"
      def sar = lookupApplication(applicationId)

      def message = 'Application did not enter ACCEPTED state' + "\n$sar"
      log.error(message)
      fail(message)
    }
  }
  /**
   * Get the expected launch time. Default is the configuration option
   * {@link FuntestProperties#KEY_TEST_INSTANCE_LAUNCH_TIME} and
   * default value {@link FuntestProperties#KEY_TEST_INSTANCE_LAUNCH_TIME}
   * @return
   */
  public int getInstanceLaunchTime() {
    return SLIDER_CONFIG.getInt(KEY_TEST_INSTANCE_LAUNCH_TIME,
        DEFAULT_INSTANCE_LAUNCH_TIME_SECONDS) * 1000;
  }

  public String getInfoAmWebUrl(String applicationName) {
    ClusterDescription cd = execStatus(applicationName);
    String urlString = cd.getInfo("info.am.web.url");
    return urlString;
  }

  public ClusterDescription execStatus(String application) {
    File statusFile = File.createTempFile("status", ".json")
    try {
      slider(EXIT_SUCCESS,
          [
              ACTION_STATUS,
              application,
              ARG_OUTPUT, statusFile.absolutePath
          ])

      assert statusFile.exists()
      return ClusterDescription.fromFile(statusFile)
    } finally {
      statusFile.delete()
    }
  }

  public int queryRequestedCount(String  application, String role) {
    ClusterDescription cd = execStatus(application)

    if (cd.statistics.size() == 0) {
      log.debug("No statistics entries")
    }
    
    if (!cd.statistics[role]) {
      log.debug("No stats for role $role")
      return 0;
    }
    def statsForRole = cd.statistics[role]

    def requested = statsForRole[StatusKeys.STATISTICS_CONTAINERS_REQUESTED]
    assert null != statsForRole[StatusKeys.STATISTICS_CONTAINERS_REQUESTED]
    return requested
  }

  /**
   * Probe: has the requested container count of a specific role been reached?
   * @param args map with: "application", "role", "limit"
   * @return success on a match, retry if not
   */
  Outcome hasRequestedContainerCountReached(Map<String, String> args) {
    String application = args['application']
    String role = args['role']
    int expectedCount = args['limit'].toInteger();

    int requestedCount = queryRequestedCount(application, role)
    log.debug("requested $role count = $requestedCount; expected=$expectedCount")
    return Outcome.fromBool(requestedCount >= expectedCount)
  }

  void expectContainerRequestedCountReached(String application, String role, int limit,
      int container_launch_timeout) {

    repeatUntilSuccess(
        "await requested container count",
        this.&hasRequestedContainerCountReached,
        container_launch_timeout,
        PROBE_SLEEP_TIME,
        [limit      : Integer.toString(limit),
         role       : role,
         application: application],
        true,
        "requested countainer count not reached") {
      int requestedCount = queryRequestedCount(application, role)

      def message = "expected request count of $role = $limit not reached, " +
                    "actual: $requestedCount after $container_launch_timeout ms"
      describe message
      ClusterDescription cd = execStatus(application);
      log.info("Parsed status \n$cd")
      fail(message)
    }
  }

  /**
   * Assert that exactly the number of containers are live
   * @param clustername name of cluster
   * @param component component to probe
   * @param count count
   * @return
   */
  public ClusterDescription assertContainersLive(String clustername,
      String component,
      int count) {
    ClusterDescription cd = execStatus(clustername)
    assertContainersLive(cd, component, count)
    return cd;
  }

  /**
   * Outcome checker for the live container count
   * @param args argument map, must contain "application", "component" and "live"
   * @return
   */
  Outcome hasLiveContainerCountReached(Map<String, String> args) {
    assert args['application']
    assert args['component']
    assert args['live']
    String application = args['application']
    String component = args['component']
    int expectedCount = args['live'].toInteger();
    ClusterDescription cd = execStatus(application)
    def actual = extractLiveContainerCount(cd, component)
    log.debug("live $component count = $actual; expected=$expectedCount")
    return Outcome.fromBool(actual >= expectedCount)
  }

  /**
   * Wait for the live container count to be reached
   * @param application application name
   * @param component component name
   * @param expected expected count
   * @param container_launch_timeout launch timeout
   */
  void expectLiveContainerCountReached(
      String application,
      String component,
      int expected,
      int container_launch_timeout) {

    repeatUntilSuccess(
        "await live container count",
        this.&hasLiveContainerCountReached,
        container_launch_timeout,
        PROBE_SLEEP_TIME,
        [live      : Integer.toString(expected),
         component  : component,
         application: application],
        true,
        "live countainer count not reached") {
      describe "live container count not reached"
      assertContainersLive(application, component, expected)
    }
  }

  public int queryFailedCount(String  application, String role) {
    ClusterDescription cd = execStatus(application)
    if (cd.statistics.size() == 0) {
      log.debug("No statistics entries")
    }
    if (!cd.statistics[role]) {
      log.debug("No stats for role $role")
      return 0;
    }
    def statsForRole = cd.statistics[role]

    def failed = statsForRole[StatusKeys.STATISTICS_CONTAINERS_FAILED]
    assert null != failed
    return failed
  }

  /**
   * Probe: has the failed container count of a specific role been reached?
   * @param args map with: "application", "role", "limit"
   * @return success on a match, retry if not
   */
  Outcome hasFailedContainerCountReached(Map<String, String> args) {
    String application = args['application']
    String role = args['role']
    int expectedCount = args['limit'].toInteger();

    int failedCount = queryFailedCount(application, role)
    log.debug("failed $role count = $failedCount; expected=$expectedCount")
    return Outcome.fromBool(failedCount >= expectedCount)
  }

  /**
   * Wait for the failed container count to be reached
   * @param application application name
   * @param component component name
   * @param expected expected count
   * @param container_launch_timeout launch timeout
   */
  void expectFailedContainerCountReached(
      String application,
      String component,
      int expected,
      int container_launch_timeout) {
    repeatUntilSuccess(
        "await failed container count",
        this.&hasFailedContainerCountReached,
        container_launch_timeout,
        PROBE_SLEEP_TIME,
        [limit      : Integer.toString(expected),
         role       : component,
         application: application],
        true,
        "failed countainer count not reached") {
      describe "failed container count not reached"
    }
  }

  /**
   * Spin for <code>REGISTRY_STARTUP_TIMEOUT</code> waiting
   * for the output of the registry command to contain the specified
   * values
   * @param outfile file to save the output to
   * @param commands commands to execute
   * @param match text to match on
   */
  public void awaitRegistryOutfileContains(
      File outfile,
      List<String> commands,
      String match) {
    def errorText = "failed to find $match in output. Check your hadoop versions and the agent logs"
    repeatUntilSuccess("registry",
        this.&generatedFileContains,
        REGISTRY_STARTUP_TIMEOUT,
        PROBE_SLEEP_TIME,
        [
            text    : match,
            filename: outfile.absolutePath,
            command : commands
        ],
        true,
        errorText) {
      slider(0, commands).dumpOutput()
      if (!outfile.length()) {
        log.warn("No exported entries.\n" +
                 "Is your application using a consistent version of Hadoop? ")
      }
      fail(errorText + "\n" + outfile.text)
    }
  }
 
  /**
   * probe for the output {@code  command: List} containing {@code text}
   * @param args argument map containing the required parameters
   * @return probe outcome
   */
  protected Outcome commandOutputContains(Map args) {
    String text = args['text'];
    List<String> command = (List < String >)args['command']
    SliderShell shell = slider(0, command)
    return Outcome.fromBool(shell.outputContains(text))
  }

  /**
   * probe for a command {@code command: List} succeeeding
   * @param args argument map containing the required parameters
   * @return probe outcome
   */
  protected Outcome commandSucceeds(Map args) {
    List<String> command = (List<String>) args['command']
    SliderShell shell = slider(command)
    return Outcome.fromBool(shell.ret == 0)
  }

  /**
   * probe for a command {@code command: List} generating a file 'filename'
   * which must contain the text 'text'
   * @param args argument map containing the required parameters
   * @return probe outcome
   */
  protected Outcome generatedFileContains(Map args) {
    List<String> command = (List<String>) args['command']
    String text = args['text'];
    String filename = args['filename'];
    File f = new File(filename)
    f.delete()
    SliderShell shell = slider(command)
    if (shell.ret != 0) {
      return Outcome.Retry
    }
    shell.dumpOutput()
    assert f.exists()
    return Outcome.fromBool(f.text.contains(text))
  }

  /**
   * Probe callback for is the the app root web page up.
   * Having a securty exception is in fact legit.
   * @param args map where 'applicationId' must be set
   * @return the outcome
   */
  protected static Outcome isRootWebPageUp(
      Map<String, String> args) {

    assert args['applicationId'] != null
    String applicationId = args['applicationId'];
    def sar = lookupApplication(applicationId)
    assert sar != null;
    if (!sar.url) {
      return Outcome.Retry;
    }
    try {
      getWebPage(sar.url)
      return Outcome.Success
    } catch (SSLException e) {
      // SSL exception -implies negotiation failure. At this point there is clearly something
      // at the far end -we just don't have the credentials to trust it.
      return Outcome.Success;
    } catch (IOException e) {
      return Outcome.Retry;
    } catch (Exception e) {
      return Outcome.Retry;
    }
  }

  /**
   * Await for the root web page of an app to come up
   * @param applicationId app ID
   * @param launch_timeout launch timeout
   */
  void expectRootWebPageUp(
      String applicationId, int launch_timeout) {

    repeatUntilSuccess(
        "await root web page",
        this.&isRootWebPageUp,
        launch_timeout,
        PROBE_SLEEP_TIME,
        [
         applicationId: applicationId
        ],
        false,
        "web page not up") {

      def sar = lookupApplication(applicationId)
      assert sar != null;
      assert sar.url
      // this is the final failure cause
      getWebPage(sar.url)
    }
  }

  /**
   * Probe callback for the the app root web page up.
   * Raising an SSL exception is considered a sign of liveness.
   * @param args map where 'applicationId' must be set
   * @return the outcome
   */
  protected static Outcome isApplicationURLPublished(
      Map<String, String> args) {

    assert args['applicationId'] != null
    String applicationId = args['applicationId'];
    def sar = lookupApplication(applicationId)
    assert sar != null;
    return sar.url? Outcome.Success : Outcome.Retry
  }

  /**
   * Await for the URL of an app to be listed in the application report
   * @param applicationId app ID
   * @param launch_timeout launch timeout
   */
  void awaitApplicationURLPublished(String applicationId, int launch_timeout) {
    repeatUntilSuccess(
        "await application URL published",
        this.&isApplicationURLPublished,
        launch_timeout,
        PROBE_SLEEP_TIME,
        [
            applicationId: applicationId
        ],
        true,
        "application URL not published") {

      def sar = lookupApplication(applicationId)
      log.error("$applicationId => $sar")
    }
  }

  void assumeTestClusterNotWindows() {
    assume(!WINDOWS_CLUSTER, "Test cluster is windows")
  }
}
