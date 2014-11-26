/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.test

import groovy.util.logging.Slf4j
import org.apache.commons.io.FileUtils
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DFSConfigKeys
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.service.ServiceOperations
import org.apache.hadoop.util.Shell
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.slider.api.ClusterNode
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.ActionFreezeArgs
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.common.tools.Duration
import org.apache.slider.common.tools.SliderFileSystem
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.exceptions.ErrorStrings
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.main.ServiceLauncherBaseTest
import org.apache.slider.server.appmaster.SliderAppMaster
import org.junit.After
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.rules.Timeout

import static org.apache.slider.test.KeysForTests.*

import static org.apache.slider.common.SliderXMLConfKeysForTesting.*;
/**
 * Base class for mini cluster tests -creates a field for the
 * mini yarn cluster
 */
//@CompileStatic
@Slf4j
public abstract class YarnMiniClusterTestBase extends ServiceLauncherBaseTest {
  /**
   * Mini YARN cluster only
   */
  public static final int CLUSTER_GO_LIVE_TIME = 3 * 60 * 1000
  public static final int CLUSTER_STOP_TIME = 1 * 60 * 1000

  public static final int SIGTERM = -15
  public static final int SIGKILL = -9
  public static final int SIGSTOP = -17
  public static
  final String NO_ARCHIVE_DEFINED = "Archive configuration option not set: "
  /**
   * RAM for the YARN containers: {@value}
   */
  public static final String YRAM = "256"

  public static final String FIFO_SCHEDULER = "org.apache.hadoop.yarn.server" +
    ".resourcemanager.scheduler.fifo.FifoScheduler";


  public static final YarnConfiguration SLIDER_CONFIG = SliderUtils.createConfiguration();
  
  public static boolean kill_supported;
  
  static {
    SLIDER_CONFIG.setInt(SliderXmlConfKeys.KEY_AM_RESTART_LIMIT, 1)
    SLIDER_CONFIG.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 100)
    SLIDER_CONFIG.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false)
    SLIDER_CONFIG.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false)
    SLIDER_CONFIG.setBoolean(SliderXmlConfKeys.KEY_SLIDER_AM_DEPENDENCY_CHECKS_DISABLED,
        true)
    SLIDER_CONFIG.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 1)
  }


  public int thawWaitTime = DEFAULT_THAW_WAIT_TIME_SECONDS * 1000
  public int freezeWaitTime = DEFAULT_TEST_FREEZE_WAIT_TIME_SECONDS * 1000
  public int sliderTestTimeout = DEFAULT_TEST_TIMEOUT_SECONDS * 1000
  public boolean teardownKillall = DEFAULT_TEARDOWN_KILLALL

  protected MiniDFSCluster hdfsCluster
  protected MiniYARNCluster miniCluster
  protected boolean switchToImageDeploy = false
  protected boolean imageIsRemote = false
  protected URI remoteImageURI
  private int clusterCount =1;

  protected List<SliderClient> clustersToTeardown = [];

  /**
   * This is set in a system property
   */

  @Rule
  public Timeout testTimeout = new Timeout(
      getTimeOptionMillis(testConfiguration,
          KEY_TEST_TIMEOUT,
          DEFAULT_TEST_TIMEOUT_SECONDS * 1000)
  )

  /**
   * Clent side test: validate system env before launch
   */
  @BeforeClass
  public static void checkClientEnv() {
    SliderUtils.validateSliderClientEnvironment(null)
  }

  protected String buildClustername(String clustername) {
    return clustername ?: createClusterName()
  }

  /**
   * Create the cluster name from the method name and an auto-incrementing
   * counter.
   * @return a cluster name
   */
  protected String createClusterName() {
    def base = methodName.getMethodName().toLowerCase(Locale.ENGLISH)
    if (clusterCount++ > 1) {
      base += "-$clusterCount"
    }
    return base
  }


  @Override
  void setup() {
    super.setup()
    def testConf = testConfiguration;
    thawWaitTime = getTimeOptionMillis(testConf,
        KEY_TEST_THAW_WAIT_TIME,
        thawWaitTime)
    freezeWaitTime = getTimeOptionMillis(testConf,
        KEY_TEST_FREEZE_WAIT_TIME,
        freezeWaitTime)
    sliderTestTimeout = getTimeOptionMillis(testConf,
        KEY_TEST_TIMEOUT,
        sliderTestTimeout)
    teardownKillall =
        testConf.getBoolean(KEY_TEST_TEARDOWN_KILLALL,
            teardownKillall)

  }

  @After
  public void teardown() {
    describe("teardown")
    stopRunningClusters();
    stopMiniCluster();
  }

  protected void addToTeardown(SliderClient client) {
    clustersToTeardown << client;
  }

  protected void addToTeardown(ServiceLauncher<SliderClient> launcher) {
    SliderClient sliderClient = launcher?.service
    if (sliderClient) {
      addToTeardown(sliderClient)
    }
  }

  /**
   * Work out if kill is supported
   */
  @BeforeClass 
  public static void checkKillSupport() {
    kill_supported = !Shell.WINDOWS
  }

  /**
   * Kill any java process with the given grep pattern
   * @param grepString string to grep for
   */
  public int killJavaProcesses(String grepString, int signal) {

    def commandString
    if (!Shell.WINDOWS) {
      GString killCommand = "jps -l| grep ${grepString} | awk '{print \$1}' | xargs kill $signal"
      log.info("Command command = $killCommand")

      commandString = ["bash", "-c", killCommand]
    } else {
      // windows
      if (!kill_supported) {
        return -1;
      }

      /*
      "jps -l | grep "String" | awk "{print $1}" | xargs -n 1 taskkill /PID"
       */
      GString killCommand = "\"jps -l | grep \"${grepString}\" | gawk \"{print \$1}\" | xargs -n 1 taskkill /f /PID\""
      commandString = ["CMD", "/C", killCommand]
    }
    Process command = commandString.execute()
    def exitCode = command.waitFor()

    log.info(command.in.text)
    log.error(command.err.text)
    return exitCode
  }

  /**
   * Kill all processes which match one of the list of grepstrings
   * @param greps
   * @param signal
   */
  public void killJavaProcesses(List<String> greps, int signal) {
    for (String grep : greps) {
      killJavaProcesses(grep, signal)
    }
  }

  protected YarnConfiguration getConfiguration() {
    return SLIDER_CONFIG;
  }

  /**
   * Stop any running cluster that has been added
   */
  public void stopRunningClusters() {
    clustersToTeardown.each { SliderClient client ->
      try {
        maybeStopCluster(client, "", "Teardown at end of test case", true);
      } catch (Exception e) {
        log.warn("While stopping cluster " + e, e);
      }
    }
  }

  public void stopMiniCluster() {
    Log commonslog = LogFactory.getLog(this.class)
    ServiceOperations.stopQuietly(commonslog, miniCluster)
    hdfsCluster?.shutdown();
  }

  /**
   * Create and start a minicluster
   * @param name cluster/test name; if empty one is created from the junit method
   * @param conf configuration to use
   * @param noOfNodeManagers #of NMs
   * @param numLocalDirs #of local dirs
   * @param numLogDirs #of log dirs
   * @param startZK create a ZK micro cluster
   * @param startHDFS create an HDFS mini cluster
   * @return the name of the cluster
   */
  protected String createMiniCluster(String name,
                                   YarnConfiguration conf,
                                   int noOfNodeManagers,
                                   int numLocalDirs,
                                   int numLogDirs,
                                   boolean startHDFS) {
    assertNativeLibrariesPresent();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
    conf.set(YarnConfiguration.RM_SCHEDULER, FIFO_SCHEDULER);
    patchDiskCapacityLimits(conf)
    SliderUtils.patchConfiguration(conf)
    name = buildClustername(name)
    miniCluster = new MiniYARNCluster(
        name,
        noOfNodeManagers,
        numLocalDirs,
        numLogDirs)
    miniCluster.init(conf)
    miniCluster.start();
    // health check
    assertMiniClusterDisksHealthy(miniCluster)
    if (startHDFS) {
      createMiniHDFSCluster(name, conf)
    }
    return name
  }

  public patchDiskCapacityLimits(YarnConfiguration conf) {
    conf.setFloat(
        YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE,
        99.0f)
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY, 2 * 1024 * 1024)
  }

  /**
   * Probe for the disks being healthy in a mini cluster. Only the first
   * NM is checked
   * @param miniCluster
   */
  public static void assertMiniClusterDisksHealthy(MiniYARNCluster miniCluster) {
    def healthy = miniCluster.getNodeManager(
        0).nodeHealthChecker.diskHandler.areDisksHealthy()
    assert healthy, "Disks on test cluster unhealthy —may be full"
  }

  /**
   * Create a mini HDFS cluster and save it to the hdfsClusterField
   * @param name
   * @param conf
   */
  public void createMiniHDFSCluster(String name, YarnConfiguration conf) {
    hdfsCluster = buildMiniHDFSCluster(name, conf)
  }

  /**
   * Inner work building the mini dfs cluster
   * @param name
   * @param conf
   * @return
   */
  public static MiniDFSCluster buildMiniHDFSCluster(
      String name,
      YarnConfiguration conf) {
    assertNativeLibrariesPresent();

    File baseDir = new File("./target/hdfs/$name").absoluteFile;
    //use file: to rm it recursively
    FileUtil.fullyDelete(baseDir)
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.absolutePath)
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf)

    def cluster = builder.build()
    return cluster
  }

  /**
   * Launch the client with the specific args against the MiniMR cluster
   * launcher ie expected to have successfully completed
   * @param conf configuration
   * @param args arg list
   * @return the return code
   */
  protected ServiceLauncher<SliderClient> launchClientAgainstMiniMR(Configuration conf,
                                                                      List args) {
    ServiceLauncher<SliderClient> launcher = launchClientNoExitCodeCheck(conf, args)
    int exited = launcher.serviceExitCode
    if (exited != 0) {
      throw new SliderException(exited, "Launch failed with exit code $exited")
    }
    return launcher;
  }

  /**
   * Launch the client with the specific args against the MiniMR cluster
   * without any checks for exit codes
   * @param conf configuration
   * @param args arg list
   * @return the return code
   */
  public ServiceLauncher<SliderClient> launchClientNoExitCodeCheck(
      Configuration conf,
      List args) {
    assert miniCluster != null
    return launchClientAgainstRM(RMAddr, args, conf)
  }


  /**
   * Kill all Slider Services. 
   * @param signal
   */
  public int killAM(int signal) {
    killJavaProcesses(SliderAppMaster.SERVICE_CLASSNAME_SHORT, signal)
  }

  /**
   * List any java process with the given grep pattern
   * @param grepString string to grep for
   */
  public String lsJavaProcesses() {
    Process bash = ["jps","-v"].execute()
    bash.waitFor()
    String out = bash.in.text
    log.info(out)
    String err = bash.err.text
    log.error(err)
    return out + "\n" + err
  }


  
  public YarnConfiguration getTestConfiguration() {
    YarnConfiguration conf = getConfiguration()
    conf.addResource(SLIDER_TEST_XML)
    return conf;
  }

  protected String getRMAddr() {
    assert miniCluster != null
    String addr = miniCluster.config.get(YarnConfiguration.RM_ADDRESS)
    assert addr != null;
    assert addr != "";
    return addr
  }

  /**
   * return the default filesystem, which is HDFS if the miniDFS cluster is
   * up, file:// if not
   * @return a filesystem string to pass down
   */
  protected String getFsDefaultName() {
    return buildFsDefaultName(hdfsCluster)
  }

  public static String buildFsDefaultName(MiniDFSCluster miniDFSCluster) {
    if (miniDFSCluster) {
      return "hdfs://localhost:${miniDFSCluster.nameNodePort}/"
    } else {
      return "file:///"
    }
  }

  protected String getWaitTimeArg() {
    return WAIT_TIME_ARG;
  }

  protected int getWaitTimeMillis(Configuration conf) {

    return WAIT_TIME * 1000;
  }

  /**
   * Create a cluster
   * @param clustername cluster name
   * @param roles map of rolename to count
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @param clusterOps map of key=value cluster options to set with the --option arg
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher<SliderClient> createCluster(
      String clustername,
      Map<String, Integer> roles,
      List<String> extraArgs,
      boolean deleteExistingData,
      boolean blockUntilRunning,
      Map<String, String> clusterOps) {
    createOrBuildCluster(
        SliderActions.ACTION_CREATE,
        clustername,
        roles,
        extraArgs,
        deleteExistingData,
        blockUntilRunning,
        clusterOps)
  }

  /**
   * Create or build a cluster (the action is set by the first verb)
   * @param action operation to invoke: SliderActions.ACTION_CREATE or SliderActions.ACTION_BUILD
   * @param clustername cluster name
   * @param roles map of rolename to count
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @param clusterOps map of key=value cluster options to set with the --option arg
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher<SliderClient> createOrBuildCluster(String action, String clustername,
    Map<String, Integer> roles, List<String> extraArgs, boolean deleteExistingData,
    boolean blockUntilRunning, Map<String, String> clusterOps) {
    assert clustername != null
    assert miniCluster != null
    // update action should keep existing data
    def config = miniCluster.config
    if (deleteExistingData && !SliderActions.ACTION_UPDATE.equals(action)) {
      HadoopFS dfs = HadoopFS.get(new URI(fsDefaultName), config)

      def sliderFileSystem = new SliderFileSystem(dfs, config)
      Path clusterDir = sliderFileSystem.buildClusterDirPath(clustername)
      log.info("deleting instance data at $clusterDir")
      //this is a safety check to stop us doing something stupid like deleting /
      assert clusterDir.toString().contains("/.slider/")
      rigorousDelete(sliderFileSystem, clusterDir, 60000)
    }


    List<String> componentList = [];
    roles.each { String role, Integer val ->
      log.info("Component $role := $val")
      componentList << Arguments.ARG_COMPONENT << role << Integer.toString(val)
    }

    List<String> argsList = [
        action, clustername,
        Arguments.ARG_MANAGER, RMAddr,
        Arguments.ARG_FILESYSTEM, fsDefaultName,
        Arguments.ARG_DEBUG,
        Arguments.ARG_CONFDIR, confDir
    ]
    if (blockUntilRunning) {
      argsList << Arguments.ARG_WAIT << WAIT_TIME_ARG
    }

    argsList += extraCLIArgs
    argsList += componentList;
    argsList += imageCommands

    //now inject any cluster options
    clusterOps.each { String opt, String val ->
      argsList << Arguments.ARG_OPTION << opt << val;
    }

    if (extraArgs != null) {
      argsList += extraArgs;
    }
    ServiceLauncher<SliderClient> launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(config),
        //varargs list of command line params
        argsList
    )
    assert launcher.serviceExitCode == 0
    SliderClient client = launcher.service
    if (blockUntilRunning) {
      client.monitorAppToRunning(new Duration(CLUSTER_GO_LIVE_TIME))
    }
    return launcher;
  }

  /**
   * Delete with some pauses and backoff; designed to handle slow delete
   * operation in windows
   * @param dfs
   * @param path
   */
  public void rigorousDelete(
      SliderFileSystem sliderFileSystem,
      Path path, long timeout) {

    if (path.toUri().scheme == "file") {
      File dir = new File(path.toUri().getPath());
      rigorousDelete(dir, timeout)
    } else {
      Duration duration = new Duration(timeout)
      duration.start()
      HadoopFS dfs = sliderFileSystem.fileSystem
      boolean deleted = false;
      while (!deleted && !duration.limitExceeded) {
        dfs.delete(path, true)
        deleted = !dfs.exists(path)
        if (!deleted) {
          sleep(1000)
        }
      }
    }
    sliderFileSystem.verifyDirectoryNonexistent(path)
  }

  /**
   * Delete with some pauses and backoff; designed to handle slow delete
   * operation in windows
   * @param dir dir to delete
   * @param timeout timeout in millis
   */
  public void rigorousDelete(File dir, long timeout) {
    Duration duration = new Duration(timeout)
    duration.start()
    boolean deleted = false;
    while (!deleted && !duration.limitExceeded) {
      FileUtils.deleteQuietly(dir)
      deleted = !dir.exists()
      if (!deleted) {
        sleep(1000)
      }
    }
    if (!deleted) {
      // noisy delete raises an IOE
      FileUtils.deleteDirectory(dir)
    }
  }

  /**
   * Add arguments to launch Slider with.
   *
   * Extra arguments are added after standard arguments and before roles.
   *
   * @return additional arguments to launch Slider with
   */
  protected List<String> getExtraCLIArgs() {
    []
  }

  public String getConfDir() {
    return resourceConfDirURI
  }

  /**
   * Get the key for the application
   * @return
   */
  public String getApplicationHomeKey() {
    failNotImplemented()
    null
  }

  /**
   * Get the archive path -which defaults to the local one
   * @return
   */
  public String getArchivePath() {
    return localArchive
  }
  /**
   * Get the local archive -the one defined in the test configuration
   * @return a possibly null/empty string
   */
  public final String getLocalArchive() {
    return testConfiguration.getTrimmed(archiveKey)
  }
  /**
   * Get the key for archives in tests
   * @return
   */
  public String getArchiveKey() {
    failNotImplemented()
    null
  }

  /**
   * Merge a k-v pair into a simple k=v string; simple utility
   * @param key key
   * @param val value
   * @return the string to use after a -D option
   */
  public String define(String key, String val) {
    return "$key=$val"
  }

  public void assumeTestEnabled(boolean flag) {
    assume(flag, "test disabled")
  }
  
  public void assumeArchiveDefined() {
    String archive = archivePath
    boolean defined = archive != null && archive != ""
    if (!defined) {
      log.warn(NO_ARCHIVE_DEFINED + archiveKey);
    }
    assume(defined,NO_ARCHIVE_DEFINED + archiveKey)
  }
  
  /**
   * Assume that application home is defined. This does not check that the
   * path is valid -that is expected to be a failure on tests that require
   * application home to be set.
   */
  public void assumeApplicationHome() {
    assume(applicationHome != null && applicationHome != "",
        "Application home dir option not set " + applicationHomeKey)
  }


  public String getApplicationHome() {
    return testConfiguration.getTrimmed(applicationHomeKey)
  }

  public List<String> getImageCommands() {
    if (switchToImageDeploy) {
      // its an image that had better be defined
      assert archivePath
      if (!imageIsRemote) {
        // its not remote, so assert it exists
        File f = new File(archivePath)
        assert f.exists()
        return [Arguments.ARG_IMAGE, f.toURI().toString()]
      } else {
        assert remoteImageURI

        // if it is remote, then its whatever the archivePath property refers to
        return [Arguments.ARG_IMAGE, remoteImageURI.toString()];
      }
    } else {
      assert applicationHome
      assert new File(applicationHome).exists();
      return [Arguments.ARG_APP_HOME, applicationHome]
    }
  }

  /**
   * Start a cluster that has already been defined
   * @param clustername cluster name
   * @param extraArgs list of extra args to add to the creation command
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher<SliderClient> thawCluster(String clustername, List<String> extraArgs, boolean blockUntilRunning) {
    assert clustername != null
    assert miniCluster != null

    List<String> argsList = [
        SliderActions.ACTION_THAW, clustername,
        Arguments.ARG_MANAGER, RMAddr,
        Arguments.ARG_WAIT, WAIT_TIME_ARG,
        Arguments.ARG_FILESYSTEM, fsDefaultName,
    ]
    argsList += extraCLIArgs

    if (extraArgs != null) {
      argsList += extraArgs;
    }
    ServiceLauncher<SliderClient> launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        argsList
    )
    assert launcher.serviceExitCode == 0
    SliderClient client = (SliderClient) launcher.service
    if (blockUntilRunning) {
      client.monitorAppToRunning(new Duration(CLUSTER_GO_LIVE_TIME))
    }
    return launcher;
  }


  /**
   * Get the resource configuration dir in the source tree
   * @return
   */
  public File getResourceConfDir() {
    File f = new File(testConfigurationPath).absoluteFile;
    if (!f.exists()) {
      throw new FileNotFoundException("Resource configuration directory $f not found")
    }
    return f;
  }

  public String getTestConfigurationPath() {
    failNotImplemented()
    null;
  }

  /**
   get a URI string to the resource conf dir that is suitable for passing down
   to the AM -and works even when the default FS is hdfs
   */
  public String getResourceConfDirURI() {;
    return resourceConfDir.absoluteFile.toURI().toString()
  }

  /**
   * Log an application report
   * @param report
   */
  public void logReport(ApplicationReport report) {
    log.info(SliderUtils.reportToString(report));
  }

  /**
   * Log a list of application reports
   * @param apps
   */
  public void logApplications(List<ApplicationReport> apps) {
    apps.each { ApplicationReport r -> logReport(r) }
  }

  /**
   * Wait for the cluster live; fail if it isn't within the (standard) timeout
   * @param client client
   * @return the app report of the live cluster
   */
  public ApplicationReport waitForClusterLive(SliderClient client) {
    return waitForClusterLive(client, CLUSTER_GO_LIVE_TIME)
  }

  /**
   * force kill the application after waiting for
   * it to shut down cleanly
   * @param client client to talk to
   * @return the final application report
   */
  public ApplicationReport waitForAppToFinish(SliderClient client) {

    int waitTime = getWaitTimeMillis(client.config)
    return waitForAppToFinish(client, waitTime)
  }

  /**
   * force kill the application after waiting for
   * it to shut down cleanly
   * @param client client to talk to 
   * @param waitTime time in milliseconds to wait
   * @return the final application report
   */
  public static ApplicationReport waitForAppToFinish(
      SliderClient client,
      int waitTime) {
    ApplicationReport report = client.monitorAppToState(
        YarnApplicationState.FINISHED,
        new Duration(waitTime));
    if (report == null) {
      log.info("Forcibly killing application")
      dumpClusterStatus(client, "final application status")
      //list all the nodes' details
      List<ClusterNode> nodes = listNodesInRole(client, "")
      if (nodes.empty) {
        log.info("No live nodes")
      }
      nodes.each { ClusterNode node -> log.info(node.toString())}
      client.forceKillApplication("timed out waiting for application to complete");
      report = client.applicationReport
    }
    return report;
  }

  /**
   * stop the cluster via the stop action -and wait for {@link #CLUSTER_STOP_TIME}
   * for the cluster to stop. If it doesn't
   * @param sliderClient client
   * @param clustername cluster
   * @return the exit code
   */
  public int clusterActionFreeze(SliderClient sliderClient, String clustername,
                                String message = "action stop",
                                boolean force = false) {
    log.info("Stopping cluster $clustername: $message")
    ActionFreezeArgs freezeArgs  = new ActionFreezeArgs();
    freezeArgs.waittime = CLUSTER_STOP_TIME
    freezeArgs.message = message
    freezeArgs.force = force
    int exitCode = sliderClient.actionFreeze(clustername,
        freezeArgs);
    if (exitCode != 0) {
      log.warn("Cluster stop failed with error code $exitCode")
    }
    return exitCode
  }

  /**
   * Teardown-time cluster termination; will stop the cluster iff the client
   * is not null
   * @param sliderClient client
   * @param clustername name of cluster to teardown
   * @return
   */
  public int maybeStopCluster(
      SliderClient sliderClient,
      String clustername,
      String message,
      boolean force = false) {
    if (sliderClient != null) {
      if (!clustername) {
        clustername = sliderClient.deployedClusterName;
      }
      //only stop a cluster that exists
      if (clustername) {
        return clusterActionFreeze(sliderClient, clustername, message, force);
      }
    }
    return 0;
  }


  String roleMapToString(Map<String,Integer> roles) {
    StringBuilder builder = new StringBuilder()
    roles.each { String name, int value ->
      builder.append("$name->$value ")
    }
    return builder.toString()
  }

  /**
   * Turn on test runs against a copy of the archive that is
   * uploaded to HDFS -this method copies up the
   * archive then switches the tests into archive mode
   */
  public void enableTestRunAgainstUploadedArchive() {
    Path remotePath = copyLocalArchiveToHDFS(localArchive)
    // image mode
    switchToRemoteImageDeploy(remotePath);
  }

  /**
   * Switch to deploying a remote image
   * @param remotePath the remote path to use
   */
  public void switchToRemoteImageDeploy(Path remotePath) {
    switchToImageDeploy = true
    imageIsRemote = true
    remoteImageURI = remotePath.toUri()
  }

  /**
   * Copy a local archive to HDFS
   * @param localArchive local archive
   * @return the path of the uploaded image
   */
  public Path copyLocalArchiveToHDFS(String localArchive) {
    assert localArchive
    File localArchiveFile = new File(localArchive)
    assert localArchiveFile.exists()
    assert hdfsCluster
    Path remoteUnresolvedArchive = new Path(localArchiveFile.name)
    assert FileUtil.copy(
        localArchiveFile,
        hdfsCluster.fileSystem,
        remoteUnresolvedArchive,
        false,
        testConfiguration)
    Path remotePath = hdfsCluster.fileSystem.resolvePath(
        remoteUnresolvedArchive)
    return remotePath
  }

  /**
   * Assert that an operation failed because a cluster is in use
   * @param e exception
   */
  public static void assertFailureClusterInUse(SliderException e) {
    assertExceptionDetails(e,
        SliderExitCodes.EXIT_APPLICATION_IN_USE,
        ErrorStrings.E_CLUSTER_RUNNING)
  }

  /**
   * Create a SliderFileSystem instance bonded to the running FS.
   * The YARN cluster must be up and running already
   * @return
   */
  public SliderFileSystem createSliderFileSystem() {
    HadoopFS dfs = HadoopFS.get(new URI(fsDefaultName), configuration)
    SliderFileSystem hfs = new SliderFileSystem(dfs, configuration)
    return hfs
  }

}
