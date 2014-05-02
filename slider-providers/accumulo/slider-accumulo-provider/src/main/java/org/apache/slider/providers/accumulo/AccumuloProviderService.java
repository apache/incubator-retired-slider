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

package org.apache.slider.providers.accumulo;

import com.google.common.net.HostAndPort;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.launch.CommandLineBuilder;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.providers.AbstractProviderService;
import org.apache.slider.providers.ProviderCore;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.registry.zk.BlockingZKWatcher;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.server.services.docstore.utility.EventCallback;
import org.apache.slider.server.services.docstore.utility.EventNotifyingService;
import org.apache.slider.server.services.docstore.utility.ForkedProcessService;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * Server-side accumulo provider
 */
public class AccumuloProviderService extends AbstractProviderService implements
                                                                     ProviderCore,
                                                                     AccumuloKeys,
    SliderKeys {

  protected static final Logger log =
    LoggerFactory.getLogger(AccumuloClientProvider.class);
  private AccumuloClientProvider clientProvider;
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  
  private SliderFileSystem fileSystem = null;

  public AccumuloProviderService() {
    super("accumulo");
  }


  @Override
  public List<ProviderRole> getRoles() {
    return AccumuloRoles.ROLES;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    clientProvider = new AccumuloClientProvider(conf);
  }

  @Override
  public void validateInstanceDefinition(AggregateConf instanceDefinition) throws
      SliderException {
    clientProvider.validateInstanceDefinition(instanceDefinition);
  }

  @Override
  public Configuration loadProviderConfigurationInformation(File confDir)
    throws BadCommandArgumentsException, IOException {

    return loadProviderConfigurationInformation(confDir, SITE_XML);
  }

  /*
   ======================================================================
   Server interface below here
   ======================================================================
  */

  @Override
  public void buildContainerLaunchContext(ContainerLauncher launcher,
      AggregateConf instanceDefinition,
      Container container,
      String role,
      SliderFileSystem fileSystem,
      Path generatedConfPath,
      MapOperations resourceComponent,
      MapOperations appComponent,
      Path containerTmpDirPath) throws
                                                                            IOException,
      SliderException {
    
    this.fileSystem = fileSystem;
    this.instanceDefinition = instanceDefinition;
    
    // Set the environment
    launcher.putEnv(SliderUtils.buildEnvMap(appComponent));

    Map<String, String> env = SliderUtils.buildEnvMap(appComponent);
    launcher.setEnv(ACCUMULO_LOG_DIR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    ConfTreeOperations appConf =
      instanceDefinition.getAppConfOperations();
    String hadoop_home =
      ApplicationConstants.Environment.HADOOP_COMMON_HOME.$();
    MapOperations appConfGlobal = appConf.getGlobalOptions();
    hadoop_home = appConfGlobal.getOption(OPTION_HADOOP_HOME, hadoop_home);
    launcher.setEnv(HADOOP_HOME, hadoop_home);
    launcher.setEnv(HADOOP_PREFIX, hadoop_home);
    
    // By not setting ACCUMULO_HOME, this will cause the Accumulo script to
    // compute it on its own to an absolute path.

    launcher.setEnv(ACCUMULO_CONF_DIR,
            ProviderUtils.convertToAppRelativePath(
              SliderKeys.PROPAGATED_CONF_DIR_NAME));
    launcher.setEnv(ZOOKEEPER_HOME, appConfGlobal.getMandatoryOption(OPTION_ZK_HOME));

    //local resources


    //add the configuration resources
    launcher.addLocalResources(fileSystem.submitDirectory(
        generatedConfPath,
        SliderKeys.PROPAGATED_CONF_DIR_NAME));

    //Add binaries
    //now add the image if it was set
    String imageURI = instanceDefinition.getInternalOperations()
                                        .get(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    fileSystem.maybeAddImagePath(launcher.getLocalResources(), imageURI);

    CommandLineBuilder commandLine = new CommandLineBuilder();
    
    String heap = "-Xmx" + appComponent.getOption(RoleKeys.JVM_HEAP, DEFAULT_JVM_HEAP);
    String opt = "ACCUMULO_OTHER_OPTS";
    if (SliderUtils.isSet(heap)) {
      if (AccumuloKeys.ROLE_MASTER.equals(role)) {
        opt = "ACCUMULO_MASTER_OPTS";
      } else if (AccumuloKeys.ROLE_TABLET.equals(role)) {
        opt = "ACCUMULO_TSERVER_OPTS";
      } else if (AccumuloKeys.ROLE_MONITOR.equals(role)) {
        opt = "ACCUMULO_MONITOR_OPTS";
      } else if (AccumuloKeys.ROLE_GARBAGE_COLLECTOR.equals(role)) {
        opt = "ACCUMULO_GC_OPTS";
      }
      launcher.setEnv(opt, heap);
    }

    //this must stay relative if it is an image
    commandLine.add(providerUtils.buildPathToScript(instanceDefinition,
      "bin", "accumulo"));

    //role is translated to the accumulo one
    commandLine.add(AccumuloRoles.serviceForRole(role));
    
    // Add any role specific arguments to the command line
    String additionalArgs = ProviderUtils.getAdditionalArgs(appComponent);
    if (!StringUtils.isBlank(additionalArgs)) {
      commandLine.add(additionalArgs);
    }

    commandLine.addOutAndErrFiles(role + "-out.txt", role + "-err.txt");


    launcher.addCommand(commandLine.build());
  }
  
  public List<String> buildProcessCommandList(AggregateConf instance,
                                          File confDir,
                                          Map<String, String> env,
                                          String... commands) throws
                                                                IOException,
      SliderException {
    env.put(ACCUMULO_LOG_DIR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    String hadoop_home = System.getenv(HADOOP_HOME);
    MapOperations globalOptions =
      instance.getAppConfOperations().getGlobalOptions();
    hadoop_home = globalOptions.getOption(OPTION_HADOOP_HOME, hadoop_home);
    if (hadoop_home == null) {
      throw new BadConfigException(
        "Undefined env variable/config option: " + HADOOP_HOME);
    }
    ProviderUtils.validatePathReferencesLocalDir("HADOOP_HOME", hadoop_home);
    env.put(HADOOP_HOME, hadoop_home);
    env.put(HADOOP_PREFIX, hadoop_home);
    //buildup accumulo home env variable to be absolute or relative
    String accumulo_home = providerUtils.buildPathToHomeDir(instance,
      "bin", "accumulo");
    File image = new File(accumulo_home);
    String accumuloPath = image.getAbsolutePath();
    env.put(ACCUMULO_HOME, accumuloPath);
    ProviderUtils.validatePathReferencesLocalDir("ACCUMULO_HOME", accumuloPath);
    env.put(ACCUMULO_CONF_DIR, confDir.getAbsolutePath());
    String zkHome = globalOptions.getMandatoryOption(OPTION_ZK_HOME);
    ProviderUtils.validatePathReferencesLocalDir("ZOOKEEPER_HOME", zkHome);

    env.put(ZOOKEEPER_HOME, zkHome);


    String accumuloScript = AccumuloClientProvider.buildScriptBinPath(instance);
    List<String> launchSequence = new ArrayList<String>(8);
    launchSequence.add(0, accumuloScript);
    Collections.addAll(launchSequence, commands);
    return launchSequence;
  }

  /**
   * Accumulo startup is a bit more complex than HBase, as it needs
   * to pre-initialize the data directory.
   *
   * This is done by running an init operation before starting the
   * real master. If the init fails, that is reported to the AM, which
   * then fails the application. 
   * If the init succeeds, the next service in the queue is started -
   * a composite service that starts the Accumulo Master and, in parallel,
   * sends a delayed event to the AM
   *
   * @param instanceDefinition component description
   * @param confDir local dir with the config
   * @param env environment variables above those generated by
   * @param execInProgress callback for the event notification
   * @throws IOException IO problems
   * @throws SliderException anything internal
   */
  @Override
  public boolean exec(AggregateConf instanceDefinition,
                      File confDir,
                      Map<String, String> env,
                      EventCallback execInProgress) throws
                                                 IOException,
      SliderException {


    //now pull in these files and do a bit of last-minute validation
    File siteXML = new File(confDir, SITE_XML);
    Configuration accumuloSite = ConfigHelper.loadConfFromFile(
      siteXML);
    String zkQuorum =
      accumuloSite.get(AccumuloConfigFileOptions.ZOOKEEPER_HOST);
    if (zkQuorum == null) {
      throw new BadConfigException("Accumulo site.xml %s does not contain %s",
                                   siteXML,
                                   AccumuloConfigFileOptions.ZOOKEEPER_HOST);
    } else {
      log.info("ZK Quorum is {}", zkQuorum);
    }
    //now test this
    int timeout = 5000;
    try {
      verifyZookeeperLive(zkQuorum, timeout);
      log.info("Zookeeper is live");
    } catch (KeeperException e) {
      throw new BadClusterStateException("Failed to connect to Zookeeper at %s after %d seconds",
                                         zkQuorum, timeout);
    } catch (InterruptedException e) {
      throw new BadClusterStateException(
        "Interrupted while trying to connect to Zookeeper at %s",
        zkQuorum);
    }
    boolean inited = isInited(instanceDefinition);
    if (inited) {
      // cluster is inited, so don't run anything
      return false;
    }
    List<String> commands;

    log.info("Initializing accumulo datastore {}");
    ConfTreeOperations appConfOperations =
      instanceDefinition.getAppConfOperations();

    ConfTreeOperations internalOperations =
      instanceDefinition.getInternalOperations();
    ConfTreeOperations resourceOperations =
      instanceDefinition.getResourceOperations();
    String accumuloInstanceName = internalOperations.get(OptionKeys.APPLICATION_NAME);
    commands = buildProcessCommandList(instanceDefinition, confDir, env,
                            "init",
                            PARAM_INSTANCE_NAME,
                            providerUtils.getUserName() + "-" + accumuloInstanceName,
                            PARAM_PASSWORD,
                            appConfOperations.getGlobalOptions().getMandatoryOption(
                              OPTION_ACCUMULO_PASSWORD),
                            "--clear-instance-name");


    ForkedProcessService accumulo =
      queueCommand(getName(), env, commands);
    //add a timeout to this process
    accumulo.setTimeout(
      appConfOperations.getGlobalOptions().getOptionInt(
        OPTION_ACCUMULO_INIT_TIMEOUT,
        INIT_TIMEOUT_DEFAULT), 1);
    
    //callback to AM to trigger cluster review is set up to happen after
    //the init/verify action has succeeded
    EventNotifyingService notifier = new EventNotifyingService(execInProgress,
           internalOperations.getGlobalOptions().getOptionInt(
             OptionKeys.INTERNAL_CONTAINER_STARTUP_DELAY,
             OptionKeys.DEFAULT_CONTAINER_STARTUP_DELAY));
    // register the service for lifecycle management; 
    // this service is started after the accumulo process completes
    addService(notifier);

    // now trigger the command sequence
    maybeStartCommandSequence();
    return true;
  }

  /**
   * probe to see if accumulo has already been installed.
   * @param cd cluster description
   * @return true if the relevant data directory looks inited
   * @throws IOException IO problems
   */
  private boolean isInited(AggregateConf cd) throws
                                             IOException,
                                             BadConfigException {
    String dataDir = cd.getInternalOperations()
                               .getGlobalOptions()
                               .getMandatoryOption(
                                 OptionKeys.INTERNAL_DATA_DIR_PATH);
    Path accumuloInited = new Path(dataDir, INSTANCE_ID);
    FileSystem fs2 = FileSystem.get(accumuloInited.toUri(), getConf());
    return fs2.exists(accumuloInited);
  }



  private void verifyZookeeperLive(String zkQuorum, int timeout) throws
                                                                 IOException,
                                                                 KeeperException,
                                                                 InterruptedException {

    BlockingZKWatcher watcher = new BlockingZKWatcher();
    ZooKeeper zookeeper = new ZooKeeper(zkQuorum, 10000, watcher, true);
    zookeeper.getChildren("/", watcher);

    watcher.waitForZKConnection(timeout);
    
  }

  @Override
  public Map<String, String> buildProviderStatus() {
    
    Map<String,String> status = new HashMap<String, String>();
    
    
    return status;
  }


  /* non-javadoc
   * @see org.apache.slider.providers.ProviderService#buildMonitorDetails()
   */
  @Override
  public TreeMap<String,URL> buildMonitorDetails(ClusterDescription clusterDesc) {
    TreeMap<String,URL> map = new TreeMap<String,URL>();
    
    map.put("Active Accumulo Master (RPC): " + getInfoAvoidingNull(clusterDesc, AccumuloKeys.MASTER_ADDRESS), null);
    
    String monitorKey = "Active Accumulo Monitor: ";
    String monitorAddr = getInfoAvoidingNull(clusterDesc, AccumuloKeys.MONITOR_ADDRESS);
    if (!StringUtils.isBlank(monitorAddr)) {
      try {
        HostAndPort hostPort = HostAndPort.fromString(monitorAddr);
        map.put(monitorKey, new URL("http", hostPort.getHostText(), hostPort.getPort(), ""));
      } catch (Exception e) {
        log.debug("Caught exception parsing Accumulo monitor URL", e);
        map.put(monitorKey + "N/A", null);
      }
    } else {
      map.put(monitorKey + "N/A", null);
    }

    return map;
  }
}
