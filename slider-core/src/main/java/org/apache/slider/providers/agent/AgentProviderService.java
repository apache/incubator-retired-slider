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

package org.apache.slider.providers.agent;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterDescriptionKeys;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.CommandLineBuilder;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.info.CustomRegistryConstants;
import org.apache.slider.core.registry.info.RegisteredEndpoint;
import org.apache.slider.core.registry.info.ServiceInstanceData;
import org.apache.slider.providers.AbstractProviderService;
import org.apache.slider.providers.ProviderCompleted;
import org.apache.slider.providers.ProviderCore;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.providers.agent.application.metadata.Application;
import org.apache.slider.providers.agent.application.metadata.CommandScript;
import org.apache.slider.providers.agent.application.metadata.Component;
import org.apache.slider.providers.agent.application.metadata.ComponentExport;
import org.apache.slider.providers.agent.application.metadata.ConfigFile;
import org.apache.slider.providers.agent.application.metadata.DefaultConfig;
import org.apache.slider.providers.agent.application.metadata.Export;
import org.apache.slider.providers.agent.application.metadata.ExportGroup;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.providers.agent.application.metadata.OSPackage;
import org.apache.slider.providers.agent.application.metadata.OSSpecific;
import org.apache.slider.providers.agent.application.metadata.PropertyInfo;
import org.apache.slider.server.appmaster.actions.ProviderReportedContainerLoss;
import org.apache.slider.server.appmaster.actions.RegisterComponentInstance;
import org.apache.slider.server.appmaster.state.ContainerPriority;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.agent.AgentCommandType;
import org.apache.slider.server.appmaster.web.rest.agent.AgentRestOperations;
import org.apache.slider.server.appmaster.web.rest.agent.CommandReport;
import org.apache.slider.server.appmaster.web.rest.agent.ComponentStatus;
import org.apache.slider.server.appmaster.web.rest.agent.ExecutionCommand;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeat;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeatResponse;
import org.apache.slider.server.appmaster.web.rest.agent.Register;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationResponse;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationStatus;
import org.apache.slider.server.appmaster.web.rest.agent.StatusCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.slider.server.appmaster.web.rest.RestPaths.SLIDER_PATH_AGENTS;

/**
 * This class implements the server-side logic for application deployment through Slider application package
 */
public class AgentProviderService extends AbstractProviderService implements
    ProviderCore,
    AgentKeys,
    SliderKeys, AgentRestOperations {


  protected static final Logger log =
      LoggerFactory.getLogger(AgentProviderService.class);
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private static final String LABEL_MAKER = "___";
  private static final String CONTAINER_ID = "container_id";
  private static final String GLOBAL_CONFIG_TAG = "global";
  private static final String LOG_FOLDERS_TAG = "LogFolders";
  private static final String COMPONENT_DATA_TAG = "ComponentInstanceData";
  private static final String SHARED_PORT_TAG = "SHARED";
  private static final String PER_CONTAINER_TAG = "{PER_CONTAINER}";
  private static final int MAX_LOG_ENTRIES = 20;
  private static final int DEFAULT_HEARTBEAT_MONITOR_INTERVAL = 60 * 1000;

  private final Object syncLock = new Object();
  private int heartbeatMonitorInterval = 0;
  private AgentClientProvider clientProvider;
  private AtomicInteger taskId = new AtomicInteger(0);
  private volatile Metainfo metainfo = null;
  private Map<String, DefaultConfig> defaultConfigs = null;
  private ComponentCommandOrder commandOrder = null;
  private HeartbeatMonitor monitor;
  private Boolean canAnyMasterPublish = null;
  private AgentLaunchParameter agentLaunchParameter = null;
  private String clusterName = null;

  private final Map<String, ComponentInstanceState> componentStatuses =
      new ConcurrentHashMap<String, ComponentInstanceState>();
  private final Map<String, Map<String, String>> componentInstanceData =
      new ConcurrentHashMap<String, Map<String, String>>();
  private final Map<String, Map<String, String>> exportGroups =
      new ConcurrentHashMap<String, Map<String, String>>();
  private final Map<String, Map<String, String>> allocatedPorts =
      new ConcurrentHashMap<String, Map<String, String>>();
  private final Map<String, String> workFolders =
      Collections.synchronizedMap(new LinkedHashMap<String, String>(MAX_LOG_ENTRIES, 0.75f, false) {
        protected boolean removeEldestEntry(Map.Entry eldest) {
          return size() > MAX_LOG_ENTRIES;
        }
      });

  /**
   * Create an instance of AgentProviderService
   */
  public AgentProviderService() {
    super("AgentProviderService");
    setAgentRestOperations(this);
    setHeartbeatMonitorInterval(DEFAULT_HEARTBEAT_MONITOR_INTERVAL);
  }

  @Override
  public List<ProviderRole> getRoles() {
    return AgentRoles.getRoles();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    clientProvider = new AgentClientProvider(conf);
  }

  @Override
  public Configuration loadProviderConfigurationInformation(File confDir) throws
      BadCommandArgumentsException,
      IOException {
    return new Configuration(false);
  }

  @Override
  public void validateInstanceDefinition(AggregateConf instanceDefinition)
      throws
      SliderException {
    clientProvider.validateInstanceDefinition(instanceDefinition);
  }

  // Reads the metainfo.xml in the application package and loads it
  private void buildMetainfo(AggregateConf instanceDefinition,
                             SliderFileSystem fileSystem) throws IOException, SliderException {
    String appDef = instanceDefinition.getAppConfOperations()
        .getGlobalOptions().getMandatoryOption(AgentKeys.APP_DEF);

    if (metainfo == null) {
      synchronized (syncLock) {
        if (metainfo == null) {
          readAndSetHeartbeatMonitoringInterval(instanceDefinition);
          initializeAgentDebugCommands(instanceDefinition);

          metainfo = getApplicationMetainfo(fileSystem, appDef);
          if (metainfo == null || metainfo.getApplication() == null) {
            log.error("metainfo.xml is unavailable or malformed at {}.", appDef);
            throw new SliderException(
                "metainfo.xml is required in app package.");
          }
          commandOrder = new ComponentCommandOrder(metainfo.getApplication()
                                                       .getCommandOrder());
          defaultConfigs = initializeDefaultConfigs(fileSystem, appDef, metainfo);
          monitor = new HeartbeatMonitor(this, getHeartbeatMonitorInterval());
          monitor.start();
        }
      }
    }
  }

  @Override
  public void initializeApplicationConfiguration(
      AggregateConf instanceDefinition, SliderFileSystem fileSystem)
      throws IOException, SliderException {
    buildMetainfo(instanceDefinition, fileSystem);
  }

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

    String appDef = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getMandatoryOption(AgentKeys.APP_DEF);

    initializeApplicationConfiguration(instanceDefinition, fileSystem);

    log.info("Build launch context for Agent");
    log.debug(instanceDefinition.toString());

    // Set the environment
    launcher.putEnv(SliderUtils.buildEnvMap(appComponent));

    String workDir = ApplicationConstants.Environment.PWD.$();
    launcher.setEnv("AGENT_WORK_ROOT", workDir);
    log.info("AGENT_WORK_ROOT set to {}", workDir);
    String logDir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    launcher.setEnv("AGENT_LOG_ROOT", logDir);
    log.info("AGENT_LOG_ROOT set to {}", logDir);
    if (System.getenv(HADOOP_USER_NAME) != null) {
      launcher.setEnv(HADOOP_USER_NAME, System.getenv(HADOOP_USER_NAME));
    }
    // for 2-Way SSL
    launcher.setEnv(SLIDER_PASSPHRASE, SliderKeys.PASSPHRASE);

    //local resources

    // TODO: Should agent need to support App Home
    String scriptPath = new File(AgentKeys.AGENT_MAIN_SCRIPT_ROOT, AgentKeys.AGENT_MAIN_SCRIPT).getPath();
    String appHome = instanceDefinition.getAppConfOperations().
        getGlobalOptions().get(AgentKeys.PACKAGE_PATH);
    if (SliderUtils.isSet(appHome)) {
      scriptPath = new File(appHome, AgentKeys.AGENT_MAIN_SCRIPT).getPath();
    }

    // set PYTHONPATH
    List<String> pythonPaths = new ArrayList<String>();
    pythonPaths.add(AgentKeys.AGENT_MAIN_SCRIPT_ROOT);
    String pythonPath = StringUtils.join(File.pathSeparator, pythonPaths);
    launcher.setEnv(PYTHONPATH, pythonPath);
    log.info("PYTHONPATH set to {}", pythonPath);

    Path agentImagePath = null;
    String agentImage = instanceDefinition.getInternalOperations().
        get(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    if (SliderUtils.isUnset(agentImage)) {
      agentImagePath =
          new Path(new Path(new Path(instanceDefinition.getInternalOperations().get(InternalKeys.INTERNAL_TMP_DIR),
                                     container.getId().getApplicationAttemptId().getApplicationId().toString()),
                            AgentKeys.PROVIDER_AGENT),
                   SliderKeys.AGENT_TAR);
    } else {
       agentImagePath = new Path(agentImage);
    }

    // TODO: throw exception when agent tarball is not available

    if (fileSystem.getFileSystem().exists(agentImagePath)) {
      LocalResource agentImageRes = fileSystem.createAmResource(agentImagePath, LocalResourceType.ARCHIVE);
      launcher.addLocalResource(AgentKeys.AGENT_INSTALL_DIR, agentImageRes);
    } else {
      log.error("Required agent image slider-agent.tar.gz is unavailable.");
    }

    log.info("Using {} for agent.", scriptPath);
    LocalResource appDefRes = fileSystem.createAmResource(
        fileSystem.getFileSystem().resolvePath(new Path(appDef)),
        LocalResourceType.ARCHIVE);
    launcher.addLocalResource(AgentKeys.APP_DEFINITION_DIR, appDefRes);

    String agentConf = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(AgentKeys.AGENT_CONF, "");
    if (org.apache.commons.lang.StringUtils.isNotEmpty(agentConf)) {
      LocalResource agentConfRes = fileSystem.createAmResource(fileSystem
                                                                   .getFileSystem().resolvePath(new Path(agentConf)),
                                                               LocalResourceType.FILE);
      launcher.addLocalResource(AgentKeys.AGENT_CONFIG_FILE, agentConfRes);
    }

    String agentVer = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(AgentKeys.AGENT_VERSION, null);
    if (agentVer != null) {
      LocalResource agentVerRes = fileSystem.createAmResource(
          fileSystem.getFileSystem().resolvePath(new Path(agentVer)),
          LocalResourceType.FILE);
      launcher.addLocalResource(AgentKeys.AGENT_VERSION_FILE, agentVerRes);
    }

    //add the configuration resources
    launcher.addLocalResources(fileSystem.submitDirectory(
        generatedConfPath,
        SliderKeys.PROPAGATED_CONF_DIR_NAME));

    String label = getContainerLabel(container, role);
    CommandLineBuilder operation = new CommandLineBuilder();

    operation.add(AgentKeys.PYTHON_EXE);

    operation.add(scriptPath);
    operation.add(ARG_LABEL, label);
    operation.add(ARG_ZOOKEEPER_QUORUM);
    operation.add(getClusterOptionPropertyValue(OptionKeys.ZOOKEEPER_QUORUM));
    operation.add(ARG_ZOOKEEPER_REGISTRY_PATH);
    operation.add(getZkRegistryPath());

    String debugCmd = agentLaunchParameter.getNextLaunchParameter(role);
    if (debugCmd != null && debugCmd.length() != 0) {
      operation.add(ARG_DEBUG);
      operation.add(debugCmd);
    }

    operation.add("> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + AgentKeys.AGENT_OUT_FILE + " 2>&1");

    launcher.addCommand(operation.build());

    // initialize the component instance state
    getComponentStatuses().put(label,
                               new ComponentInstanceState(
                                   role,
                                   container.getId(),
                                   getClusterInfoPropertyValue(OptionKeys.APPLICATION_NAME)));
  }

  // build the zookeeper registry path
  private String getZkRegistryPath() {
    String zkRegistryRoot = getConfig().get(REGISTRY_PATH,
                                            DEFAULT_REGISTRY_PATH);
    String appType = APP_TYPE;
    String zkRegistryPath = ZKPaths.makePath(zkRegistryRoot, appType);
    String clusterName = getAmState().getInternalsSnapshot().get(
        OptionKeys.APPLICATION_NAME);
    zkRegistryPath = ZKPaths.makePath(zkRegistryPath, clusterName);
    return zkRegistryPath;
  }

  @Override
  public void rebuildContainerDetails(List<Container> liveContainers,
                                      String applicationId, Map<Integer, ProviderRole> providerRoleMap) {
    for (Container container : liveContainers) {
      // get the role name and label
      ProviderRole role = providerRoleMap.get(ContainerPriority
                                                  .extractRole(container));
      if (role != null) {
        String roleName = role.name;
        String label = getContainerLabel(container, roleName);
        log.info("Rebuilding in-memory: container {} in role {} in cluster {}",
                 container.getId(), roleName, applicationId);
        getComponentStatuses().put(
            label,
            new ComponentInstanceState(roleName, container.getId(),
                                       applicationId));
      } else {
        log.warn("Role not found for container {} in cluster {}",
                 container.getId(), applicationId);
      }
    }
  }

  /**
   * Run this service
   *
   * @param instanceDefinition component description
   * @param confDir            local dir with the config
   * @param env                environment variables above those generated by
   * @param execInProgress     callback for the event notification
   *
   * @throws IOException     IO problems
   * @throws SliderException anything internal
   */
  @Override
  public boolean exec(AggregateConf instanceDefinition,
                      File confDir,
                      Map<String, String> env,
                      ProviderCompleted execInProgress) throws
      IOException,
      SliderException {

    return false;
  }

  @Override
  public boolean isSupportedRole(String role) {
    return true;
  }

  /**
   * Handle registration calls from the agents
   *
   * @param registration
   *
   * @return
   */
  @Override
  public RegistrationResponse handleRegistration(Register registration) {
    log.info("Handling registration: " + registration);
    RegistrationResponse response = new RegistrationResponse();
    String label = registration.getHostname();
    State agentState = registration.getActualState();
    if (getComponentStatuses().containsKey(label)) {
      response.setResponseStatus(RegistrationStatus.OK);
      ComponentInstanceState componentStatus = getComponentStatuses().get(label);
      componentStatus.heartbeat(System.currentTimeMillis());
      updateComponentStatusWithAgentState(componentStatus, agentState);

      String roleName = getRoleName(label);
      String containerId = getContainerId(label);
      String hostFqdn = registration.getPublicHostname();
      Map<String, String> ports = registration.getAllocatedPorts();
      if (ports != null && !ports.isEmpty()) {
        processAllocatedPorts(hostFqdn, roleName, containerId, ports);
      }

      Map<String, String> folders = registration.getLogFolders();
      if (folders != null && !folders.isEmpty()) {
        publishLogFolderPaths(folders, containerId, roleName, hostFqdn);
      }
    } else {
      response.setResponseStatus(RegistrationStatus.FAILED);
      response.setLog("Label not recognized.");
      log.warn("Received registration request from unknown label {}", label);
    }
    log.info("Registration response: " + response);
    return response;
  }

  /**
   * Handle heartbeat response from agents
   *
   * @param heartBeat incoming heartbeat from Agent
   *
   * @return response to send back
   */
  @Override
  public HeartBeatResponse handleHeartBeat(HeartBeat heartBeat) {
    log.debug("Handling heartbeat: " + heartBeat);
    HeartBeatResponse response = new HeartBeatResponse();
    long id = heartBeat.getResponseId();
    response.setResponseId(id + 1L);

    String label = heartBeat.getHostname();
    String roleName = getRoleName(label);
    String containerId = getContainerId(label);

    StateAccessForProviders accessor = getAmState();
    CommandScript cmdScript = getScriptPathFromMetainfo(roleName);

    if (cmdScript == null || cmdScript.getScript() == null) {
      log.error("role.script is unavailable for " + roleName + ". Commands will not be sent.");
      return response;
    }

    String scriptPath = cmdScript.getScript();
    long timeout = cmdScript.getTimeout();

    if (timeout == 0L) {
      timeout = 600L;
    }

    if (!getComponentStatuses().containsKey(label)) {
      return response;
    }

    Boolean isMaster = isMaster(roleName);
    ComponentInstanceState componentStatus = getComponentStatuses().get(label);
    componentStatus.heartbeat(System.currentTimeMillis());

    publishConfigAndExportGroups(heartBeat, componentStatus, roleName);

    List<CommandReport> reports = heartBeat.getReports();
    if (reports != null && !reports.isEmpty()) {
      CommandReport report = reports.get(0);
      Map<String, String> ports = report.getAllocatedPorts();
      if (ports != null && !ports.isEmpty()) {
        processAllocatedPorts(heartBeat.getFqdn(), roleName, containerId, ports);
      }
      CommandResult result = CommandResult.getCommandResult(report.getStatus());
      Command command = Command.getCommand(report.getRoleCommand());
      componentStatus.applyCommandResult(result, command);
      log.info("Component operation. Status: {}", result);

      if (command == Command.INSTALL && report.getFolders() != null && report.getFolders().size() > 0) {
        publishLogFolderPaths(report.getFolders(), containerId, roleName, heartBeat.getFqdn());
      }
    }

    int waitForCount = accessor.getInstanceDefinitionSnapshot().
        getAppConfOperations().getComponentOptInt(roleName, AgentKeys.WAIT_HEARTBEAT, 0);

    if (id < waitForCount) {
      log.info("Waiting until heartbeat count {}. Current val: {}", waitForCount, id);
      getComponentStatuses().put(roleName, componentStatus);
      return response;
    }

    Command command = componentStatus.getNextCommand();
    try {
      if (Command.NOP != command) {
        if (command == Command.INSTALL) {
          log.info("Installing {} on {}.", roleName, containerId);
          addInstallCommand(roleName, containerId, response, scriptPath, timeout);
          componentStatus.commandIssued(command);
        } else if (command == Command.START) {
          // check against dependencies
          boolean canExecute = commandOrder.canExecute(roleName, command, getComponentStatuses().values());
          if (canExecute) {
            log.info("Starting {} on {}.", roleName, containerId);
            addStartCommand(roleName, containerId, response, scriptPath, timeout, isMarkedAutoRestart(roleName));
            componentStatus.commandIssued(command);
          } else {
            log.info("Start of {} on {} delayed as dependencies have not started.", roleName, containerId);
          }
        }
      }

      // if there is no outstanding command then retrieve config
      if (isMaster && componentStatus.getState() == State.STARTED
          && command == Command.NOP) {
        if (!componentStatus.getConfigReported()) {
          log.info("Requesting applied config for {} on {}.", roleName, containerId);
          addGetConfigCommand(roleName, containerId, response);
        }
      }

      // if restart is required then signal
      response.setRestartEnabled(false);
      if (componentStatus.getState() == State.STARTED
          && command == Command.NOP && isMarkedAutoRestart(roleName)) {
        response.setRestartEnabled(true);
      }
    } catch (SliderException e) {
      log.warn("Component instance failed operation.", e);
      componentStatus.applyCommandResult(CommandResult.FAILED, command);
    }

    log.debug("Heartbeat response: " + response);
    return response;
  }

  protected void processAllocatedPorts(String fqdn,
                                       String roleName,
                                       String containerId,
                                       Map<String, String> ports) {
    RoleInstance instance;
    try {
      instance = getAmState().getOwnedContainer(containerId);
    } catch (NoSuchNodeException e) {
      log.warn("Failed to locate instance of container {}: {}", containerId, e);
      instance = null;
    }
    for (Map.Entry<String, String> port : ports.entrySet()) {
      String portname = port.getKey();
      String portNo = port.getValue();
      log.info("Recording allocated port for {} as {}", portname, portNo);
      this.getAllocatedPorts().put(portname, portNo);
      this.getAllocatedPorts(containerId).put(portname, portNo);
      if (instance != null) {
        try {
          instance.registerPortEndpoint(Integer.valueOf(portNo), portname, "");
        } catch (NumberFormatException e) {
          log.warn("Failed to parse {}: {}", portNo, e);
        }
      }
    }

    // component specific publishes
    processAndPublishComponentSpecificData(ports, containerId, fqdn, roleName);

    // and update registration entries
    if (instance != null) {
      queueAccess.put(new RegisterComponentInstance(instance.getId(), 0,
                                                    TimeUnit.MILLISECONDS));
    }
  }

  private void updateComponentStatusWithAgentState(
      ComponentInstanceState componentStatus, State agentState) {
    if (agentState != null) {
      componentStatus.setState(agentState);
    }
  }

  @Override
  public Map<String, String> buildMonitorDetails(ClusterDescription clusterDesc) {
    Map<String, String> details = super.buildMonitorDetails(clusterDesc);
    buildRoleHostDetails(details);
    return details;
  }

  @Override
  public void applyInitialRegistryDefinitions(URL amWebURI,
                                              URL agentOpsURI,
                                              URL agentStatusURI,
                                              ServiceInstanceData instanceData) throws IOException {
    super.applyInitialRegistryDefinitions(amWebURI,
                                          agentOpsURI,
                                          agentStatusURI, instanceData);

    try {
      instanceData.internalView.endpoints.put(
          CustomRegistryConstants.AGENT_SECURE_REST_API,
          new RegisteredEndpoint(
              new URL(agentOpsURI, SLIDER_PATH_AGENTS),
              "Agent Secure REST API"));
      instanceData.internalView.endpoints.put(
          CustomRegistryConstants.AGENT_ONEWAY_REST_API,
          new RegisteredEndpoint(
              new URL(agentStatusURI, SLIDER_PATH_AGENTS),
              "Agent Oneway REST API"));
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void notifyContainerCompleted(ContainerId containerId) {
    if (containerId != null) {
      String containerIdStr = containerId.toString();
      if (getComponentInstanceData().containsKey(containerIdStr)) {
        getComponentInstanceData().remove(containerIdStr);
        log.info("Removing container specific data for {}", containerIdStr);
        publishComponentInstanceData();
      }

      if (this.allocatedPorts.containsKey(containerIdStr)) {
        this.allocatedPorts.remove(containerIdStr);
      }

      synchronized (this.componentStatuses) {
        for (String label : getComponentStatuses().keySet()) {
          if (label.startsWith(containerIdStr)) {
            getComponentStatuses().remove(label);
          }
        }
      }
    }
  }

  /**
   * Reads and sets the heartbeat monitoring interval. If bad value is provided then log it and set to default.
   *
   * @param instanceDefinition
   */
  private void readAndSetHeartbeatMonitoringInterval(AggregateConf instanceDefinition) {
    String hbMonitorInterval = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(AgentKeys.HEARTBEAT_MONITOR_INTERVAL,
                                     Integer.toString(DEFAULT_HEARTBEAT_MONITOR_INTERVAL));
    try {
      setHeartbeatMonitorInterval(Integer.parseInt(hbMonitorInterval));
    } catch (NumberFormatException e) {
      log.warn(
          "Bad value {} for {}. Defaulting to ",
          hbMonitorInterval,
          HEARTBEAT_MONITOR_INTERVAL,
          DEFAULT_HEARTBEAT_MONITOR_INTERVAL);
    }
  }

  /**
   * Reads and sets the heartbeat monitoring interval. If bad value is provided then log it and set to default.
   *
   * @param instanceDefinition
   */
  private void initializeAgentDebugCommands(AggregateConf instanceDefinition) {
    String launchParameterStr = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(AgentKeys.AGENT_INSTANCE_DEBUG_DATA, "");
    agentLaunchParameter = new AgentLaunchParameter(launchParameterStr);
  }

  @VisibleForTesting
  protected Metainfo getMetainfo() {
    return this.metainfo;
  }

  @VisibleForTesting
  protected Map<String, ComponentInstanceState> getComponentStatuses() {
    return componentStatuses;
  }

  @VisibleForTesting
  protected Metainfo getApplicationMetainfo(SliderFileSystem fileSystem,
                                            String appDef) throws IOException {
    return AgentUtils.getApplicationMetainfo(fileSystem, appDef);
  }

  @VisibleForTesting
  protected void setHeartbeatMonitorInterval(int heartbeatMonitorInterval) {
    this.heartbeatMonitorInterval = heartbeatMonitorInterval;
  }

  /**
   * Read all default configs
   *
   * @param fileSystem
   * @param appDef
   * @param metainfo
   *
   * @return
   *
   * @throws IOException
   */
  protected Map<String, DefaultConfig> initializeDefaultConfigs(SliderFileSystem fileSystem,
                                                                String appDef, Metainfo metainfo) throws IOException {
    Map<String, DefaultConfig> defaultConfigMap = new HashMap<String, DefaultConfig>();
    if (metainfo.getApplication().getConfigFiles() != null &&
        metainfo.getApplication().getConfigFiles().size() > 0) {
      for (ConfigFile configFile : metainfo.getApplication().getConfigFiles()) {
        DefaultConfig config = null;
        try {
          config = AgentUtils.getDefaultConfig(fileSystem, appDef, configFile.getDictionaryName() + ".xml");
        } catch (IOException e) {
          log.warn("Default config file not found. Only the config as input during create will be applied for {}",
                   configFile.getDictionaryName());
        }
        if (config != null) {
          defaultConfigMap.put(configFile.getDictionaryName(), config);
        }
      }
    }

    return defaultConfigMap;
  }

  protected Map<String, DefaultConfig> getDefaultConfigs() {
    return defaultConfigs;
  }

  private int getHeartbeatMonitorInterval() {
    return this.heartbeatMonitorInterval;
  }

  private String getClusterName() {
    if (clusterName == null || clusterName.length() == 0) {
      clusterName = getAmState().getInternalsSnapshot().get(OptionKeys.APPLICATION_NAME);
    }
    return clusterName;
  }

  /**
   * Publish a named property bag that may contain name-value pairs for app configurations such as hbase-site
   *
   * @param name
   * @param description
   * @param entries
   */
  protected void publishApplicationInstanceData(String name, String description,
                                                Iterable<Map.Entry<String, String>> entries) {
    PublishedConfiguration pubconf = new PublishedConfiguration();
    pubconf.description = description;
    pubconf.putValues(entries);
    log.info("publishing {}", pubconf);
    getAmState().getPublishedSliderConfigurations().put(name, pubconf);
  }

  /**
   * Get a list of all hosts for all role/container per role
   *
   * @return
   */
  protected Map<String, Map<String, ClusterNode>> getRoleClusterNodeMapping() {
    amState.refreshClusterStatus();
    return (Map<String, Map<String, ClusterNode>>)
        amState.getClusterStatus().status.get(
            ClusterDescriptionKeys.KEY_CLUSTER_LIVE);
  }

  private String getContainerLabel(Container container, String role) {
    return container.getId().toString() + LABEL_MAKER + role;
  }

  protected String getClusterInfoPropertyValue(String name) {
    StateAccessForProviders accessor = getAmState();
    assert accessor.isApplicationLive();
    ClusterDescription description = accessor.getClusterStatus();
    return description.getInfo(name);
  }

  protected String getClusterOptionPropertyValue(String name)
      throws BadConfigException {
    StateAccessForProviders accessor = getAmState();
    assert accessor.isApplicationLive();
    ClusterDescription description = accessor.getClusterStatus();
    return description.getMandatoryOption(name);
  }

  /**
   * Lost heartbeat from the container - release it and ask for a replacement (async operation)
   *
   * @param label
   * @param containerId
   */
  protected void lostContainer(
      String label,
      ContainerId containerId) {
    getComponentStatuses().remove(label);
    getQueueAccess().put(new ProviderReportedContainerLoss(containerId));
  }

  /**
   * Build the provider status, can be empty
   *
   * @return the provider status - map of entries to add to the info section
   */
  public Map<String, String> buildProviderStatus() {
    Map<String, String> stats = new HashMap<String, String>();
    return stats;
  }


  /**
   * Format the folder locations and publish in the registry service
   *
   * @param folders
   * @param containerId
   * @param hostFqdn
   * @param roleName
   */
  protected void publishLogFolderPaths(
      Map<String, String> folders, String containerId, String roleName, String hostFqdn) {
    for (Map.Entry<String, String> entry: folders.entrySet()) {
      workFolders.put(String.format("%s->%s->%s->%s", roleName, hostFqdn, entry.getKey(), containerId), 
        entry.getValue());
    }

    publishApplicationInstanceData(LOG_FOLDERS_TAG, LOG_FOLDERS_TAG,
                                   (new HashMap<String, String>(this.workFolders)).entrySet());
  }


  /**
   * Process return status for component instances
   *
   * @param heartBeat
   * @param componentStatus
   */
  protected void publishConfigAndExportGroups(
      HeartBeat heartBeat, ComponentInstanceState componentStatus, String roleName) {
    List<ComponentStatus> statuses = heartBeat.getComponentStatus();
    if (statuses != null && !statuses.isEmpty()) {
      log.info("Processing {} status reports.", statuses.size());
      for (ComponentStatus status : statuses) {
        log.info("Status report: " + status.toString());

        if (status.getConfigs() != null) {
          Application application = getMetainfo().getApplication();

          if (canAnyMasterPublishConfig() == false || canPublishConfig(roleName)) {
            // If no Master can explicitly publish then publish if its a master
            // Otherwise, wait till the master that can publish is ready

            Set<String> exportedConfigs = new HashSet();
            String exportedConfigsStr = application.getExportedConfigs();
            boolean exportedAllConfigs = exportedConfigsStr == null || exportedConfigsStr.isEmpty();
            if (!exportedAllConfigs) {
              for (String exportedConfig : exportedConfigsStr.split(",")) {
                if (exportedConfig.trim().length() > 0) {
                  exportedConfigs.add(exportedConfig.trim());
                }
              }
            }

            for (String key : status.getConfigs().keySet()) {
              if ((!exportedAllConfigs && exportedConfigs.contains(key)) ||
                  exportedAllConfigs) {
                Map<String, String> configs = status.getConfigs().get(key);
                publishApplicationInstanceData(key, key, configs.entrySet());
              }
            }
          }

          List<ExportGroup> exportGroups = application.getExportGroups();
          boolean hasExportGroups = exportGroups != null && !exportGroups.isEmpty();

          Set<String> appExports = new HashSet();
          String appExportsStr = getApplicationComponent(roleName).getAppExports();
          boolean hasNoAppExports = appExportsStr == null || appExportsStr.isEmpty();
          if (!hasNoAppExports) {
            for (String appExport : appExportsStr.split(",")) {
              if (appExport.trim().length() > 0) {
                appExports.add(appExport.trim());
              }
            }
          }

          if (hasExportGroups && appExports.size() > 0) {
            String configKeyFormat = "${site.%s.%s}";
            String hostKeyFormat = "${%s_HOST}";

            // publish export groups if any
            Map<String, String> replaceTokens = new HashMap<String, String>();
            for (Map.Entry<String, Map<String, ClusterNode>> entry : getRoleClusterNodeMapping().entrySet()) {
              String hostName = getHostsList(entry.getValue().values(), true).iterator().next();
              replaceTokens.put(String.format(hostKeyFormat, entry.getKey().toUpperCase(Locale.ENGLISH)), hostName);
            }

            for (String key : status.getConfigs().keySet()) {
              Map<String, String> configs = status.getConfigs().get(key);
              for (String configKey : configs.keySet()) {
                String lookupKey = String.format(configKeyFormat, key, configKey);
                replaceTokens.put(lookupKey, configs.get(configKey));
              }
            }

            Set<String> modifiedGroups = new HashSet<String>();
            for (ExportGroup exportGroup : exportGroups) {
              List<Export> exports = exportGroup.getExports();
              if (exports != null && !exports.isEmpty()) {
                String exportGroupName = exportGroup.getName();
                Map<String, String> map = getCurrentExports(exportGroupName);
                for (Export export : exports) {
                  if (canBeExported(exportGroupName, export.getName(), appExports)) {
                    String value = export.getValue();
                    // replace host names
                    for (String token : replaceTokens.keySet()) {
                      if (value.contains(token)) {
                        value = value.replace(token, replaceTokens.get(token));
                      }
                    }
                    map.put(export.getName(), value);
                    log.info("Preparing to publish. Key {} and Value {}", export.getName(), value);
                  }
                }
                modifiedGroups.add(exportGroupName);
              }
            }
            publishModifiedExportGroups(modifiedGroups);
          }

          log.info("Received and processed config for {}", heartBeat.getHostname());
          componentStatus.setConfigReported(true);

        }
      }
    }
  }

  private boolean canBeExported(String exportGroupName, String name, Set<String> appExports) {
    return appExports.contains(String.format("%s-%s", exportGroupName, name));
  }

  protected Map<String, String> getCurrentExports(String groupName) {
    if (!this.exportGroups.containsKey(groupName)) {
      synchronized (this.exportGroups) {
        if (!this.exportGroups.containsKey(groupName)) {
          this.exportGroups.put(groupName, new ConcurrentHashMap<String, String>());
        }
      }
    }

    return this.exportGroups.get(groupName);
  }

  private void publishModifiedExportGroups(Set<String> modifiedGroups) {
    synchronized (this.exportGroups) {
      for (String groupName : modifiedGroups) {
        publishApplicationInstanceData(groupName, groupName, this.exportGroups.get(groupName).entrySet());
      }
    }
  }

  /** Publish component instance specific data if the component demands it */
  protected void processAndPublishComponentSpecificData(Map<String, String> ports,
                                                        String containerId,
                                                        String hostFqdn,
                                                        String roleName) {
    String portVarFormat = "${site.%s}";
    String hostNamePattern = "${THIS_HOST}";
    Map<String, String> toPublish = new HashMap<String, String>();

    Application application = getMetainfo().getApplication();
    for (Component component : application.getComponents()) {
      if (component.getName().equals(roleName)) {
        if (component.getComponentExports().size() > 0) {

          for (ComponentExport export : component.getComponentExports()) {
            String templateToExport = export.getValue();
            for (String portName : ports.keySet()) {
              boolean publishData = false;
              String portValPattern = String.format(portVarFormat, portName);
              if (templateToExport.contains(portValPattern)) {
                templateToExport = templateToExport.replace(portValPattern, ports.get(portName));
                publishData = true;
              }
              if (templateToExport.contains(hostNamePattern)) {
                templateToExport = templateToExport.replace(hostNamePattern, hostFqdn);
                publishData = true;
              }
              if (publishData) {
                toPublish.put(export.getName(), templateToExport);
                log.info("Publishing {} for name {} and container {}",
                         templateToExport, export.getName(), containerId);
              }
            }
          }
        }
      }
    }

    if (toPublish.size() > 0) {
      Map<String, String> perContainerData = null;
      if (!getComponentInstanceData().containsKey(containerId)) {
        perContainerData = new ConcurrentHashMap<String, String>();
      } else {
        perContainerData = getComponentInstanceData().get(containerId);
      }
      perContainerData.putAll(toPublish);
      getComponentInstanceData().put(containerId, perContainerData);
      publishComponentInstanceData();
    }
  }

  private void publishComponentInstanceData() {
    Map<String, String> dataToPublish = new HashMap<String, String>();
    synchronized (this.componentInstanceData) {
      for (String container : getComponentInstanceData().keySet()) {
        for (String prop : getComponentInstanceData().get(container).keySet()) {
          dataToPublish.put(
              container + "." + prop, getComponentInstanceData().get(container).get(prop));
        }
      }
    }
    publishApplicationInstanceData(COMPONENT_DATA_TAG, COMPONENT_DATA_TAG, dataToPublish.entrySet());
  }

  /**
   * Return Component based on name
   *
   * @param roleName
   *
   * @return
   */
  protected Component getApplicationComponent(String roleName) {
    Application application = getMetainfo().getApplication();
    if (application == null) {
      log.error("Malformed app definition: Expect application as the top level element for metainfo.xml");
    } else {
      for (Component component : application.getComponents()) {
        if (component.getName().equals(roleName)) {
          return component;
        }
      }
    }
    return null;
  }

  /**
   * Extract script path from the application metainfo
   *
   * @param roleName
   *
   * @return
   */
  protected CommandScript getScriptPathFromMetainfo(String roleName) {
    Component component = getApplicationComponent(roleName);
    if (component != null) {
      return component.getCommandScript();
    }
    return null;
  }

  /**
   * Is the role of type MASTER
   *
   * @param roleName
   *
   * @return
   */
  protected boolean isMaster(String roleName) {
    Component component = getApplicationComponent(roleName);
    if (component != null) {
      if (component.getCategory().equals("MASTER")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Can the role publish configuration
   *
   * @param roleName
   *
   * @return
   */
  protected boolean canPublishConfig(String roleName) {
    Component component = getApplicationComponent(roleName);
    if (component != null) {
      return Boolean.TRUE.toString().equals(component.getPublishConfig());
    }
    return false;
  }

  /**
   * Checks if the role is marked auto-restart
   *
   * @param roleName
   *
   * @return
   */
  protected boolean isMarkedAutoRestart(String roleName) {
    Component component = getApplicationComponent(roleName);
    if (component != null) {
      return component.getRequiresAutoRestart();
    }
    return false;
  }

  /**
   * Can any master publish config explicitly, if not a random master is used
   *
   * @return
   */
  protected boolean canAnyMasterPublishConfig() {
    if (canAnyMasterPublish == null) {
      Application application = getMetainfo().getApplication();
      if (application == null) {
        log.error("Malformed app definition: Expect application as root element in the metainfo.xml");
      } else {
        for (Component component : application.getComponents()) {
          if (Boolean.TRUE.toString().equals(component.getPublishConfig()) &&
              component.getCategory().equals("MASTER")) {
            canAnyMasterPublish = true;
          }
        }
      }
    }

    if (canAnyMasterPublish == null) {
      canAnyMasterPublish = false;
    }
    return canAnyMasterPublish;
  }

  private String getRoleName(String label) {
    return label.substring(label.indexOf(LABEL_MAKER) + LABEL_MAKER.length());
  }

  private String getContainerId(String label) {
    return label.substring(0, label.indexOf(LABEL_MAKER));
  }

  /**
   * Add install command to the heartbeat response
   *
   * @param roleName
   * @param containerId
   * @param response
   * @param scriptPath
   *
   * @throws SliderException
   */
  @VisibleForTesting
  protected void addInstallCommand(String roleName,
                                   String containerId,
                                   HeartBeatResponse response,
                                   String scriptPath,
                                   long timeout)
      throws SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    prepareExecutionCommand(cmd);
    String clusterName = getClusterName();
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(Command.INSTALL.toString());
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    hostLevelParams.put(PACKAGE_LIST, getPackageList());
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    Map<String, Map<String, String>> configurations = buildCommandConfigurations(appConf, containerId);
    cmd.setConfigurations(configurations);

    cmd.setCommandParams(setCommandParameters(scriptPath, timeout, false));

    cmd.setHostname(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));
    response.addExecutionCommand(cmd);
  }

  private String getPackageList() {
    String pkgFormatString = "{\"type\":\"%s\",\"name\":\"%s\"}";
    String pkgListFormatString = "[%s]";
    List<String> packages = new ArrayList();
    Application application = getMetainfo().getApplication();
    if (application != null) {
      List<OSSpecific> osSpecifics = application.getOSSpecifics();
      if (osSpecifics != null && osSpecifics.size() > 0) {
        for (OSSpecific osSpecific : osSpecifics) {
          if (osSpecific.getOsType().equals("any")) {
            for (OSPackage osPackage : osSpecific.getPackages()) {
              packages.add(String.format(pkgFormatString, osPackage.getType(), osPackage.getName()));
            }
          }
        }
      }
    }

    if (packages.size() > 0) {
      return String.format(pkgListFormatString, StringUtils.join(",", packages));
    } else {
      return "[]";
    }
  }

  private void prepareExecutionCommand(ExecutionCommand cmd) {
    cmd.setTaskId(taskId.incrementAndGet());
    cmd.setCommandId(cmd.getTaskId() + "-1");
  }

  private Map<String, String> setCommandParameters(String scriptPath, long timeout, boolean recordConfig) {
    Map<String, String> cmdParams = new TreeMap<String, String>();
    cmdParams.put("service_package_folder",
                  "${AGENT_WORK_ROOT}/work/app/definition/package");
    cmdParams.put("script", scriptPath);
    cmdParams.put("schema_version", "2.0");
    cmdParams.put("command_timeout", Long.toString(timeout));
    cmdParams.put("script_type", "PYTHON");
    cmdParams.put("record_config", Boolean.toString(recordConfig));
    return cmdParams;
  }

  @VisibleForTesting
  protected void addStatusCommand(String roleName,
                                  String containerId,
                                  HeartBeatResponse response,
                                  String scriptPath,
                                  long timeout)
      throws SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();

    StatusCommand cmd = new StatusCommand();
    String clusterName = getClusterName();

    cmd.setCommandType(AgentCommandType.STATUS_COMMAND);
    cmd.setComponentName(roleName);
    cmd.setServiceName(clusterName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(StatusCommand.STATUS_COMMAND);

    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    cmd.setCommandParams(setCommandParameters(scriptPath, timeout, false));

    Map<String, Map<String, String>> configurations = buildCommandConfigurations(appConf, containerId);

    cmd.setConfigurations(configurations);

    response.addStatusCommand(cmd);
  }

  @VisibleForTesting
  protected void addGetConfigCommand(String roleName, String containerId, HeartBeatResponse response)
      throws SliderException {
    assert getAmState().isApplicationLive();

    StatusCommand cmd = new StatusCommand();
    String clusterName = getClusterName();

    cmd.setCommandType(AgentCommandType.STATUS_COMMAND);
    cmd.setComponentName(roleName);
    cmd.setServiceName(clusterName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(StatusCommand.GET_CONFIG_COMMAND);
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    hostLevelParams.put(CONTAINER_ID, containerId);

    response.addStatusCommand(cmd);
  }

  @VisibleForTesting
  protected void addStartCommand(String roleName, String containerId, HeartBeatResponse response,
                                 String scriptPath, long timeout, boolean isMarkedAutoRestart)
      throws
      SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    ConfTreeOperations internalsConf = getAmState().getInternalsSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    prepareExecutionCommand(cmd);
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);
    String hostName = getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME);
    cmd.setHostname(hostName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(Command.START.toString());
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    Map<String, String> roleParams = new TreeMap<String, String>();
    cmd.setRoleParams(roleParams);
    cmd.getRoleParams().put("auto_restart", Boolean.toString(isMarkedAutoRestart));

    cmd.setCommandParams(setCommandParameters(scriptPath, timeout, true));

    Map<String, Map<String, String>> configurations = buildCommandConfigurations(appConf, containerId);

    cmd.setConfigurations(configurations);
    response.addExecutionCommand(cmd);
  }

  protected Map<String, String> getAllocatedPorts() {
    return getAllocatedPorts(SHARED_PORT_TAG);
  }

  protected Map<String, Map<String, String>> getComponentInstanceData() {
    return this.componentInstanceData;
  }

  protected Map<String, String> getAllocatedPorts(String containerId) {
    if (!this.allocatedPorts.containsKey(containerId)) {
      synchronized (this.allocatedPorts) {
        if (!this.allocatedPorts.containsKey(containerId)) {
          this.allocatedPorts.put(containerId,
                                  new ConcurrentHashMap<String, String>());
        }
      }
    }
    return this.allocatedPorts.get(containerId);
  }

  private Map<String, Map<String, String>> buildCommandConfigurations(
      ConfTreeOperations appConf, String containerId)
      throws SliderException {

    Map<String, Map<String, String>> configurations =
        new TreeMap<String, Map<String, String>>();
    Map<String, String> tokens = getStandardTokenMap(appConf);

    Set<String> configs = new HashSet<String>();
    configs.addAll(getApplicationConfigurationTypes());
    configs.addAll(getSystemConfigurationsRequested(appConf));

    for (String configType : configs) {
      addNamedConfiguration(configType, appConf.getGlobalOptions().options,
                            configurations, tokens, containerId);
    }

    //do a final replacement of re-used configs
    dereferenceAllConfigs(configurations);

    return configurations;
  }

  protected void dereferenceAllConfigs(Map<String, Map<String, String>> configurations) {
    Map<String, String> allConfigs = new HashMap<String, String>();
    String lookupFormat = "${@//site/%s/%s}";
    for (String configType : configurations.keySet()) {
      Map<String, String> configBucket = configurations.get(configType);
      for (String configName : configBucket.keySet()) {
        allConfigs.put(String.format(lookupFormat, configType, configName), configBucket.get(configName));
      }
    }

    for (String configType : configurations.keySet()) {
      Map<String, String> configBucket = configurations.get(configType);
      for (Map.Entry<String, String> entry: configBucket.entrySet()) {
        String configName = entry.getKey();
        String configValue = entry.getValue();
        for (String lookUpKey : allConfigs.keySet()) {
          if (configValue != null && configValue.contains(lookUpKey)) {
            configValue = configValue.replace(lookUpKey, allConfigs.get(lookUpKey));
          }
        }
        configBucket.put(configName, configValue);
      }
    }
  }

  private Map<String, String> getStandardTokenMap(ConfTreeOperations appConf) throws SliderException {
    Map<String, String> tokens = new HashMap<String, String>();
    String nnuri = appConf.get("site.fs.defaultFS");
    tokens.put("${NN_URI}", nnuri);
    tokens.put("${NN_HOST}", URI.create(nnuri).getHost());
    tokens.put("${ZK_HOST}", appConf.get(OptionKeys.ZOOKEEPER_HOSTS));
    tokens.put("${DEFAULT_ZK_PATH}", appConf.get(OptionKeys.ZOOKEEPER_PATH));
    tokens.put("${DEFAULT_DATA_DIR}", getAmState()
        .getInternalsSnapshot()
        .getGlobalOptions()
        .getMandatoryOption(InternalKeys.INTERNAL_DATA_DIR_PATH));
    return tokens;
  }

  @VisibleForTesting
  protected List<String> getSystemConfigurationsRequested(ConfTreeOperations appConf) {
    List<String> configList = new ArrayList<String>();

    String configTypes = appConf.get(AgentKeys.SYSTEM_CONFIGS);
    if (configTypes != null && configTypes.length() > 0) {
      String[] configs = configTypes.split(",");
      for (String config : configs) {
        configList.add(config.trim());
      }
    }

    return new ArrayList<String>(new HashSet<String>(configList));
  }


  @VisibleForTesting
  protected List<String> getApplicationConfigurationTypes() {
    List<String> configList = new ArrayList<String>();
    configList.add(GLOBAL_CONFIG_TAG);

    List<ConfigFile> configFiles = getMetainfo().getApplication().getConfigFiles();
    for (ConfigFile configFile : configFiles) {
      log.info("Expecting config type {}.", configFile.getDictionaryName());
      configList.add(configFile.getDictionaryName());
    }

    // remove duplicates.  mostly worried about 'global' being listed
    return new ArrayList<String>(new HashSet<String>(configList));
  }

  private void addNamedConfiguration(String configName, Map<String, String> sourceConfig,
                                     Map<String, Map<String, String>> configurations,
                                     Map<String, String> tokens, String containerId) {
    Map<String, String> config = new HashMap<String, String>();
    if (configName.equals(GLOBAL_CONFIG_TAG)) {
      addDefaultGlobalConfig(config, containerId);
    }
    // add role hosts to tokens
    addRoleRelatedTokens(tokens);
    providerUtils.propagateSiteOptions(sourceConfig, config, configName, tokens);

    //apply any port updates
    if (!this.getAllocatedPorts().isEmpty()) {
      for (String key : config.keySet()) {
        String value = config.get(key);
        String lookupKey = configName + "." + key;
        if (!value.contains(PER_CONTAINER_TAG)) {
          // If the config property is shared then pass on the already allocated value
          // from any container
          if (this.getAllocatedPorts().containsKey(lookupKey)) {
            config.put(key, getAllocatedPorts().get(lookupKey));
          }
        } else {
          if (this.getAllocatedPorts(containerId).containsKey(lookupKey)) {
            config.put(key, getAllocatedPorts(containerId).get(lookupKey));
          }
        }
      }
    }

    //apply defaults only if the key is not present and value is not empty
    if (getDefaultConfigs().containsKey(configName)) {
      log.info("Adding default configs for type {}.", configName);
      for (PropertyInfo defaultConfigProp : getDefaultConfigs().get(configName).getPropertyInfos()) {
        if (!config.containsKey(defaultConfigProp.getName())) {
          if (!defaultConfigProp.getName().isEmpty() &&
              defaultConfigProp.getValue() != null &&
              !defaultConfigProp.getValue().isEmpty()) {
            config.put(defaultConfigProp.getName(), defaultConfigProp.getValue());
          }
        }
      }
    }

    configurations.put(configName, config);
  }

  protected void addRoleRelatedTokens(Map<String, String> tokens) {
    for (Map.Entry<String, Map<String, ClusterNode>> entry : getRoleClusterNodeMapping().entrySet()) {
      String tokenName = entry.getKey().toUpperCase(Locale.ENGLISH) + "_HOST";
      String hosts = StringUtils.join(",", getHostsList(entry.getValue().values(), true));
      tokens.put("${" + tokenName + "}", hosts);
    }
  }

  private Iterable<String> getHostsList(Collection<ClusterNode> values,
                                        boolean hostOnly) {
    List<String> hosts = new ArrayList<String>();
    for (ClusterNode cn : values) {
      hosts.add(hostOnly ? cn.host : cn.host + "/" + cn.name);
    }

    return hosts;
  }

  private void addDefaultGlobalConfig(Map<String, String> config, String containerId) {
    config.put("app_log_dir", "${AGENT_LOG_ROOT}");
    config.put("app_pid_dir", "${AGENT_WORK_ROOT}/app/run");
    config.put("app_install_dir", "${AGENT_WORK_ROOT}/app/install");
    config.put("app_input_conf_dir", "${AGENT_WORK_ROOT}/" + SliderKeys.PROPAGATED_CONF_DIR_NAME);
    config.put("app_container_id", containerId);
  }

  private void buildRoleHostDetails(Map<String, String> details) {
    for (Map.Entry<String, Map<String, ClusterNode>> entry :
        getRoleClusterNodeMapping().entrySet()) {
      details.put(entry.getKey() + " Host(s)/Container(s): " +
                  getHostsList(entry.getValue().values(), false),
                  "");
    }
  }
}
