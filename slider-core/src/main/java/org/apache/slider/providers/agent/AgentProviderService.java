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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterDescriptionKeys;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.CommandLineBuilder;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.providers.AbstractProviderService;
import org.apache.slider.providers.ProviderCore;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.providers.agent.application.metadata.Component;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.providers.agent.application.metadata.MetainfoParser;
import org.apache.slider.providers.agent.application.metadata.Service;
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
import org.apache.slider.server.services.utility.EventCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/** This class implements the server-side aspects of an agent deployment */
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
  private AgentClientProvider clientProvider;
  private Map<String, ComponentInstanceState> componentStatuses = new HashMap<String, ComponentInstanceState>();
  private AtomicInteger taskId = new AtomicInteger(0);
  private Metainfo metainfo = null;

  public AgentProviderService() {
    super("AgentProviderService");
    setAgentRestOperations(this);
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

    // No need to synchronize as there is low chance of multiple simultaneous reads
    if (metainfo == null) {
      metainfo = getApplicationMetainfo(fileSystem, appDef);
      if (metainfo == null) {
        log.error("metainfo.xml is unavailable or malformed at {}.", appDef);
        throw new SliderException("metainfo.xml is required in app package.");
      }
    }

    this.instanceDefinition = instanceDefinition;
    log.info("Build launch context for Agent");
    log.debug(instanceDefinition.toString());

    // Set the environment
    launcher.putEnv(SliderUtils.buildEnvMap(appComponent));

    String workDir = ApplicationConstants.Environment.PWD.$();
    launcher.setEnv("AGENT_WORK_ROOT", workDir);
    log.info("AGENT_WORK_ROOT set to {}", workDir);
    String logDir = ApplicationConstants.Environment.LOG_DIRS.$();
    launcher.setEnv("AGENT_LOG_ROOT", logDir);
    log.info("AGENT_LOG_ROOT set to {}", logDir);
    launcher.setEnv(HADOOP_USER_NAME, System.getenv(HADOOP_USER_NAME));

    //local resources

    // TODO: Should agent need to support App Home
    String scriptPath = new File(AgentKeys.AGENT_MAIN_SCRIPT_ROOT, AgentKeys.AGENT_MAIN_SCRIPT).getPath();
    String appHome = instanceDefinition.getAppConfOperations().
        getGlobalOptions().get(AgentKeys.PACKAGE_PATH);
    if (SliderUtils.isSet(appHome)) {
      scriptPath = new File(appHome, AgentKeys.AGENT_MAIN_SCRIPT).getPath();
    }

    String agentImage = instanceDefinition.getInternalOperations().
        get(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    if (agentImage != null) {
      LocalResource agentImageRes = fileSystem.createAmResource(new Path(agentImage), LocalResourceType.ARCHIVE);
      launcher.addLocalResource(AgentKeys.AGENT_INSTALL_DIR, agentImageRes);
    }

    log.info("Using {} for agent.", scriptPath);
    LocalResource appDefRes = fileSystem.createAmResource(
        fileSystem.getFileSystem().resolvePath(new Path(appDef)),
        LocalResourceType.ARCHIVE);
    launcher.addLocalResource(AgentKeys.APP_DEFINITION_DIR, appDefRes);

    String agentConf = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getMandatoryOption(AgentKeys.AGENT_CONF);
    LocalResource agentConfRes = fileSystem.createAmResource(
        fileSystem.getFileSystem().resolvePath(new Path(agentConf)),
        LocalResourceType.FILE);
    launcher.addLocalResource(AgentKeys.AGENT_CONFIG_FILE, agentConfRes);

    String agentVer = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(AgentKeys.AGENT_VERSION, null);
    if (agentVer != null) {
      LocalResource agentVerRes = fileSystem.createAmResource(
          fileSystem.getFileSystem().resolvePath(new Path(agentVer)),
          LocalResourceType.FILE);
      launcher.addLocalResource(AgentKeys.AGENT_VERSION_FILE, agentVerRes);
    }

    String label = getContainerLabel(container, role);
    CommandLineBuilder operation = new CommandLineBuilder();

    operation.add(AgentKeys.PYTHON_EXE);

    operation.add(scriptPath);
    operation.add(ARG_LABEL, label);
    operation.add(ARG_HOST);
    operation.add(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));
    operation.add(ARG_PORT);
    operation.add(getClusterInfoPropertyValue(StatusKeys.INFO_AM_WEB_PORT));

    launcher.addCommand(operation.build());

    // initialize the component instance state
    componentStatuses.put(label,
                          new ComponentInstanceState(
                              role,
                              container.getId().toString(),
                              getClusterInfoPropertyValue(OptionKeys.APPLICATION_NAME)));
  }

  protected Metainfo getMetainfo() {
    return this.metainfo;
  }

  protected Metainfo getApplicationMetainfo(SliderFileSystem fileSystem,
                                            String appDef) throws IOException {
    log.info("Reading metainfo at {}", appDef);
    InputStream metainfoStream = SliderUtils.getApplicationResourceInputStream(
        fileSystem.getFileSystem(), new Path(appDef), "metainfo.xml");
    if (metainfoStream == null) {
      log.error("metainfo.xml is unavailable at {}.", appDef);
      throw new IOException("metainfo.xml is required in app package.");
    }

    Metainfo metainfo = new MetainfoParser().parse(metainfoStream);

    return metainfo;
  }

  protected void publishComponentConfiguration(String name, String description,
                                               Iterable<Map.Entry<String, String>> entries) {
    PublishedConfiguration pubconf = new PublishedConfiguration();
    pubconf.description = description;
    pubconf.putValues(entries);
    log.info("publishing {}", pubconf);
    getStateAccessor().getPublishedConfigurations().put(name, pubconf);
  }

  protected Map<String, Map<String, ClusterNode>> getRoleClusterNodeMapping() {
    return (Map<String, Map<String, ClusterNode>>)
        stateAccessor.getClusterStatus().status.get(
        ClusterDescriptionKeys.KEY_CLUSTER_LIVE);
  }

  private String getContainerLabel(Container container, String role) {
    return container.getId().toString() + LABEL_MAKER + role;
  }

  protected String getClusterInfoPropertyValue(String name) {
    StateAccessForProviders accessor = getStateAccessor();
    assert accessor.isApplicationLive();
    ClusterDescription description = accessor.getClusterStatus();
    return description.getInfo(name);
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
                      EventCallback execInProgress) throws
      IOException,
      SliderException {

    return false;
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

  @Override
  public boolean isSupportedRole(String role) {
    return true;
  }

  @Override
  public RegistrationResponse handleRegistration(Register registration) {
    // dummy impl
    RegistrationResponse response = new RegistrationResponse();
    String label = registration.getHostname();
    if (componentStatuses.containsKey(label)) {
      response.setResponseStatus(RegistrationStatus.OK);
    } else {
      response.setResponseStatus(RegistrationStatus.FAILED);
      response.setLog("Label not recognized.");
    }
    return response;
  }

  private Command getCommand(String commandVal) {
    if (commandVal.equals(Command.START.toString())) {
      return Command.START;
    }
    if (commandVal.equals(Command.INSTALL.toString())) {
      return Command.INSTALL;
    }

    return Command.NOP;
  }

  private CommandResult getCommandResult(String commandResVal) {
    if (commandResVal.equals(CommandResult.COMPLETED.toString())) {
      return CommandResult.COMPLETED;
    }
    if (commandResVal.equals(CommandResult.FAILED.toString())) {
      return CommandResult.FAILED;
    }
    if (commandResVal.equals(CommandResult.IN_PROGRESS.toString())) {
      return CommandResult.IN_PROGRESS;
    }

    throw new IllegalArgumentException("Unrecognized value " + commandResVal);
  }

  @Override
  public HeartBeatResponse handleHeartBeat(HeartBeat heartBeat) {
    HeartBeatResponse response = new HeartBeatResponse();
    long id = heartBeat.getResponseId();
    response.setResponseId(id + 1L);

    String label = heartBeat.getHostname();
    String roleName = getRoleName(label);
    String containerId = getContainerId(label);
    StateAccessForProviders accessor = getStateAccessor();
    String scriptPath = getScriptPathFromMetainfo(roleName);

    if (scriptPath == null) {
      log.error("role.script is unavailable for " + roleName + ". Commands will not be sent.");
      return response;
    }

    if (!componentStatuses.containsKey(label)) {
      return response;
    }

    ComponentInstanceState componentStatus = componentStatuses.get(label);
    processReturnedStatus(heartBeat, componentStatus);

    List<CommandReport> reports = heartBeat.getReports();
    if (reports != null && !reports.isEmpty()) {
      CommandReport report = reports.get(0);
      CommandResult result = getCommandResult(report.getStatus());
      Command command = getCommand(report.getRoleCommand());
      componentStatus.applyCommandResult(result, command);
      log.info("Component operation. Status: {}", result);
    }

    int waitForCount = accessor.getInstanceDefinitionSnapshot().
        getAppConfOperations().getComponentOptInt(roleName, AgentKeys.WAIT_HEARTBEAT, 0);

    if (id < waitForCount) {
      log.info("Waiting until heartbeat count {}. Current val: {}", waitForCount, id);
      componentStatuses.put(roleName, componentStatus);
      return response;
    }

    Command command = componentStatus.getNextCommand();
    try {
      if (Command.NOP != command) {
        componentStatus.commandIssued(command);
        if (command == Command.INSTALL) {
          log.info("Installing component ...");
          addInstallCommand(roleName, containerId, response, scriptPath);
        } else if (command == Command.START) {
          log.info("Starting component ...");
          addStartCommand(roleName, containerId, response, scriptPath);
        }
      }
      // if there is no outstanding command then retrieve config
      Boolean isMaster = isMaster(roleName);
      if (isMaster && componentStatus.getState() == State.STARTED
          && command == Command.NOP) {
        if (!componentStatus.getConfigReported()) {
          addGetConfigCommand(roleName, containerId, response);
        }
      }
    } catch (SliderException e) {
      componentStatus.applyCommandResult(CommandResult.FAILED, command);
      log.warn("Component instance failed operation.", e);
    }

    return response;
  }

  protected void processReturnedStatus(HeartBeat heartBeat, ComponentInstanceState componentStatus) {
    List<ComponentStatus> statuses = heartBeat.getComponentStatus();
    if (statuses != null && statuses.size() > 0) {
      log.info("Processing {} status reports.", statuses.size());
      for (ComponentStatus status : statuses) {
        log.info("Status report: " + status.toString());
        if (status.getConfigs() != null) {
          for (String key : status.getConfigs().keySet()) {
            if (!key.equals(GLOBAL_CONFIG_TAG)) {
              Map<String, String> configs = status.getConfigs().get(key);
              publishComponentConfiguration(key, key, configs.entrySet());
            }
          }
          componentStatus.setConfigReported(true);
        }
      }
    }
  }

  protected String getScriptPathFromMetainfo(String roleName) {
    String scriptPath = null;
    List<Service> services = getMetainfo().getServices();
    if (services.size() != 1) {
      log.error("Malformed app definition: Expect only one service in the metainfo.xml");
    }
    Service service = services.get(0);
    for (Component component : service.getComponents()) {
      if (component.getName().equals(roleName)) {
        scriptPath = component.getCommandScript().getScript();
        break;
      }
    }
    return scriptPath;
  }

  protected Boolean isMaster(String roleName) throws SliderException {
    String scriptPath = null;
    List<Service> services = getMetainfo().getServices();
    if (services.size() != 1) {
      log.error("Malformed app definition: Expect only one service in the metainfo.xml");
    } else {
      Service service = services.get(0);
      for (Component component : service.getComponents()) {
        if (component.getName().equals(roleName)) {
          if (component.getCategory().equals("MASTER")) {
            return true;
          } else {
            return false;
          }
        }
      }
    }
    throw new SliderException(String.format("Rolename %s not found in metainfo.xml", roleName));
  }

  private String getRoleName(String label) {
    return label.substring(label.indexOf(LABEL_MAKER) + LABEL_MAKER.length());
  }

  private String getContainerId(String label) {
    return label.substring(0, label.indexOf(LABEL_MAKER));
  }

  protected void addInstallCommand(String roleName, String containerId, HeartBeatResponse response, String scriptPath)
      throws SliderException {
    assert getStateAccessor().isApplicationLive();
    ConfTreeOperations appConf = getStateAccessor().getAppConfSnapshot();
    ConfTreeOperations resourcesConf = getStateAccessor().getResourcesSnapshot();
    ConfTreeOperations internalsConf = getStateAccessor().getInternalsSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    prepareExecutionCommand(cmd);
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(Command.INSTALL.toString());
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    hostLevelParams.put(PACKAGE_LIST, "[{\"type\":\"tarball\",\"name\":\"" +
                                      appConf.getGlobalOptions().getMandatoryOption(
                                          PACKAGE_LIST) + "\"}]");
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    setInstallCommandConfigurations(cmd);

    cmd.setCommandParams(setCommandParameters(scriptPath, false));

    cmd.setHostname(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));
    response.addExecutionCommand(cmd);
  }

  private void prepareExecutionCommand(ExecutionCommand cmd) {
    cmd.setTaskId(taskId.incrementAndGet());
    cmd.setCommandId(cmd.getTaskId() + "-1");
  }

  private Map<String, String> setCommandParameters(String scriptPath, boolean recordConfig) {
    Map<String, String> cmdParams = new TreeMap<String, String>();
    cmdParams.put("service_package_folder",
                  "${AGENT_WORK_ROOT}/work/app/definition/package");
    cmdParams.put("script", scriptPath);
    cmdParams.put("schema_version", "2.0");
    cmdParams.put("command_timeout", "300");
    cmdParams.put("script_type", "PYTHON");
    cmdParams.put("record_config", Boolean.toString(recordConfig));
    return cmdParams;
  }

  private void setInstallCommandConfigurations(ExecutionCommand cmd) {
    ConfTreeOperations appConf = getStateAccessor().getAppConfSnapshot();
    Map<String, Map<String, String>> configurations = buildCommandConfigurations(appConf);
    cmd.setConfigurations(configurations);
  }

  protected void addStatusCommand(String roleName, String containerId, HeartBeatResponse response, String scriptPath)
      throws SliderException {
    assert getStateAccessor().isApplicationLive();
    ConfTreeOperations appConf = getStateAccessor().getAppConfSnapshot();
    ConfTreeOperations internalsConf = getStateAccessor().getInternalsSnapshot();

    StatusCommand cmd = new StatusCommand();
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);

    cmd.setCommandType(AgentCommandType.STATUS_COMMAND);
    cmd.setComponentName(roleName);
    cmd.setServiceName(clusterName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(StatusCommand.STATUS_COMMAND);

    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    cmd.setCommandParams(setCommandParameters(scriptPath, false));

    Map<String, Map<String, String>> configurations = buildCommandConfigurations(appConf);

    cmd.setConfigurations(configurations);

    response.addStatusCommand(cmd);
  }

  protected void addGetConfigCommand(String roleName, String containerId, HeartBeatResponse response)
      throws SliderException {
    assert getStateAccessor().isApplicationLive();
    ConfTreeOperations internalsConf = getStateAccessor().getInternalsSnapshot();

    StatusCommand cmd = new StatusCommand();
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);

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

  protected void addStartCommand(String roleName, String containerId, HeartBeatResponse response, String scriptPath)
      throws
      SliderException {
    assert getStateAccessor().isApplicationLive();
    ConfTreeOperations appConf = getStateAccessor().getAppConfSnapshot();
    ConfTreeOperations internalsConf = getStateAccessor().getInternalsSnapshot();

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

    cmd.setCommandParams(setCommandParameters(scriptPath, true));

    Map<String, Map<String, String>> configurations = buildCommandConfigurations(appConf);

    cmd.setConfigurations(configurations);
    response.addExecutionCommand(cmd);
  }

  private Map<String, Map<String, String>> buildCommandConfigurations(ConfTreeOperations appConf) {

    Map<String, Map<String, String>> configurations = new TreeMap<String, Map<String, String>>();
    Map<String, String> tokens = getStandardTokenMap(appConf);

    List<String> configs = getApplicationConfigurationTypes(appConf);

    //Add global
    for (String configType : configs) {
      addNamedConfiguration(configType, appConf.getGlobalOptions().options,
                            configurations, tokens);
    }

    return configurations;
  }

  private Map<String, String> getStandardTokenMap(ConfTreeOperations appConf) {
    Map<String, String> tokens = new HashMap<String, String>();
    String nnuri = appConf.get("site.fs.defaultFS");
    tokens.put("${NN_URI}", nnuri);
    tokens.put("${NN_HOST}", URI.create(nnuri).getHost());
    tokens.put("${ZK_HOST}", appConf.get(OptionKeys.ZOOKEEPER_HOSTS));
    return tokens;
  }

  private List<String> getApplicationConfigurationTypes(ConfTreeOperations appConf) {
    // for now, reading this from appConf.  In the future, modify this method to
    // process metainfo.xml
    List<String> configList = new ArrayList<String>();
    configList.add(GLOBAL_CONFIG_TAG);

    String configTypes = appConf.get("config_types");
    String[] configs = configTypes.split(",");

    configList.addAll(Arrays.asList(configs));

    // remove duplicates.  mostly worried about 'global' being listed
    return new ArrayList<String>(new HashSet<String>(configList));
  }

  private void addNamedConfiguration(String configName, Map<String, String> sourceConfig,
                                     Map<String, Map<String, String>> configurations,
                                     Map<String, String> tokens) {
    Map<String, String> config = new HashMap<String, String>();
    if (configName.equals(GLOBAL_CONFIG_TAG)) {
      addDefaultGlobalConfig(config);
    }
    // add role hosts to tokens
    addRoleRelatedTokens(tokens);
    providerUtils.propagateSiteOptions(sourceConfig, config, configName, tokens);
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
    List<String> hosts = new ArrayList<>();
    for (ClusterNode cn : values) {
      hosts.add(hostOnly ? cn.host : cn.host + "/" + cn.name);
    }

    return hosts;
  }

  private void addDefaultGlobalConfig(Map<String, String> config) {
    config.put("app_log_dir", "${AGENT_LOG_ROOT}/app/log");
    config.put("app_pid_dir", "${AGENT_WORK_ROOT}/app/run");
    config.put("app_install_dir", "${AGENT_WORK_ROOT}/app/install");
  }

  @Override
  public Map<String, URL> buildMonitorDetails(ClusterDescription clusterDesc) {
    Map<String, URL> details = new LinkedHashMap<String, URL>();
    buildEndpointDetails(details);
    buildRoleHostDetails(details);
    return details;
  }

  private void buildRoleHostDetails(Map<String, URL> details) {
    for (Map.Entry<String, Map<String, ClusterNode>> entry :
        getRoleClusterNodeMapping().entrySet()) {
      details.put(entry.getKey() + " Host(s)/Container(s): " +
                  getHostsList(entry.getValue().values(), false),
                  null);
    }
  }

  private void buildEndpointDetails(Map<String, URL> details) {
    try {
      List services =
          registry.listInstancesByType(SliderKeys.APP_TYPE);
      assert services.size() >= 1;
      Map payload = (Map) services.get(0);
      Map<String, Map> endpointMap =
          (Map<String, Map>) ((Map) payload.get("externalView")).get("endpoints");
      for (Map.Entry<String, Map> endpoint : endpointMap.entrySet()) {
        Map<String, String> val = endpoint.getValue();
        if ("http".equals(val.get("protocol"))) {
          URL url = new URL(val.get("value"));
          details.put(val.get("description"), url);
        }
      }
    } catch (IOException e) {
      log.error("Error creating list of slider URIs", e);
    }
  }
}
