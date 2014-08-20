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

package org.apache.slider.providers.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.launch.CommandLineBuilder;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.SliderInternalStateException;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.info.ServiceInstanceData;
import org.apache.slider.providers.AbstractProviderService;
import org.apache.slider.providers.ProviderCompleted;
import org.apache.slider.providers.ProviderCore;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.server.appmaster.web.rest.agent.AgentRestOperations;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeat;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeatResponse;
import org.apache.slider.server.appmaster.web.rest.agent.Register;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationResponse;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.slider.server.appmaster.web.rest.RestPaths.SLIDER_PATH_PUBLISHER;

/**
 * This class implements the server-side aspects
 * of an HBase Cluster
 */
public class HBaseProviderService extends AbstractProviderService 
    implements ProviderCore, HBaseKeys, SliderKeys, AgentRestOperations{

  protected static final Logger log =
    LoggerFactory.getLogger(HBaseProviderService.class);
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private HBaseClientProvider clientProvider;
  private Configuration siteConf;

  public HBaseProviderService() {
    super("HBaseProviderService");
    setAgentRestOperations(this);
  }

  @Override
  public List<ProviderRole> getRoles() {
    return HBaseRoles.getRoles();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    clientProvider = new HBaseClientProvider(conf);
  }

  @Override
  public Configuration loadProviderConfigurationInformation(File confDir)
    throws BadCommandArgumentsException, IOException {

    return loadProviderConfigurationInformation(confDir, SITE_XML);
  }

  /**
   * Validate the cluster specification. This can be invoked on both
   * server and client
   * @param instanceDefinition the instance definition to validate
   */
  @Override // Client and Server
  public void validateInstanceDefinition(AggregateConf instanceDefinition) 
      throws SliderException {
    clientProvider.validateInstanceDefinition(instanceDefinition);
  }

  @Override
  public void buildContainerLaunchContext(ContainerLauncher launcher,
      AggregateConf instanceDefinition,
      Container container,
      String role,
      SliderFileSystem coreFS,
      Path generatedConfPath,
      MapOperations resourceComponent,
      MapOperations appComponent,
      Path containerTmpDirPath) throws IOException, SliderException {

    // Set the environment
    launcher.putEnv(SliderUtils.buildEnvMap(appComponent));

    String logDirs = providerUtils.getLogdir();
    String logDir;
    int idx = logDirs.indexOf(",");
    if (idx > 0) {
      // randomly choose a log dir candidate
      String[] segments = logDirs.split(",");
      Random rand = new Random();
      logDir = segments[rand.nextInt(segments.length)];
    } else logDir = logDirs;
    launcher.setEnv(HBASE_LOG_DIR, logDir);

    launcher.setEnv(PROPAGATED_CONFDIR,
        ProviderUtils.convertToAppRelativePath(
            SliderKeys.PROPAGATED_CONF_DIR_NAME) );

    //local resources

    //add the configuration resources
    launcher.addLocalResources(coreFS.submitDirectory(
        generatedConfPath,
        SliderKeys.PROPAGATED_CONF_DIR_NAME));
    //Add binaries
    //now add the image if it was set
    String imageURI = instanceDefinition.getInternalOperations()
                  .get(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    coreFS.maybeAddImagePath(launcher.getLocalResources(), imageURI);

    CommandLineBuilder cli = new CommandLineBuilder();

    String heap = appComponent.getOption(RoleKeys.JVM_HEAP, DEFAULT_JVM_HEAP);
    if (SliderUtils.isSet(heap)) {
      String adjustedHeap = SliderUtils.translateTrailingHeapUnit(heap);
      launcher.setEnv(HBASE_HEAPSIZE, adjustedHeap);
    }
    
    String gcOpts = appComponent.getOption(RoleKeys.GC_OPTS, DEFAULT_GC_OPTS);
    if (SliderUtils.isSet(gcOpts)) {
      launcher.setEnv(HBASE_GC_OPTS, gcOpts);
    }
    
    //this must stay relative if it is an image
    cli.add(providerUtils.buildPathToScript(
        instanceDefinition,
        "bin",
        HBASE_SCRIPT));
    //config dir is relative to the generated file
    cli.add(ARG_CONFIG);
    cli.add("$PROPAGATED_CONFDIR");

    String roleCommand;
    String logfile;
    //now look at the role

/* JDK7
    switch (role) {
      case ROLE_WORKER:
        //role is region server
        roleCommand = REGION_SERVER;
        logfile = "/region-server.txt";
        break;
      case ROLE_MASTER:
        roleCommand = MASTER;

        logfile = "/master.txt";
        break;
      case ROLE_REST_GATEWAY:
        roleCommand = REST_GATEWAY;

        logfile = "/rest-gateway.txt";
        break;
      case ROLE_THRIFT_GATEWAY:
        roleCommand = THRIFT_GATEWAY;

        logfile = "/thrift-gateway.txt";
        break;
      case ROLE_THRIFT2_GATEWAY:
        roleCommand = THRIFT2_GATEWAY;

        logfile = "/thrift2-gateway.txt";
        break;
      default:
        throw new SliderInternalStateException("Cannot start role %s", role);
    }

*/
    if (ROLE_WORKER.equals(role)) {
      //role is region server
      roleCommand = REGION_SERVER;
      logfile = "/region-server.txt";
      
    } else if (ROLE_MASTER.equals(role)) {
      roleCommand = MASTER;
      logfile = "/master.txt";

    } else if (ROLE_REST_GATEWAY.equals(role)) {
      roleCommand = REST_GATEWAY;
      logfile = "/rest-gateway.txt";

    } else if (ROLE_THRIFT_GATEWAY.equals(role)) {
      roleCommand = THRIFT_GATEWAY;
      logfile = "/thrift-gateway.txt";

    } else if (ROLE_THRIFT2_GATEWAY.equals(role)) {
      roleCommand = THRIFT2_GATEWAY;
      logfile = "/thrift2-gateway.txt";
    }
    
    else {
      throw new SliderInternalStateException("Cannot start role %s", role);
    }
    
    cli.add(roleCommand);
    cli.add(ACTION_START);
    //log details
    cli.addOutAndErrFiles(logfile, null);
    launcher.addCommand(cli.build());

  }

  @Override
  public void applyInitialRegistryDefinitions(URL amWebURI,
                                              URL agentOpsURI,
                                              URL agentStatusURI,
                                              ServiceInstanceData instanceData) throws
      IOException {
    super.applyInitialRegistryDefinitions(amWebURI, agentOpsURI,
                                          agentStatusURI,
                                          instanceData
    );
  }

  @Override
  protected void serviceStart() throws Exception {
    registerHBaseServiceEntry();
    super.serviceStart();
  }

  private void registerHBaseServiceEntry() throws IOException {

    String name = amState.getApplicationName() ; 
//    name += ".hbase";
    ServiceInstanceData instanceData = new ServiceInstanceData(name,
        HBASE_SERVICE_TYPE);
    log.info("registering {}/{}", name, HBASE_SERVICE_TYPE );
    PublishedConfiguration publishedSite =
        new PublishedConfiguration("HBase site", siteConf);
    PublishedConfigSet configSet =
        amState.getOrCreatePublishedConfigSet(HBASE_SERVICE_TYPE);
    instanceData.externalView.configurationsURL = SliderUtils.appendToURL(
        amWebAPI.toExternalForm(), SLIDER_PATH_PUBLISHER, HBASE_SERVICE_TYPE);
    configSet.put(HBASE_SITE_PUBLISHED_CONFIG, publishedSite);

    registry.registerServiceInstance(instanceData, null);
  }

  /**
   * Run this service
   *
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
                      ProviderCompleted execInProgress) throws
                                                 IOException,
      SliderException {

    return false;
  }


  /**
   * This is a validation of the application configuration on the AM.
   * Here is where things like the existence of keytabs and other
   * not-seen-client-side properties can be tested, before
   * the actual process is spawned. 
   * @param instanceDefinition clusterSpecification
   * @param confDir configuration directory
   * @param secure flag to indicate that secure mode checks must exist
   * @throws IOException IO problemsn
   * @throws SliderException any failure
   */
  @Override
  public void validateApplicationConfiguration(AggregateConf instanceDefinition,
                                               File confDir,
                                               boolean secure
                                              ) throws IOException,
      SliderException {
    String siteXMLFilename = SITE_XML;
    File siteXML = new File(confDir, siteXMLFilename);
    if (!siteXML.exists()) {
      throw new BadCommandArgumentsException(
        "Configuration directory %s doesn't contain %s - listing is %s",
        confDir, siteXMLFilename, SliderUtils.listDir(confDir));
    }

    //now read it in
    siteConf = ConfigHelper.loadConfFromFile(siteXML);
    //look in the site spec to see that it is OK
    clientProvider.validateHBaseSiteXML(siteConf, secure, siteXMLFilename);
    
    if (secure) {
      //secure mode: take a look at the keytab of master and RS
      SliderUtils.verifyKeytabExists(siteConf,
          HBaseConfigFileOptions.KEY_MASTER_KERBEROS_KEYTAB);
      SliderUtils.verifyKeytabExists(siteConf,
          HBaseConfigFileOptions.KEY_REGIONSERVER_KERBEROS_KEYTAB);

    }
  }


  /**
   * Build the provider status, can be empty
   * @return the provider status - map of entries to add to the info section
   */
  public Map<String, String> buildProviderStatus() {
    Map<String, String> stats = new HashMap<String, String>();

    return stats;
  }
  

  @Override
  public Map<String, String> buildMonitorDetails(ClusterDescription clusterDesc) {
    Map<String, String> details = super.buildMonitorDetails(clusterDesc);

    details.put("Active HBase Master (RPC): " 
                + getInfoAvoidingNull(clusterDesc, StatusKeys.INFO_MASTER_ADDRESS),"");

    return details;
  }

  @Override
  public RegistrationResponse handleRegistration(Register registration) {
    // dummy impl
    RegistrationResponse response = new RegistrationResponse();
    response.setResponseStatus(RegistrationStatus.OK);
    return response;
  }

  @Override
  public HeartBeatResponse handleHeartBeat(HeartBeat heartBeat) {
    // dummy impl
    long id = heartBeat.getResponseId();
    HeartBeatResponse response = new HeartBeatResponse();
    response.setResponseId(id + 1L);
    return response;
  }


}
