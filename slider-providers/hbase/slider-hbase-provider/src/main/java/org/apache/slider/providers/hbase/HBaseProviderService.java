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
import org.apache.slider.common.SliderKeys;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.launch.CommandLineBuilder;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.SliderInternalStateException;
import org.apache.slider.providers.AbstractProviderService;
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
import org.apache.slider.server.services.docstore.utility.EventCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class implements the server-side aspects
 * of an HBase Cluster
 */
public class HBaseProviderService extends AbstractProviderService implements
                                                                  ProviderCore,
                                                                  HBaseKeys,
    SliderKeys,
    AgentRestOperations{

  public static final String ERROR_UNKNOWN_ROLE = "Unknown role ";
  protected static final Logger log =
    LoggerFactory.getLogger(HBaseProviderService.class);
  protected static final String NAME = "hbase";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private HBaseClientProvider clientProvider;
  private Configuration siteConf;
  private SliderFileSystem sliderFileSystem = null;

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
   * @param instanceDefinition
   */
  @Override // Client and Server
  public void validateInstanceDefinition(AggregateConf instanceDefinition) throws
      SliderException {
    clientProvider.validateInstanceDefinition(instanceDefinition);
  }

  @Override
  public void buildContainerLaunchContext(ContainerLauncher launcher,
      AggregateConf instanceDefinition,
      Container container,
      String role,
      SliderFileSystem sliderFileSystem,
      Path generatedConfPath,
      MapOperations resourceComponent,
      MapOperations appComponent,
      Path containerTmpDirPath) throws IOException, SliderException {

    this.sliderFileSystem = sliderFileSystem;
    this.instanceDefinition = instanceDefinition;
    // Set the environment
    launcher.putEnv(SliderUtils.buildEnvMap(appComponent));

    launcher.setEnv(HBASE_LOG_DIR, providerUtils.getLogdir());

    launcher.setEnv("PROPAGATED_CONFDIR",
        ProviderUtils.convertToAppRelativePath(
            SliderKeys.PROPAGATED_CONF_DIR_NAME) );


    //local resources

    //add the configuration resources
    launcher.addLocalResources(sliderFileSystem.submitDirectory(
        generatedConfPath,
        SliderKeys.PROPAGATED_CONF_DIR_NAME));
    //Add binaries
    //now add the image if it was set
    String imageURI = instanceDefinition.getInternalOperations().get(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    sliderFileSystem.maybeAddImagePath(launcher.getLocalResources(), imageURI);

    CommandLineBuilder cli = new CommandLineBuilder();

    String heap = appComponent.getOption(RoleKeys.JVM_HEAP, DEFAULT_JVM_HEAP);
    if (SliderUtils.isSet(heap)) {
      String adjustedHeap = SliderUtils.translateTrailingHeapUnit(heap);
      launcher.setEnv("HBASE_HEAPSIZE", adjustedHeap);
    }
    
    String gcOpts = appComponent.getOption(RoleKeys.GC_OPTS, DEFAULT_GC_OPTS);
    if (SliderUtils.isSet(gcOpts)) {
      launcher.setEnv("SERVER_GC_OPTS", gcOpts);
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
    if (ROLE_WORKER.equals(role)) {
      //role is region server
      roleCommand = REGION_SERVER;
      logfile = "/region-server.txt";
    } else if (ROLE_MASTER.equals(role)) {
      roleCommand = MASTER;
      
      logfile ="/master.txt";
    } else {
      throw new SliderInternalStateException("Cannot start role %s", role);
    }

    cli.add(roleCommand);
    cli.add(ACTION_START);
    //log details
    cli.addOutAndErrFiles(logfile, null);
    launcher.addCommand(cli.build());

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
                      EventCallback execInProgress) throws
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
  
  /* non-javadoc
   * @see org.apache.slider.providers.ProviderService#buildMonitorDetails()
   */
  @Override
  public TreeMap<String,URL> buildMonitorDetails(ClusterDescription clusterDesc) {
    TreeMap<String,URL> map = new TreeMap<String,URL>();
    
    map.put("Active HBase Master (RPC): " + getInfoAvoidingNull(clusterDesc, StatusKeys.INFO_MASTER_ADDRESS), null);

    return map;
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
