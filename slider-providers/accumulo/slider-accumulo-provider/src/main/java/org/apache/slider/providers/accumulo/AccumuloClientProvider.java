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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.launch.AbstractLauncher;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.providers.AbstractClientProvider;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.common.tools.SliderFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Client-side accumulo provider
 */
public class AccumuloClientProvider extends AbstractClientProvider implements
                                                       AccumuloKeys {

  protected static final Logger log =
    LoggerFactory.getLogger(AccumuloClientProvider.class);
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private static final String INSTANCE_RESOURCE_BASE =
      "/org/apache/slider/providers/accumulo/instance/";


  protected AccumuloClientProvider(Configuration conf) {
    super(conf);
  }

  public static List<ProviderRole> getProviderRoles() {
    return AccumuloRoles.ROLES;
  }

  @Override
  public String getName() {
    return PROVIDER_ACCUMULO;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return AccumuloRoles.ROLES;
  }


  @Override
  public void prepareInstanceConfiguration(AggregateConf aggregateConf) throws
      SliderException,
                                                                        IOException {
    String resourceTemplate = INSTANCE_RESOURCE_BASE + "resources.json";
    String appConfTemplate = INSTANCE_RESOURCE_BASE + "appconf.json";
    mergeTemplates(aggregateConf, null, resourceTemplate, appConfTemplate);
    aggregateConf.getAppConfOperations().set(OPTION_ACCUMULO_PASSWORD,
                                             createAccumuloPassword());
  }


  public String createAccumuloPassword() {
    return UUID.randomUUID().toString();
  }

  public void setDatabasePath(Map<String, String> sitexml, String dataPath) {
    Path path = new Path(dataPath);
    URI parentUri = path.toUri();
    String authority = parentUri.getAuthority();
    String fspath =
      parentUri.getScheme() + "://" + (authority == null ? "" : authority) + "/";
    sitexml.put(AccumuloConfigFileOptions.INSTANCE_DFS_URI, fspath);
    sitexml.put(AccumuloConfigFileOptions.INSTANCE_DFS_DIR,
                parentUri.getPath());
  }

  /**
   * Build the accumulo-site.xml file
   * This the configuration used by Accumulo directly
   * @param instanceDescription this is the cluster specification used to define this
   * @return a map of the dynamic bindings for this Slider instance
   */
  public Map<String, String> buildSiteConfFromInstance(
    AggregateConf instanceDescription)
    throws BadConfigException {


    ConfTreeOperations appconf =
      instanceDescription.getAppConfOperations();

    MapOperations globalAppOptions = appconf.getGlobalOptions();
    MapOperations globalInstanceOptions =
      instanceDescription.getInternalOperations().getGlobalOptions();


    Map<String, String> sitexml = new HashMap<String, String>();

    providerUtils.propagateSiteOptions(globalAppOptions, sitexml);

    propagateClientFSBinding(sitexml);
    setDatabasePath(sitexml,
                    globalInstanceOptions.getMandatoryOption(OptionKeys.INTERNAL_DATA_DIR_PATH));


    String quorum =
      globalAppOptions.getMandatoryOption(OptionKeys.ZOOKEEPER_QUORUM);
    sitexml.put(AccumuloConfigFileOptions.ZOOKEEPER_HOST, quorum);

    return sitexml;

  }


  public void propagateClientFSBinding(Map<String, String> sitexml) throws
                                                                    BadConfigException {
    String fsDefaultName =
      getConf().get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
    if (fsDefaultName == null) {
      throw new BadConfigException("Key not found in conf: {}",
                                   CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    }
    sitexml.put(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsDefaultName);
    sitexml.put(SliderXmlConfKeys.FS_DEFAULT_NAME_CLASSIC, fsDefaultName);
  }

  @Override 
  public void preflightValidateClusterConfiguration(SliderFileSystem sliderFileSystem,
                                                    String clustername,
                                                    Configuration configuration,
                                                    AggregateConf instanceDefinition,
                                                    Path clusterDirPath,
                                                    Path generatedConfDirPath,
                                                    boolean secure) throws
      SliderException,
                                                                    IOException {
    super.preflightValidateClusterConfiguration(sliderFileSystem, clustername,
                                                configuration,
                                                instanceDefinition,
                                                clusterDirPath,
                                                generatedConfDirPath, secure);

  }

  /**
   * Add Accumulo and its dependencies (only) to the job configuration.
   * <p>
   * This is intended as a low-level API, facilitating code reuse between this
   * class and its mapred counterpart. It also of use to external tools that
   * need to build a MapReduce job that interacts with Accumulo but want
   * fine-grained control over the jars shipped to the cluster.
   * </p>
   *
   * @see org.apache.hadoop.hbase.mapred.TableMapReduceUtil
   * @see <a href="https://issues.apache.org/;jira/browse/PIG-3285">PIG-3285</a>
   *
   * @param providerResources provider resources to add resource to
   * @param sliderFileSystem filesystem
   * @param libdir relative directory to place resources
   * @param tempPath path in the cluster FS for temp files
   * @throws IOException IO problems
   * @throws SliderException Slider-specific issues
   */
  private void addAccumuloDependencyJars(Map<String, LocalResource> providerResources,
                                            SliderFileSystem sliderFileSystem,
                                            String libdir,
                                            Path tempPath) throws
                                                           IOException,
      SliderException {
    String[] jars =
      {
      /*  "zookeeper.jar",*/
      };
    Class<?>[] classes = {
      //zk
/*      org.apache.zookeeper.ClientCnxn.class*/
    };
    
    ProviderUtils.addDependencyJars(providerResources, sliderFileSystem, tempPath,
                                    libdir, jars,
                                    classes);
  }

  @Override
  public void prepareAMAndConfigForLaunch(SliderFileSystem fileSystem,
      Configuration serviceConf,
      AbstractLauncher launcher,
      AggregateConf instanceDescription,
      Path snapshotConfDirPath,
      Path generatedConfDirPath,
      Configuration clientConfExtras,
      String libdir,
      Path tempPath,
      boolean miniClusterTestRun) throws IOException, SliderException {
    //load in the template site config
    log.debug("Loading template configuration from {}", snapshotConfDirPath);
      Configuration siteConf = ConfigHelper.loadTemplateConfiguration(
        serviceConf,
          snapshotConfDirPath,
        AccumuloKeys.SITE_XML,
        AccumuloKeys.SITE_XML_RESOURCE);



    Map<String, LocalResource> providerResources;
    providerResources = fileSystem.submitDirectory(generatedConfDirPath,
                                                   SliderKeys.PROPAGATED_CONF_DIR_NAME);


    ProviderUtils.addProviderJar(providerResources,
        this,
        "slider-accumulo-provider.jar",
        fileSystem,
        tempPath,
        libdir,
        miniClusterTestRun);


    addAccumuloDependencyJars(providerResources, fileSystem, libdir, tempPath);
    launcher.addLocalResources(providerResources);
    
    //construct the cluster configuration values

    ConfTreeOperations appconf =
      instanceDescription.getAppConfOperations();


    Map<String, String> clusterConfMap = buildSiteConfFromInstance(
      instanceDescription);

    //merge them
    ConfigHelper.addConfigMap(siteConf,
                              clusterConfMap.entrySet(),
                              "Accumulo Provider");

    //now, if there is an extra client conf, merge it in too
    if (clientConfExtras != null) {
      ConfigHelper.mergeConfigurations(siteConf, clientConfExtras,
                                       "Slider Client");
    }

    if (log.isDebugEnabled()) {
      log.debug("Merged Configuration");
      ConfigHelper.dumpConf(siteConf);
    }

    Path sitePath = ConfigHelper.saveConfig(serviceConf,
                                            siteConf,
                                            generatedConfDirPath,
                                            AccumuloKeys.SITE_XML);

    log.debug("Saving the config to {}", sitePath);
    launcher.submitDirectory(generatedConfDirPath,
                             SliderKeys.PROPAGATED_CONF_DIR_NAME);

  }

  private static Set<String> knownRoleNames = new HashSet<String>();
  static {
    knownRoleNames.add(SliderKeys.COMPONENT_AM);
    for (ProviderRole role : AccumuloRoles.ROLES) {
      knownRoleNames.add(role.name);
    }
  }

  @Override
  public void validateInstanceDefinition(AggregateConf instanceDefinition) throws
      SliderException {
    super.validateInstanceDefinition(instanceDefinition);

    ConfTreeOperations resources =
      instanceDefinition.getResourceOperations();
    Set<String> unknownRoles = resources.getComponentNames();
    unknownRoles.removeAll(knownRoleNames);
    if (!unknownRoles.isEmpty()) {
      throw new BadCommandArgumentsException("There is unknown role: %s",
                                             unknownRoles.iterator().next());
    }
    providerUtils.validateNodeCount(instanceDefinition,
                                    AccumuloKeys.ROLE_TABLET,
                                    1, -1);


    providerUtils.validateNodeCount(instanceDefinition,
                                    AccumuloKeys.ROLE_MASTER, 1, -1);

    providerUtils.validateNodeCount(instanceDefinition,
                                    AccumuloKeys.ROLE_GARBAGE_COLLECTOR,
                                    0, -1);

    providerUtils.validateNodeCount(instanceDefinition,
                                    AccumuloKeys.ROLE_MONITOR,
                                    0, -1);

    providerUtils.validateNodeCount(instanceDefinition,
                                    AccumuloKeys.ROLE_TRACER , 0, -1);

    MapOperations globalAppConfOptions =
      instanceDefinition.getAppConfOperations().getGlobalOptions();
    globalAppConfOptions.verifyOptionSet(AccumuloKeys.OPTION_ZK_HOME);
    globalAppConfOptions.verifyOptionSet(AccumuloKeys.OPTION_HADOOP_HOME);
  }


  /**
   * Get the path to the script
   * @return the script
   */
  public static String buildScriptBinPath(AggregateConf instanceDefinition)
    throws FileNotFoundException {
    return providerUtils.buildPathToScript(instanceDefinition, "bin", "accumulo");
  }


}
