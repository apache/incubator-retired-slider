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

package org.apache.slider.providers.slideram;

import com.beust.jcommander.JCommander;
import com.google.gson.GsonBuilder;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.server.entity.ServiceNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.AbstractLauncher;
import org.apache.slider.core.launch.CommandLineBuilder;
import org.apache.slider.providers.AbstractClientProvider;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.jaxrs.JacksonJaxbJsonProvider;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * handles the setup of the Slider AM.
 * This keeps aspects of role, cluster validation and Clusterspec setup
 * out of the core slider client
 */
public class SliderAMClientProvider extends AbstractClientProvider implements
    SliderKeys {


  protected static final Logger log =
    LoggerFactory.getLogger(SliderAMClientProvider.class);
  protected static final String NAME = "SliderAM";
  public static final String INSTANCE_RESOURCE_BASE = PROVIDER_RESOURCE_BASE_ROOT +
                                                       "slideram/instance/";
  public static final String INTERNAL_JSON =
    INSTANCE_RESOURCE_BASE + "internal.json";
  public static final String APPCONF_JSON =
    INSTANCE_RESOURCE_BASE + "appconf.json";
  public static final String RESOURCES_JSON =
    INSTANCE_RESOURCE_BASE + "resources.json";

  public SliderAMClientProvider(Configuration conf) {
    super(conf);
  }

  /**
   * List of roles
   */
  public static final List<ProviderRole> ROLES =
    new ArrayList<ProviderRole>();

  public static final int KEY_AM = ROLE_AM_PRIORITY_INDEX;

  /**
   * Initialize role list
   */
  static {
    ROLES.add(new ProviderRole(COMPONENT_AM, KEY_AM,
                               PlacementPolicy.EXCLUDE_FROM_FLEXING));
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return ROLES;
  }


  @Override //Client
  public void preflightValidateClusterConfiguration(SliderFileSystem sliderFileSystem,
                                                    String clustername,
                                                    Configuration configuration,
                                                    AggregateConf instanceDefinition,
                                                    Path clusterDirPath,
                                                    Path generatedConfDirPath,
                                                    boolean secure) throws
      SliderException,
                                                                    IOException {

    super.preflightValidateClusterConfiguration(sliderFileSystem, clustername, configuration, instanceDefinition, clusterDirPath, generatedConfDirPath, secure);
    //add a check for the directory being writeable by the current user
    String
      dataPath = instanceDefinition.getInternalOperations()
                                   .getGlobalOptions()
                                   .getMandatoryOption(
                                     OptionKeys.INTERNAL_DATA_DIR_PATH);

    Path path = new Path(dataPath);
    sliderFileSystem.verifyDirectoryWriteAccess(path);
    Path historyPath = new Path(clusterDirPath, SliderKeys.HISTORY_DIR_NAME);
    sliderFileSystem.verifyDirectoryWriteAccess(historyPath);
  }

  /**
   * The Slider AM sets up all the dependency JARs above slider.jar itself
   * {@inheritDoc}
   */
  public void prepareAMAndConfigForLaunch(SliderFileSystem fileSystem,
      Configuration serviceConf,
      AbstractLauncher launcher,
      AggregateConf instanceDescription,
      Path snapshotConfDirPath,
      Path generatedConfDirPath,
      Configuration clientConfExtras,
      String libdir,
      Path tempPath, boolean miniClusterTestRun)
    throws IOException, SliderException {

    Map<String, LocalResource> providerResources =
        new HashMap<String, LocalResource>();


    ProviderUtils.addProviderJar(providerResources,
        this,
        SLIDER_JAR,
        fileSystem,
        tempPath,
        libdir,
        miniClusterTestRun);

    Class<?>[] classes = {
      JCommander.class,
      GsonBuilder.class,
      
      CuratorFramework.class,
      CuratorZookeeperClient.class,
      ServiceInstance.class,
      ServiceNames.class,

      JacksonJaxbJsonProvider.class,
      JsonFactory.class,
      JsonNodeFactory.class,
      JaxbAnnotationIntrospector.class,
      
    };
    String[] jars =
      {
        JCOMMANDER_JAR,
        GSON_JAR,
        
        "curator-framework.jar",
        "curator-client.jar",
        "curator-x-discovery.jar",
        "curator-x-discovery-service.jar",
        
        "jackson-jaxrs",
        "jackson-core-asl",
        "jackson-mapper-asl",
        "jackson-xc",
        
      };
    ProviderUtils.addDependencyJars(providerResources, fileSystem, tempPath,
                                    libdir, jars,
                                    classes);
    
    launcher.addLocalResources(providerResources);
    //also pick up all env variables from a map
    launcher.copyEnvVars(
      instanceDescription.getInternalOperations().getOrAddComponent(
        SliderKeys.COMPONENT_AM));
  }

  /**
   * Update the AM resource with any local needs
   * @param capability capability to update
   */
  public void prepareAMResourceRequirements(MapOperations sliderAM,
                                            Resource capability) {
    capability.setMemory(sliderAM.getOptionInt(
      ResourceKeys.YARN_MEMORY,
      capability.getMemory()));
    capability.setVirtualCores(
        sliderAM.getOptionInt(ResourceKeys.YARN_CORES, capability.getVirtualCores()));
  }
  
  /**
   * Extract any JVM options from the cluster specification and
   * add them to the command line
   */
  public void addJVMOptions(AggregateConf aggregateConf,
                            CommandLineBuilder cmdLine) throws
                                                        BadConfigException {
    MapOperations sliderAM =
      aggregateConf.getAppConfOperations().getMandatoryComponent(
        SliderKeys.COMPONENT_AM);
    cmdLine.sysprop("java.net.preferIPv4Stack", "true");
    cmdLine.sysprop("java.awt.headless", "true");
    String heap = sliderAM.getOption(RoleKeys.JVM_HEAP,
                                   DEFAULT_JVM_HEAP);
    cmdLine.setJVMHeap(heap);
    String jvmopts = sliderAM.getOption(RoleKeys.JVM_OPTS, "");
    if (SliderUtils.isSet(jvmopts)) {
      cmdLine.add(jvmopts);
    }
  }


  @Override
  public void prepareInstanceConfiguration(AggregateConf aggregateConf) throws
      SliderException,
                                                                        IOException {
    mergeTemplates(aggregateConf,
                   INTERNAL_JSON, RESOURCES_JSON, APPCONF_JSON
                  );
  }
}
