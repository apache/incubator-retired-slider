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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.AbstractLauncher;
import org.apache.slider.providers.AbstractClientProvider;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.providers.agent.application.metadata.Application;
import org.apache.slider.providers.agent.application.metadata.Component;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** This class implements  the client-side aspects of the agent deployer */
public class AgentClientProvider extends AbstractClientProvider
    implements AgentKeys, SliderKeys {


  protected static final Logger log =
      LoggerFactory.getLogger(AgentClientProvider.class);
  protected static final String NAME = "agent";
  private static final ProviderUtils providerUtils = new ProviderUtils(log);


  protected AgentClientProvider(Configuration conf) {
    super(conf);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return AgentRoles.getRoles();
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
    super.preflightValidateClusterConfiguration(sliderFileSystem, clustername,
                                                configuration,
                                                instanceDefinition,
                                                clusterDirPath,
                                                generatedConfDirPath, secure);

    String appDef = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getMandatoryOption(AgentKeys.APP_DEF);
    Path appDefPath = new Path(appDef);
    sliderFileSystem.verifyFileExists(appDefPath);

    String agentConf = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(AgentKeys.AGENT_CONF, "");
    if (StringUtils.isNotEmpty(agentConf)) {
      sliderFileSystem.verifyFileExists(new Path(agentConf));
    }

    String appHome = instanceDefinition.getAppConfOperations().
        getGlobalOptions().get(AgentKeys.PACKAGE_PATH);
    if (SliderUtils.isUnset(appHome)) {
      String agentImage = instanceDefinition.getInternalOperations().
          get(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH);
      sliderFileSystem.verifyFileExists(new Path(agentImage));
    }
  }

  @Override
  public void validateInstanceDefinition(AggregateConf instanceDefinition, SliderFileSystem fs) throws
      SliderException {
    super.validateInstanceDefinition(instanceDefinition, fs);
    log.debug("Validating conf {}", instanceDefinition);
    ConfTreeOperations resources =
        instanceDefinition.getResourceOperations();

    providerUtils.validateNodeCount(instanceDefinition, ROLE_NODE,
                                    0, -1);

    try {
      // Validate the app definition
      instanceDefinition.getAppConfOperations().
          getGlobalOptions().getMandatoryOption(AgentKeys.APP_DEF);
    } catch (BadConfigException bce) {
      throw new BadConfigException("Application definition must be provided. " + bce.getMessage());
    }
    String appDef = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getMandatoryOption(AgentKeys.APP_DEF);
    log.info("Validating app definition {}", appDef);
    String extension = appDef.substring(appDef.lastIndexOf(".") + 1, appDef.length());
    if (!"zip".equals(extension.toLowerCase(Locale.ENGLISH))) {
      throw new BadConfigException("App definition must be packaged as a .zip file. File provided is " + appDef);
    }

    Set<String> names = resources.getComponentNames();
    names.remove(SliderKeys.COMPONENT_AM);
    Map<Integer, String> priorityMap = new HashMap<Integer, String>();

    Metainfo metaInfo = null;
    if (fs != null) {
      try {
        metaInfo = AgentUtils.getApplicationMetainfo(fs, appDef);
      } catch (IOException ioe) {
        // Ignore missing metainfo file for now
        log.info("Missing metainfo.xml {}", ioe.getMessage());
      }
    }

    for (String name : names) {
      MapOperations component = resources.getMandatoryComponent(name);

      if (metaInfo != null) {
        Component componentDef = metaInfo.getApplicationComponent(name);
        if (componentDef == null) {
          throw new BadConfigException(
              "Component %s is not a member of application.", name);
        }
      }

      int priority =
          component.getMandatoryOptionInt(ResourceKeys.COMPONENT_PRIORITY);
      if (priority <= 0) {
        throw new BadConfigException("Component %s %s value out of range %d",
                                     name,
                                     ResourceKeys.COMPONENT_PRIORITY,
                                     priority);
      }

      String existing = priorityMap.get(priority);
      if (existing != null) {
        throw new BadConfigException(
            "Component %s has a %s value %d which duplicates that of %s",
            name,
            ResourceKeys.COMPONENT_PRIORITY,
            priority,
            existing);
      }
      priorityMap.put(priority, name);
    }

    // fileSystem may be null for tests
    if (metaInfo != null) {
      for (String name : names) {
        Component componentDef = metaInfo.getApplicationComponent(name);
        if (componentDef == null) {
          throw new BadConfigException(
              "Component %s is not a member of application.", name);
        }

        MapOperations componentConfig = resources.getMandatoryComponent(name);
        int count =
            componentConfig.getMandatoryOptionInt(ResourceKeys.COMPONENT_INSTANCES);
        int definedMinCount = componentDef.getMinInstanceCountInt();
        int definedMaxCount = componentDef.getMaxInstanceCountInt();
        if (count < definedMinCount || count > definedMaxCount) {
          throw new BadConfigException("Component %s, %s value %d out of range. "
                                       + "Expected minimum is %d and maximum is %d",
                                       name,
                                       ResourceKeys.COMPONENT_INSTANCES,
                                       count,
                                       definedMinCount,
                                       definedMaxCount);
        }
      }
    }
  }


  @Override
  public void prepareAMAndConfigForLaunch(SliderFileSystem fileSystem,
                                          Configuration serviceConf,
                                          AbstractLauncher launcher,
                                          AggregateConf instanceDefinition,
                                          Path snapshotConfDirPath,
                                          Path generatedConfDirPath,
                                          Configuration clientConfExtras,
                                          String libdir,
                                          Path tempPath,
                                          boolean miniClusterTestRun) throws
      IOException,
      SliderException {
    String agentImage = instanceDefinition.getInternalOperations().
        get(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    if (SliderUtils.isUnset(agentImage)) {
      Path agentPath = new Path(tempPath.getParent(), AgentKeys.PROVIDER_AGENT);
      log.info("Automatically uploading the agent tarball at {}", agentPath);
      fileSystem.getFileSystem().mkdirs(agentPath);
      if(ProviderUtils.addAgentTar(this, AGENT_TAR, fileSystem, agentPath)) {
        instanceDefinition.getInternalOperations().set(
            InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH,
            new Path(agentPath, AGENT_TAR).toUri());
      }
    }
  }

  @Override
  public Set<String> getApplicationTags(SliderFileSystem fileSystem,
                                        String appDef) throws SliderException {
    Set<String> tags;
    Metainfo metainfo;
    try {
      metainfo = AgentUtils.getApplicationMetainfo(fileSystem, appDef);
    } catch (IOException e) {
      log.error("Error retrieving metainfo from {}", appDef, e);
      throw new SliderException("Error retrieving metainfo", e);
    }

    if(metainfo == null) {
      log.error("Error retrieving metainfo from {}", appDef);
      throw new SliderException("Error parsing metainfo file, possibly bad structure.");
    }

    Application application = metainfo.getApplication();
    tags = new HashSet<String>();
    tags.add("Name: " + application.getName());
    tags.add("Version: " + application.getVersion());
    tags.add("Description: " + SliderUtils.truncate(application.getComment(), 80));

    return tags;
  }
}
