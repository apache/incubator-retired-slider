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
import org.apache.slider.api.OptionKeys;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
    sliderFileSystem.verifyFileExists(new Path(appDef));

    String agentConf = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getMandatoryOption(AgentKeys.AGENT_CONF);
    sliderFileSystem.verifyFileExists(new Path(agentConf));

    String appHome = instanceDefinition.getAppConfOperations().
        getGlobalOptions().get(AgentKeys.PACKAGE_PATH);
    if (SliderUtils.isUnset(appHome)) {
      String agentImage = instanceDefinition.getInternalOperations().
          get(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH);
      sliderFileSystem.verifyFileExists(new Path(agentImage));
    }
  }

  @Override
  public void validateInstanceDefinition(AggregateConf instanceDefinition) throws
      SliderException {
    super.validateInstanceDefinition(instanceDefinition);
    log.debug("Validating conf {}", instanceDefinition);
    ConfTreeOperations resources =
        instanceDefinition.getResourceOperations();
    ConfTreeOperations appConf =
        instanceDefinition.getAppConfOperations();

    providerUtils.validateNodeCount(instanceDefinition, ROLE_NODE,
                                    0, -1);

    Set<String> names = resources.getComponentNames();
    names.remove(SliderKeys.COMPONENT_AM);
    Map<Integer, String> priorityMap = new HashMap<Integer, String>();
    for (String name : names) {
      MapOperations component = resources.getMandatoryComponent(name);

      // Validate count against the metainfo.xml

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

    try {
      // Validate the app definition
      instanceDefinition.getAppConfOperations().
          getGlobalOptions().getMandatoryOption(AgentKeys.APP_DEF);
    } catch (BadConfigException bce) {
      throw new BadConfigException("Application definition must be provided." + bce.getMessage());
    }

    String appHome = instanceDefinition.getAppConfOperations().
        getGlobalOptions().get(AgentKeys.PACKAGE_PATH);
    String agentImage = instanceDefinition.getInternalOperations().
        get(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH);

    if (SliderUtils.isUnset(appHome) && SliderUtils.isUnset(agentImage)) {
      throw new BadConfigException("Either agent package path " +
                                   AgentKeys.PACKAGE_PATH + " or image root " +
                                   OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH
                                   + " must be provided");
    }

    try {
      // Validate the agent config
      instanceDefinition.getAppConfOperations().
          getGlobalOptions().getMandatoryOption(AgentKeys.AGENT_CONF);
    } catch (BadConfigException bce) {
      throw new BadConfigException("Agent config "+ AgentKeys.AGENT_CONF 
                                   + " property must be provided.");
    }
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
      Path tempPath, boolean miniClusterTestRun) throws
      IOException,
      SliderException {
  }
}
