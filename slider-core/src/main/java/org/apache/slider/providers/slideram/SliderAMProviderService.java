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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.info.CommonRegistryConstants;
import org.apache.slider.core.registry.info.CustomRegistryConstants;
import org.apache.slider.core.registry.info.RegisteredEndpoint;
import org.apache.slider.core.registry.info.RegistryView;
import org.apache.slider.core.registry.info.ServiceInstanceData;
import org.apache.slider.providers.AbstractProviderService;
import org.apache.slider.providers.ProviderCompleted;
import org.apache.slider.providers.ProviderCore;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.agent.AgentKeys;
import org.apache.slider.server.appmaster.PublishedArtifacts;
import org.apache.slider.server.appmaster.web.rest.RestPaths;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.slider.server.appmaster.web.rest.RestPaths.SLIDER_PATH_MANAGEMENT;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.SLIDER_PATH_PUBLISHER;

/**
 * Exists just to move some functionality out of AppMaster into a peer class
 * of the actual service provider doing the real work
 */
public class SliderAMProviderService extends AbstractProviderService implements
    ProviderCore,
    AgentKeys,
    SliderKeys {

  public SliderAMProviderService() {
    super("SliderAMProviderService");
  }

  @Override
  public Configuration loadProviderConfigurationInformation(File confDir) throws
      BadCommandArgumentsException,
      IOException {
    return null;
  }

  @Override
  public void buildContainerLaunchContext(ContainerLauncher containerLauncher,
      AggregateConf instanceDefinition,
      Container container,
      String role,
      SliderFileSystem sliderFileSystem,
      Path generatedConfPath,
      MapOperations resourceComponent,
      MapOperations appComponent,
      Path containerTmpDirPath) throws IOException, SliderException {
    
  }

  @Override
  public boolean exec(AggregateConf instanceDefinition,
      File confDir,
      Map<String, String> env,
      ProviderCompleted execInProgress) throws IOException, SliderException {
    return false;
  }

  @Override
  public List<ProviderRole> getRoles() {
    return new ArrayList<ProviderRole>(0);
  }

  @Override
  public void validateInstanceDefinition(AggregateConf instanceDefinition) throws
      SliderException {

  }

  @Override
  public void applyInitialRegistryDefinitions(URL amWebURI,
                                              URL agentOpsURI,
                                              URL agentStatusURI,
                                              ServiceInstanceData instanceData) throws IOException {
    super.applyInitialRegistryDefinitions(amWebURI,
                                          agentOpsURI,
                                          agentStatusURI, instanceData
    );

    // now publish site.xml files
    YarnConfiguration defaultYarnConfig = new YarnConfiguration();
    amState.getPublishedSliderConfigurations().put(
        PublishedArtifacts.COMPLETE_CONFIG,
        new PublishedConfiguration(
            "Complete slider application settings",
            getConfig(), getConfig())
    );
    amState.getPublishedSliderConfigurations().put(
        PublishedArtifacts.YARN_SITE_CONFIG,
        new PublishedConfiguration(
            "YARN site settings",
            ConfigHelper.loadFromResource("yarn-site.xml"),
            defaultYarnConfig) );

    amState.getPublishedSliderConfigurations().put(
        PublishedArtifacts.CORE_SITE_CONFIG,
        new PublishedConfiguration(
            "Core site settings",
            ConfigHelper.loadFromResource("core-site.xml"),
            defaultYarnConfig) );
    amState.getPublishedSliderConfigurations().put(
        PublishedArtifacts.HDFS_SITE_CONFIG,
        new PublishedConfiguration(
            "HDFS site settings",
            ConfigHelper.loadFromResource("hdfs-site.xml"),
            new HdfsConfiguration(true)) );


    try {
      RegistryView externalView = instanceData.externalView;
      RegisteredEndpoint webUI =
          new RegisteredEndpoint(amWebURI, "Application Master Web UI");

      externalView.endpoints.put(CommonRegistryConstants.WEB_UI, webUI);

      externalView.endpoints.put(
          CustomRegistryConstants.MANAGEMENT_REST_API,
          new RegisteredEndpoint(
              new URL(amWebURI, SLIDER_PATH_MANAGEMENT),
              "Management REST API") );

      externalView.endpoints.put(
          CustomRegistryConstants.REGISTRY_REST_API,
          new RegisteredEndpoint(
              new URL(amWebURI, RestPaths.SLIDER_PATH_REGISTRY + "/" +
                                RestPaths.REGISTRY_SERVICE),
              "Registry Web Service" ) );

      URL publisherURL = new URL(amWebURI, SLIDER_PATH_PUBLISHER);
      externalView.endpoints.put(
          CustomRegistryConstants.PUBLISHER_REST_API,
          new RegisteredEndpoint(
              publisherURL,
              "Publisher Service") );
      
    /*
     * Set the configurations URL.
     */
      externalView.configurationsURL = SliderUtils.appendToURL(
          publisherURL.toExternalForm(), RestPaths.SLIDER_CONFIGSET);

    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }
}
