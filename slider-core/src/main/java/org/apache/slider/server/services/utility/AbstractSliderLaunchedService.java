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

package org.apache.slider.server.services.utility;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.draft1.RegistryWriterService;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.zk.ZookeeperUtils;
import org.apache.slider.server.services.curator.CuratorHelper;
import org.apache.slider.server.services.registry.SliderRegistryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base service for the standard slider client/server services
 */
public abstract class AbstractSliderLaunchedService extends
    LaunchedWorkflowCompositeService {
  private static final Logger log =
    LoggerFactory.getLogger(AbstractSliderLaunchedService.class);

  public AbstractSliderLaunchedService(String name) {
    super(name);
    // make sure all the yarn configs get loaded
    new YarnConfiguration();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    String quorum = lookupZKQuorum();
    conf.set(RegistryConstants.REGISTRY_ZK_QUORUM, quorum);
    super.serviceInit(conf);
  }

  /**
   * Start the registration service
   * @return the instance
   * @throws BadConfigException
   */
  protected SliderRegistryService startRegistrationService()
      throws BadConfigException {

    String registryQuorum = lookupZKQuorum();
    String zkPath = getConfig().get(
        SliderXmlConfKeys.REGISTRY_PATH,
        SliderXmlConfKeys.DEFAULT_REGISTRY_PATH);
    return startSliderRegistrationService(registryQuorum, zkPath);
  }

  /**
   * look up the registry quorum from the config
   * @return the quorum string
   * @throws BadConfigException if it is not there or invalid
   */
  public String lookupZKQuorum() throws BadConfigException {
    String registryQuorum = getConfig().get(RegistryConstants.REGISTRY_ZK_QUORUM);
    registryQuorum = getConfig().get(
        SliderXmlConfKeys.REGISTRY_ZK_QUORUM,
        registryQuorum);
    if (SliderUtils.isUnset(registryQuorum)) {
      throw new BadConfigException(
          "No Zookeeper quorum provided in the"
          + " configuration property " + RegistryConstants.REGISTRY_ZK_QUORUM
      );
    }
    ZookeeperUtils.splitToHostsAndPortsStrictly(registryQuorum);
    return registryQuorum;
  }

  /**
   * Start the registration service
   * @param zkConnection
   * @param zkPath
   * @return
   */
  public SliderRegistryService startSliderRegistrationService(
      String zkConnection,
      String zkPath) {
    CuratorHelper curatorHelper =
      new CuratorHelper(getConfig(), zkConnection);

    //registry will start curator as well as the binder, in the correct order
    SliderRegistryService registryBinderService =
      curatorHelper.createRegistryBinderService(zkPath);
    deployChildService(registryBinderService);
    return registryBinderService;
  }

  /**
   * Start the YARN registration service
   * @param zkConnection
   * @param zkPath
   * @return
   */
  public RegistryWriterService startYarnRegistryService()
      throws BadConfigException {

    Configuration conf = getConfig();
    // push back the slider registry entry if needed
    String quorum = lookupZKQuorum();
    conf.set(RegistryConstants.REGISTRY_ZK_QUORUM, quorum);
    RegistryWriterService registryWriterService =
        new RegistryWriterService("YarnRegistry");
    deployChildService(registryWriterService);
    return registryWriterService;
  }

  /**
   * Utility method to require an argument to be set (non null, non-empty)
   * @param argname argument name
   * @param value value
   * @throws BadCommandArgumentsException if the condition is not met
   */
  protected static void requireArgumentSet(String argname, String value)
      throws BadCommandArgumentsException {
    if (isUnset(value)) {
      throw new BadCommandArgumentsException("Required argument "
                                             + argname
                                             + " missing");
    }
  }


}
