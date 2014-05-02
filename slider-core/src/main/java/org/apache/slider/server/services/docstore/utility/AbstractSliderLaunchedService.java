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

package org.apache.slider.server.services.docstore.utility;


import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.registry.info.ServiceInstanceData;
import org.apache.slider.core.registry.zk.ZookeeperUtils;
import org.apache.slider.server.services.curator.CuratorHelper;
import org.apache.slider.server.services.curator.RegistryBinderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.slider.common.SliderXmlConfKeys.DEFAULT_REGISTRY_PATH;
import static org.apache.slider.common.SliderXmlConfKeys.REGISTRY_PATH;

/**
 * Base service for the standard slider client/server services
 */
public abstract class AbstractSliderLaunchedService extends
                                                    CompoundLaunchedService {
  private static final Logger log =
    LoggerFactory.getLogger(AbstractSliderLaunchedService.class);

  public AbstractSliderLaunchedService(String name) {
    super(name);
    // make sure all the yarn configs get loaded
    new YarnConfiguration();
  }

  /**
   * Start the registration service
   * @return the instance
   * @throws BadConfigException
   */
  protected RegistryBinderService<ServiceInstanceData> startRegistrationService()
      throws BadConfigException {

    String registryQuorum = lookupZKQuorum();
    String zkPath = getConfig().get(REGISTRY_PATH, DEFAULT_REGISTRY_PATH);
    return startRegistrationService(registryQuorum, zkPath);
  }

  /**
   * look up the registry quorum from the config
   * @return the quorum string
   * @throws BadConfigException if it is not there or invalid
   */
  public String lookupZKQuorum() throws BadConfigException {
    String registryQuorum = getConfig().get(SliderXmlConfKeys.REGISTRY_ZK_QUORUM);
    if (SliderUtils.isUnset(registryQuorum)) {
      throw new BadConfigException(
          "No Zookeeper quorum provided in the"
          + " configuration property " + SliderXmlConfKeys.REGISTRY_ZK_QUORUM
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
  public RegistryBinderService<ServiceInstanceData> startRegistrationService(
    String zkConnection, String zkPath) {
    CuratorHelper curatorHelper =
      new CuratorHelper(getConfig(), zkConnection);

    //registry will start curator as well as the binder, in the correct order
    RegistryBinderService<ServiceInstanceData> registryBinderService =
      curatorHelper.createRegistryBinderService(zkPath);
    boolean started = deployChildService(registryBinderService);
    return registryBinderService;
  }


}
