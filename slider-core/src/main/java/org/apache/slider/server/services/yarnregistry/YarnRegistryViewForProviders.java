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

package org.apache.slider.server.services.yarnregistry;

import org.apache.hadoop.yarn.registry.client.binding.BindingUtils;
import org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.yarn.registry.client.services.RegistryOperationsService;
import org.apache.hadoop.yarn.registry.client.api.CreateFlags;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;

import java.io.IOException;

public class YarnRegistryViewForProviders {

  private final RegistryOperationsService registryOperations;

  private final String user;

  private final String sliderServiceclass;
  private final String instanceName;

  public YarnRegistryViewForProviders(RegistryOperationsService registryOperations,
      String user, String sliderServiceclass, String instanceName) {
    this.registryOperations = registryOperations;
    this.user = user;
    this.sliderServiceclass = sliderServiceclass;
    this.instanceName = instanceName;
  }

  public String getUser() {
    return user;
  }

  public String getSliderServiceclass() {
    return sliderServiceclass;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public RegistryOperationsService getRegistryOperationsService() {
    return registryOperations;
  }

  /**
   * Add a component under the slider name/entry
   * @param componentName component name
   * @param record record to put
   * @throws IOException
   */
  public void putComponent(String componentName,
      ServiceRecord record) throws
      IOException {
    putComponent(sliderServiceclass, instanceName,
        componentName,
        record
    );
  }

  /**
   * Add a component 
   * @param serviceClass service class to use under ~user
   * @param componentName component name
   * @param record record to put
   * @throws IOException
   */
  public void putComponent(String serviceClass,
      String serviceName,
      String componentName,
      ServiceRecord record) throws IOException {
    String path = BindingUtils.componentPath(
        user, serviceClass, serviceName, componentName);
    registryOperations.mkdir(RegistryPathUtils.parentOf(path), true);
    registryOperations.create(path, record,
        CreateFlags.OVERWRITE);
  }


  /**
   * Add a service under
   * @param componentName
   * @param record
   * @param ephemeral
   * @throws IOException
   */
  public void putService(String username,
      String serviceClass,
      String serviceName,
      ServiceRecord record) throws IOException {

    
    String path = BindingUtils.servicePath(
        username, serviceClass, serviceName);
    registryOperations.mkdir(RegistryPathUtils.parentOf(path), true);
    registryOperations.create(path, record, CreateFlags.OVERWRITE);

  }


  public void rmComponent(String componentName) throws IOException {
    String path = BindingUtils.componentPath(
        user, sliderServiceclass, instanceName,
        componentName);
    registryOperations.delete(path, false);
  }
}
