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

import org.apache.hadoop.yarn.registry.client.draft1.RegistryWriter;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;

import java.io.IOException;

public class YarnRegistryViewForProviders {

  private final RegistryWriter registryWriter;

  private final String user;

  private final String sliderServiceclass;
  private final String instanceName;

  public YarnRegistryViewForProviders(RegistryWriter registryWriter,
      String user, String sliderServiceclass, String instanceName) {
    this.registryWriter = registryWriter;
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

  public RegistryWriter getRegistryWriter() {
    return registryWriter;
  }

  /**
   * Add a component under the slider name/entry
   * @param componentName
   * @param entry
   * @param ephemeral
   * @throws IOException
   */
  public void putComponent(String componentName,
      ServiceRecord entry,
      boolean ephemeral) throws
      IOException {
    registryWriter.putComponent(user, sliderServiceclass, instanceName,
        componentName,
        entry,
        ephemeral);
  }

  /**
   * Add a component under the slider name/entry
   * @param componentName
   * @param entry
   * @param ephemeral
   * @throws IOException
   */
  public void putComponent(String serviceClass,
      String serviceName,
      String componentName, ServiceRecord entry, boolean ephemeral) throws
      IOException {
    registryWriter.putComponent(user, serviceClass, serviceName, componentName,
        entry,
        ephemeral);
  }


}
