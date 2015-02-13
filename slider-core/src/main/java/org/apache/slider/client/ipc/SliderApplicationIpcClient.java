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

package org.apache.slider.client.ipc;

import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.api.types.ContainerInformation;
import org.apache.slider.api.types.PingInformation;
import org.apache.slider.api.SliderApplicationApi;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Implementation of the Slider RESTy Application API over IPC.
 */
public class SliderApplicationIpcClient implements SliderApplicationApi {

  private static final Logger log =
      LoggerFactory.getLogger(SliderApplicationIpcClient.class);

  private final SliderClusterOperations operations;

  public SliderApplicationIpcClient(SliderClusterOperations operations) {
    this.operations = operations;
  }

  @Override
  public AggregateConf getDesiredModel() throws IOException {
    return operations.getModelDesired();
  }

  @Override
  public ConfTreeOperations getDesiredAppconf() throws IOException {
    return operations.getModelDesiredAppconf();
  }

  @Override
  public ConfTreeOperations getDesiredResources() throws IOException {
    return operations.getModelDesiredResources();
  }

  @Override
  public AggregateConf getResolvedModel() throws IOException {
    return operations.getModelResolved();
  }

  @Override
  public ConfTreeOperations getResolvedAppconf() throws IOException {
    return operations.getModelResolvedAppconf();
  }

  @Override
  public ConfTreeOperations getResolvedResources() throws IOException {
    return operations.getModelResolvedResources();
  }

  @Override
  public ConfTreeOperations getLiveResources() throws IOException {
    return operations.getLiveResources();
  }

  @Override
  public Map<String, ContainerInformation> enumContainers() throws IOException {
    return operations.enumContainers();
  }

  @Override
  public ContainerInformation getContainer(String containerId) throws
      IOException {
    return operations.getContainer(containerId);
  }

  @Override
  public Map<String, ComponentInformation> enumComponents() throws IOException {
    return operations.enumComponents();
  }

  @Override
  public ComponentInformation getComponent(String componentName) throws
      IOException {
    return operations.getComponent(componentName);

  }

  @Override
  public PingInformation ping(String text) throws IOException {
    return null;
  }

  @Override
  public void stop(String text) throws IOException {
    operations.stop(text);
  }

  @Override
  public ApplicationLivenessInformation getApplicationLiveness() throws
      IOException {
    return operations.getApplicationLiveness();
  }
}
