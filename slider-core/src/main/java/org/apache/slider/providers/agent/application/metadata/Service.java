/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.providers.agent.application.metadata;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class Service {
  String name;
  String comment;
  String version;
  List<Component> components;
  List<OSSpecific> osSpecifics;
  ConfigurationDependencies configDependencies;

  public Service() {
    components = new ArrayList<Component>();
    osSpecifics = new ArrayList<OSSpecific>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public ConfigurationDependencies getConfigDependencies() {
    return configDependencies;
  }

  public void setConfigDependencies(ConfigurationDependencies configDependencies) {
    this.configDependencies = configDependencies;
  }

  public void addComponent(Component component) {
    components.add(component);
  }

  public List<Component> getComponents() {
    return components;
  }

  public void addOSSpecific(OSSpecific osSpecific) {
    osSpecifics.add(osSpecific);
  }

  public List<OSSpecific> getOSSpecifics() {
    return osSpecifics;
  }
}
