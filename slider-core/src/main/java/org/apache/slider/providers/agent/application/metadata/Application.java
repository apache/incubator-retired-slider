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
 * Application type defined in the metainfo
 */
public class Application {
  String name;
  String comment;
  String version;
  String exportedConfigs;
  List<Component> components;
  List<ExportGroup> exportGroups;
  List<OSSpecific> osSpecifics;
  List<CommandOrder> commandOrders;
  ConfigurationDependencies configDependencies;

  public Application() {
    exportGroups = new ArrayList<>();
    components = new ArrayList<>();
    osSpecifics = new ArrayList<>();
    commandOrders = new ArrayList<>();
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

  public String getExportedConfigs() {
    return exportedConfigs;
  }

  public void setExportedConfigs(String exportedConfigs) {
    this.exportedConfigs = exportedConfigs;
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

  public void addExportGroup(ExportGroup exportGroup) {
    exportGroups.add(exportGroup);
  }

  public List<ExportGroup> getExportGroups() {
    return exportGroups;
  }

  public void addOSSpecific(OSSpecific osSpecific) {
    osSpecifics.add(osSpecific);
  }

  public List<OSSpecific> getOSSpecifics() {
    return osSpecifics;
  }

  public void addCommandOrder(CommandOrder commandOrder) {
    commandOrders.add(commandOrder);
  }

  public List<CommandOrder> getCommandOrder() {
    return commandOrders;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("{");
    sb.append(",\n\"name\": ").append(name);
    sb.append(",\n\"comment\": ").append(comment);
    sb.append(",\n\"version\" :").append(version);
    sb.append(",\n\"components\" : {");
    for (Component component : components) {
      sb.append("\n").append(component);
    }
    sb.append("\n},");
    sb.append('}');
    return sb.toString();
  }
}
