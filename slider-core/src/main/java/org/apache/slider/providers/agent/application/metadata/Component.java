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
public class Component {
  String name;
  String category;
  String publishConfig;
  String minInstanceCount;
  String maxInstanceCount;
  String autoStartOnFailure;
  String appExports;
  CommandScript commandScript;
  List<ComponentExport> componentExports;

  public Component() {
    publishConfig = Boolean.FALSE.toString();
    componentExports = new ArrayList<ComponentExport>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCategory() {
    return category;
  }

  public void setCategory(String category) {
    this.category = category;
  }

  public String getPublishConfig() {
    return publishConfig;
  }

  public void setPublishConfig(String publishConfig) {
    this.publishConfig = publishConfig;
  }

  public String getAutoStartOnFailure() {
    return autoStartOnFailure;
  }

  public void setAutoStartOnFailure(String autoStartOnFailure) {
    this.autoStartOnFailure = autoStartOnFailure;
  }

  public String getAppExports() {
    return appExports;
  }

  public void setAppExports(String appExports) {
    this.appExports = appExports;
  }

  public String getMinInstanceCount() {
    return minInstanceCount;
  }

  public void setMinInstanceCount(String minInstanceCount) {
    this.minInstanceCount = minInstanceCount;
  }

  public String getMaxInstanceCount() {
    return maxInstanceCount;
  }

  public void setMaxInstanceCount(String maxInstanceCount) {
    this.maxInstanceCount = maxInstanceCount;
  }

  public CommandScript getCommandScript() {
    return commandScript;
  }

  public void addCommandScript(CommandScript commandScript) {
    this.commandScript = commandScript;
  }

  public void addComponentExport(ComponentExport export) {
    componentExports.add(export);
  }

  public List<ComponentExport> getComponentExports() {
    return componentExports;
  }

  public Boolean getRequiresAutoRestart() {
    return Boolean.parseBoolean(this.autoStartOnFailure);
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("{");
    sb.append(",\n\"name\": ").append(name);
    sb.append(",\n\"category\": ").append(category);
    sb.append(",\n\"commandScript\" :").append(commandScript);
    sb.append('}');
    return sb.toString();
  }
}
