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
  CommandScript commandScript;
  List<Export> exports;

  public Component() {
    publishConfig = Boolean.FALSE.toString();
    exports = new ArrayList<>();
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

  public void addExport(Export export) {
    exports.add(export);
  }

  public List<Export> getExports() {
    return exports;
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
