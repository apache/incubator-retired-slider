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

import org.apache.slider.providers.agent.application.metadata.json.MetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application metainfo uber class
 */
public class Metainfo {
  protected static final Logger log =
      LoggerFactory.getLogger(Metainfo.class);

  String schemaVersion;
  Application application;
  MetaInfo jsonEncoded;

  public Metainfo() {
  }

  // TODO: Temporary code - the plan is to move to the json parsed MetaInfo
  public Metainfo(MetaInfo jsonEncoded) {
    jsonEncoded = jsonEncoded;
    // initialize the class
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(String schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  public Application getApplication() {
    return application;
  }

  public void setApplication(Application application) {
    this.application = application;
  }

  public Component getApplicationComponent(String roleName) {
    if (application == null) {
      log.error("Malformed app definition: Expect application as the top level element for metainfo.xml");
    } else {
      for (Component component : application.getComponents()) {
        if (component.getName().equals(roleName)) {
          return component;
        }
      }
    }
    return null;
  }

  public static Metainfo fromJsonObject(MetaInfo jsonEncoded) {
    return new Metainfo(jsonEncoded);
  }
}
