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

package org.apache.slider.core.registry.docstore;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.slider.common.tools.ConfigHelper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public abstract class PublishedConfigurationOutputter {

  protected final PublishedConfiguration owner;

  protected PublishedConfigurationOutputter(PublishedConfiguration owner) {
    this.owner = owner;
  }

  public void save(File dest) throws IOException {
    
  }

  public String asString() throws IOException {
    return "";
  }

  public static PublishedConfigurationOutputter createOutputter(ConfigFormat format,
      PublishedConfiguration owner) {
    Preconditions.checkNotNull(owner);
    switch (format) {
      case XML:
        return new XmlOutputter(owner);
      case PROPERTIES:
        return new PropertiesOutputter(owner);
      case JSON:
        return new JsonOutputter(owner);
      default:
        throw new RuntimeException("Unsupported format :" + format);
    }
  }
  
  public static class XmlOutputter extends PublishedConfigurationOutputter {


    private final Configuration configuration;

    public XmlOutputter(PublishedConfiguration owner) {
      super(owner);
      configuration = owner.asConfiguration();
    }

    @Override
    public void save(File dest) throws IOException {
      FileOutputStream out = new FileOutputStream(dest);
      try {
        configuration.writeXml(out);
      } finally {
        out.close();
      }
    }

    @Override
    public String asString() throws IOException {
      return ConfigHelper.toXml(configuration);
    }

    public Configuration getConfiguration() {
      return configuration;
    }
  }
  
  public static class PropertiesOutputter extends PublishedConfigurationOutputter {

    private final Properties properties;

    public PropertiesOutputter(PublishedConfiguration owner) {
      super(owner);
      properties = owner.asProperties();
    }

    @Override
    public void save(File dest) throws IOException {
      FileOutputStream out = new FileOutputStream(dest);
      try {
        properties.store(out, "");
      } finally {
        out.close();
      }

    }

    @Override
    public String asString() throws IOException {
      return properties.toString();
    }
  }
    
    
  public static class JsonOutputter extends PublishedConfigurationOutputter {

    public JsonOutputter(PublishedConfiguration owner) {
      super(owner);
    }

    @Override
    public void save(File dest) throws IOException {
        FileUtils.writeStringToFile(dest, asString(), Charsets.UTF_8);
    }

    @Override
    public String asString() throws IOException {
      return owner.asJson();
    }
  }
    
  
  
}
