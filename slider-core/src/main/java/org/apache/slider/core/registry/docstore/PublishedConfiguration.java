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

import org.apache.hadoop.conf.Configuration;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.core.exceptions.BadConfigException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * JSON-serializable description of a published key-val configuration.
 * 
 * The values themselves are not serialized in the external view; they have
 * to be served up by the far end
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class PublishedConfiguration {

  public String description;

  public int elements;
  
  public long updated;
  
  public String updatedTime;

  public void setUpdated(long updated) {
    this.updated = updated;
    this.updatedTime = new Date(updated).toString();
  }

  public long getUpdated() {
    return updated;
  }

  @JsonIgnore
  private Map<String, String> values = new HashMap<String, String>();

  /**
   * Set the values from an iterable (this includes a Hadoop Configuration
   * and Java properties object).
   * Any existing value set is discarded
   * @param entries entries to put
   */
  @JsonIgnore
  public void putValues(Iterable<Map.Entry<String, String>> entries) {
    values = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : entries) {
      values.put(entry.getKey(), entry.getValue());
    }
    
  }

  /**
   * Convert to Hadoop XML
   * @return
   */
  public Configuration asConfiguration() {
    Configuration conf = new Configuration(false);
    try {
      ConfigHelper.addConfigMap(conf, values, "");
    } catch (BadConfigException e) {
      // triggered on a null value; switch to a runtime (and discard the stack)
      throw new RuntimeException(e.toString());
    }
    return conf;
  }
  
  public String asConfigurationXML() throws IOException {
    return ConfigHelper.toXml(asConfiguration());
  }

  /**
   * Convert values to properties
   * @return a property file
   */
  public Properties asProperties() {
    Properties props = new Properties();
    props.putAll(values);
    return props;
  }

  /**
   * Return the values as json string
   * @return
   * @throws IOException
   */
  public String asJson() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(values);
    return json;
  }
}
