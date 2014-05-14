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

import org.apache.slider.server.appmaster.web.rest.RestPaths;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Represents a set of configurations for an application, component, etc.
 * Json serialisable; accessors are synchronized
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class PublishedConfigSet {

  public static final String VALID_NAME_PATTERN = RestPaths.PUBLISHED_CONFIGURATION_REGEXP;
  public static final String E_INVALID_NAME =
      "Invalid configuration name -it must match the pattern " +
      VALID_NAME_PATTERN;
  private static final Pattern validNames = Pattern.compile(VALID_NAME_PATTERN);
  
  public Map<String, PublishedConfiguration> configurations =
      new HashMap<>();

  /**
   * Put a name -it will be converted to lower case before insertion.
   * Any existing entry will be overwritten (that includes an entry
   * with a different case in the original name)
   * @param name name of entry
   * @param conf configuration
   * @throws IllegalArgumentException if not a valid name
   */
  public void put(String name, PublishedConfiguration conf) {
    String name1 = name.toLowerCase(Locale.ENGLISH);
    validateName(name1);
    configurations.put(name1, conf);
  }

  /**
   * Validate the name -restricting it to the set defined in 
   * {@link #VALID_NAME_PATTERN}
   * @param name name to validate
   * @throws IllegalArgumentException if not a valid name
   */
  public static void validateName(String name) {
    if (!validNames.matcher(name).matches()) {
      throw new IllegalArgumentException(E_INVALID_NAME);
    }
  }

  public PublishedConfiguration get(String name) {
    return configurations.get(name);
  }
  
  public boolean contains(String name) {
    return configurations.containsKey(name);
  }
  
  public int size() {
    return configurations.size();
  }
  
  public Set<String> keys() {
    TreeSet<String> keys = new TreeSet<String>();
    keys.addAll(configurations.keySet());
    return keys;
  }

  public PublishedConfigSet shallowCopy() {
    PublishedConfigSet that = new PublishedConfigSet();
    for (Map.Entry<String, PublishedConfiguration> entry : configurations
        .entrySet()) {
      that.put(entry.getKey(), entry.getValue().shallowCopy());
    }
    return that;
  }
}
