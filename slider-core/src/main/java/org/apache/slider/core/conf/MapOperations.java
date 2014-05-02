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

package org.apache.slider.core.conf;

import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Standard map operations.
 *
 * This delegates the standard map interface to the map passed in,
 * so it can be used to add more actions to the map.
 */
public class MapOperations implements Map<String, String> {
  private static final Logger log =
    LoggerFactory.getLogger(MapOperations.class);

  /**
   * Global options
   */
  public final Map<String, String> options;

  public final String name;

  public MapOperations() {
    options = new HashMap<String, String>();
    name = "";
  }

  /**
   * Create an instance
   * @param name
   * @param options
   */
  public MapOperations(String name, Map<String, String> options) {
    assert options != null : "null map";
    this.options = options;
    this.name = name;
  }


  /**
   * Get a cluster option or value
   *
   * @param key
   * @param defVal
   * @return option in map or the default
   */
  public String getOption(String key, String defVal) {
    String val = options.get(key);
    return val != null ? val : defVal;
  }


  /**
   * Get a cluster option or value
   *
   * @param key
   * @return the value
   * @throws BadConfigException if the option is missing
   */
  public String getMandatoryOption(String key) throws BadConfigException {
    String val = options.get(key);
    if (val == null) {
      if (log.isDebugEnabled()) {
        log.debug("Missing key {} from config containing {}",
                  key, this);
      }
      throw new BadConfigException("Missing option " + key);
    }
    return val;
  }

  /**
   * Get an integer option; use {@link Integer#decode(String)} so as to take hex
   * oct and bin values too.
   *
   * @param option option name
   * @param defVal default value
   * @return parsed value
   * @throws NumberFormatException if the role could not be parsed.
   */
  public int getOptionInt(String option, int defVal) {
    String val = getOption(option, Integer.toString(defVal));
    return Integer.decode(val);
  }
  /**
   * Get a mandatory integer option; use {@link Integer#decode(String)} so as to take hex
   * oct and bin values too.
   *
   * @param option option name
   * @return parsed value
   * @throws NumberFormatException if the option could not be parsed.
   * @throws BadConfigException if the option could not be found
   */
  public int getMandatoryOptionInt(String option) throws BadConfigException {
    getMandatoryOption(option);
    return getOptionInt(option, 0);
  }

  /**
   * Verify that an option is set: that is defined AND non-empty
   * @param key
   * @throws BadConfigException
   */
  public void verifyOptionSet(String key) throws BadConfigException {
    if (SliderUtils.isUnset(getOption(key, null))) {
      throw new BadConfigException("Unset option %s", key);
    }
  }
  
  public void mergeWithoutOverwrite(Map<String, String> that) {
    SliderUtils.mergeMapsIgnoreDuplicateKeys(options, that);
  }

  /**
   * Merge a map by prefixed keys
   * @param that the map to merge in
   * @param prefix prefix to match on
   * @param overwrite flag to enable overwrite
   */
  public void mergeMapPrefixedKeys(Map<String, String> that,
                                    String prefix,
                                    boolean overwrite) {
    for (Map.Entry<String, String> entry : that.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(prefix)) {
        if (overwrite || get(key) == null) {
          put(key, entry.getValue());
        }
      }
    }
  }

  /**
   * Set a property if it is not already set
   * @param key key
   * @param value value
   */
  public void putIfUnset(String key, String value) {
    if (get(key) == null) {
      put(key, value);
    }
  }
  
  public void set(String key, Object value) {
    assert value != null;
    put(key, value.toString());
  }

  public int size() {
    return options.size();
  }

  public boolean isEmpty() {
    return options.isEmpty();
  }

  public boolean containsValue(Object value) {
    return options.containsValue(value);
  }

  public boolean containsKey(Object key) {
    return options.containsKey(key);
  }

  public String get(Object key) {
    return options.get(key);
  }

  public String put(String key, String value) {
    return options.put(key, value);
  }

  public String remove(Object key) {
    return options.remove(key);
  }

  public void putAll(Map<? extends String, ? extends String> m) {
    options.putAll(m);
  }

  public void clear() {
    options.clear();
  }

  public Set<String> keySet() {
    return options.keySet();
  }

  public Collection<String> values() {
    return options.values();
  }

  public Set<Map.Entry<String, String>> entrySet() {
    return options.entrySet();
  }

  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  public boolean equals(Object o) {
    return options.equals(o);
  }

  @Override
  public int hashCode() {
    return options.hashCode();
  }

  public boolean isSet(String key) {
    return SliderUtils.isSet(get(key));
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(name).append("=\n");

    for (Entry<String, String> entry : options.entrySet()) {
      builder.append("  ")
             .append(entry.getKey())
             .append('=')
             .append(entry.getValue())
             .append('\n');
    }
    return builder.toString();
  }
}
