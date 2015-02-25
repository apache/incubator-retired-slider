/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.providers.agent.application.metadata.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * Represents the metadata associated with the application.
 */
public class MetaInfoParser {
  protected static final Logger
      log = LoggerFactory.getLogger(MetaInfoParser.class);

  private final GsonBuilder gsonBuilder = new GsonBuilder();
  private Gson gson;

  /**
   * Creator.
   */
  public MetaInfoParser() {
    gson = gsonBuilder.create();
  }


  /**
   * Convert to a JSON string
   *
   * @return a JSON string description
   *
   * @throws IOException Problems mapping/writing the object
   */
  public String toJsonString(MetaInfo metaInfo) throws IOException {
    return gson.toJson(metaInfo);
  }

  /**
   * Convert from JSON
   *
   * @param json input
   *
   * @return the parsed JSON
   *
   * @throws IOException IO
   */
  public MetaInfo fromJsonString(String json)
      throws IOException {
    return gson.fromJson(json, MetaInfo.class);
  }

  /**
   * Parse metainfo from an IOStream
   * @param is
   * @return
   * @throws IOException
   */
  public MetaInfo fromInputStream(InputStream is) throws IOException {
    StringWriter writer = new StringWriter();
    IOUtils.copy(is, writer);
    return fromJsonString(writer.toString());
  }
}
