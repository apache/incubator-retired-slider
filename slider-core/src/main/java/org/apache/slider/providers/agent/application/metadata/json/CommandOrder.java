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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents package description.
 */
public class CommandOrder {
  protected static final Logger
      log = LoggerFactory.getLogger(CommandOrder.class);


  private String command;
  private String requires;

  /**
   * Creator.
   */
  public CommandOrder() {
  }


  public void setCommand(String name) {
    command = command;
  }

  public String getCommand() {
    return command;
  }

  public void setRequires(String type) {
    requires = requires;
  }

  public String getRequires() {
    return requires;
  }
}
