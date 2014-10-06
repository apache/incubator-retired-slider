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

package org.apache.slider.common.params;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.UsageException;

import java.io.File;

import static org.apache.slider.common.params.SliderActions.ACTION_REGISTRY;
import static org.apache.slider.common.params.SliderActions.ACTION_RESOLVE;
import static org.apache.slider.common.params.SliderActions.DESCRIBE_ACTION_REGISTRY;

/**
 * Resolve registry entries
 * 
 * --path {path}
 * --out {destfile}
 * --verbose
 * --list
 */
@Parameters(commandNames = {ACTION_REGISTRY},
            commandDescription = DESCRIBE_ACTION_REGISTRY)
public class ActionResolveArgs extends AbstractActionArgs {

  public static final String USAGE =
      "Usage: " + SliderActions.ACTION_RESOLVE
      + " "
      + Arguments.ARG_PATH + " <path> "
      + "[" + Arguments.ARG_LIST + "] "
      + "[" + Arguments.ARG_VERBOSE + "] "
      + "[" + Arguments.ARG_OUTPUT + " <filename> ] "
      ;
  public ActionResolveArgs() {
  }

  @Override
  public String getActionName() {
    return ACTION_RESOLVE;
  }

  /**
   * Get the min #of params expected
   * @return the min number of params in the {@link #parameters} field
   */
  @Override
  public int getMinParams() {
    return 0;
  }
  
  @Parameter(names = {ARG_LIST}, 
      description = "list services")
  public boolean list;


  @Parameter(names = {ARG_PATH},
      description = "resolve a path")
  public String path;

  @Parameter(names = {ARG_OUTPUT, ARG_OUTPUT_SHORT},
      description = "Output destination")
  public File out;

 @Parameter(names = {ARG_VERBOSE},
      description = "verbose output")
  public boolean verbose;

  @Parameter(names = {ARG_INTERNAL},
      description = "fetch internal registry entries")
  public boolean internal;

  
}
