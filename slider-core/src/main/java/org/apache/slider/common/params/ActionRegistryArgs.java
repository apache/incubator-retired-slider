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
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.ErrorStrings;

import java.io.File;

@Parameters(commandNames = {SliderActions.ACTION_REGISTRY},
            commandDescription = SliderActions.DESCRIBE_ACTION_REGISTRY)

public class ActionRegistryArgs extends AbstractActionArgs {
  @Override
  public String getActionName() {
    return SliderActions.ACTION_REGISTRY;
  }


  /**
   * Get the min #of params expected
   * @return the min number of params in the {@link #parameters} field
   */
  @Override
  public int getMinParams() {
    return 0;
  }

  //--format 
  @Parameter(names = ARG_FORMAT,
      description = "Format for a response: [text|xml|json|properties]")
  public String format = FORMAT_XML;


  @Parameter(names = {ARG_DEST},
      description = "Output destination")
  public File dest;

  @Parameter(names = {ARG_LIST}, 
      description = "list services")
  public String list;

  @Parameter(names = {ARG_LISTCONF}, 
      description = "list configurations")
  public String listConf;

  @Parameter(names = {ARG_GETCONF},
      description = "get files")
  public String getConf;


  @Parameter(names = {ARG_LISTFILES}, 
      description = "list files")
  public String listFiles;

  @Parameter(names = {ARG_GETFILES},
      description = "get files")
  public String getFiles;

  @Override
  public void validate() throws BadCommandArgumentsException {
    super.validate();

    //verify that at most one of the operations is set
    int gets = s(getConf) + s(getFiles);
    int lists = s(list) + s(listConf) + s(listFiles);
    int set = lists + gets;
    if (set > 1) {
      throw new BadCommandArgumentsException(
          ErrorStrings.ERROR_TOO_MANY_ARGUMENTS);
    }
    if (dest != null && (lists > 0 || set == 0)) {
      throw new BadCommandArgumentsException("Argument " + ARG_DEST
                           + " is only supported on 'get' operations");
    }
  }
  
  @SuppressWarnings("VariableNotUsedInsideIf")
  private int s(String arg) {
    return arg != null ? 1 : 0;
  }
}
