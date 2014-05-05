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
import org.apache.slider.core.exceptions.ErrorStrings;

import java.io.File;


/**
 * Registry actions
 * 
 * --instance {app name}, if  a / is in it, refers underneath?
 * --dest {destfile}
 * --list : list instances of slider service
 * --listfiles 
 */
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
  
  @Parameter(names = {ARG_LIST}, 
      description = "list services")
  public boolean list;

  @Parameter(names = {ARG_LISTCONF}, 
      description = "list configurations")
  public boolean listConf;

  @Parameter(names = {ARG_GETCONF},
      description = "get configuration")
  public String getConf;


  @Parameter(names = {ARG_LISTFILES}, 
      description = "list files")
  public String listFiles;

  @Parameter(names = {ARG_GETFILES},
      description = "get files")
  public String getFiles;


  //--format 
  @Parameter(names = ARG_FORMAT,
      description = "Format for a response: [text|xml|json|properties]")
  public String format;


  @Parameter(names = {ARG_DEST},
      description = "Output destination")
  public File dest;

  @Parameter(names = {ARG_NAME},
      description = "name of an instance")
  public String name;

  @Parameter(names = {ARG_SERVICETYPE},
      description = "optional service type")
  public String serviceType = SliderKeys.APP_TYPE;


  @Parameter(names = {ARG_VERBOSE},
      description = "verbose output")
  public boolean verbose;

  @Parameter(names = {ARG_INTERNAL},
      description = "fetch internal registry entries")
  public boolean internal;
  
  /**
   * validate health of all the different operations
   * @throws BadCommandArgumentsException
   */
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
    if (is(format) && !is(getConf)) {
      throw new BadCommandArgumentsException("Argument " + ARG_FORMAT
           + " is only supported by " + ARG_GETCONF);

    }
    if (!list && !is(name)) {
      throw new BadCommandArgumentsException("Argument " + ARG_NAME
           +" missing");

    }
  }
  
  private int s(String arg) {
    return is(arg) ? 1 : 0;
  }

  private boolean is(String arg) {
    return arg != null;
  }

  private int s(boolean arg) {
    return arg ? 1 : 0;
  }


  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("ActionRegistryArgs{");
    sb.append("list=").append(list);
    sb.append(", listConf=").append(listConf);
    sb.append(", getConf='").append(getConf).append('\'');
    sb.append(", listFiles='").append(listFiles).append('\'');
    sb.append(", getFiles='").append(getFiles).append('\'');
    sb.append(", format='").append(format).append('\'');
    sb.append(", dest=").append(dest);
    sb.append(", name='").append(name).append('\'');
    sb.append(", serviceType='").append(serviceType).append('\'');
    sb.append(", verbose=").append(verbose);
    sb.append(", internal=").append(internal);
    sb.append('}');
    return sb.toString();
  }
}
