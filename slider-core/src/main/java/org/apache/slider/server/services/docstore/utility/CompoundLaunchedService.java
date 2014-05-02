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

package org.apache.slider.server.services.docstore.utility;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.main.LauncherExitCodes;
import org.apache.slider.core.main.RunService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompoundLaunchedService extends CompoundService
    implements RunService {
  private static final Logger log = LoggerFactory.getLogger(
      CompoundLaunchedService.class);
  private String[] argv;
  
  public CompoundLaunchedService(String name) {
    super(name);
  }

  public CompoundLaunchedService() {
    super("CompoundLaunchedService");
  }

  public CompoundLaunchedService(Service... children) {
    super(children);
  }

  /**
   * Implementation of set-ness, groovy definition of true/false for a string
   * @param s
   * @return
   */
  protected static boolean isUnset(String s) {
    return SliderUtils.isUnset(s);
  }

  protected static boolean isSet(String s) {
    return SliderUtils.isSet(s);
  }

  protected String[] getArgv() {
    return argv;
  }

  /**
   * Pre-init argument binding
   * @param config the initial configuration build up by the
   * service launcher.
   * @param args argument list list of arguments passed to the command line
   * after any launcher-specific commands have been stripped.
   * @return the configuration
   * @throws Exception
   */
  @Override
  public Configuration bindArgs(Configuration config, String... args) throws
                                                                      Exception {
    this.argv = args;
    if (log.isDebugEnabled()) {
      log.debug("Binding {} Arguments:", args.length);

      StringBuilder builder = new StringBuilder();
      for (String arg : args) {
        builder.append('"').append(arg).append("\" ");
      }
      log.debug(builder.toString());
    }
    return config;
  }

  @Override
  public int runService() throws Throwable {
    return LauncherExitCodes.EXIT_SUCCESS;
  }

  @Override
  public void addService(Service service) {
    Preconditions.checkNotNull(service, "null service");
    super.addService(service);
  }

  /**
   * Run a child service -initing and starting it if this
   * service has already passed those parts of its own lifecycle
   * @param service the service to start
   */
  protected boolean deployChildService(Service service) {
    service.init(getConfig());
    addService(service);
    if (isInState(STATE.STARTED)) {
      service.start();
      return true;
    }
    return false;
  }

  protected void requireArgumentSet(String argname, String argfield)
      throws BadCommandArgumentsException {
    if (isUnset(argfield)) {
      throw new BadCommandArgumentsException("Required argument "
                                             + argname
                                             + " missing");
    }
  }

  protected void requireArgumentSet(String argname, Object argfield) throws
                                               BadCommandArgumentsException {
    if (argfield == null) {
      throw new BadCommandArgumentsException("Required argument "
                                             + argname
                                             + " missing");
    }
  }
}
