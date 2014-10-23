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

package org.apache.slider.funtest.commands

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.registry.client.api.RegistryConstants

import static org.apache.slider.common.params.Arguments.*
import static org.apache.slider.core.main.LauncherExitCodes.*;
import org.apache.slider.funtest.framework.CommandTestBase
import org.junit.Test

@CompileStatic
@Slf4j
public class ResolveCommandIT extends CommandTestBase {

  @Test
  public void testRegistryIsNotLocalhost() throws Throwable {
    def quorum = SLIDER_CONFIG.get(RegistryConstants.KEY_REGISTRY_ZK_QUORUM)
    assert quorum != RegistryConstants.DEFAULT_REGISTRY_ZK_QUORUM;
  }
  
  @Test
  public void testResolveRoot() throws Throwable {
    resolve(0, 
        [ARG_LIST, ARG_PATH, "/"])
  }
  
  @Test
  public void testResolveUnknownPath() throws Throwable {
    resolve(EXIT_NOT_FOUND, 
        [ARG_LIST, ARG_PATH, "/undefined"])
  }

  @Test
  public void testResolveRootServiceRecord() throws Throwable {
    resolve(EXIT_NOT_FOUND, 
        [ARG_PATH, "/"])
  }

  @Test
  public void testResolveHomeServiceRecord() throws Throwable {
    resolve(EXIT_NOT_FOUND, 
        [ARG_PATH, "~"])
  }

}
