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

package org.apache.slider.client

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.params.ActionTokensArgs
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.core.exceptions.BadClusterStateException
import org.apache.slider.core.exceptions.NotFoundException
import org.apache.slider.core.main.ServiceLauncherBaseTest
import org.junit.Test

/**
 * Test the argument parsing/validation logic
 */
@CompileStatic
@Slf4j
class TestSliderTokensCommand extends ServiceLauncherBaseTest {

  public static YarnConfiguration config = createTestConfig()

  public static YarnConfiguration createTestConfig() {
    def configuration = new YarnConfiguration()
    configuration.set(YarnConfiguration.RM_ADDRESS, "127.0.0.1:8032")
    return configuration
  }

  @Test
  public void testBadSourceArgs() throws Throwable {
    launchExpectingException(SliderClient,
      config,
      ActionTokensArgs.DUPLICATE_ARGS,
      [SliderActions.ACTION_TOKENS,
       Arguments.ARG_SOURCE, "target/tokens.bin",
       Arguments.ARG_OUTPUT, "target/tokens.bin",
      ])
  }

  @Test
  public void testKTNoPrincipal() throws Throwable {
    launchExpectingException(SliderClient,
      config,
      ActionTokensArgs.MISSING_KT_PROVIDER,
      [SliderActions.ACTION_TOKENS,
       Arguments.ARG_KEYTAB, "target/keytab",
      ])
  }

  @Test
  public void testPrincipalNoKT() throws Throwable {
    launchExpectingException(SliderClient,
      config,
      ActionTokensArgs.MISSING_KT_PROVIDER,
      [SliderActions.ACTION_TOKENS,
       Arguments.ARG_PRINCIPAL, "bob@REALM",
      ])
  }

  /**
   * A missing keytab is an error
   * @throws Throwable
   */
  @Test
  public void testMissingKT() throws Throwable {
    def ex = launchExpectingException(SliderClient,
      config,
      TokensOperation.E_NO_KEYTAB,
      [SliderActions.ACTION_TOKENS,
       Arguments.ARG_PRINCIPAL, "bob@REALM",
       Arguments.ARG_KEYTAB, "target/keytab",
      ])
    if (!(ex instanceof NotFoundException)) {
      throw ex
    }
  }

  @Test
  public void testMissingSourceFile() throws Throwable {
    def ex = launchExpectingException(SliderClient,
      config,
      TokensOperation.E_MISSING_SOURCE_FILE,
      [SliderActions.ACTION_TOKENS,
       Arguments.ARG_SOURCE, "target/tokens.bin",
      ])
    if (!(ex instanceof NotFoundException)) {
      throw ex
    }
  }

  @Test
  public void testListHarmlessWhenInsecure() throws Throwable {
    execSliderCommand(0, config, [SliderActions.ACTION_TOKENS])
  }

  @Test
  public void testCreateFailsWhenInsecure() throws Throwable {
    def ex = launchExpectingException(SliderClient,
      config,
      TokensOperation.E_INSECURE,
      [SliderActions.ACTION_TOKENS,
       Arguments.ARG_OUTPUT, "target/tokens.bin",
      ])
    if (!(ex instanceof BadClusterStateException)) {
      throw ex
    }
  }



}
