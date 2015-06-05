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
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.CommandTestBase
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class CommandEnvironmentIT extends CommandTestBase {

  public static final String TESTPROPERTY_SET_IN_FUNTEST = "testpropertySetInFuntest"
  public static final String TEST_PROPERTY_VALUE = "TestPropertyValue"
  File originalScript
  
  @Before
  public void cacheScript() {
    originalScript = SliderShell.scriptFile
  }
  
  @After
  public void restoreScript() {
    SliderShell.scriptFile = originalScript
  }

  @Test
  public void testJVMOptionPassdownBash() throws Throwable {
    SliderShell shell = diagnostics(false)
    assertOutputContains(shell, TESTPROPERTY_SET_IN_FUNTEST, 2)
    assertOutputContains(shell, TEST_PROPERTY_VALUE, 2)
  }

  @Test
  public void testJVMOptionPassdownPython() throws Throwable {
    SliderShell shell = diagnostics(true)
    assertOutputContains(shell, TESTPROPERTY_SET_IN_FUNTEST, 2)
    assertOutputContains(shell, TEST_PROPERTY_VALUE, 2)
  }

  @Test
  public void testLibdirPython() throws Throwable {
    SliderShell shell = diagnostics(true)
    assertOutputContains(shell, SliderKeys.PROPERTY_LIB_DIR)
  }

  @Test
  public void testLibdirBash() throws Throwable {
    SliderShell shell = diagnostics(false)
    assertOutputContains(shell, SliderKeys.PROPERTY_LIB_DIR)
  }


  @Test
  public void testConfdirPython() throws Throwable {
    SliderShell shell = diagnostics(true)
    assertOutputContains(shell, SliderKeys.PROPERTY_CONF_DIR)
  }

  @Test
  public void testConfdirBash() throws Throwable {
    SliderShell shell = diagnostics(false)
    assertOutputContains(shell, SliderKeys.PROPERTY_CONF_DIR)
  }

  public SliderShell diagnostics(boolean python) {
    if (python) {
      SliderShell.scriptFile = SLIDER_SCRIPT_PYTHON;
    } else {
      assumeNotWindows()
      SliderShell.scriptFile = SLIDER_SCRIPT;
    }
    SliderShell shell = new SliderShell([
        SliderActions.ACTION_DIAGNOSTICS,
        Arguments.ARG_CLIENT,
        Arguments.ARG_VERBOSE
    ])
    shell.setEnv(SliderKeys.SLIDER_JVM_OPTS, 
        "-D${TESTPROPERTY_SET_IN_FUNTEST}=${TEST_PROPERTY_VALUE}")
    shell.execute(0)
    return shell
  }

}
