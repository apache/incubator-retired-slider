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
import org.junit.Test

@CompileStatic
@Slf4j
public class DiagnosticsCommandIT extends CommandTestBase {

  @Test
  public void testClientDiagnostics() throws Throwable {
    def shell = slider(0,
        [
            SliderActions.ACTION_DIAGNOSTICS,
            Arguments.ARG_CLIENT,
            Arguments.ARG_VERBOSE
        ]
    )
    println(shell.stdoutHistory)
    println()
    println(shell.stdErrHistory)
  }

  @Test
  public void testJVMOptionPassdown() throws Throwable {
    SliderShell shell = new SliderShell([
        SliderActions.ACTION_DIAGNOSTICS,
        Arguments.ARG_CLIENT,
        Arguments.ARG_VERBOSE
    ])

    def name = "testpropertySetInFuntest"

    def val = "TestPropertyValue"
    shell.setEnv(SliderKeys.SLIDER_JVM_OPTS, "-D" + name + "=" + val)
    shell.execute(0)
    assert shell.outputContains(name)
    assert shell.outputContains(val)
    assert shell.outputContains(SliderKeys.PROPERTY_LIB_DIR)
    assert shell.outputContains(SliderKeys.PROPERTY_CONF_DIR)

  }

}
