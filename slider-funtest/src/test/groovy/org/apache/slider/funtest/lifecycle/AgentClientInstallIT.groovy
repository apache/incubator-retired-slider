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

package org.apache.slider.funtest.lifecycle

import groovy.io.FileType
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.StatusKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class AgentClientInstallIT extends AgentCommandTestBase
  implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {


  @Test
  public void testAgentClientInstall() throws Throwable {
    assumeNotWindows()
    describe "Install command logger client"
    File zipFileName = new File(TEST_APP_PKG_DIR, TEST_APP_PKG_FILE).canonicalFile
    File tmpFile = File.createTempFile("del", "");
    File dest = new File(tmpFile.getParentFile(), tmpFile.getName() + "dir");
    String CLIENT_CONFIG =
        "../slider-core/src/test/app_packages/test_command_log/client-config.json"

    dest.mkdir()

    SliderShell shell = slider(EXIT_SUCCESS,
        [
            ACTION_CLIENT,
            "--install",
            ARG_DEST, dest.canonicalPath,
            ARG_PACKAGE, zipFileName.absolutePath,
            ARG_CONFIG, CLIENT_CONFIG
        ])
    logShell(shell)

    def list = []

    dest.eachFileRecurse (FileType.FILES) { file ->
      list << file.toString()
    }

    String expectedFile1 = new File(dest, "operations.log").toString();
    String expectedFile2 = new File(new File(dest, "command-logger-app"), "operations.log").toString();

    assert list.contains(expectedFile1)
    assert list.contains(expectedFile2)
  }
}
