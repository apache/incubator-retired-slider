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

package org.apache.slider.client

import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.security.KerberosDiags
import org.apache.hadoop.yarn.conf.YarnConfiguration
import static org.apache.slider.common.Constants.SUN_SECURITY_KRB5_DEBUG
import org.apache.slider.common.params.ActionDiagnosticArgs
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.ClientArgs
import org.apache.slider.common.params.SliderActions
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.test.YarnZKMiniClusterTestBase
import org.junit.Test

@Slf4j
class TestDiagnostics extends YarnZKMiniClusterTestBase {

  @Test
  public void testClientDiags() throws Throwable {
    //launch fake master
    String clustername = createMiniCluster("", configuration, 1, true)
    ServiceLauncher<SliderClient> launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [SliderActions.ACTION_DIAGNOSTICS,
         Arguments.ARG_CLIENT]
    )
    def client = launcher.service
    def diagnostics = new ActionDiagnosticArgs()
    diagnostics.client = true
    diagnostics.verbose = true
    describe("Verbose diagnostics")
    client.actionDiagnostic(diagnostics)
  }

  /**
   * help should print out help string and then succeed
   * @throws Throwable
   */
  @Test
  public void testKDiag() throws Throwable {
    ServiceLauncher launcher = launch(SliderClient,
      SliderUtils.createConfiguration(),
      [
        ClientArgs.ACTION_KDIAG,
        ClientArgs.ARG_KEYLEN, "128",
        ClientArgs.ARG_SYSPROP,
        define(SUN_SECURITY_KRB5_DEBUG, "true")])

    assert 0 == launcher.serviceExitCode
  }

  @Test
  public void testKDiagExceptionConstruction() throws Throwable {
    assert new KerberosDiags.KerberosDiagsFailure("CAT", "%02d", 3).toString().contains("03")
    assert new KerberosDiags.KerberosDiagsFailure("CAT", "%w").toString().contains("%w")
    assert new KerberosDiags.KerberosDiagsFailure("CAT", new Exception(), "%w")
      .toString().contains("%w")
  }

  @Test
  public void testKDiagPrintln() throws Throwable {
    assert "%w" == KerberosDiags.format("%w")
    assert "%s" == KerberosDiags.format("%s")
    assert "false" == KerberosDiags.format("%s", false)
    def sw = new StringWriter()
    def kdiag = new KerberosDiags(new Configuration(),
      new PrintWriter(sw), [], null, "self", 16, false)
    try {
      kdiag.println("%02d", 3)
      kdiag.println("%s")
      kdiag.println("%w")
    } finally {
      kdiag.close()
    }
    def output = sw.toString()
    assert output.contains("03")
    assert output.contains("%s")
    assert output.contains("%w")
  }

  @Test
  public void testKDiagDumpFile() throws Throwable {
    def file1 = new File("./target/kdiaginput.txt")

    def s = 'invalid %w string %s'
    file1 << s
    def sw = new StringWriter()
    def kdiag = new KerberosDiags(new Configuration(),
      new PrintWriter(sw), [], null, "self", 16, false)
    try {
      kdiag.dump(file1)
    } finally {
      kdiag.close()
    }
    def output = sw.toString()
    assert output.contains(s)
  }
}
