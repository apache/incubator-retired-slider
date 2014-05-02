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

package org.apache.slider.providers

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.api.RoleKeys
import org.apache.slider.test.SliderTestBase
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j

class TestProviderUtils extends SliderTestBase {

  ProviderUtils providerUtils = new ProviderUtils(log)

  File archive = new File("target/testProviderUtils/tar")
  File hbase = new File(archive,"hbase-0.96.0")
  File bin = new File(hbase,"bin")
  File script = new File(bin, "hbase")

  @Before
  public void setup() {
    archive.mkdirs()
    bin.mkdirs()
    script.withPrintWriter { PrintWriter out ->
      out.println("Hello, world")
    }
  }

  @Test
  public void testScriptExists() throws Throwable {
    assert script.exists()
    assert script.file
  }
  @Test
  public void testFullSearch() throws Throwable {
    File sh = providerUtils.findBinScriptInExpandedArchive(archive,"bin","hbase")
    assert sh == script
  }
  
  
  @Test
  public void testFailScriptMissing() throws Throwable {
    
    File sh = providerUtils.findBinScriptInExpandedArchive(archive,"bin","hbase")
    assert sh == script
  }
  
  @Test
  public void testAdditionalArgs() {
    final String extraArgs = "--address 0.0.0.0";
    Map<String,String> roleOptions = [ (RoleKeys.ROLE_NAME):"foo", 
      (RoleKeys.ROLE_ADDITIONAL_ARGS):(extraArgs)
    ];
    
    String actualExtraArgs = ProviderUtils.getAdditionalArgs(roleOptions);
    
    assert extraArgs == actualExtraArgs;
  }
  
  @Test
  public void testUndefinedAdditionalArgs() {
    Map<String,String> roleOptions = [ (RoleKeys.ROLE_NAME):"foo", 
      ("newkey"):"1",
    ];
    
    String actualExtraArgs = ProviderUtils.getAdditionalArgs(roleOptions);
    
    assert "" == actualExtraArgs;
  }
  
  
}
