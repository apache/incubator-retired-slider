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

package org.apache.slider.funtest.abstracttests

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.funtest.framework.ConfLoader
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.test.SliderTestUtils
import org.junit.Test
import org.apache.hadoop.fs.FileSystem as HadoopFS 

/**
 * Simple tests to verify that the build has been set up: if these
 * fail then the arguments to the test run are incomplete.
 *
 * This deliberately doesn't depend on SliderCommandTestBase,
 * so that individual tests fail with more diagnostics
 * than the @BeforeClass failing
 */
@CompileStatic
@Slf4j
abstract class AbstractTestBuildSetup extends SliderTestUtils implements FuntestProperties {


  String getRequiredSysprop(String name) {
    String val = System.getProperty(name)
    if (!val) {
      fail("System property not set : $name")

    }
    return val;
  }

  public String getSliderConfDirProp() {
    return getRequiredSysprop(SLIDER_CONF_DIR_PROP)
  }

  public File getSliderConfDirectory() {
    return getCanonicalFile(sliderConfDirProp)
  }

  File getCanonicalFile(String dir) {
    assert dir
    return new File(dir).canonicalFile
  }

  File getBinDirectory() {
    return getCanonicalFile(getBinDirProp())
  }

  public String getBinDirProp() {
    return getRequiredSysprop(SLIDER_BIN_DIR_PROP)
  }

  public File getConfXML() {
    new File(sliderConfDirectory, CLIENT_CONFIG_FILENAME).canonicalFile
  }

  public File getSliderScript() {
    new File(binDirectory, "bin/" + SCRIPT_NAME).canonicalFile
  }

  /**
   * Load the client XML file
   * @return
   */
  public Configuration loadSliderConf() {
    Configuration conf = ConfLoader.loadSliderConf(confXML)
    return conf
  }

  @Test
  public void testConfDirSet() throws Throwable {
    assert getSliderConfDirProp()
    log.info("Slider Configuration directory $sliderConfDirProp")
  }

  @Test
  public void testConfDirExists() throws Throwable {
    assert sliderConfDirectory.exists()
  }


  @Test
  public void testConfDirHasClientXML() throws Throwable {
    File clientXMLFile = confXML
    assert clientXMLFile.exists()
    clientXMLFile.toString()
  }


  @Test
  public void testBinDirExists() throws Throwable {
    log.info("binaries dir = $binDirectory")
  }

  @Test
  public void testBinScriptExists() throws Throwable {
    assert sliderScript.exists()
  }

  @Test
  public void testConfLoad() throws Throwable {
    Configuration conf = loadSliderConf()
  }

  @Test
  public void testConfHasFileURL() throws Throwable {
    Configuration conf = loadSliderConf()
    assert conf.get(KEY_TEST_CONF_XML)
    String confXml = conf.get(KEY_TEST_CONF_XML)
    URL confURL = new URL(confXml)
    log.info("$KEY_TEST_CONF_XML = $confXML  -as URL: $confURL")
    Path path = new Path(confURL.toURI())
    
    def fs = HadoopFS.get(path.toUri(), conf)
    assert fs.exists(path)
  }

  @Test
  public void testConfHasDefaultFS() throws Throwable {
    Configuration conf = loadSliderConf()
    String fs = conf.get("fs.defaultFS")
    log.info("Test Filesystem $fs")
    assert fs != null
  }


  @Test
  public void testConfHasRM() throws Throwable {
    Configuration conf = loadSliderConf()
    String val = conf.get(YarnConfiguration.RM_ADDRESS)
    log.info("$YarnConfiguration.RM_ADDRESS = $val")
    assert val != YarnConfiguration.DEFAULT_RM_ADDRESS
  }

  @Test
  public void testSecuritySettingsValid() throws Throwable {
    Configuration conf = loadSliderConf();
    if (SliderUtils.isHadoopClusterSecure(conf)) {
      UserGroupInformation.setLoginUser(null)
    }
    if (SliderUtils.maybeInitSecurity(conf)) {
      log.info("Security enabled")
      SliderUtils.forceLogin()
    }
    log.info("Login User = ${UserGroupInformation.loginUser}")
  }


}
