/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.funtest.accumulo

import groovy.util.logging.Slf4j
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.funtest.framework.AccumuloSliderShell
import org.junit.BeforeClass

import java.nio.ByteBuffer

@Slf4j
class AccumuloScriptIT extends AccumuloBasicIT {
  public static final String RESOURCES_DIR = sysprop("test.app.resources.dir")
  public static final String ACCUMULO_HOME = RESOURCES_DIR + "/install"
  public static final String ACCUMULO_CONF = sysprop("test.app.resources.dir") + "/conf"

  @Override
  public String getClusterName() {
    return "test_script";
  }

  @Override
  public String getDescription() {
    return "Test accumulo-slider client script $clusterName"
  }

  @BeforeClass
  public static void setShell() {
    AccumuloSliderShell.scriptFile = new File(sysprop("test.app.scripts.dir"),
      "accumulo-slider").canonicalFile
    AccumuloSliderShell.setEnv("SLIDER_HOME", SLIDER_TAR_DIR)
    AccumuloSliderShell.setEnv("SLIDER_CONF_DIR", SLIDER_CONF_DIR)
    AccumuloSliderShell.setEnv("ACCUMULO_HOME", ACCUMULO_HOME)
  }

  @Override
  public void clusterLoadOperations(ClusterDescription cd, SliderClient sliderClient) {
    String clusterName = getClusterName()
    AccumuloSliderShell.run(0, "--app $clusterName quicklinks")
    AccumuloSliderShell.run(0, "--app $clusterName install $ACCUMULO_HOME")
    AccumuloSliderShell.run(0, "--app $clusterName --appconf $ACCUMULO_CONF getconf")
    runBoth("shell -u $USER -p $PASSWORD -e tables")
    runBoth("login-info")

    AccumuloSliderShell info = runOne("info")
    String monitor = getMonitorUrl(sliderClient, getClusterName())
    assert info.outputContains(monitor.substring(monitor.indexOf("://")+3)),
      "accumulo info output did not contain monitor"

    runOne("version")
    runOne("classpath")
    runOne("create-token -u $USER -p $PASSWORD -f $RESOURCES_DIR/token")
    runOne("admin checkTablets")
    runOne("admin listInstances")
    runOne("admin ping")
    runOne("admin dumpConfig -a -d $RESOURCES_DIR")
    runOne("admin volumes")

    // TODO: test tool, jar, classname, rfile-info
    // runOne("shell -u $USER -p $PASSWORD -e \"createtable testtable\"")
  }

  public AccumuloSliderShell runOne(String cmd) {
    return AccumuloSliderShell.run(0, "--appconf $ACCUMULO_CONF $cmd")
  }

  public void runBoth(String cmd) {
    String clusterName = getClusterName()
    AccumuloSliderShell.run(0, "--app $clusterName $cmd")
    AccumuloSliderShell.run(0, "--appconf $ACCUMULO_CONF $cmd")
  }
}
