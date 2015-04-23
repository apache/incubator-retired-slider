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
import org.apache.accumulo.examples.simple.helloworld.InsertWithBatchWriter
import org.apache.accumulo.examples.simple.helloworld.ReadData
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.hadoop.registry.client.api.RegistryConstants
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.funtest.framework.AccumuloSliderShell
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.BeforeClass

@Slf4j
class AccumuloScriptIT extends AccumuloBasicIT {
  public static final String CLIENT_INSTALL_DIR = sysprop("test.client.install.dir")
  public static final String CLIENT_HOME_DIR = sysprop("test.client.home.dir")
  public static final String CLIENT_INSTALL_CONF = sysprop("test.app.resources.dir") + "/clientInstallConfig-default.json"

  public static final File ACCUMULO_SLIDER_SCRIPT = new File(CLIENT_HOME_DIR + "/bin", "accumulo-slider").canonicalFile
  public static final File ACCUMULO_SCRIPT = new File(CLIENT_HOME_DIR + "/bin", "accumulo").canonicalFile
  public static final File ACCUMULO_TOOL_SCRIPT = new File(CLIENT_HOME_DIR + "/bin", "tool.sh").canonicalFile
  public static final File ACCUMULO_CLIENT_CONF = new File(CLIENT_HOME_DIR + "/conf", "client.conf").canonicalFile

  public static final Map<String,String> customProps = new HashMap<String,String>()

  @BeforeClass
  public static void setProps() {
    customProps.put("instance.zookeeper.timeout", "60s")
    customProps.put("trace.zookeeper.path", "/tracers2")
    customProps.put("trace.span.receiver.some.property", "some.value");
    customProps.put("trace.span.receiver.other.property", "comma,separated,value");
  }

  protected String templateName() {
    return sysprop("test.app.resources.dir") + "/appConfig_script.json"
  }

  protected ConfTree modifyTemplate(ConfTree confTree) {
    for (Map.Entry<String,String> p : customProps.entrySet()) {
      confTree.global.put("site.accumulo-site." + p.getKey(), p.getValue())
    }
    return confTree
  }

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
    AccumuloSliderShell.setEnv("SLIDER_HOME", SLIDER_TAR_DIR)
    AccumuloSliderShell.setEnv("SLIDER_CONF_DIR", SLIDER_CONF_DIR)
    File dir = new File(CLIENT_INSTALL_DIR)
    if (!dir.exists()) {
      dir.mkdir()
    }
  }

  public static AccumuloSliderShell accumulo_slider(String cmd) {
    AccumuloSliderShell.scriptFile = ACCUMULO_SLIDER_SCRIPT
    return AccumuloSliderShell.run(0, cmd)
  }

  public static AccumuloSliderShell accumulo(String cmd) {
    AccumuloSliderShell.scriptFile = ACCUMULO_SCRIPT
    return AccumuloSliderShell.run(0, cmd)
  }

  public static AccumuloSliderShell tool(String cmd) {
    AccumuloSliderShell.scriptFile = ACCUMULO_TOOL_SCRIPT
    return AccumuloSliderShell.run(0, cmd)
  }

  @Override
  public void clusterLoadOperations(ClusterDescription cd, SliderClient sliderClient) {
    String clusterName = getClusterName()

    SliderShell shell = slider(EXIT_SUCCESS,
      [
        ACTION_CLIENT, ARG_INSTALL,
        ARG_PACKAGE, TEST_APP_PKG_DIR+"/"+TEST_APP_PKG_FILE,
        ARG_DEST, CLIENT_INSTALL_DIR,
        ARG_CONFIG, CLIENT_INSTALL_CONF,
        ARG_NAME, clusterName
      ])
    logShell(shell)

    assert ACCUMULO_CLIENT_CONF.exists(), "client.conf did not exist after " +
      "client install command"

    PropertiesConfiguration conf = new PropertiesConfiguration()
    conf.setDelimiterParsingDisabled(true)
    conf.load(ACCUMULO_CLIENT_CONF)

    for (Map.Entry<String,String> p : customProps.entrySet()) {
      assert conf.getString(p.getKey()).equals(p.getValue()), "client.conf " +
        "did not contain expected property $p"
    }

    accumulo_slider("--app $clusterName quicklinks")

    accumulo("shell -u $USER -p $PASSWORD -e tables")
    accumulo("login-info")

    AccumuloSliderShell info = accumulo("info")
    String monitor = getMonitorUrl(sliderClient, getClusterName())
    assert info.outputContains(monitor.substring(monitor.indexOf("://")+3)),
      "accumulo info output did not contain monitor"

    accumulo("version")
    accumulo("classpath")
    accumulo("admin listInstances")
    accumulo("admin ping")

    String zookeepers = SLIDER_CONFIG.get(
      RegistryConstants.KEY_REGISTRY_ZK_QUORUM,
      FuntestProperties.DEFAULT_SLIDER_ZK_HOSTS)
    String instance = tree.global.get("site.client.instance.name")
    accumulo("shell -u $USER -p $PASSWORD -e \"createtable test1\"")
    accumulo(InsertWithBatchWriter.class.getName() + " -i $instance -z " +
      "$zookeepers -u $USER -p $PASSWORD -t test1")
    accumulo(ReadData.class.getName() + " -i $instance -z $zookeepers -u " +
      "$USER -p $PASSWORD -t test1 --startKey row_0 --endKey row_101")
  }
}
