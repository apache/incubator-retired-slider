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

package org.apache.slider.providers.hbase.minicluster.masterless

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.slider.common.SliderKeys
import org.apache.slider.api.ClusterDescription
import org.apache.slider.common.tools.SliderFileSystem
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.core.main.ServiceLauncher
import org.apache.hadoop.fs.FileSystem as HadoopFS

import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Slf4j

class TestSliderConfDirToMasterlessAM extends HBaseMiniClusterTestBase {


  @Test
  public void testSliderConfDirToMasterlessAM() throws Throwable {
    String clustername = "test_slider_conf_dir_to_masterless_am"
    YarnConfiguration conf = configuration
    createMiniCluster(clustername, conf, 1, true)

    describe "verify that a conf dir will propagate via the sytem proerpty"

    File localConf = File.createTempDir("conf","dir")
    String name = "slider.xml"
    File sliderXML = new File(localConf, name)
    def out = new FileWriter(sliderXML)
    out.write(['a','b','c'] as char[])
    out.write("as string")
    out.flush()
    out.close()
    try {
      System.setProperty(SliderKeys.PROPERTY_CONF_DIR,localConf.absolutePath);
      ServiceLauncher<SliderClient> launcher = createMasterlessAM(clustername, 0, true, true)
      SliderClient client = launcher.service
      addToTeardown(client);
      ApplicationReport report = waitForClusterLive(client)

      ClusterDescription cd = waitForRoleCount(client,SliderKeys.COMPONENT_AM,
          1, hbaseClusterStartupTime)
      HadoopFS fs = HadoopFS.getLocal(conf);
      
      Path clusterDir = new SliderFileSystem(fs, conf).buildClusterDirPath(clustername)
      assert fs.exists(clusterDir);
      Path sliderConfDir = new Path(clusterDir, SliderKeys.SUBMITTED_CONF_DIR)
      assert fs.exists(sliderConfDir);
      Path remoteXml = new Path(sliderConfDir,name)
      assert fs.exists(remoteXml)
      

      clusterActionFreeze(client, clustername)
    } finally {
      SliderUtils.deleteDirectoryTree(localConf)
    }



  }


}
