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

package org.apache.slider.agent.freezethaw

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.ActionLookupArgs
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.exceptions.BadCommandArgumentsException
import org.apache.slider.core.exceptions.NotFoundException
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.main.ServiceLaunchException
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.persist.ApplicationReportSerDeser
import org.junit.Test

/**
 * stop and start an AM. For fun authorization is turned on, so this
 * verifies that the AM comes up successfully
 */
@CompileStatic
@Slf4j

class TestFreezeThawFlexStandaloneAM extends AgentMiniClusterTestBase {

  File getConfDirFile() {
    return new File("target/testFreezeThawFlexStandaloneAM/conf")
  }

  @Override
  String getConfDir() {
    return confDirFile.toURI().toString()
  }

  @Test
  public void testFreezeThawFlexStandaloneAM() throws Throwable {
    YarnConfiguration conf = configuration
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false);
    String clustername = createMiniCluster("", conf, 1, 1, 1, true, false)
    
    describe "create a standalone AM, stop it, start it"
    //copy the confdir somewhere
    Path resConfPath = new Path(resourceConfDirURI)
    Path tempConfPath = new Path(confDir)
    SliderUtils.copyDirectory(conf, resConfPath, tempConfPath, null)


    ServiceLauncher<SliderClient> launcher = createStandaloneAM(
        clustername,
        true,
        true)
    SliderClient sliderClient = launcher.service
    addToTeardown(sliderClient);

    assert 0 == clusterActionFreeze(sliderClient, clustername)
    def report = sliderClient.applicationReport
    assert report.finalApplicationStatus == FinalApplicationStatus.SUCCEEDED

    // here we do something devious: delete our copy of the configuration
    // this makes sure the remote config gets picked up
    HadoopFS localFS = HadoopFS.get(tempConfPath.toUri(), conf)
    localFS.delete(tempConfPath,true)
    
    //now start the cluster
    File appreport = new File("target/$clustername/appreport.json")
    ServiceLauncher launcher2 = thawCluster(clustername,
        [Arguments.ARG_OUTPUT, appreport.absolutePath],
        true);

    SliderClient newCluster = launcher2.service
    addToTeardown(newCluster);
    ApplicationReportSerDeser serDeser = new ApplicationReportSerDeser();
    def sar = serDeser.fromFile(appreport)
    log.info(sar.toString())
    assert sar.applicationId != null

    describe("lookup")

    // now via lookup
    appreport.delete()
    def lookup1 = new ActionLookupArgs()
    lookup1.id = sar.applicationId

    assert 0 == newCluster.actionLookup(lookup1)
    lookup1.outputFile = appreport
    assert 0 == newCluster.actionLookup(lookup1)
    sar = serDeser.fromFile(appreport)
    assert sar.state == YarnApplicationState.RUNNING.toString()
    

    newCluster.getClusterDescription(clustername);
    
    describe("no change flex")
    // while running, flex it with no changes
    newCluster.flex(clustername, [:]);

    // force freeze now
    
    assert 0 == clusterActionFreeze(newCluster, clustername, "forced", true)
    report = newCluster.applicationReport
    assert report.finalApplicationStatus == FinalApplicationStatus.KILLED

    assert 0 == newCluster.actionLookup(lookup1)
    sar = serDeser.fromFile(appreport)
    assert sar.finalStatus == FinalApplicationStatus.KILLED.toString()
    
    //stop again
    assert 0 == clusterActionFreeze(newCluster, clustername)

    // and add some invalid lookup operations for
    
    def lookup2 = new ActionLookupArgs()
    lookup2.id = "invalid"
    try {
      newCluster.actionLookup(lookup2)
      fail("expected $lookup2 to fail")
    } catch (BadCommandArgumentsException expected) {
    }
    try {
      lookup2.id = "application_1414593568640_0002"
      newCluster.actionLookup(lookup2)
      fail("expected $lookup2 to fail")
    } catch (NotFoundException expected) {
    }
  }

}
