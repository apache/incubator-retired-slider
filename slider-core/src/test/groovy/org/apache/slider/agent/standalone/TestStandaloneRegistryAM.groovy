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

package org.apache.slider.agent.standalone

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.api.ClusterNode
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.ActionRegistryArgs
import org.apache.slider.core.exceptions.BadCommandArgumentsException
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.persist.JsonSerDeser
import org.apache.slider.core.registry.docstore.PublishedConfigSet
import org.apache.slider.core.registry.docstore.PublishedConfiguration
import org.apache.slider.core.registry.info.CustomRegistryConstants
import org.apache.slider.core.registry.info.ServiceInstanceData
import org.apache.slider.core.registry.retrieve.RegistryRetriever
import org.apache.slider.server.appmaster.PublishedArtifacts
import org.apache.slider.server.appmaster.web.rest.RestPaths
import org.apache.slider.server.services.curator.CuratorServiceInstance
import org.apache.slider.server.services.registry.SliderRegistryService
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Slf4j

class TestStandaloneRegistryAM extends AgentMiniClusterTestBase {


  public static final String ARTIFACT_NAME = PublishedArtifacts.COMPLETE_CONFIG

  @Test
  public void testRegistryAM() throws Throwable {
    

    describe "create a masterless AM then perform registry operations on it"

    //launch fake master
    String clustername = "test_standalone_registry_am"
    createMiniCluster(clustername, configuration, 1, true)
    ServiceLauncher<SliderClient> launcher
    launcher = createMasterlessAM(clustername, 0, true, false)
    SliderClient client = launcher.service
    addToTeardown(client);

    ApplicationReport report = waitForClusterLive(client)
    logReport(report)
    List<ApplicationReport> apps = client.applications;

    List<ClusterNode> clusterNodes = client.listClusterNodesInRole(
        SliderKeys.COMPONENT_AM)
    assert ((List<ClusterNode>)clusterNodes).size() == 1

    ClusterNode masterNode = clusterNodes[0]
    log.info("Master node = ${masterNode}");

    List<ClusterNode> nodes
    String[] uuids = client.listNodeUUIDsByRole(SliderKeys.COMPONENT_AM)
    assert uuids.length == 1;
    nodes = client.listClusterNodes(uuids);
    assert ((List<ClusterNode>)nodes).size() == 1;
    describe "AM Node UUID=${uuids[0]}"

    nodes = listNodesInRole(client, SliderKeys.COMPONENT_AM)
    assert ((List<ClusterNode>)nodes).size() == 1;
    nodes = listNodesInRole(client, "")
    assert ((List<ClusterNode>)nodes).size() == 1;
    ClusterNode master = nodes[0]
    assert master.role == SliderKeys.COMPONENT_AM




    String username = client.username
    def yarnRegistryClient = client.YARNRegistryClient
    describe("list of all applications")
    logApplications(apps)
    describe("apps of user $username")
    List<ApplicationReport> userInstances = yarnRegistryClient.listInstances()
    logApplications(userInstances)
    assert userInstances.size() == 1
    describe("named app $clustername")
    ApplicationReport instance = yarnRegistryClient.findInstance(clustername)
    logReport(instance)
    assert instance != null

    //switch to the ZK-based registry

    describe "service registry names"
    SliderRegistryService registryService = client.registry
    def names = registryService.queryForNames();
    dumpRegistryNames(names)

    List<String> instanceIds = client.listRegistryInstanceIDs()


    dumpRegistryInstanceIDs(instanceIds)
    assert instanceIds.size() == 1

    List<CuratorServiceInstance<ServiceInstanceData>> instances =
        client.listRegistryInstances()
    dumpRegistryInstances(instances)

    assert instances.size() == 1

    def amInstance = instances[0]
    def serviceInstanceData = amInstance.payload

    def externalEndpoints = serviceInstanceData.externalView.endpoints

    // hit the registry web page

    def registryEndpoint = externalEndpoints.get(
        CustomRegistryConstants.REGISTRY_REST_API)
    assert registryEndpoint != null
    def registryURL = registryEndpoint.asURL()
    describe("Registry WADL @ $registryURL")
    
    def publisherEndpoint = externalEndpoints.get(CustomRegistryConstants.PUBLISHER_REST_API)
    assert publisherEndpoint != null
    def publisherURL = publisherEndpoint.asURL()
    def publisher = publisherURL.toString()
    describe("Publisher")

    def publishedJSON = GET(publisherURL)
//    log.info(publishedJSON)
    JsonSerDeser< PublishedConfigSet> serDeser= new JsonSerDeser<>(
        PublishedConfigSet)
    def configSet = serDeser.fromJson(publishedJSON)
    assert configSet.size() >= 1
    assert configSet.contains(ARTIFACT_NAME)
    PublishedConfiguration publishedYarnSite = configSet.get(ARTIFACT_NAME)

    assert publishedYarnSite.empty
    
    //get the full URL
    def yarnSitePublisher = appendToURL(publisher, ARTIFACT_NAME)

    String confJSON = GET(yarnSitePublisher)
//    log.info(confJSON)
    JsonSerDeser< PublishedConfiguration> confSerDeser =
        new JsonSerDeser<PublishedConfiguration>(PublishedConfiguration)

    publishedYarnSite = confSerDeser.fromJson(confJSON)
    
    assert !publishedYarnSite.empty


    //get the XML
    def yarnSiteXML = yarnSitePublisher + ".xml"


    String confXML = GET(yarnSiteXML)
    log.info("Conf XML at $yarnSiteXML = \n $confXML")

    String properties = GET(yarnSitePublisher + ".properties")
    Properties parsedProps = new Properties()
    parsedProps.load(new StringReader(properties))
    assert parsedProps.size() > 0
    def rmAddrFromDownloadedProperties = parsedProps.get(YarnConfiguration.RM_ADDRESS)
    def rmHostnameFromDownloadedProperties = parsedProps.get(YarnConfiguration.RM_HOSTNAME)
    assert rmAddrFromDownloadedProperties
    assert rmHostnameFromDownloadedProperties

    String json = GET(yarnSitePublisher + ".json")



    describe("Registry List")
    log.info(GET(registryURL, RestPaths.REGISTRY_SERVICE ))


    describe "Registry Retrieval Class"
    // retrieval

    RegistryRetriever retriever = new RegistryRetriever(serviceInstanceData)
    log.info retriever.toString()
    
    assert retriever.hasConfigurations(true)
    PublishedConfigSet externalConfSet = retriever.getConfigurations(true)
    externalConfSet.keys().each { String key ->
      def config = externalConfSet.get(key)
      log.info "$key -- ${config.description}"
    }
    assert externalConfSet[ARTIFACT_NAME]


    def yarnSite = retriever.retrieveConfiguration(
        externalConfSet,
        ARTIFACT_NAME,
        true)
    assert !yarnSite.empty
    def siteXML = yarnSite.asConfiguration()
    def rmHostnameViaClientSideXML = parsedProps.get(
        YarnConfiguration.RM_HOSTNAME)
    assert rmHostnameViaClientSideXML == rmHostnameFromDownloadedProperties
    def rmAddrViaClientSideXML = siteXML.get(YarnConfiguration.RM_ADDRESS)

  /* TODO SLIDER-52 PublishedConfiguration XML conf values are not resolved until client-side
   assert rmAddrViaClientSideXML == rmAddrFromDownloadedProperties
  */  
    describe "fetch missing artifact"
    try {
      retriever.retrieveConfiguration(externalConfSet, "no-such-artifact", true)
      fail("expected a failure")
    } catch (FileNotFoundException expected) {
      // expected
    }
    describe "Internal configurations"
    assert !retriever.hasConfigurations(false)
    try {
      retriever.getConfigurations(false)
      fail("expected a failure")
    } catch (FileNotFoundException expected) {
      // expected
    }


    // retrieval via API
    ActionRegistryArgs registryArgs = new ActionRegistryArgs()
    registryArgs.verbose = true

    // list all
    registryArgs.list = true;
    describe registryArgs.toString()
    client.actionRegistry(registryArgs)

    // list a named instance and expect a  failure
    registryArgs.list = true;
    registryArgs.name = "unknown"
    try {
      client.actionRegistryList(registryArgs)
    } catch (UnknownApplicationInstanceException ignored) {
      // expected 
    }

    // list all instances of an alternate type and expect failure
    registryArgs.list = true;
    registryArgs.name = null
    registryArgs.serviceType = "org.apache.hadoop"
    try {
      client.actionRegistryList(registryArgs)
    } catch (UnknownApplicationInstanceException ignored) {
      // expected 
    }

    //set the name
    registryArgs.name = serviceInstanceData.id;
    registryArgs.serviceType = SliderKeys.APP_TYPE
    

    //now expect list to work
    describe registryArgs.toString()

    def listedInstance = client.actionRegistryList(registryArgs)
    assert listedInstance[0].id == serviceInstanceData.id
    

    // listconf 
    registryArgs.list = false;
    registryArgs.listConf = true
    describe registryArgs.toString() 
    
    client.actionRegistry(registryArgs)

    // listconf --internal
    registryArgs.list = false;
    registryArgs.listConf = true
    registryArgs.internal = true
    describe registryArgs.toString()
    assert 0 != client.actionRegistry(registryArgs)

    registryArgs.list = false;
    registryArgs.listConf = false
    registryArgs.internal = false
    registryArgs.format = "properties"

    def yarn_site_config = PublishedArtifacts.YARN_SITE_CONFIG
    registryArgs.getConf = yarn_site_config
    
    
    describe registryArgs.toString()
    client.actionRegistry(registryArgs)

    File outputDir = new File("target/test_standalone_registry_am/output")
    outputDir.mkdirs()

    registryArgs.dest = outputDir
    describe registryArgs.toString()
    client.actionRegistry(registryArgs)
    assert new File(outputDir, yarn_site_config + ".properties").exists()

    registryArgs.format = "xml"
    client.actionRegistry(registryArgs)
    assert new File(outputDir, yarn_site_config + ".xml").exists()

    describe registryArgs.toString()

    def unknownFilename = "undefined-file"
    registryArgs.getConf = unknownFilename
    try {
      client.actionRegistry(registryArgs)
      fail("attempt to retrieve the file $unknownFilename succeeded")
    } catch (BadCommandArgumentsException expected) {
      assert expected.toString().contains(unknownFilename)
    }


    describe "freeze cluster"
    //now kill that cluster
    assert 0 == clusterActionFreeze(client, clustername)
    //list it & See if it is still there
    ApplicationReport oldInstance = yarnRegistryClient.findInstance(
        clustername)
    assert oldInstance != null
    assert oldInstance.yarnApplicationState >= YarnApplicationState.FINISHED


    sleep(20000)

    // now verify that the service is not in the registry 
    instances = client.listRegistryInstances()
    assert instances.size() == 0

  }


}
