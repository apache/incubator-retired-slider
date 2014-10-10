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
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.PathNotFoundException
import org.apache.hadoop.registry.client.binding.RegistryPathUtils
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.registry.client.api.RegistryConstants
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils
import org.apache.hadoop.registry.client.impl.RegistryOperationsClient
import org.apache.hadoop.registry.client.types.RegistryPathStatus
import org.apache.hadoop.registry.client.types.ServiceRecord
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes
import org.apache.slider.common.params.ActionResolveArgs
import org.apache.slider.common.params.Arguments
import org.apache.slider.core.exceptions.BadCommandArgumentsException
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.core.main.LauncherExitCodes

import static org.apache.hadoop.registry.client.binding.RegistryUtils.*
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.api.ClusterNode
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.ActionRegistryArgs
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.persist.JsonSerDeser
import org.apache.slider.core.registry.docstore.PublishedConfigSet
import org.apache.slider.core.registry.docstore.PublishedConfiguration
import org.apache.slider.core.registry.docstore.UriMap
import org.apache.slider.core.registry.info.CustomRegistryConstants
import org.apache.slider.core.registry.retrieve.RegistryRetriever
import org.apache.slider.server.appmaster.PublishedArtifacts
import org.apache.slider.server.appmaster.web.rest.RestPaths
import org.junit.Test

import static org.apache.slider.core.registry.info.CustomRegistryConstants.*

/**
 *  work with a YARN registry
 */
@CompileStatic
@Slf4j

class TestStandaloneYarnRegistryAM extends AgentMiniClusterTestBase {


  public static final String ARTIFACT_NAME = PublishedArtifacts.COMPLETE_CONFIG
  public static final String HBASE = "hbase/localhost@HADOOP.APACHE.ORG"

  @Test
  public void testStandaloneYarnRegistryAM() throws Throwable {
    

    describe "create a masterless AM then perform YARN registry operations on it"

    
    String clustername = createMiniCluster(configuration, 1, true)
    
    // get local binding
    def registryOperations = microZKCluster.registryOperations
    try {
      registryOperations.stat(RegistryConstants.PATH_SYSTEM_SERVICES)
    } catch (PathNotFoundException e) {
      log.warn(" RM is not apparently running registry services: {}", e, e)
    }
    
    ServiceLauncher<SliderClient> launcher
    launcher = createStandaloneAM(clustername, true, false)
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
    def yarnRegistryClient = client.yarnAppListClient
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

    // sleep to allow registration to complete
    sleep(5000)


    describe "service registry names"
    def registryService = client.registryOperations

    RegistryOperationsClient registryOperationsClient =
        registryService as RegistryOperationsClient
    try {
      def yarnRegistryDump = registryOperationsClient.dumpPath(false) 
      log.info("yarn service registry: \n${yarnRegistryDump}\n")
    } catch (IOException ignored) {

    }
        

    def self = currentUser()
    def children = statChildren(registryService, homePathForUser(self));
    Collection<RegistryPathStatus> serviceTypes = children.values()
    dumpCollection(serviceTypes)

    def recordsPath = serviceclassPath(self, SliderKeys.APP_TYPE)

    Map<String, ServiceRecord> recordMap = extractServiceRecords(
        registryService,
        recordsPath);
    def serviceRecords = recordMap.values();
    dumpCollection(serviceRecords)
    assert serviceRecords.size() == 1

    def serviceRecord = serviceRecords[0]
    log.info(serviceRecord.toString())

    assert serviceRecord[YarnRegistryAttributes.YARN_ID] != null
    assert serviceRecord[YarnRegistryAttributes.YARN_PERSISTENCE] != ""
    def externalEndpoints = serviceRecord.external;
    assert externalEndpoints.size() > 0

    def am_ipc_protocol = AM_IPC_PROTOCOL
    serviceRecord.getExternalEndpoint(am_ipc_protocol)
    assert null != am_ipc_protocol;

    assert null != serviceRecord.getExternalEndpoint(MANAGEMENT_REST_API)
    assert null != serviceRecord.getExternalEndpoint(PUBLISHER_REST_API)
    // internals
    assert null != serviceRecord.getInternalEndpoint(AGENT_ONEWAY_REST_API)
    assert null != serviceRecord.getInternalEndpoint(AGENT_SECURE_REST_API)

    // use the resolve operation

    describe "resolve CLI action"
    File destDir= new File("target/resolve")
    ServiceRecordMarshal serviceRecordMarshal = new ServiceRecordMarshal()
    FileUtil.fullyDelete(destDir)
    File resolveListDir= new File(destDir, "list")
    ActionResolveArgs resolveList = new ActionResolveArgs(
        path:recordsPath,
        list:true)

    // to stdout
    assert 0 == client.actionResolve(resolveList)
    // to a file
    resolveList.out = resolveListDir;
    try {
      client.actionResolve(resolveList)
    } catch (BadCommandArgumentsException ex) {
      assertExceptionDetails(ex, LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR,
          Arguments.ARG_OUTPUT)
    }
    
    // list to a destination dir
    resolveList.out = null
    resolveList.destdir = resolveListDir
    assert 0 == client.actionResolve(resolveList)
    File resolvedFile = new File(resolveListDir, clustername + ".json")
    assertFileExists("resolved file", resolvedFile)
    serviceRecordMarshal.fromFile(resolvedFile)

    // list the parent path, expect success and no entries
    File listParentDir = new File(destDir, "listParent")
    String parentPath = RegistryPathUtils.parentOf(recordsPath)
    assert 0 == client.actionResolve(new ActionResolveArgs(
        path: parentPath,
        list: true,
        destdir: listParentDir))
    assertFileExists("output dir", listParentDir)
    assert null != listParentDir.list()
    assert 0 == listParentDir.list().length

    // look for a record a path not in the registry expect failure
    ActionResolveArgs listUnknownPath = new ActionResolveArgs(
        path: recordsPath +"/unknown",
        list: true)
    // the record is not there, even if the path is
    assert SliderExitCodes.EXIT_NOT_FOUND == client.actionResolve(
        listUnknownPath)
    
    
    // look for a record at the same path as the listing; expect failure
    ActionResolveArgs resolveRecordAtListPath = new ActionResolveArgs(
        path: recordsPath,
        list: false)
    // the record is not there, even if the path is
    assert SliderExitCodes.EXIT_NOT_FOUND == client.actionResolve(
        resolveRecordAtListPath)

    // look at a single record
    def instanceRecordPath = recordsPath + "/" + clustername
    ActionResolveArgs resolveRecordCommand = new ActionResolveArgs(
        path: instanceRecordPath)
    // to stdout
    client.actionResolve(resolveRecordCommand)
    
    // then to a file
    FileUtil.fullyDelete(destDir)
    File singleFile = new File(destDir, "singlefile.json")
    singleFile.delete()
    resolveRecordCommand.out = singleFile
    assert 0 == client.actionResolve(resolveRecordCommand)
    assertFileExists("\"slider $resolveRecordCommand\"", singleFile)
    def recordFromFile = serviceRecordMarshal.fromFile(singleFile)
    assert recordFromFile[YarnRegistryAttributes.YARN_ID] ==
           serviceRecord[YarnRegistryAttributes.YARN_ID]
    assert recordFromFile[YarnRegistryAttributes.YARN_PERSISTENCE] ==
           serviceRecord[YarnRegistryAttributes.YARN_PERSISTENCE]

    // hit the registry web page
    def registryEndpoint = serviceRecord.getExternalEndpoint(
        CustomRegistryConstants.REGISTRY_REST_API)
    assert registryEndpoint != null
    def registryURL = RegistryTypeUtils.retrieveAddressURLs(registryEndpoint)[0]

    // list the path at the record, expect success and no entries
    File listUnderRecordDir = new File(destDir, "listUnderRecord")
    ActionResolveArgs listUnderRecordCommand = new ActionResolveArgs(
        path: instanceRecordPath,
        list:true,
        destdir: listUnderRecordDir)
    assert 0 == client.actionResolve(listUnderRecordCommand)
    assert 0 == listUnderRecordDir.list().length


           // Look at the Registry WADL
    describe("Registry WADL @ $registryURL")
    def publisherEndpoint = serviceRecord.getExternalEndpoint(
        CustomRegistryConstants.PUBLISHER_REST_API)

    def publisherURL = RegistryTypeUtils.retrieveAddressURLs(publisherEndpoint)[0]
    def publisher = publisherURL.toString()
    describe("Publisher")

    JsonSerDeser<UriMap> uriMapDeser = new JsonSerDeser<>(UriMap)
    def setlisting = GET(publisherURL)

    log.info(setlisting)

    UriMap uris = uriMapDeser.fromJson(setlisting)
    assert uris.uris[RestPaths.SLIDER_CONFIGSET]
    def publishedJSON = GET(publisherURL, RestPaths.SLIDER_CONFIGSET)
    JsonSerDeser< PublishedConfigSet> serDeser= new JsonSerDeser<>(
        PublishedConfigSet)
    def configSet = serDeser.fromJson(publishedJSON)
    assert configSet.size() >= 1
    assert configSet.contains(ARTIFACT_NAME)
    PublishedConfiguration publishedYarnSite = configSet.get(ARTIFACT_NAME)

    assert publishedYarnSite.empty
    
    //get the full URL
    def yarnSitePublisher = appendToURL(publisher,
        RestPaths.SLIDER_CONFIGSET,
        ARTIFACT_NAME)

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
    log.info(GET(registryURL))


    describe "Registry Retrieval Class"
    // retrieval

    RegistryRetriever retriever = new RegistryRetriever(serviceRecord)
    log.info retriever.toString()
    
    assert retriever.hasConfigurations(true)
    PublishedConfigSet externalConfSet = retriever.getConfigurations(true)
    dumpConfigurationSet(externalConfSet)
    assert externalConfSet[ARTIFACT_NAME]


    describe "verify SLIDER-52 processing"
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

    log.info("RM from downloaded props = $rmAddrFromDownloadedProperties")
    assert rmAddrViaClientSideXML == rmAddrFromDownloadedProperties
    
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
    } catch (UnknownApplicationInstanceException expected) {
      // expected 
    }

    // list all instances of an alternate type and expect failure
    registryArgs.list = true;
    registryArgs.name = null
    registryArgs.serviceType = "org-apache-hadoop"
    try {
      client.actionRegistryList(registryArgs)
    } catch (UnknownApplicationInstanceException expected) {
      // expected 
    }

    registryArgs.serviceType = ""

    //set the name
    registryArgs.name = clustername;
    registryArgs.serviceType = SliderKeys.APP_TYPE
    

    //now expect list to work
    describe registryArgs.toString()

    def listedInstance = client.actionRegistryList(registryArgs)

    def resolvedRecord = listedInstance[0]
    assert resolvedRecord[YarnRegistryAttributes.YARN_ID] == 
           serviceRecord[YarnRegistryAttributes.YARN_ID]
    assert resolvedRecord[YarnRegistryAttributes.YARN_PERSISTENCE] == 
           serviceRecord[YarnRegistryAttributes.YARN_PERSISTENCE]

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
    assert SliderExitCodes.EXIT_NOT_FOUND == client.actionRegistry(registryArgs)

    registryArgs.list = false;
    registryArgs.listConf = false
    registryArgs.internal = false

    def yarn_site_config = PublishedArtifacts.YARN_SITE_CONFIG
    registryArgs.getConf = yarn_site_config

    //properties format
    registryArgs.format = "properties"
    describe registryArgs.toString()

    client.actionRegistry(registryArgs)


    File outputDir = new File("target/test_standalone_registry_am/output")
    outputDir.mkdirs()

    // create a new registry args with the defaults back in
    registryArgs = new ActionRegistryArgs(clustername)
    registryArgs.getConf = yarn_site_config
    registryArgs.out = outputDir
    describe registryArgs.toString()
    client.actionRegistry(registryArgs)
    assert new File(outputDir, yarn_site_config + ".xml").exists()

    registryArgs.format = "properties"
    client.actionRegistry(registryArgs)
    assert new File(outputDir, yarn_site_config + ".properties").exists()

    describe registryArgs.toString()

    def unknownFilename = "undefined-file"
    registryArgs.getConf = unknownFilename
    assert SliderExitCodes.EXIT_NOT_FOUND == client.actionRegistry(registryArgs)

    describe "stop cluster"
    //now kill that cluster
    assert 0 == clusterActionFreeze(client, clustername)
    //list it & See if it is still there
    ApplicationReport oldInstance = yarnRegistryClient.findInstance(
        clustername)
    assert oldInstance != null
    assert oldInstance.yarnApplicationState >= YarnApplicationState.FINISHED

    
    // verify hbase to path generation filters things
    def hbase = homePathForUser(HBASE)
    def hbaseServices = serviceclassPath(hbase, SliderKeys.APP_TYPE)

    
    assert SliderExitCodes.EXIT_NOT_FOUND == client.actionResolve(
        new ActionResolveArgs(
            path: hbaseServices,
            list: true))
    assert SliderExitCodes.EXIT_NOT_FOUND == client.actionResolve(
        new ActionResolveArgs(path: hbaseServices))

  }
}
