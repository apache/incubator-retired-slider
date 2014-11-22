/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.chaos.remote

/**
 * Parses RHAT HA Clustat files pulled back over the network
 */
class Clustat {

  Node xml;

  Clustat(String source) {
    xml = new XmlParser().parseText(source)
  }

/**
 * ser
 * @param service something like service:NameNodeService
 * @return the owner
 */
  Node serviceGroup(String service) {
    NodeList groups = xml.groups
    def found = groups.group.findAll { it.@name == service}
    if (found.empty) {
      return null
    }
    Node serviceGroup = found[0]
    return serviceGroup
  }

  String hostRunningService(String service) {
    Node serviceGroup = serviceGroup("service:NameNodeService")
    return serviceGroup?.@owner
  }

  /**
   * Get all the nodes in the cluster
   * @return the XML nodes element; every node is an elt underneath with @name==server
   */
  NodeList clusterNodes() {
    return xml.nodes.node
  }

  Node clusterNode(String name) {
    def all = clusterNodes() findAll { it.@name == name}
    return all.size() > 0 ? all[0] : null
  }

}
