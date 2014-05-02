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

package org.apache.slider.client;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.api.proto.Messages;
import org.apache.slider.common.tools.Duration;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.WaitTimeoutException;
import org.apache.slider.core.persist.ConfTreeSerDeser;
import org.codehaus.jackson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Cluster operations at a slightly higher level than the RPC code
 */
public class SliderClusterOperations {
  protected static final Logger
    log = LoggerFactory.getLogger(SliderClusterOperations.class);
  
  private final SliderClusterProtocol appMaster;

  public SliderClusterOperations(SliderClusterProtocol appMaster) {
    this.appMaster = appMaster;
  }

  /**
   * Get a node from the AM
   * @param appMaster AM
   * @param uuid uuid of node
   * @return deserialized node
   * @throws IOException IO problems
   * @throws NoSuchNodeException if the node isn't found
   */
  public ClusterNode getNode(String uuid)
    throws IOException, NoSuchNodeException, YarnException {
    Messages.GetNodeRequestProto req =
      Messages.GetNodeRequestProto.newBuilder().setUuid(uuid).build();
    Messages.GetNodeResponseProto node = appMaster.getNode(req);
    return ClusterNode.fromProtobuf(node.getClusterNode());
  }

  public List<ClusterNode> convertNodeWireToClusterNodes(List<Messages.RoleInstanceState> nodes)
    throws IOException {
    List<ClusterNode> nodeList = new ArrayList<ClusterNode>(nodes.size());
    for (Messages.RoleInstanceState node : nodes) {
      nodeList.add(ClusterNode.fromProtobuf(node));
    }
    return nodeList;
  }

  /**
   * Echo text (debug action)
   * @param text text
   * @return the text, echoed back
   * @throws YarnException
   * @throws IOException
   */
  public String echo(String text) throws
                                          YarnException,
                                          IOException {
    Messages.EchoRequestProto.Builder builder =
      Messages.EchoRequestProto.newBuilder();
    builder.setText(text);
    Messages.EchoRequestProto req =
      builder.build();
    Messages.EchoResponseProto response =
      appMaster.echo(req);
    return response.getText();
  }


  /**
   * Connect to a live cluster and get its current state
   * @return its description
   */
  public ClusterDescription getClusterDescription()
    throws YarnException, IOException {
    
    Messages.GetJSONClusterStatusRequestProto req =
      Messages.GetJSONClusterStatusRequestProto.newBuilder().build();
    Messages.GetJSONClusterStatusResponseProto resp =
      appMaster.getJSONClusterStatus(req);
    String statusJson = resp.getClusterSpec();
    try {
      return ClusterDescription.fromJson(statusJson);
    } catch (JsonParseException e) {
      log.error(
        "Exception " + e + " parsing:\n" + statusJson,
        e);
      throw e;
    }
  }

  public AggregateConf getInstanceDefinition()
    throws YarnException, IOException {
    Messages.GetInstanceDefinitionRequestProto.Builder builder =
      Messages.GetInstanceDefinitionRequestProto.newBuilder();

    Messages.GetInstanceDefinitionRequestProto request = builder.build();
    Messages.GetInstanceDefinitionResponseProto response =
      appMaster.getInstanceDefinition(request);

    ConfTreeSerDeser confTreeSerDeser = new ConfTreeSerDeser();

    ConfTree internal = confTreeSerDeser.fromJson(response.getInternal());
    ConfTree resources = confTreeSerDeser.fromJson(response.getResources());
    ConfTree app = confTreeSerDeser.fromJson(response.getApplication());
    AggregateConf instanceDefinition =
      new AggregateConf(resources, app, internal);
    return instanceDefinition;
  }
  /**
   * Kill a container
   * @param id container ID
   * @return a success flag
   * @throws YarnException
   * @throws IOException
   */
  public boolean killContainer(String id) throws
                                          YarnException,
                                          IOException {
    Messages.KillContainerRequestProto.Builder builder =
      Messages.KillContainerRequestProto.newBuilder();
    builder.setId(id);
    Messages.KillContainerRequestProto req =
      builder.build();
    Messages.KillContainerResponseProto response =
      appMaster.killContainer(req);
    return response.getSuccess();
  }

  /**
   * List all node UUIDs in a role
   * @param role role name or "" for all
   * @return an array of UUID strings
   * @throws IOException
   * @throws YarnException
   */
  public String[] listNodeUUIDsByRole(String role) throws
                                                   IOException,
                                                   YarnException {
    Collection<String> uuidList = innerListNodeUUIDSByRole(role);
    String[] uuids = new String[uuidList.size()];
    return uuidList.toArray(uuids);
  }

  public List<String> innerListNodeUUIDSByRole(String role) throws
                                                             IOException,
                                                             YarnException {
    Messages.ListNodeUUIDsByRoleRequestProto req =
      Messages.ListNodeUUIDsByRoleRequestProto
              .newBuilder()
              .setRole(role)
              .build();
    Messages.ListNodeUUIDsByRoleResponseProto resp =
      appMaster.listNodeUUIDsByRole(req);
    return resp.getUuidList();
  }

  /**
   * List all nodes in a role. This is a double round trip: once to list
   * the nodes in a role, another to get their details
   * @param role
   * @return an array of ContainerNode instances
   * @throws IOException
   * @throws YarnException
   */
  public List<ClusterNode> listClusterNodesInRole(String role) throws
                                                               IOException,
                                                               YarnException {

    Collection<String> uuidList = innerListNodeUUIDSByRole(role);
    Messages.GetClusterNodesRequestProto req =
      Messages.GetClusterNodesRequestProto
              .newBuilder()
              .addAllUuid(uuidList)
              .build();
    Messages.GetClusterNodesResponseProto resp = appMaster.getClusterNodes(req);
    return convertNodeWireToClusterNodes(resp.getClusterNodeList());
  }

  /**
   * Get the details on a list of uuids
   * @param uuids
   * @return a possibly empty list of node details
   * @throws IOException
   * @throws YarnException
   */
  @VisibleForTesting
  public List<ClusterNode> listClusterNodes(String[] uuids) throws
                                                            IOException,
                                                            YarnException {

    Messages.GetClusterNodesRequestProto req =
      Messages.GetClusterNodesRequestProto
              .newBuilder()
              .addAllUuid(Arrays.asList(uuids))
              .build();
    Messages.GetClusterNodesResponseProto resp = appMaster.getClusterNodes(req);
    return convertNodeWireToClusterNodes(resp.getClusterNodeList());
  }

  /**
   * Wait for an instance of a named role to be live (or past it in the lifecycle)
   * @param clustername cluster
   * @param role role to look for
   * @param timeout time to wait
   * @return the state. If still in CREATED, the cluster didn't come up
   * in the time period. If LIVE, all is well. If >LIVE, it has shut for a reason
   * @throws IOException IO
   * @throws SliderException Slider
   * @throws WaitTimeoutException if the wait timed out
   */
  @VisibleForTesting
  public int waitForRoleInstanceLive(String role, long timeout)
    throws WaitTimeoutException, IOException, YarnException {
    Duration duration = new Duration(timeout).start();
    boolean live = false;
    int state = ClusterDescription.STATE_CREATED;

    log.info("Waiting {} millis for a live node in role {}", timeout, role);
    while (!live) {
      // see if there is a node in that role yet
      List<String> uuids = innerListNodeUUIDSByRole(role);
      String[] containers = uuids.toArray(new String[uuids.size()]);
      int roleCount = containers.length;
      ClusterNode roleInstance = null;
      if (roleCount != 0) {

        // if there is, get the node
        roleInstance = getNode(containers[0]);
        if (roleInstance != null) {
          state = roleInstance.state;
          live = state >= ClusterDescription.STATE_LIVE;
        }
      }
      if (!live) {
        if (duration.getLimitExceeded()) {
          throw new WaitTimeoutException(
            String.format("Timeout after %d millis" +
                          " waiting for a live instance of type %s; " +
                          "instances found %d %s",
                          timeout, role, roleCount,
                          (roleInstance != null
                           ? (" instance -\n" + roleInstance.toString())
                           : "")
                         ));
        } else {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ignored) {
            // ignored
          }
        }
      }
    }
    return state;
  }
  
  public boolean flex(ConfTree resources) throws IOException, YarnException {
    Messages.FlexClusterRequestProto request =
      Messages.FlexClusterRequestProto.newBuilder()
              .setClusterSpec(resources.toJson())
              .build();
    Messages.FlexClusterResponseProto response =
      appMaster.flexCluster(request);
    return response.getResponse();
  }


  /**
   * Commit (possibly delayed) AM suicide
   *
   * @param signal exit code
   * @param text text text to log
   * @param delay delay in millis
   * @throws YarnException
   * @throws IOException
   */
  public void amSuicide(String text, int signal, int delay) throws
                                  YarnException,
                                  IOException {
    Messages.AMSuicideRequestProto.Builder builder =
      Messages.AMSuicideRequestProto.newBuilder();
    builder.setText(text);
    builder.setSignal(signal);
    builder.setDelay(delay);
    Messages.AMSuicideRequestProto req =
      builder.build();
    Messages.AMSuicideResponseProto response =
      appMaster.amSuicide(req);
  }


}
