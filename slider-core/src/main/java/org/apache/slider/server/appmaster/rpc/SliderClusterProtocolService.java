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

package org.apache.slider.server.appmaster.rpc;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.api.proto.Messages;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.exceptions.ServiceNotReadyException;
import org.apache.slider.core.main.LauncherExitCodes;
import org.apache.slider.core.persist.ConfTreeSerDeser;
import org.apache.slider.server.appmaster.AppMasterActionOperations;
import org.apache.slider.server.appmaster.actions.ActionFlexCluster;
import org.apache.slider.server.appmaster.actions.ActionHalt;
import org.apache.slider.server.appmaster.actions.ActionKillContainer;
import org.apache.slider.server.appmaster.actions.ActionStopSlider;
import org.apache.slider.server.appmaster.actions.AsyncAction;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Implement the {@link SliderClusterProtocol}.
 */
public class SliderClusterProtocolService extends AbstractService
    implements SliderClusterProtocol {

  protected static final Logger log =
      LoggerFactory.getLogger(SliderClusterProtocol.class);

  private final QueueAccess actionQueues;
  private final StateAccessForProviders appState;
  private final MetricsAndMonitoring metricsAndMonitoring;
  private final AppMasterActionOperations amOperations;
  
  /**
   * This is the prefix used for metrics
   */
  public static final String PROTOCOL_PREFIX =
      "org.apache.slider.api.SliderClusterProtocol.";

  /**
   * Constructor
   * @param amOperations access to any AM operations
   * @param appState state view
   * @param actionQueues queues for actions
   * @param metricsAndMonitoring metrics
   */
  public SliderClusterProtocolService(AppMasterActionOperations amOperations,
      StateAccessForProviders appState,
      QueueAccess actionQueues,
      MetricsAndMonitoring metricsAndMonitoring) {
    super("SliderClusterProtocolService");
    Preconditions.checkArgument(amOperations != null, "null amOperations");
    Preconditions.checkArgument(appState != null, "null appState");
    Preconditions.checkArgument(actionQueues != null, "null actionQueues");
    Preconditions.checkArgument(metricsAndMonitoring != null, "null metricsAndMonitoring");
    this.appState = appState;
    this.actionQueues = actionQueues;
    this.metricsAndMonitoring = metricsAndMonitoring;
    this.amOperations = amOperations;
  }

  @Override   //SliderClusterProtocol
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion,
      int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }


  @Override   //SliderClusterProtocol
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return SliderClusterProtocol.versionID;
  }

  /**
   * General actions to perform on a slider RPC call coming in
   * @param operation operation to log
   * @throws IOException problems
   * @throws ServiceNotReadyException if the RPC service is constructed
   * but not fully initialized
   */
  protected void onRpcCall(String operation) throws IOException {
    log.debug("Received call to {}", operation);
    metricsAndMonitoring.markMeterAndCounter(PROTOCOL_PREFIX + operation);
  }

  /**
   * Schedule an action
   * @param action for delayed execution
   */
  public void schedule(AsyncAction action) {
    actionQueues.schedule(action);
  }

  /**
   * Queue an action for immediate execution in the executor thread
   * @param action action to execute
   */
  public void queue(AsyncAction action) {
    actionQueues.put(action);
  }

  @Override //SliderClusterProtocol
  public Messages.StopClusterResponseProto stopCluster(Messages.StopClusterRequestProto request)
      throws IOException, YarnException {
    onRpcCall("stop");
    String message = request.getMessage();
    if (message == null) {
      message = "application stopped by client";
    }
    ActionStopSlider stopSlider =
        new ActionStopSlider(message,
            1000, TimeUnit.MILLISECONDS,
            LauncherExitCodes.EXIT_SUCCESS,
            FinalApplicationStatus.SUCCEEDED,
            message);
    log.info("SliderAppMasterApi.stopCluster: {}", stopSlider);
    schedule(stopSlider);
    return Messages.StopClusterResponseProto.getDefaultInstance();
  }

  @Override //SliderClusterProtocol
  public Messages.FlexClusterResponseProto flexCluster(Messages.FlexClusterRequestProto request)
      throws IOException, YarnException {
    onRpcCall("flex");
    String payload = request.getClusterSpec();
    ConfTreeSerDeser confTreeSerDeser = new ConfTreeSerDeser();
    ConfTree updatedResources = confTreeSerDeser.fromJson(payload);
    schedule(new ActionFlexCluster("flex", 1, TimeUnit.MILLISECONDS,
        updatedResources));
    return Messages.FlexClusterResponseProto.newBuilder().setResponse(
        true).build();
  }

  @Override //SliderClusterProtocol
  public Messages.GetJSONClusterStatusResponseProto getJSONClusterStatus(
      Messages.GetJSONClusterStatusRequestProto request)
      throws IOException, YarnException {
    onRpcCall("getstatus");
    String result;
    //quick update
    //query and json-ify
    ClusterDescription cd = appState.refreshClusterStatus();
    result = cd.toJsonString();
    String stat = result;
    return Messages.GetJSONClusterStatusResponseProto.newBuilder()
                                                     .setClusterSpec(stat)
                                                     .build();
  }

  @Override
  public Messages.GetInstanceDefinitionResponseProto getInstanceDefinition(
      Messages.GetInstanceDefinitionRequestProto request)
      throws IOException, YarnException {

    onRpcCall("getinstancedefinition");
    String internal;
    String resources;
    String app;
    AggregateConf instanceDefinition =
        appState.getInstanceDefinitionSnapshot();
    internal = instanceDefinition.getInternal().toJson();
    resources = instanceDefinition.getResources().toJson();
    app = instanceDefinition.getAppConf().toJson();
    assert internal != null;
    assert resources != null;
    assert app != null;
    log.debug("Generating getInstanceDefinition Response");
    Messages.GetInstanceDefinitionResponseProto.Builder builder =
        Messages.GetInstanceDefinitionResponseProto.newBuilder();
    builder.setInternal(internal);
    builder.setResources(resources);
    builder.setApplication(app);
    return builder.build();
  }

  @Override //SliderClusterProtocol
  public Messages.ListNodeUUIDsByRoleResponseProto listNodeUUIDsByRole(Messages.ListNodeUUIDsByRoleRequestProto request)
      throws IOException, YarnException {
    onRpcCall("listnodes)");
    String role = request.getRole();
    Messages.ListNodeUUIDsByRoleResponseProto.Builder builder =
        Messages.ListNodeUUIDsByRoleResponseProto.newBuilder();
    List<RoleInstance> nodes = appState.enumLiveNodesInRole(role);
    for (RoleInstance node : nodes) {
      builder.addUuid(node.id);
    }
    return builder.build();
  }

  @Override //SliderClusterProtocol
  public Messages.GetNodeResponseProto getNode(Messages.GetNodeRequestProto request)
      throws IOException, YarnException {
    onRpcCall("getnode");
    RoleInstance instance = appState.getLiveInstanceByContainerID(
        request.getUuid());
    return Messages.GetNodeResponseProto.newBuilder()
                                        .setClusterNode(instance.toProtobuf())
                                        .build();
  }

  @Override //SliderClusterProtocol
  public Messages.GetClusterNodesResponseProto getClusterNodes(
      Messages.GetClusterNodesRequestProto request)
      throws IOException, YarnException {
    onRpcCall("getclusternodes");
    List<RoleInstance>
        clusterNodes = appState.getLiveInstancesByContainerIDs(
        request.getUuidList());

    Messages.GetClusterNodesResponseProto.Builder builder =
        Messages.GetClusterNodesResponseProto.newBuilder();
    for (RoleInstance node : clusterNodes) {
      builder.addClusterNode(node.toProtobuf());
    }
    //at this point: a possibly empty list of nodes
    return builder.build();
  }

  @Override
  public Messages.EchoResponseProto echo(Messages.EchoRequestProto request)
      throws IOException, YarnException {
    onRpcCall("echo");
    Messages.EchoResponseProto.Builder builder =
        Messages.EchoResponseProto.newBuilder();
    String text = request.getText();
    log.info("Echo request size ={}", text.length());
    log.info(text);
    //now return it
    builder.setText(text);
    return builder.build();
  }

  @Override
  public Messages.KillContainerResponseProto killContainer(Messages.KillContainerRequestProto request)
      throws IOException, YarnException {
    onRpcCall("killcontainer");
    String containerID = request.getId();
    log.info("Kill Container {}", containerID);
    //throws NoSuchNodeException if it is missing
    RoleInstance instance =
        appState.getLiveInstanceByContainerID(containerID);
    queue(new ActionKillContainer(instance.getId(), 0, TimeUnit.MILLISECONDS,
        amOperations));
    Messages.KillContainerResponseProto.Builder builder =
        Messages.KillContainerResponseProto.newBuilder();
    builder.setSuccess(true);
    return builder.build();
  }


  @Override
  public Messages.AMSuicideResponseProto amSuicide(
      Messages.AMSuicideRequestProto request)
      throws IOException, YarnException {
    onRpcCall("amsuicide");
    int signal = request.getSignal();
    String text = request.getText();
    if (text == null) {
      text = "";
    }
    int delay = request.getDelay();
    log.info("AM Suicide with signal {}, message {} delay = {}", signal, text,
        delay);
    ActionHalt action = new ActionHalt(signal, text, delay,
        TimeUnit.MILLISECONDS);
    schedule(action);
    return Messages.AMSuicideResponseProto.getDefaultInstance();
  }
}
