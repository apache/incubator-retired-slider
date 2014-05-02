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
package org.apache.slider.server.appmaster.model.mock

import org.apache.hadoop.ipc.ProtocolSignature
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.slider.api.SliderClusterProtocol
import org.apache.slider.api.proto.Messages
import org.apache.slider.api.proto.Messages.AMSuicideRequestProto
import org.apache.slider.api.proto.Messages.AMSuicideResponseProto
import org.apache.slider.api.proto.Messages.EchoRequestProto
import org.apache.slider.api.proto.Messages.EchoResponseProto
import org.apache.slider.api.proto.Messages.FlexClusterRequestProto
import org.apache.slider.api.proto.Messages.FlexClusterResponseProto
import org.apache.slider.api.proto.Messages.GetClusterNodesRequestProto
import org.apache.slider.api.proto.Messages.GetClusterNodesResponseProto
import org.apache.slider.api.proto.Messages.GetJSONClusterStatusRequestProto
import org.apache.slider.api.proto.Messages.GetJSONClusterStatusResponseProto
import org.apache.slider.api.proto.Messages.GetNodeRequestProto
import org.apache.slider.api.proto.Messages.GetNodeResponseProto
import org.apache.slider.api.proto.Messages.KillContainerRequestProto
import org.apache.slider.api.proto.Messages.KillContainerResponseProto
import org.apache.slider.api.proto.Messages.ListNodeUUIDsByRoleRequestProto
import org.apache.slider.api.proto.Messages.ListNodeUUIDsByRoleResponseProto
import org.apache.slider.api.proto.Messages.StopClusterRequestProto
import org.apache.slider.api.proto.Messages.StopClusterResponseProto

/**
 * 
 */
class MockSliderClusterProtocol implements SliderClusterProtocol {

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return 0;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
    return null;
  }

  @Override
  public StopClusterResponseProto stopCluster(StopClusterRequestProto request) throws IOException, YarnException {
    return null;
  }

  @Override
  public FlexClusterResponseProto flexCluster(FlexClusterRequestProto request) throws IOException, YarnException {
    return null;
  }

  @Override
  public GetJSONClusterStatusResponseProto getJSONClusterStatus(GetJSONClusterStatusRequestProto request) throws IOException, YarnException {
    return null;
  }

  @Override
  public ListNodeUUIDsByRoleResponseProto listNodeUUIDsByRole(ListNodeUUIDsByRoleRequestProto request) throws IOException, YarnException {
    return null;
  }

  @Override
  public GetNodeResponseProto getNode(GetNodeRequestProto request) throws IOException, YarnException {
    return null;
  }

  @Override
  public GetClusterNodesResponseProto getClusterNodes(GetClusterNodesRequestProto request) throws IOException, YarnException {
    return null;
  }

  @Override
  public EchoResponseProto echo(EchoRequestProto request) throws IOException, YarnException {
    return null;
  }

  @Override
  public KillContainerResponseProto killContainer(KillContainerRequestProto request) throws IOException, YarnException {
    return null;
  }

  @Override
  public AMSuicideResponseProto amSuicide(AMSuicideRequestProto request) throws IOException, YarnException {
    return null;
  }

  @Override
  Messages.GetInstanceDefinitionResponseProto getInstanceDefinition(
      Messages.GetInstanceDefinitionRequestProto request)
  throws IOException, YarnException {
    return null
  }
}
