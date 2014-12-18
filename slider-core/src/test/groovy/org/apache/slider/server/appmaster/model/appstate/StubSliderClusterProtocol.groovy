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

package org.apache.slider.server.appmaster.model.appstate

import org.apache.hadoop.ipc.ProtocolSignature
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.slider.api.SliderClusterProtocol
import org.apache.slider.api.proto.Messages

class StubSliderClusterProtocol implements SliderClusterProtocol {
  @Override
  Messages.StopClusterResponseProto stopCluster(
      Messages.StopClusterRequestProto request)
  throws IOException, YarnException {
    throw new UnsupportedOperationException()
  }

  @Override
  Messages.FlexClusterResponseProto flexCluster(
      Messages.FlexClusterRequestProto request)
  throws IOException, YarnException {
    throw new UnsupportedOperationException()
  }

  @Override
  Messages.GetJSONClusterStatusResponseProto getJSONClusterStatus(
      Messages.GetJSONClusterStatusRequestProto request)
  throws IOException, YarnException {
    throw new UnsupportedOperationException()
  }

  @Override
  Messages.ListNodeUUIDsByRoleResponseProto listNodeUUIDsByRole(
      Messages.ListNodeUUIDsByRoleRequestProto request)
  throws IOException, YarnException {
    throw new UnsupportedOperationException()
  }

  @Override
  Messages.GetNodeResponseProto getNode(Messages.GetNodeRequestProto request)
  throws IOException, YarnException {
    throw new UnsupportedOperationException()
  }

  @Override
  Messages.GetClusterNodesResponseProto getClusterNodes(
      Messages.GetClusterNodesRequestProto request)
  throws IOException, YarnException {
    throw new UnsupportedOperationException()
  }

  @Override
  Messages.EchoResponseProto echo(Messages.EchoRequestProto request)
  throws IOException, YarnException {
    throw new UnsupportedOperationException()
  }

  @Override
  Messages.KillContainerResponseProto killContainer(
      Messages.KillContainerRequestProto request)
  throws IOException, YarnException {
    throw new UnsupportedOperationException()
  }

  @Override
  Messages.AMSuicideResponseProto amSuicide(
      Messages.AMSuicideRequestProto request)
  throws IOException, YarnException {
    throw new UnsupportedOperationException()
  }

  @Override
  Messages.GetInstanceDefinitionResponseProto getInstanceDefinition(
      Messages.GetInstanceDefinitionRequestProto request)
  throws IOException, YarnException {
    throw new UnsupportedOperationException()
  }

  @Override
  long getProtocolVersion(String protocol, long clientVersion)
  throws IOException {
    return 0
  }

  @Override
  ProtocolSignature getProtocolSignature(
      String protocol,
      long clientVersion,
      int clientMethodsHash) throws IOException {
    throw new UnsupportedOperationException()
  }
}