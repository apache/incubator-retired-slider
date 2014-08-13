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

package org.apache.slider.server.appmaster.actions;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation;
import org.apache.slider.server.appmaster.state.AppState;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ActionKillContainer extends AsyncAction {

  private final ContainerId containerId;
  
  public ActionKillContainer(ContainerId containerId, long delay,
      TimeUnit timeUnit) {
    super("kill container", delay, timeUnit);
    this.containerId = containerId;
  }

  @Override
  public void execute(SliderAppMaster appMaster,
      QueueAccess queueService,
      AppState appState) throws Exception {
    List<AbstractRMOperation> opsList = new LinkedList<>();
    ContainerReleaseOperation release = new ContainerReleaseOperation(containerId);
    opsList.add(release);
    //now apply the operations
    appMaster.executeRMOperations(opsList);
  }
}
