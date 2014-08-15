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
package org.apache.slider.providers.agent;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.slider.server.appmaster.model.mock.MockContainerId;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

/**
 *
 */
public class TestHeartbeatMonitor {
  protected static final Logger log =
      LoggerFactory.getLogger(TestHeartbeatMonitor.class);

  @Test
  public void testRegularHeartbeat() throws Exception {
    AgentProviderService provider = createNiceMock(AgentProviderService.class);
    HeartbeatMonitor hbm = new HeartbeatMonitor(provider, 1 * 1000);
    Assert.assertFalse(hbm.isAlive());
    expect(provider.getComponentStatuses()).andReturn(null).anyTimes();
    replay(provider);
    hbm.start();
    Assert.assertTrue(hbm.isAlive());
    hbm.shutdown();
    Thread.sleep(1 * 1000);
    Assert.assertFalse(hbm.isAlive());
  }

  @Test
  public void testHeartbeatMonitorWithHealthy() throws Exception {
    AgentProviderService provider = createNiceMock(AgentProviderService.class);
    HeartbeatMonitor hbm = new HeartbeatMonitor(provider, 500);
    Assert.assertFalse(hbm.isAlive());
    Map<String, ComponentInstanceState> statuses = new HashMap<String, ComponentInstanceState>();
    ContainerId container1 = new MockContainerId(1);
    ComponentInstanceState state = new ComponentInstanceState("HBASE_MASTER",
        container1, "Aid");
    state.setState(State.STARTED);
    state.heartbeat(System.currentTimeMillis());
    statuses.put("label_1", state);
    expect(provider.getComponentStatuses()).andReturn(statuses).anyTimes();
    replay(provider);
    hbm.start();
    Assert.assertTrue(hbm.isAlive());
    Thread.sleep(1 * 1000);
    hbm.shutdown();
    Thread.sleep(1 * 1000);
    Assert.assertFalse(hbm.isAlive());
  }

  @Test
  public void testHeartbeatMonitorWithUnhealthyAndThenLost() throws Exception {
    AgentProviderService provider = createNiceMock(AgentProviderService.class);
    long now = 100000;
    int wakeupInterval = 2 * 1000;

    Map<String, ComponentInstanceState> statuses = new HashMap<String, ComponentInstanceState>();
    ContainerId masterContainer = new MockContainerId(1); 
    ContainerId slaveContainer = new MockContainerId(2); 
    ComponentInstanceState masterState = new ComponentInstanceState("HBASE_MASTER",
        masterContainer, "Aid1");
    String masterLabel = "Aid1_Cid1_HBASE_MASTER";
    statuses.put(masterLabel, masterState);

    ComponentInstanceState slaveState = new ComponentInstanceState("HBASE_REGIONSERVER",
        slaveContainer, "Aid1");
    String slaveLabel = "Aid1_Cid2_HBASE_REGIONSERVER";
    statuses.put(slaveLabel, slaveState);

    masterState.setState(State.STARTED);
    masterState.heartbeat(now);
    slaveState.setState(State.STARTED);
    slaveState.heartbeat(now);
    expect(provider.getComponentStatuses()).andReturn(statuses).anyTimes();
    replay(provider);


    HeartbeatMonitor heartbeatMonitor = new HeartbeatMonitor(provider,
        wakeupInterval);
    Assert.assertFalse(heartbeatMonitor.isAlive());
    now += wakeupInterval;
    masterState.setState(State.STARTED);
    masterState.heartbeat(now);
    
    slaveState.setState(State.STARTED);
    // just dial back by at least 2 sec but no more than 4
    slaveState.heartbeat(now - (wakeupInterval + 100));


    assertInState(ContainerState.HEALTHY, masterState, now);
    assertInState(ContainerState.HEALTHY, slaveState, now);
    
    //tick #1
    heartbeatMonitor.doWork(now);

    assertInState(ContainerState.HEALTHY, masterState, now);
    assertInState(ContainerState.UNHEALTHY, slaveState, now);

    // heartbeat from the master
    masterState.heartbeat(now + 1500);

    // tick #2
    now += wakeupInterval;
    heartbeatMonitor.doWork(now);

    assertInState(ContainerState.HEALTHY, masterState, now);
    assertInState(ContainerState.HEARTBEAT_LOST, slaveState, now);
  }

  protected void assertInState(ContainerState expectedState,
      ComponentInstanceState componentInstanceState, long now) {
    ContainerState actualState = componentInstanceState.getContainerState();
    if (!expectedState.equals(actualState)) {
      // mismatch
      Assert.fail(String.format("at [%06d] Expected component state %s " +
                                "but found state %s in in component %s",
          now, expectedState, actualState, componentInstanceState));
    }
  }

  @Test
  public void testHeartbeatTransitions() {
    ContainerId container2 = new MockContainerId(2);
    ComponentInstanceState slaveState = new ComponentInstanceState("HBASE_REGIONSERVER",
        container2, "Aid1");
    slaveState.setState(State.STARTED);

    long lastHeartbeat = System.currentTimeMillis();
    assertInState(ContainerState.INIT, slaveState, 0);
    slaveState.heartbeat(lastHeartbeat);
    assertInState(ContainerState.HEALTHY, slaveState, lastHeartbeat);

    slaveState.setContainerState(ContainerState.UNHEALTHY);
    lastHeartbeat = System.currentTimeMillis();
    assertInState(ContainerState.UNHEALTHY, slaveState, lastHeartbeat);
    slaveState.heartbeat(lastHeartbeat);
    assertInState(ContainerState.HEALTHY, slaveState, lastHeartbeat);

    slaveState.setContainerState(ContainerState.HEARTBEAT_LOST);
    assertInState(ContainerState.HEARTBEAT_LOST, slaveState, lastHeartbeat);
    lastHeartbeat = System.currentTimeMillis();
    slaveState.heartbeat(lastHeartbeat);
    assertInState(ContainerState.HEARTBEAT_LOST, slaveState, lastHeartbeat);
  }
}
