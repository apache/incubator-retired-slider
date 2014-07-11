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
    Map<String, ComponentInstanceState> statuses = new HashMap<>();
    ComponentInstanceState state = new ComponentInstanceState("HBASE_MASTER", "Cid", "Aid");
    state.setState(State.STARTED);
    state.setLastHeartbeat(System.currentTimeMillis());
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
    HeartbeatMonitor hbm = new HeartbeatMonitor(provider, 2 * 1000);
    Assert.assertFalse(hbm.isAlive());
    Map<String, ComponentInstanceState> statuses = new HashMap<>();
    ComponentInstanceState masterState = new ComponentInstanceState("HBASE_MASTER", "Cid1", "Aid1");
    masterState.setState(State.STARTED);
    masterState.setLastHeartbeat(System.currentTimeMillis());
    statuses.put("Aid1_Cid1_HBASE_MASTER", masterState);

    ComponentInstanceState slaveState = new ComponentInstanceState("HBASE_REGIONSERVER", "Cid2", "Aid1");
    slaveState.setState(State.STARTED);
    slaveState.setLastHeartbeat(System.currentTimeMillis());
    statuses.put("Aid1_Cid2_HBASE_REGIONSERVER", slaveState);

    expect(provider.getComponentStatuses()).andReturn(statuses).anyTimes();
    expect(provider.releaseContainer("Aid1_Cid2_HBASE_REGIONSERVER")).andReturn(true).once();
    replay(provider);
    hbm.start();

    Thread.sleep(1 * 1000);
    // just dial back by at least 2 sec but no more than 4
    slaveState.setLastHeartbeat(System.currentTimeMillis() - (2 * 1000 + 100));
    masterState.setLastHeartbeat(System.currentTimeMillis());

    Thread.sleep(1 * 1000 + 500);
    masterState.setLastHeartbeat(System.currentTimeMillis());

    log.info("Slave container state {}", slaveState.getContainerState());
    Assert.assertEquals(ContainerState.HEALTHY, masterState.getContainerState());
    Assert.assertEquals(ContainerState.UNHEALTHY, slaveState.getContainerState());

    Thread.sleep(1 * 1000);
    // some lost heartbeats are ignored (e.g. ~ 1 sec)
    masterState.setLastHeartbeat(System.currentTimeMillis() - 1 * 1000);

    Thread.sleep(1 * 1000 + 500);

    log.info("Slave container state {}", slaveState.getContainerState());
    Assert.assertEquals(ContainerState.HEALTHY, masterState.getContainerState());
    Assert.assertEquals(ContainerState.HEARTBEAT_LOST, slaveState.getContainerState());
    hbm.shutdown();
  }

  @Test
  public void testHeartbeatTransitions() {
    ComponentInstanceState slaveState = new ComponentInstanceState("HBASE_REGIONSERVER", "Cid2", "Aid1");
    slaveState.setState(State.STARTED);

    Assert.assertEquals(ContainerState.INIT, slaveState.getContainerState());
    slaveState.setLastHeartbeat(System.currentTimeMillis());
    Assert.assertEquals(ContainerState.HEALTHY, slaveState.getContainerState());

    slaveState.setContainerState(ContainerState.UNHEALTHY);
    Assert.assertEquals(ContainerState.UNHEALTHY, slaveState.getContainerState());
    slaveState.setLastHeartbeat(System.currentTimeMillis());
    Assert.assertEquals(ContainerState.HEALTHY, slaveState.getContainerState());

    slaveState.setContainerState(ContainerState.HEARTBEAT_LOST);
    Assert.assertEquals(ContainerState.HEARTBEAT_LOST, slaveState.getContainerState());
    slaveState.setLastHeartbeat(System.currentTimeMillis());
    Assert.assertEquals(ContainerState.HEARTBEAT_LOST, slaveState.getContainerState());
  }
}
