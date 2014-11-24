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

package org.apache.slider.server.appmaster.model.monkey

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.api.InternalKeys
import org.apache.slider.server.appmaster.actions.ActionHalt
import org.apache.slider.server.appmaster.actions.ActionKillContainer
import org.apache.slider.server.appmaster.actions.QueueService
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockRMOperationHandler
import org.apache.slider.server.appmaster.monkey.ChaosKillAM
import org.apache.slider.server.appmaster.monkey.ChaosKillContainer
import org.apache.slider.server.appmaster.monkey.ChaosMonkeyService
import org.apache.slider.server.appmaster.monkey.ChaosTarget
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation
import org.apache.slider.server.appmaster.state.RoleInstance
import org.junit.Before
import org.junit.Test

import java.util.concurrent.TimeUnit

@CompileStatic
@Slf4j
class TestMockMonkey extends BaseMockAppStateTest {

  /**
   * This queue service is NOT started; tests need to poll the queue
   * rather than expect them to execute
   */
  QueueService queues
  ChaosMonkeyService monkey

  @Before
  public void init() {
    def configuration = new YarnConfiguration()
    queues = new QueueService();
    queues.init(configuration)
    monkey = new ChaosMonkeyService(metrics.metrics, queues)
    monkey.init(configuration)
  }
  
  @Test
  public void testMonkeyStart() throws Throwable {
    monkey.start()
    monkey.stop()
  }

  @Test
  public void testMonkeyPlay() throws Throwable {
    ChaosCounter counter = new ChaosCounter()
    monkey.addTarget("target", counter, InternalKeys.PROBABILITY_PERCENT_100)
    assert 1 == monkey.targetCount;
    monkey.play()
    assert counter.count == 1
  }

  @Test
  public void testMonkeySchedule() throws Throwable {
    ChaosCounter counter = new ChaosCounter()
    assert 0 == monkey.targetCount;
    monkey.addTarget("target", counter, InternalKeys.PROBABILITY_PERCENT_100)
    assert 1 == monkey.targetCount;
    assert monkey.schedule(0, 1, TimeUnit.SECONDS)
    assert 1 == queues.scheduledActions.size()
  }

  @Test
  public void testMonkeyDoesntAddProb0Actions() throws Throwable {
    ChaosCounter counter = new ChaosCounter()
    monkey.addTarget("target", counter, 0)
    assert 0 == monkey.targetCount;
    monkey.play()
    assert counter.count == 0
  }


  @Test
  public void testMonkeyScheduleProb0Actions() throws Throwable {
    ChaosCounter counter = new ChaosCounter()
    monkey.addTarget("target", counter, 0)
    assert !monkey.schedule(0, 1, TimeUnit.SECONDS)
    assert 0 == queues.scheduledActions.size()
  }


  @Test
  public void testMonkeyPlaySometimes() throws Throwable {
    ChaosCounter counter = new ChaosCounter()
    ChaosCounter counter2 = new ChaosCounter()
    monkey.addTarget("target1", counter, InternalKeys.PROBABILITY_PERCENT_1 * 50)
    monkey.addTarget("target2", counter2, InternalKeys.PROBABILITY_PERCENT_1 * 25)

    for (int i = 0; i < 100; i++) {
      monkey.play()
    }
    log.info("Counter1 = ${counter.count} counter2 = ${counter2.count}")
    /*
     * Relying on probability here to give approximate answers 
     */
    assert counter.count > 25 
    assert counter.count < 75 
    assert counter2.count < counter.count 
  }

  @Test
  public void testAMKiller() throws Throwable {

    def chaos = new ChaosKillAM(queues, -1)
    chaos.chaosAction();
    assert queues.scheduledActions.size() == 1
    def action = queues.scheduledActions.take()
    assert action instanceof ActionHalt
  }
  
  
  @Test
  public void testContainerKillerEmptyApp() throws Throwable {

    
    def chaos = new ChaosKillContainer(appState,
        queues,
        new MockRMOperationHandler())
    chaos.chaosAction();
    assert queues.scheduledActions.size() == 0
  }
  
     
  @Test
  public void testContainerKillerIgnoresAM() throws Throwable {

    addAppMastertoAppState()
    assert 1 == appState.liveNodes.size()
    
    def chaos = new ChaosKillContainer(appState,
        queues,
        new MockRMOperationHandler())
    chaos.chaosAction();
    assert queues.scheduledActions.size() == 0
  }
  
   
  
  @Test
  public void testContainerKiller() throws Throwable {
    MockRMOperationHandler ops = new MockRMOperationHandler();
    role0Status.desired = 1
    List<RoleInstance> instances = createAndStartNodes()
    assert instances.size() == 1
    def instance = instances[0]
    
    def chaos = new ChaosKillContainer(appState, queues, ops)
    chaos.chaosAction();
    assert queues.scheduledActions.size() == 1
    def action = queues.scheduledActions.take()
    ActionKillContainer killer = (ActionKillContainer) action
    assert killer.containerId == instance.containerId;
    killer.execute(null, queues, appState)
    assert ops.releases == 1;

    ContainerReleaseOperation operation = (ContainerReleaseOperation) ops.operations[0]
    assert operation.containerId == instance.containerId
  }
  
  

  /**
   * Chaos target that just implement a counter
   */
  private static class ChaosCounter implements ChaosTarget {
    int count;
    
    @Override
    void chaosAction() {
      count++;
    }


    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "ChaosCounter{");
      sb.append("count=").append(count);
      sb.append('}');
      return sb.toString();
    }
  }
}
