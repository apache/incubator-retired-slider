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

package org.apache.slider.server.appmaster.actions

import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.service.ServiceOperations
import org.apache.slider.server.appmaster.SliderAppMaster
import org.apache.slider.server.appmaster.state.AppState
import org.apache.slider.server.services.workflow.ServiceThreadFactory
import org.apache.slider.server.services.workflow.WorkflowExecutorService
import org.junit.After
import org.junit.Before
import org.junit.Test

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

@Slf4j
//@CompileStatic
class TestActions {

  QueueService queues;
  WorkflowExecutorService<ExecutorService> executorService;


  @Before
  void createService() {
    queues = new QueueService();

    def conf = new Configuration()
    queues.init(conf)

    queues.start();

    executorService = new WorkflowExecutorService<>("AmExecutor",
        Executors.newCachedThreadPool(
            new ServiceThreadFactory("AmExecutor", true)));

    executorService.init(conf)
    executorService.start();
  }

  @After
  void destroyService() {
    ServiceOperations.stop(executorService);
    ServiceOperations.stop(queues);
  }

  @Test
  public void testBasicService() throws Throwable {
    queues.start();
  }

  @Test
  public void testDelayLogic() throws Throwable {
    ActionNoteExecuted action = new ActionNoteExecuted("", 1000)
    long now = System.currentTimeMillis();

    def delay = action.getDelay(TimeUnit.MILLISECONDS)
    assert delay >= 800
    assert delay <= 1800

    ActionNoteExecuted a2 = new ActionNoteExecuted("a2", 10000)
    assert action.compareTo(a2) < 0
    assert a2.compareTo(action) > 0
    assert action.compareTo(action)== 0
    
  }

  @Test
  public void testActionDelayedExecutorTermination() throws Throwable {
    long start = System.currentTimeMillis()
    
    ActionStopQueue stopAction = new ActionStopQueue(1000);
    queues.scheduledActions.add(stopAction);
    queues.run();
    AsyncAction take = queues.actionQueue.take();
    assert take == stopAction
    long stop = System.currentTimeMillis();
    assert stop - start > 500
    assert stop - start < 1500
  }

  @Test
  public void testImmediateQueue() throws Throwable {
    ActionNoteExecuted noteExecuted = new ActionNoteExecuted("executed", 0)
    queues.put(noteExecuted)
    queues.put(new ActionStopQueue(0))
    QueueExecutor ex = new QueueExecutor(queues)
    ex.run();
    assert queues.actionQueue.empty
    assert noteExecuted.executed.get()
  }

  @Test
  public void testActionOrdering() throws Throwable {

    ActionNoteExecuted note1 = new ActionNoteExecuted("note1", 500)
    def stop = new ActionStopQueue(1500)
    ActionNoteExecuted note2 = new ActionNoteExecuted("note2", 800)

    List<AsyncAction> actions = [note1, stop, note2]
    Collections.sort(actions)
    assert actions[0] == note1
    assert actions[1] == note2
    assert actions[2] == stop
  }
  
  @Test
  public void testDelayedQueueWithReschedule() throws Throwable {
    
    ActionNoteExecuted note1 = new ActionNoteExecuted("note1", 500)
    def stop = new ActionStopQueue(1500)
    ActionNoteExecuted note2 = new ActionNoteExecuted("note2", 800)
    
    assert note2.compareTo(stop) < 0
    assert note1.nanos < note2.nanos
    assert note2.nanos < stop.nanos
    queues.schedule(note1)
    queues.schedule(note2)
    queues.schedule(stop)
    // async to sync expected to run in order
    runQueuesToCompletion()
    assert note1.executed.get()
    assert note2.executed.get()
  }

  public void runQueuesToCompletion() {
    queues.run();
    assert queues.scheduledActions.empty
    assert !queues.actionQueue.empty
    QueueExecutor ex = new QueueExecutor(queues)
    ex.run();
    // flush all stop commands from the queue
    queues.flushActionQueue(ActionStopQueue.class)
    
    assert queues.actionQueue.empty
  }

  @Test
  public void testRenewedActionFiresOnceAtLeast() throws Throwable {
    ActionNoteExecuted note1 = new ActionNoteExecuted("note1", 500)
    RenewingAction renewer = new RenewingAction(
        note1,
        500,
        100,
        TimeUnit.MILLISECONDS,
        3)
    queues.schedule(renewer);
    def stop = new ActionStopQueue(4, TimeUnit.SECONDS)
    queues.schedule(stop);
    // this runs all the delayed actions FIRST, so can't be used
    // to play tricks of renewing actions ahead of the stop action
    runQueuesToCompletion()
    assert renewer.executionCount == 1
    assert note1.executionCount == 1
    // assert the renewed item is back in
    assert queues.scheduledActions.contains(renewer)
  }


  @Test
  public void testRenewingActionOperations() throws Throwable {
    ActionNoteExecuted note1 = new ActionNoteExecuted("note1", 500)
    RenewingAction renewer = new RenewingAction(
        note1,
        100,
        100,
        TimeUnit.MILLISECONDS,
        3)
    queues.renewing("note", renewer)
    assert queues.removeRenewingAction("note")
    queues.stop()
    assert queues.waitForServiceToStop(10000)
  }
  
  public class ActionNoteExecuted extends AsyncAction {
    public final AtomicBoolean executed = new AtomicBoolean(false);
    public final AtomicLong executionTimeNanos = new AtomicLong()
    private final AtomicLong executionCount = new AtomicLong()

    public ActionNoteExecuted(String text, int delay) {
      super(text, delay);
    }

    @Override
    public void execute(
        SliderAppMaster appMaster,
        QueueAccess queueService,
        AppState appState) throws Exception {
      log.info("Executing $name");
      executed.set(true);
      executionTimeNanos.set(System.nanoTime())
      executionCount.incrementAndGet()
      log.info(this.toString())
      
      synchronized (this) {
        this.notify();
      }
    }

    @Override
    String toString() {
      return super.toString() +
             " executed=${executed.get()}; count=${executionCount.get()};"
    }

    long getExecutionCount() {
      return executionCount.get()
    }
  }
}
