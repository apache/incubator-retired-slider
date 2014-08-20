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

package org.apache.slider.server.appmaster.monkey;

import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.service.AbstractService;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.actions.RenewingAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * A chaos monkey service which will invoke ChaosTarget events 
 */
public class ChaosMonkeyService extends AbstractService {
  protected static final Logger log =
      LoggerFactory.getLogger(ChaosMonkeyService.class);
  public static final int PERCENT_1 = 100;
  public static final double PERCENT_1D = 100.0;
  
  /**
   * the percentage value as multiplied up
   */
  public static final int PERCENT_100 = 100 * PERCENT_1;
  private final MetricRegistry metrics;
  private final QueueAccess queues;
  private final Random random = new Random();

  private static final List<ChaosEntry> chaosEntries =
      new ArrayList<ChaosEntry>();

  public ChaosMonkeyService(MetricRegistry metrics, QueueAccess queues) {
    super("ChaosMonkeyService");
    this.metrics = metrics;
    this.queues = queues;
  }


  public synchronized void addTarget(String name,
      ChaosTarget target, long probability) {
    log.info("Adding {} with probability {}", name, probability / PERCENT_1);
    chaosEntries.add(new ChaosEntry(name, target, probability, metrics));
  }

  /**
   * Iterate through all the entries and invoke chaos on those wanted
   */
  public void play() {
    for (ChaosEntry chaosEntry : chaosEntries) {
      long p = random.nextInt(PERCENT_100);
      chaosEntry.maybeInvokeChaos(p);
    }
  }

  public RenewingAction<MonkeyPlayAction> getChaosAction(long time, TimeUnit timeUnit) {
    RenewingAction<MonkeyPlayAction> action = new RenewingAction<MonkeyPlayAction>(
        new MonkeyPlayAction(this, 0, TimeUnit.MILLISECONDS),
        time,
        time,
        timeUnit,
        0
    );
    return action;
  }
}
