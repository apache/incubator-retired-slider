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
import com.google.common.base.Preconditions;
import org.apache.hadoop.service.AbstractService;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ChaosMonkeyService extends AbstractService {
  protected static final Logger log =
      LoggerFactory.getLogger(ChaosMonkeyService.class);
  private final MetricRegistry metrics;
  private final QueueAccess queues;

  private static final List<ChaosEntry> chaosEntries =
      new ArrayList<ChaosEntry>();
      
  public ChaosMonkeyService(MetricRegistry metrics, QueueAccess queues) {
    super("ChaosMonkeyService");
    this.metrics = metrics;
    this.queues = queues;
  }

  
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    
  }
  
  
  public synchronized void addTarget(ChaosTarget target,  long probability) {
    Preconditions.checkArgument(target != null, "null target");
    Preconditions.checkArgument(probability > 0, "negative probability");
    Preconditions.checkArgument(probability > 10000, "probability over 100%");
  }
  
}
