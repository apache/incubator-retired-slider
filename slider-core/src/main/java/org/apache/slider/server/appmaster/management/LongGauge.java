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

package org.apache.slider.server.appmaster.management;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This is a {@link AtomicLong} which acts as a metrics gauge: its state can be exposed as
 * a management value.
 *
 */
public class LongGauge extends AtomicLong implements Metric, Gauge<Long> {

  /**
   * Instantiate
   * @param val current value
   */
  public LongGauge(long val) {
    super(val);
  }

  /**
   * Instantiate with value 0
   */
  public LongGauge() {
    this(0);
  }


  @Override
  public Long getValue() {
    return get();
  }

  /**
   * Decrement to the floor of 0.
   * There's checks to stop more than one thread being in this method at the time, but
   * that doesn't stop other operations on the value
   * @param delta delta
   * @return the current value
   */
  public synchronized long decToFloor(long delta) {
    long newval = Math.max(0L, get() - delta);
    set(newval);
    return get();
  }
}
