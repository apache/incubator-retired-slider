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
 * This is a long which acts as a gauge
 */
public class LongGauge implements Metric, Gauge<Long> {

  private final AtomicLong value;

  /**
   * Instantiate
   * @param val current value
   */
  public LongGauge(long val) {
    this.value = new AtomicLong(val);
  }

  public LongGauge() {
    this(0);
  }

  /**
   * Set to a new value.
   * @param val value
   */
  public synchronized void set(long val) {
    value.set(val);
  }

  public void inc() {
    inc(1);
  }

  public void dec() {
    dec(1);
  }

  public synchronized void inc(int delta) {
    set(value.get() + delta);
  }

  public synchronized void dec(int delta) {
    set(value.get() - delta);
  }

  public long get() {
    return value.get();
  }

  @Override
  public Long getValue() {
    return get();
  }

  @Override
  public String toString() {
   return value.toString();
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return value.equals(obj);
  }
}
