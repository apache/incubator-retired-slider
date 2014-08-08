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

import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.server.appmaster.SliderAppMaster;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AsyncAction implements Delayed {

  private static final AtomicLong sequencer = new AtomicLong(0);

  public final String name;
  public final long nanos;
  private final EnumSet<ActionAttributes> attrs;
  private final long sequenceNumber = sequencer.incrementAndGet();


  protected AsyncAction(String name) {
    this(name, 0);
  }

  protected AsyncAction(String name,
      int delayMillis, EnumSet<ActionAttributes> attrs) {
    this.name = name;
    this.nanos = convertAndOffset(delayMillis);
    this.attrs = attrs;
  }

  protected AsyncAction(String name,
      int delayMillis,
      ActionAttributes... attributes) {
    this(name, delayMillis);
    Collections.addAll(attrs, attributes);
  }

  protected AsyncAction(String name,
      int delayMillis) {
    this.name = name;
    this.nanos = convertAndOffset(delayMillis);
    attrs = EnumSet.noneOf(ActionAttributes.class);
  }

  private long convertAndOffset(int delay) {
    return now() + TimeUnit.NANOSECONDS.convert(delay, TimeUnit.MILLISECONDS);
  }

  protected long now() {
    return System.nanoTime();
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(nanos - now(), TimeUnit.NANOSECONDS);
  }

  @Override
  public int compareTo(Delayed that) {
    if (this == that) {
      return 0;
    }
    return SliderUtils.compareTo(
        getDelay(TimeUnit.NANOSECONDS),
        that.getDelay(TimeUnit.NANOSECONDS));
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder(super.toString());
    sb.append(" name='").append(name).append('\'');
    sb.append(", nanos=").append(nanos);
    sb.append(", attrs=").append(attrs);
    sb.append(", sequenceNumber=").append(sequenceNumber);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Ask if an action has a specific attribute
   * @param attr attribute
   * @return true iff the action has the specific attribute
   */
  public boolean hasAttr(ActionAttributes attr) {
    return attrs.contains(attr);
  }

  /**
   * Actual application
   * @param appMaster
   * @throws IOException
   */
  public abstract void execute(SliderAppMaster appMaster) throws Exception;

  public enum ActionAttributes {
    SHRINKS_CLUSTER,
    EXPANDS_CLUSTER,
    HALTS_CLUSTER,
  }


}
