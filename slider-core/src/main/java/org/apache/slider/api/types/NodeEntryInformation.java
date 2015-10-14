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

package org.apache.slider.api.types;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Serialized node entry information. Must be kept in sync with the protobuf equivalent.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class NodeEntryInformation {

  public int priority;

  /**
   * instance explicitly requested on this node: it's OK if an allocation
   * comes in that has not been (and when that happens, this count should 
   * not drop).
   */
  public int requested;

  /** number of starting instances */
  public int starting;

  /** incrementing counter of instances that failed to start */
  public int startFailed;

  /** incrementing counter of instances that failed */
  public int failed;

  /**
   * Counter of "failed recently" events. These are all failures
   * which have happened since it was last reset.
   */
  public int failedRecently;

  /** incrementing counter of instances that have been pre-empted. */
  public int preempted;

  /**
   * Number of live nodes. 
   */
  public int live;

  /** number of containers being released off this node */
  public int releasing;

  /** timestamp of last use */
  public long lastUsed;
}
