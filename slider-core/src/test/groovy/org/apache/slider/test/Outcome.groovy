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

package org.apache.slider.test

/**
 * Outcome for probes
 */

class Outcome {

  public final String name;

  private Outcome(String name) {
    this.name = name
  }

  static Outcome Success = new Outcome(
      "Success")
  static Outcome Retry = new Outcome("Retry")
  static Outcome Fail = new Outcome("Fail")

  /**
   * build from a bool, where false is mapped to retry
   * @param b boolean
   * @return an outcome
   */
  static Outcome fromBool(boolean b) {
    return b ? Success : Retry;
  }

}
