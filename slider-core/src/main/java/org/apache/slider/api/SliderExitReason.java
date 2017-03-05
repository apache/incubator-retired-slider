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
package org.apache.slider.api;

/**
 * A high level reason for an application failure. For most of the cases it is
 * difficult to decipher if the Slider app failed due to an application error.
 * This gap can be bridged a little better when we get to SLIDER-1208.
 *
 */
public enum SliderExitReason {
  STOP_COMMAND_ISSUED, SLIDER_AM_ERROR, SLIDER_AGENT_ERROR, CHAOS_MONKEY, YARN_ERROR, APP_ERROR;
}
