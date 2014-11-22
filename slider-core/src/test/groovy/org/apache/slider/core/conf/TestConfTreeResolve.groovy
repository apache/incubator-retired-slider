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

package org.apache.slider.core.conf

import groovy.util.logging.Slf4j
import static org.apache.slider.api.InternalKeys.*
import org.junit.Assert
import org.junit.Test

import static org.apache.slider.core.conf.ExampleConfResources.*

/**
 * Test 
 */
@Slf4j
class TestConfTreeResolve extends Assert {
  @Test
  public void testOverride() throws Throwable {

    def orig = ExampleConfResources.loadResource(overridden) 

    ConfTreeOperations origOperations = new ConfTreeOperations(orig)
    origOperations.validate()


    def global = origOperations.globalOptions
    assert global["g1"] == "a"
    assert global["g2"] == "b"

    def simple = origOperations.getMandatoryComponent("simple")
    assert simple.size() == 0

    def master = origOperations.getMandatoryComponent("master")
    assert master["name"] == "m"
    assert master["g1"] == "overridden"

    def worker = origOperations.getMandatoryComponent("worker")
    log.info("worker = $worker")
    assert worker.size() == 3

    assert worker["name"] == "worker"
    assert worker["g1"] == "overridden-by-worker"
    assert worker["g2"] == null
    assert worker["timeout"] == "1000"

    // here is the resolution
    origOperations.resolve()

    global = origOperations.globalOptions
    log.info("global = $global")
    assert global["g1"] == "a"
    assert global["g2"] == "b"

    simple = origOperations.getMandatoryComponent("simple")
    assert simple.size() == 2
    simple.getMandatoryOption("g1")
    assert simple["g1"]


    master = origOperations.getMandatoryComponent("master")
    log.info("master = $master")
    assert master.size() == 3
    assert master["name"] == "m"
    assert master["g1"] == "overridden"
    assert master["g2"] == "b"

    worker = origOperations.getMandatoryComponent("worker")
    log.info("worker = $worker")
    assert worker.size() == 4

    assert worker["name"] == "worker"
    assert worker["g1"] == "overridden-by-worker"
    assert worker["g2"] == "b"
    assert worker["timeout"] == "1000"

  }

  @Test
  public void testTimeIntervalLoading() throws Throwable {

    def orig = ExampleConfResources.loadResource(internal)

    MapOperations internals = new MapOperations(orig.global)
    def s = internals.getOptionInt(
        CHAOS_MONKEY_INTERVAL + MapOperations.SECONDS,
        0)
    assert s == 60
    long monkeyInterval = internals.getTimeRange(
        CHAOS_MONKEY_INTERVAL,
        DEFAULT_CHAOS_MONKEY_INTERVAL_DAYS,
        DEFAULT_CHAOS_MONKEY_INTERVAL_HOURS,
        DEFAULT_CHAOS_MONKEY_INTERVAL_MINUTES,
        0);
    assert monkeyInterval == 60;
  }
}
