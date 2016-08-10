/**
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

package org.apache.slider.providers.agent;

import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.providers.agent.application.metadata.CommandOrder;
import org.apache.slider.server.appmaster.model.mock.MockContainerId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.apache.slider.api.RoleKeys.ROLE_PREFIX;

public class TestComponentCommandOrder {
  protected static final Logger log =
      LoggerFactory.getLogger(TestComponentCommandOrder.class);
  private final MockContainerId containerId = new MockContainerId(1);

  private static ConfTreeOperations resources = new ConfTreeOperations(
      new ConfTree());

  @BeforeClass
  public static void init() {
    resources.getOrAddComponent("A");
    resources.getOrAddComponent("B");
    resources.getOrAddComponent("C");
    resources.getOrAddComponent("D");
    resources.getOrAddComponent("E");
  }

  @Test
  public void testComponentCommandOrder() throws Exception {
    CommandOrder co1 = new CommandOrder();
    co1.setCommand("A-START");
    co1.setRequires("B-STARTED");
    CommandOrder co2 = new CommandOrder();
    co2.setCommand("A-START");
    co2.setRequires("C-STARTED");
    CommandOrder co3 = new CommandOrder();
    co3.setCommand("B-START");
    co3.setRequires("C-STARTED,D-STARTED,E-INSTALLED");

    ComponentCommandOrder cco = new ComponentCommandOrder(
        Arrays.asList(co1, co2, co3), resources);
    ComponentInstanceState cisB = new ComponentInstanceState("B",
        containerId, "aid");
    ComponentInstanceState cisC = new ComponentInstanceState("C", containerId, "aid");
    ComponentInstanceState cisD = new ComponentInstanceState("D", containerId, "aid");
    ComponentInstanceState cisE = new ComponentInstanceState("E", containerId, "aid");
    ComponentInstanceState cisE2 = new ComponentInstanceState("E", containerId, "aid");
    cisB.setState(State.STARTED);
    cisC.setState(State.INSTALLED);
    Assert.assertTrue(cco.canExecute("A", Command.START, Arrays.asList(cisB)));
    Assert.assertFalse(cco.canExecute("A", Command.START, Arrays.asList(cisB, cisC)));

    cisC.setState(State.STARTING);
    Assert.assertFalse(cco.canExecute("A", Command.START, Arrays.asList(cisB, cisC)));

    cisC.setState(State.INSTALL_FAILED);
    Assert.assertFalse(cco.canExecute("A", Command.START, Arrays.asList(cisB, cisC)));

    cisD.setState(State.INSTALL_FAILED);
    cisE.setState(State.STARTED);
    Assert.assertTrue(cco.canExecute("E", Command.START, Arrays.asList(cisB, cisC, cisD, cisE)));

    Assert.assertTrue(cco.canExecute("B", Command.INSTALL, Arrays.asList(cisB, cisC, cisD, cisE)));
    Assert.assertFalse(cco.canExecute("B", Command.START, Arrays.asList(cisB, cisC, cisD, cisE)));

    cisD.setState(State.INSTALLING);
    Assert.assertFalse(cco.canExecute("B", Command.START, Arrays.asList(cisB, cisC, cisD, cisE)));

    cisC.setState(State.STARTED);
    cisD.setState(State.STARTED);
    Assert.assertTrue(cco.canExecute("B", Command.START, Arrays.asList(cisB, cisC, cisD, cisE)));

    cisE2.setState(State.INSTALLING);
    Assert.assertFalse(cco.canExecute("B", Command.START, Arrays.asList(cisE, cisE2)));

    cisE2.setState(State.INSTALLED);
    Assert.assertTrue(cco.canExecute("B", Command.START, Arrays.asList(cisE, cisE2)));

    cisE2.setState(State.STARTED);
    Assert.assertTrue(cco.canExecute("B", Command.START, Arrays.asList(cisE, cisE2)));

    cisE2.setState(State.STARTING);
    Assert.assertTrue(cco.canExecute("B", Command.START, Arrays.asList(cisE, cisE2)));
  }

  @Test
  public void testComponentCommandOrderBadInput() throws Exception {
    CommandOrder co = new CommandOrder();
    co.setCommand(" A-START");
    co.setRequires("B-STARTED , C-STARTED");

    ComponentInstanceState cisB = new ComponentInstanceState("B", containerId, "aid");
    ComponentInstanceState cisC = new ComponentInstanceState("C", containerId, "aid");
    cisB.setState(State.STARTED);
    cisC.setState(State.STARTED);

    ComponentCommandOrder cco = new ComponentCommandOrder(Arrays.asList(co),
        resources);
    Assert.assertTrue(cco.canExecute("A", Command.START, Arrays.asList(cisB, cisC)));

    co.setCommand(" A-STAR");
    co.setRequires("B-STARTED , C-STARTED");
    try {
      cco = new ComponentCommandOrder(Arrays.asList(co), resources);
      Assert.fail("Instantiation should have failed.");
    } catch (IllegalArgumentException ie) {
      log.info(ie.getMessage());
    }

    co.setCommand(" -START");
    co.setRequires("B-STARTED , C-STARTED");
    try {
      cco = new ComponentCommandOrder(Arrays.asList(co), resources);
      Assert.fail("Instantiation should have failed.");
    } catch (IllegalArgumentException ie) {
      log.info(ie.getMessage());
    }

    co.setCommand(" A-START");
    co.setRequires("B-STRTED , C-STARTED");
    try {
      cco = new ComponentCommandOrder(Arrays.asList(co), resources);
      Assert.fail("Instantiation should have failed.");
    } catch (IllegalArgumentException ie) {
      log.info(ie.getMessage());
    }

    co.setCommand(" A-START");
    co.setRequires("B-STARTED , C-");
    try {
      cco = new ComponentCommandOrder(Arrays.asList(co), resources);
      Assert.fail("Instantiation should have failed.");
    } catch (IllegalArgumentException ie) {
      log.info(ie.getMessage());
    }

    co.setCommand(" A-INSTALL");
    co.setRequires("B-STARTED");
    try {
      cco = new ComponentCommandOrder(Arrays.asList(co), resources);
      Assert.fail("Instantiation should have failed.");
    } catch (IllegalArgumentException ie) {
      log.info(ie.getMessage());
    }
  }

  @Test
  public void testComponentCommandOrderBadComponent() throws Exception {
    ConfTreeOperations resourcesGood = new ConfTreeOperations(new ConfTree());
    resourcesGood.getOrAddComponent("A");
    resourcesGood.getOrAddComponent("Z");
    ConfTreeOperations resourcesBad = new ConfTreeOperations(new ConfTree());

    CommandOrder co1 = new CommandOrder();
    co1.setCommand("A-START");
    co1.setRequires("Z-STARTED");
    CommandOrder co2 = new CommandOrder();
    co2.setCommand("Z-START");
    co2.setRequires("A-STARTED");

    ComponentCommandOrder cco = new ComponentCommandOrder(
        Arrays.asList(co1), resourcesGood);
    try {
      cco = new ComponentCommandOrder(Arrays.asList(co1), resourcesBad);
      Assert.fail("Instantiation should have failed.");
    } catch (IllegalArgumentException ie) {
      log.info(ie.getMessage());
    }

    cco = new ComponentCommandOrder(Arrays.asList(co2), resourcesGood);
    try {
      cco = new ComponentCommandOrder(Arrays.asList(co2), resourcesBad);
      Assert.fail("Instantiation should have failed.");
    } catch (IllegalArgumentException ie) {
      log.info(ie.getMessage());
    }
  }

  @Test
  public void testComponentCommandOrderPrefixes() throws Exception {
    ConfTreeOperations resources = new ConfTreeOperations(new ConfTree());
    resources.getOrAddComponent("a-A").put(ROLE_PREFIX, "a-");
    resources.getOrAddComponent("b-B1").put(ROLE_PREFIX, "b-");
    resources.getOrAddComponent("b-B2").put(ROLE_PREFIX, "b-");
    resources.getOrAddComponent("c-C").put(ROLE_PREFIX, "c-");

    CommandOrder co1 = new CommandOrder();
    co1.setCommand("b-START");
    co1.setRequires("a-STARTED");
    CommandOrder co2 = new CommandOrder();
    co2.setCommand("c-START");
    co2.setRequires("b-STARTED");

    ComponentCommandOrder cco = new ComponentCommandOrder(
        Arrays.asList(co1, co2), resources);

    ComponentInstanceState cisA = new ComponentInstanceState("a-A", containerId, "aid");
    ComponentInstanceState cisB1 = new ComponentInstanceState("b-B1", containerId, "aid");
    ComponentInstanceState cisB2 = new ComponentInstanceState("b-B2", containerId, "aid");
    ComponentInstanceState cisC = new ComponentInstanceState("c-C", containerId, "aid");
    cisA.setState(State.INSTALLED);
    cisB1.setState(State.INSTALLED);
    cisB2.setState(State.INSTALLED);
    cisC.setState(State.INSTALLED);
    List<ComponentInstanceState> states = Arrays.asList(cisA, cisB1, cisB2, cisC);
    Assert.assertTrue(cco.canExecute("a-A", Command.START, states));
    Assert.assertFalse(cco.canExecute("b-B1", Command.START, states));
    Assert.assertFalse(cco.canExecute("b-B2", Command.START, states));
    Assert.assertFalse(cco.canExecute("c-C", Command.START, states));
    cisA.setState(State.STARTED);
    Assert.assertTrue(cco.canExecute("b-B1", Command.START, states));
    Assert.assertTrue(cco.canExecute("b-B2", Command.START, states));
    Assert.assertFalse(cco.canExecute("c-C", Command.START, states));
    cisB1.setState(State.STARTED);
    Assert.assertFalse(cco.canExecute("c-C", Command.START, states));
    cisB2.setState(State.STARTED);
    Assert.assertTrue(cco.canExecute("c-C", Command.START, states));
  }
}
