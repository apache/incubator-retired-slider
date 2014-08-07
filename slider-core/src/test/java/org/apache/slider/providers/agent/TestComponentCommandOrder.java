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

import org.apache.slider.providers.agent.application.metadata.CommandOrder;
import org.apache.slider.server.appmaster.model.mock.MockContainerId;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class TestComponentCommandOrder {
  protected static final Logger log =
      LoggerFactory.getLogger(TestComponentCommandOrder.class);
  private final MockContainerId containerId = new MockContainerId(1);

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

    ComponentCommandOrder cco = new ComponentCommandOrder(Arrays.asList(co1, co2, co3));
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

    ComponentCommandOrder cco = new ComponentCommandOrder(Arrays.asList(co));
    Assert.assertTrue(cco.canExecute("A", Command.START, Arrays.asList(cisB, cisC)));

    co.setCommand(" A-STAR");
    co.setRequires("B-STARTED , C-STARTED");
    try {
      cco = new ComponentCommandOrder(Arrays.asList(co));
      Assert.fail("Instantiation should have failed.");
    } catch (IllegalArgumentException ie) {
      log.info(ie.getMessage());
    }

    co.setCommand(" -START");
    co.setRequires("B-STARTED , C-STARTED");
    try {
      cco = new ComponentCommandOrder(Arrays.asList(co));
      Assert.fail("Instantiation should have failed.");
    } catch (IllegalArgumentException ie) {
      log.info(ie.getMessage());
    }

    co.setCommand(" A-START");
    co.setRequires("B-STRTED , C-STARTED");
    try {
      cco = new ComponentCommandOrder(Arrays.asList(co));
      Assert.fail("Instantiation should have failed.");
    } catch (IllegalArgumentException ie) {
      log.info(ie.getMessage());
    }

    co.setCommand(" A-START");
    co.setRequires("B-STARTED , C-");
    try {
      cco = new ComponentCommandOrder(Arrays.asList(co));
      Assert.fail("Instantiation should have failed.");
    } catch (IllegalArgumentException ie) {
      log.info(ie.getMessage());
    }

    co.setCommand(" A-INSTALL");
    co.setRequires("B-STARTED");
    try {
      cco = new ComponentCommandOrder(Arrays.asList(co));
      Assert.fail("Instantiation should have failed.");
    } catch (IllegalArgumentException ie) {
      log.info(ie.getMessage());
    }
  }
}
