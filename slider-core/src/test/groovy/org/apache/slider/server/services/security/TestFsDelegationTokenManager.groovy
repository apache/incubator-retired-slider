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

package org.apache.slider.server.services.security

import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.RawLocalFileSystem
import org.apache.hadoop.hdfs.DFSConfigKeys
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.SecretManager
import org.apache.hadoop.security.token.Token
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager
import org.apache.hadoop.service.ServiceOperations
import org.apache.hadoop.util.Time
import org.apache.slider.common.tools.CoreFileSystem
import org.apache.slider.server.appmaster.actions.ActionStopQueue
import org.apache.slider.server.appmaster.actions.QueueExecutor
import org.apache.slider.server.appmaster.actions.QueueService
import org.junit.After
import org.junit.Before
import org.junit.Test

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

@Slf4j
//@CompileStatic
class TestFsDelegationTokenManager {

  QueueService queues;
  FsDelegationTokenManager tokenManager;
  Configuration conf;
  UserGroupInformation currentUser;


  @Before
  void setup() {
    queues = new QueueService();

    conf = new Configuration()
    conf.set(
            DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION,
            "TOKEN")
    conf.setLong(
            DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
            1000)
    queues.init(conf)
    queues.start();

    HadoopFS fs = new TestFileSystem()

    CoreFileSystem coreFileSystem = new CoreFileSystem(fs, conf)

    String[] groups = new String[1];
    groups[0] = 'testGroup1'

    currentUser = UserGroupInformation.createUserForTesting("test", groups)
    UserGroupInformation.setLoginUser(currentUser)

    tokenManager = new FsDelegationTokenManager(queues) {
        @Override
        protected int getRenewingLimit() {
            return 5
        }

        @Override
        protected org.apache.hadoop.fs.FileSystem getRemoteFileSystemForRenewal(Configuration config) throws IOException {
            return new TestFileSystem();
        }

        @Override
        protected String getRenewingActionName() {
            return "TEST RENEW"
        }
    }

  }

  public static class DummySecretManager extends
          AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

    public DummySecretManager(long delegationKeyUpdateInterval,
                              long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
                              long delegationTokenRemoverScanInterval) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
            delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
      return null;
    }

    @Override
    public byte[] createPassword(DelegationTokenIdentifier dtId) {
      return new byte[1];
    }
  }

  public class TestFileSystem extends RawLocalFileSystem {
      int sequenceNum = 0;
      SecretManager<DelegationTokenIdentifier> mgr =
          new DummySecretManager(0, 0, 0, 0);
      @Override
      Token<DelegationTokenIdentifier> getDelegationToken(String renewer) throws IOException {
        return new TestToken(getIdentifier(), mgr);
      }

      @Override
      Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException {
          Token[] tokens = new Token[1]
          tokens[0] = new TestToken(getIdentifier(), mgr)
          return tokens
      }

      private DelegationTokenIdentifier getIdentifier() {
          def user = new Text(currentUser.getUserName())
          def id = new DelegationTokenIdentifier(user, user, user)
          id.setSequenceNumber(sequenceNum++)
          id.setMaxDate(Time.now() + 10000)

          return id
      }
  }

  public class TestToken extends Token<DelegationTokenIdentifier> {
      static long maxCount = 0;
      private final AtomicLong renewCount = new AtomicLong()
      private final AtomicLong totalCount = new AtomicLong()
      public final AtomicBoolean expired = new AtomicBoolean(false);
      public final AtomicBoolean cancelled = new AtomicBoolean(false);

      TestToken(DelegationTokenIdentifier id, SecretManager<DelegationTokenIdentifier> mgr) {
          super(id, mgr)
      }

      @Override
      Text getService() {
          return new Text("HDFS")
      }

      @Override
      long renew(Configuration conf) throws IOException, InterruptedException {
          totalCount.getAndIncrement();
          if (maxCount > 0 && renewCount.getAndIncrement() > maxCount) {
              renewCount.set(0L)
              expired.set(true)
              throw new IOException("Expired")
          }


          return Time.now() + 1000;
      }

      @Override
      void cancel(Configuration conf) throws IOException, InterruptedException {
        cancelled.set(true)
      }
  }

  @After
  void destroyService() {
    ServiceOperations.stop(queues);
  }

  public void runQueuesToCompletion() {
    new Thread(queues).start();
    QueueExecutor ex = new QueueExecutor(queues)
    ex.run();
  }

  public void runQueuesButNotToCompletion() {
    new Thread(queues).start();
    QueueExecutor ex = new QueueExecutor(queues)
    new Thread(ex).start();
    Thread.sleep(1000)
    tokenManager.cancelDelegationToken(conf)
  }

  @Test
  public void testRenew() throws Throwable {
    tokenManager.acquireDelegationToken(conf)
    def stop = new ActionStopQueue(10, TimeUnit.SECONDS)
    queues.schedule(stop);
    runQueuesToCompletion()

    TestToken token = (TestToken) currentUser.getTokens()[0]
    assert token.totalCount.get() > 4
  }

  @Test
  public void testCancel() throws Throwable {
    tokenManager.acquireDelegationToken(conf)
    def stop = new ActionStopQueue(10, TimeUnit.SECONDS)
    queues.schedule(stop);
    runQueuesButNotToCompletion()

    TestToken token = (TestToken) currentUser.getTokens()[0]
    assert token.cancelled.get()
    assert queues.lookupRenewingAction("TEST RENEW") == null
  }


    @Test
  public void testRenewPastExpiry() throws Throwable {
    try {
      TestToken.maxCount = 3L
      tokenManager.acquireDelegationToken(conf)
      TestToken origToken = currentUser.getTokens()[0]
      def stop = new ActionStopQueue(10, TimeUnit.SECONDS)
      queues.schedule(stop);
      runQueuesToCompletion()

      TestToken token = (TestToken) currentUser.getTokens()[0]
      assert token != null
      assert token != origToken
      assert origToken.getService().equals(token.getService())
      assert origToken.totalCount.get() > 4
      assert origToken.expired.get()
    } finally {
      TestToken.maxCount = 0
    }
  }
}
