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
package org.apache.slider.server.services.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.util.Time;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.actions.AsyncAction;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.actions.RenewingAction;
import org.apache.slider.server.appmaster.state.AppState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FsDelegationTokenManager {
  private final QueueAccess queue;
  private RenewingAction<RenewAction> renewingAction;
  private UserGroupInformation remoteUser;
  private UserGroupInformation currentUser;
  private static final Logger
      log = LoggerFactory.getLogger(FsDelegationTokenManager.class);
  private long renewInterval;
  private RenewAction renewAction;
  private String tokenName;

  public FsDelegationTokenManager(QueueAccess queue) throws IOException {
    this.queue = queue;
    this.currentUser = UserGroupInformation.getCurrentUser();
  }

  private void createRemoteUser(Configuration configuration) throws IOException {
    Configuration loginConfig = new Configuration(configuration);
    loginConfig.set(DFSConfigKeys.HADOOP_SECURITY_AUTHENTICATION,
                    "kerberos");
    // using HDFS principal...
    this.remoteUser = UserGroupInformation
        .loginUserFromKeytabAndReturnUGI(
            SecurityUtil.getServerPrincipal(
                loginConfig.get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY),
                InetAddress.getLocalHost().getCanonicalHostName()),
            loginConfig.get(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY));
    log.info("Created remote user {}.  UGI reports current user is {}",
             this.remoteUser, UserGroupInformation.getCurrentUser());
  }

  public void acquireDelegationToken(Configuration configuration)
      throws IOException, InterruptedException {
    if (remoteUser == null) {
      createRemoteUser(configuration);
    }
    if (SliderUtils.isHadoopClusterSecure(configuration) &&
        renewingAction == null) {
      renewInterval = configuration.getLong(
          DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
          DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
      // constructor of action will retrieve initial token.  One may already be
      // associated with user, but its lifecycle/management is not clear so let's
      // create and manage a token explicitly
      renewAction = new RenewAction("HDFS renew",
                                           configuration);
      // set retrieved token as the user associated delegation token and
      // start a renewing action to renew
      Token<?> token = renewAction.getToken();
      currentUser.addToken(token.getService(), token);
      log.info("HDFS delegation token {} acquired and set as credential for current user", token);
      renewingAction = new RenewingAction<RenewAction>(renewAction,
                                          (int) renewInterval,
                                          (int) renewInterval,
                                          TimeUnit.MILLISECONDS,
                                          getRenewingLimit());
      log.info("queuing HDFS delegation token renewal interval of {} milliseconds",
               renewInterval);
      queue(renewingAction);
    }
  }

  public void cancelDelegationToken(Configuration configuration)
      throws IOException, InterruptedException {
    queue.removeRenewingAction(getRenewingActionName());
    if (renewAction != null) {
      renewAction.getToken().cancel(configuration);
    }
    log.info("Renewing action {} removed and HDFS delegation token renewal "
             + "cancelled", getRenewingActionName());
  }

  protected int getRenewingLimit() {
    return 0;
  }

  protected void queue(RenewingAction<RenewAction> action) {
    queue.renewing(getRenewingActionName(),
                   action);
  }

  protected String getRenewingActionName() {
    if (tokenName == null) {
      tokenName = "HDFS renewing token " + UUID.randomUUID();
    }
    return tokenName;
  }

  class RenewAction extends AsyncAction {
    Configuration configuration;
    Token<?> token;
    private long tokenExpiryTime;
    private final FileSystem fs;

    RenewAction(String name,
                Configuration configuration)
        throws IOException, InterruptedException {
      super(name);
      this.configuration = configuration;
      fs = getFileSystem();
      // get initial token by creating a kerberos authenticated user and
      // invoking token methods as that user
      synchronized (fs) {
        this.token = remoteUser.doAs(new PrivilegedExceptionAction<Token<?>>() {
          @Override
          public Token<?> run() throws Exception {
            log.info("Obtaining HDFS delgation token with user {}",
                     remoteUser.getShortUserName());
            Token token = fs.getDelegationToken(
                remoteUser.getShortUserName());
            tokenExpiryTime = getTokenExpiryTime(token);
            log.info("Initial delegation token obtained with expiry time of {}", getPrintableExpirationTime(
                tokenExpiryTime));
            return token;
          }
        });
      }
      log.info("Initial request returned delegation token {}", token);
    }

    private long getTokenExpiryTime(Token token) throws IOException {
      AbstractDelegationTokenIdentifier id =
          (AbstractDelegationTokenIdentifier)token.decodeIdentifier();
      return id.getMaxDate();
    }

    protected FileSystem getFileSystem()
        throws IOException, InterruptedException {
      // return non-cache FS reference
      return remoteUser.doAs(new PrivilegedExceptionAction<FileSystem>() {
        @Override
        public FileSystem run() throws Exception {
          Configuration config = new Configuration(configuration);
          config.setBoolean("fs.hdfs.impl.disable.cache", true);
          return getRemoteFileSystemForRenewal(config);
        }
      });
    }

    @Override
    public void execute(SliderAppMaster appMaster, QueueAccess queueService,
                        AppState appState)
        throws Exception {
      if (fs != null) {
        synchronized(fs) {
          try {
            long expires = remoteUser.doAs(new PrivilegedExceptionAction<Long>() {
              @Override
              public Long run() throws Exception {
                long expires = token.renew(fs.getConf());
                log.info("HDFS delegation token renewed.  Renewal cycle ends at {}",
                         getPrintableExpirationTime(expires));
                return expires;
              }
            });
            long calculatedInterval = tokenExpiryTime - Time.now();
            if ( calculatedInterval < renewInterval ) {
              // time to get a new token since the token will expire before
              // next renewal interval.  Could modify this to be closer to expiry
              // time if deemed necessary....
              log.info("Interval of {} less than renew interval.  Getting new token",
                       calculatedInterval);
              getNewToken();
            } else {
              updateRenewalTime(renewInterval);
            }
          } catch (IOException ie) {
            // token has expired.  get a new one...
            log.info("Exception raised by renew", ie);
            getNewToken();
          }
        }
      }
    }

    private String getPrintableExpirationTime(long expires) {
      Date d = new Date(expires);
      return DateFormat.getDateTimeInstance().format(d);
    }

    private void getNewToken()
        throws InterruptedException, IOException {
      try {
        Text service = token.getService();
        Token<?>[] tokens = remoteUser.doAs(new PrivilegedExceptionAction<Token<?>[]>() {
            @Override
            public Token<?>[] run() throws Exception {
              return fs.addDelegationTokens(remoteUser.getShortUserName(), null);
            }
        });
        if (tokens.length == 0) {
          throw new IOException("addDelegationTokens returned no tokens");
        }
        token = findMatchingToken(service, tokens);
        currentUser.addToken(token.getService(), token);

        tokenExpiryTime = getTokenExpiryTime(token);

        log.info("Expired HDFS delegation token replaced and added as credential"
                 + " to current user.  Token expires at {}",
                 getPrintableExpirationTime(tokenExpiryTime));
        updateRenewalTime(renewInterval);
      } catch (IOException ie2) {
        throw new IOException("Can't get new delegation token ", ie2);
      }
    }

    private void updateRenewalTime(long interval) {
      long delay = interval - interval/10;
      renewingAction.updateInterval(delay, TimeUnit.MILLISECONDS);
      log.info("Token renewal set for {} ms from now", delay);
    }

    private Token<?> findMatchingToken(Text service, Token<?>[] tokens) {
      Token<?> token = null;
      int i = 0;
      while (token == null && i < tokens.length) {
        if (tokens[i].getService().equals(service)) {
          token = tokens[i];
        }
        i++;
      }

      return token;
    }

    Token<?> getToken() {
      synchronized (fs) {
        return token;
      }
    }
  }

  protected FileSystem getRemoteFileSystemForRenewal(Configuration config)
      throws IOException {
    return FileSystem.get(config);
  }
}
