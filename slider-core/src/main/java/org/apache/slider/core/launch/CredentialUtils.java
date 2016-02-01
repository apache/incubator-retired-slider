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

package org.apache.slider.core.launch;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.common.SliderXmlConfKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.*;

/**
 * Utils to work with credentials and tokens.
 *
 * Designed to be movable to Hadoop core
 */
public final class CredentialUtils {

  public static final String JOB_CREDENTIALS_BINARY
      = SliderXmlConfKeys.MAPREDUCE_JOB_CREDENTIALS_BINARY;

  private CredentialUtils() {
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(CredentialUtils.class);

  /**
   * Save credentials to a byte buffer. Returns null if there were no
   * credentials to save
   * @param credentials credential set
   * @return a byte buffer of serialized tokens
   * @throws IOException if the credentials could not be written to the stream
   */
  public static ByteBuffer marshallCredentials(Credentials credentials) throws IOException {
    ByteBuffer buffer = null;
    if (!credentials.getAllTokens().isEmpty()) {
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      dob.close();
      buffer = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }
    return buffer;
  }

  /**
   * Load the credentials from the environment. This looks at
   * the value of {@link UserGroupInformation#HADOOP_TOKEN_FILE_LOCATION}
   * and attempts to read in the value
   * @param env environment to resolve the variable from
   * @param conf configuration use when reading the tokens
   * @return a set of credentials, or null if the environment did not
   * specify any
   * @throws IOException if a location for credentials was defined, but
   * the credentials could not be loaded.
   */
  public static Credentials loadFromEnvironment(Map<String, String> env,
      Configuration conf)
      throws IOException {
    String tokenFilename = env.get(HADOOP_TOKEN_FILE_LOCATION);
    String source = HADOOP_TOKEN_FILE_LOCATION;
    if (tokenFilename == null) {
      tokenFilename = conf.get(JOB_CREDENTIALS_BINARY);
      source = "Configuration option " + JOB_CREDENTIALS_BINARY;
    }
    if (tokenFilename != null) {
      // use delegation tokens, i.e. from Oozie
      File file = new File(tokenFilename.trim());
      String details = String.format("Token File %s from environment variable %s",
          file,
          source);
      LOG.debug("Using {}", details);
      if (!file.exists()) {
        throw new FileNotFoundException("No " + details);
      }
      if (!file.isFile() && !file.canRead()) {
        throw new IOException("Cannot read " + details);
      }
      Credentials creds = Credentials.readTokenStorageFile(file, conf);
      return creds;
    } else {
      return null;
    }
  }

  /**
   * Look up and return the resource manager's principal. This method
   * automatically does the <code>_HOST</code> replacement in the principal and
   * correctly handles HA resource manager configurations.
   *
   * From: YARN-4629
   * @param conf the {@link Configuration} file from which to read the
   * principal
   * @return the resource manager's principal string
   * @throws IOException thrown if there's an error replacing the host name
   */
  public static String getRMPrincipal(Configuration conf) throws IOException {
    String principal = conf.get(RM_PRINCIPAL, "");
    String hostname;
    Preconditions.checkState(!principal.isEmpty(), "Not set: " + RM_PRINCIPAL);

    if (HAUtil.isHAEnabled(conf)) {
      YarnConfiguration yarnConf = new YarnConfiguration(conf);
      if (yarnConf.get(RM_HA_ID) == null) {
        // If RM_HA_ID is not configured, use the first of RM_HA_IDS.
        // Any valid RM HA ID should work.
        String[] rmIds = yarnConf.getStrings(RM_HA_IDS);
        Preconditions.checkState((rmIds != null) && (rmIds.length > 0),
            "Not set " + RM_HA_IDS);
        yarnConf.set(RM_HA_ID, rmIds[0]);
      }

      hostname = yarnConf.getSocketAddr(
          RM_ADDRESS,
          DEFAULT_RM_ADDRESS,
          DEFAULT_RM_PORT).getHostName();
    } else {
      hostname = conf.getSocketAddr(
          RM_ADDRESS,
          DEFAULT_RM_ADDRESS,
          DEFAULT_RM_PORT).getHostName();
    }
    return SecurityUtil.getServerPrincipal(principal, hostname);
  }

  /**
   * Create and add any filesystem delegation tokens with
   * the RM(s) configured to be able to renew them. Returns null
   * on an insecure cluster (i.e. harmless)
   * @param conf configuration
   * @param fs filesystem
   * @param credentials credentials to update
   * @return a list of all added tokens.
   * @throws IOException
   */
  public static Token<?>[] addRMRenewableFSDelegationTokens(Configuration conf,
      FileSystem fs,
      Credentials credentials) throws IOException {
    Preconditions.checkArgument(conf != null);
    Preconditions.checkArgument(credentials != null);
    if (UserGroupInformation.isSecurityEnabled()) {
      String tokenRenewer = CredentialUtils.getRMPrincipal(conf);
      return fs.addDelegationTokens(tokenRenewer, credentials);
    }
    return null;
  }

  /**
   * Add an FS delegation token which can be renewed by the current user
   * @param fs filesystem
   * @param credentials credentials to update
   * @throws IOException problems.
   */
  public static void addSelfRenewableFSDelegationTokens(
      FileSystem fs,
      Credentials credentials) throws IOException {
    Preconditions.checkArgument(fs != null);
    Preconditions.checkArgument(credentials != null);
    fs.addDelegationTokens(
        UserGroupInformation.getLoginUser().getShortUserName(),
        credentials);
  }

  /**
   * Filter a list of tokens from a set of credentials
   * @param credentials credential source (a new credential set os re
   * @param filter List of tokens to strip out
   * @return a new, filtered, set of credentials
   */
  public static Credentials filterTokens(Credentials credentials,
      List<Text> filter) {
    Credentials result = new Credentials(credentials);
    Iterator<Token<? extends TokenIdentifier>> iter =
        result.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<? extends TokenIdentifier> token = iter.next();
      LOG.debug("Token {}", token.getKind());
      if (filter.contains(token.getKind())) {
        LOG.debug("Filtering token {}", token.getKind());
        iter.remove();
      }
    }
    return result;
  }

  public static String dumpTokens(Credentials credentials, String separator) {
    Collection<Token<? extends TokenIdentifier>> allTokens
        = credentials.getAllTokens();
    StringBuilder buffer = new StringBuilder(allTokens.size()* 128);
    DateFormat df = DateFormat.getDateTimeInstance(
        DateFormat.SHORT, DateFormat.SHORT);
    for (Token<? extends TokenIdentifier> token : allTokens) {
      buffer.append(toString(token)).append(separator);
    }
    return buffer.toString();
  }

  public static String toString(Token<? extends TokenIdentifier> token) {
    DateFormat df = DateFormat.getDateTimeInstance(
        DateFormat.SHORT, DateFormat.SHORT);
    StringBuilder buffer = new StringBuilder(128);
    buffer.append(token.toString());
    try {
      TokenIdentifier ti = token.decodeIdentifier();
      buffer.append("; ").append(ti);
      if (ti instanceof AbstractDelegationTokenIdentifier) {
        AbstractDelegationTokenIdentifier dt
            = (AbstractDelegationTokenIdentifier) ti;
        buffer.append(" Issued: ")
            .append(df.format(new Date(dt.getIssueDate())));
        buffer.append(" Max Date: ")
            .append(df.format(new Date(dt.getMaxDate())));
      }
    } catch (IOException e) {
      LOG.debug("Failed to decode {}: {}", token, e, e);
    }
    return buffer.toString();
  }
}
