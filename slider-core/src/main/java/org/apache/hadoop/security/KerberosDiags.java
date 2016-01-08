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

package org.apache.hadoop.security;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.apache.hadoop.security.UserGroupInformation.*;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;

/**
 * Kerberos diagnostics
 * At some point this may move to hadoop core, so please keep use of slider
 * methods and classes to ~0.
 *
 * This operation expands some of the diagnostic output of the security code,
 * but not all. For completeness
 *
 * Set the environment variable {@code HADOOP_JAAS_DEBUG=true}
 * Set the log level for {@code org.apache.hadoop.security=DEBUG}
 */
public class KerberosDiags implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KerberosDiags.class);
  public static final String KRB5_CCNAME = "KRB5CCNAME";
  public static final String JAVA_SECURITY_KRB5_CONF
    = "java.security.krb5.conf";
  public static final String JAVA_SECURITY_KRB5_REALM
    = "java.security.krb5.realm";
  public static final String SUN_SECURITY_KRB5_DEBUG
    = "sun.security.krb5.debug";
  public static final String SUN_SECURITY_SPNEGO_DEBUG
    = "sun.security.spnego.debug";
  public static final String KERBEROS_KINIT_COMMAND
    = "hadoop.kerberos.kinit.command";

  private final Configuration conf;
  private final List<String> services;
  private final PrintWriter out;
  private final File keytab;
  private final String principal;

  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  public KerberosDiags(Configuration conf,
    PrintWriter out,
    List<String> services,
    File keytab, String principal) {
    this.conf = conf;
    this.services = services;
    this.keytab = keytab;
    this.principal = principal;
    this.out = out;
  }

  @Override
  public void close() throws IOException {
    flush();
  }

  /**
   * Execute diagnostics.
   * <p>
   * Things it would be nice if UGI made accessible
   * <ol>
   *   <li>A way to enable JAAS debug programatically</li>
   *   <li>Access to the TGT</li>
   * </ol>
   * @return true if security was enabled and all probes were successful
   * @throws KerberosDiagsFailure explicitly raised failure
   * @throws Exception other security problems
   */
  @SuppressWarnings("deprecation")
  public boolean execute() throws Exception {
    title("Kerberos Diagnostics scan at %s",
      new Date(System.currentTimeMillis()));
    int aesLen = Cipher.getMaxAllowedKeyLength("AES");
    println("Maximum AES encryption key length %d", aesLen);
    failif (aesLen < 256,
      "Java Cryptography Extensions are not installed on this JVM."
        +" Kerberos will not work.");
    boolean securityDisabled = SecurityUtil.getAuthenticationMethod(conf)
      .equals(UserGroupInformation.AuthenticationMethod.SIMPLE);
    if (securityDisabled) {
      println("security disabled");
      return false;
    }
    title("System Properties");
    for (String prop : new String[]{
      JAVA_SECURITY_KRB5_CONF,
      JAVA_SECURITY_KRB5_REALM,
      SUN_SECURITY_KRB5_DEBUG,
      SUN_SECURITY_SPNEGO_DEBUG,
    }) {
      printSysprop(prop);
    }

    title("Environment Variables");
    for (String env : new String[]{
      "HADOOP_JAAS_DEBUG",
      KRB5_CCNAME,
      HADOOP_USER_NAME,
      HADOOP_PROXY_USER,
      HADOOP_TOKEN_FILE_LOCATION,
    }) {
      printEnv(env);
    }
    for (String prop : new String[]{
      KERBEROS_KINIT_COMMAND,
      HADOOP_SECURITY_AUTHENTICATION,
      HADOOP_SECURITY_AUTHORIZATION,
      "hadoop.kerberos.min.seconds.before.relogin",    // not in 2.6
      "hadoop.security.dns.interface",   // not in 2.6
      "hadoop.security.dns.nameserver",  // not in 2.6
      HADOOP_RPC_PROTECTION,
      HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS,
      HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX,
      HADOOP_SECURITY_GROUP_MAPPING,
      "hadoop.security.impersonation.provider.class",    // not in 2.6
      "dfs.data.transfer.protection" // HDFS

    }) {
      printConfOpt(prop);
    }
    validateKrb5File();
    validateSasl(HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS);
    validateSasl("dfs.data.transfer.saslproperties.resolver.class");
    validateKinit();

    boolean krb5Debug = getAndSet(SUN_SECURITY_KRB5_DEBUG);
    boolean spnegoDebug = getAndSet(SUN_SECURITY_SPNEGO_DEBUG);

    try {
      title("Logging in");
      UserGroupInformation loginUser = getLoginUser();
      dumpUser("Log in user", loginUser);
      println("Ticket based login: %b", isLoginTicketBased());
      println("Keytab based login: %b", isLoginKeytabBased());
      validateUser("Login user", loginUser);
      loginFromKeytab();

      return true;
    } finally {
      // restore original system properties
      System.setProperty(SUN_SECURITY_KRB5_DEBUG,
        Boolean.toString(krb5Debug));
      System.setProperty(SUN_SECURITY_SPNEGO_DEBUG,
        Boolean.toString(spnegoDebug));
    }
  }

  /**
   * Locate the krb5.conf file and dump it. No-op on windows
   * @throws IOException
   */
  private void validateKrb5File() throws IOException {
    if (!Shell.WINDOWS) {
      title("Locating Kerberos configuration file");
      String krbPath = "/etc/krb5.conf";
      String jvmKrbPath = System.getProperty(JAVA_SECURITY_KRB5_CONF);
      if (jvmKrbPath != null) {
        println("Setting kerberos path from sysprop %s: %s",
          JAVA_SECURITY_KRB5_CONF, jvmKrbPath);
        krbPath = jvmKrbPath;
      }

      String krb5name = System.getenv(KRB5_CCNAME);
      if (krb5name != null) {
        println("Setting kerberos path from environment variable %s: %s",
          KRB5_CCNAME, krb5name);
        krbPath = krb5name;
        if (jvmKrbPath != null) {
          println("Warning - both %s and %s were set - %s takes priority",
            JAVA_SECURITY_KRB5_CONF, KRB5_CCNAME, KRB5_CCNAME);
        }
      }

      File krbFile = new File(krbPath);
      println("Kerberos configuration file = %s", krbFile);
      failif(!krbFile.exists(),
        "Kerberos configuration file %s not found", krbFile);
      dump(krbFile);
    }
  }

  private void loginFromKeytab() throws IOException {
    UserGroupInformation ugi;
    String identity;
    if (keytab != null) {
      File kt = keytab.getCanonicalFile();
      println("Using keytab %s principal %s", kt, principal);
      identity = principal;
      failif(!kt.exists(), "Keytab not found: %s", kt);
      failif(!kt.isFile(), "Keytab is not a valid file: %s", kt);
      failif(StringUtils.isEmpty(principal), "No principal defined");
      ugi = loginUserFromKeytabAndReturnUGI(principal, kt.getPath());
      dumpUser(identity, ugi);
      validateUser(principal, ugi);

      title("Attempting to log in from keytab again");
      // package scoped -hence the reason why this class must be in the
      // hadoop.security package
      setShouldRenewImmediatelyForTests(true);
      // attempt a new login
      ugi.reloginFromKeytab();
    } else {
      println("No keytab: logging is as current user");
    }
  }

  private void dumpUser(String message, UserGroupInformation ugi)
    throws IOException {
    title(message);
    println("UGI=%s", ugi);
    println("Has kerberos credentials: %b", ugi.hasKerberosCredentials());
    println("Authentication method: %s", ugi.getAuthenticationMethod());
    println("Real Authentication method: %s",
      ugi.getRealAuthenticationMethod());
    title("Group names");
    for (String name : ugi.getGroupNames()) {
      println(name);
    }
    title("Credentials");
    Credentials credentials = ugi.getCredentials();
    List<Text> secretKeys = credentials.getAllSecretKeys();
    title("Secret keys");
    if (!secretKeys.isEmpty()) {
      for (Text secret: secretKeys) {
        println("%s", secret);
      }
    } else {
      println("(none)");
    }

    dumpTokens(ugi);
  }

  private void validateUser(String message, UserGroupInformation user) {
    failif(!user.hasKerberosCredentials(),
      "%s: No kerberos credentials for  %s", message, user);
    failif(user.getAuthenticationMethod() == null,
      "%s: Null AuthenticationMethod for %s", message, user);
  }

  private void validateKinit() {
    String kinit = conf.getTrimmed(KERBEROS_KINIT_COMMAND, "");
    if (!kinit.isEmpty()) {
      File kinitPath = new File(kinit);
      println("%s = %s", KERBEROS_KINIT_COMMAND, kinitPath);
      if (kinitPath.isAbsolute()) {
        failif(!kinitPath.exists(), "%s executable does not exist: %s",
          KERBEROS_KINIT_COMMAND, kinitPath);
        failif(!kinitPath.isFile(), "%s path does not refer to a file: %s",
          KERBEROS_KINIT_COMMAND, kinitPath);
      } else {
        println("Executable %s is relative -must be on the PATH", kinit);
        printEnv("PATH");
      }
    }
  }

  private void validateSasl(String saslPropsResolverKey) {
    title("Resolving SASL property %s", saslPropsResolverKey);
    String saslPropsResolver = conf.getTrimmed(saslPropsResolverKey);
    try {
      Class<? extends SaslPropertiesResolver> resolverClass = conf.getClass(
        saslPropsResolverKey,
        SaslPropertiesResolver.class, SaslPropertiesResolver.class);
      println("Resolver is %s", resolverClass.toString());
    } catch (RuntimeException e) {
      throw new KerberosDiagsFailure(e, "Failed to load %s class %s",
        saslPropsResolverKey, saslPropsResolver);
    }
  }

  public void dumpTokens(UserGroupInformation user) {
    Collection<Token<? extends TokenIdentifier>> tokens
      = user.getCredentials().getAllTokens();
    title("Token Count: %d", tokens.size());
    for (Token<? extends TokenIdentifier> token : tokens) {
      println("Token %s", token.getKind());
    }
  }

  /**
   * Set the System property to true; return the old value for caching
   * @param sysprop property
   * @return the previous value
   */
  private boolean getAndSet(String sysprop) {
    boolean old = Boolean.getBoolean(sysprop);
    System.setProperty(sysprop, "true");
    return old;
  }

  /**
   * Flush all active output channels, including {@Code System.err},
   * so as to stay in sync with any JRE log messages.
   */
  private void flush() {
    if (out != null) {
      out.flush();
    } else {
      System.out.flush();
    }
    System.err.flush();
  }

  private void println(String format, Object... args) {
    String msg = String.format(format, args);
    if (out != null) {
      out.println(msg);
    } else {
      LOG.info(msg);
    }
    flush();
  }

  private void title(String format, Object... args) {
    println("");
    println("");
    String msg = "== " + String.format(format, args) + " ==";
    println(msg);
    println("");
  }

  private void printSysprop(String key) {
    println("%s = \"%s\"", key, System.getProperty(key, "(unset)"));
  }

  private void printConfOpt(String key) {
    println("%s = \"%s\"", key, conf.get(key, "(unset)"));
  }


  private void printEnv(String key) {
    String env = System.getenv(key);
    println("%s = \"%s\"", key, env != null ? env : "(unset)");
  }

  private void dump(File file) throws IOException {
    try (FileInputStream in = new FileInputStream(file)) {
      for (String line : IOUtils.readLines(in)) {
        println(line);
      }
    }
    println("");
  }

  /**
   * Format and raise a failure
   * @param message string formatting message
   * @param args any arguments for the formatting
   * @throws KerberosDiagsFailure containing the formatted text
   */
  private void fail(String message, Object... args)
    throws KerberosDiagsFailure {
    throw new KerberosDiagsFailure(message, args);
  }

  /**
   * Conditional failure with string formatted arguments
   * @param condition failure condition
   * @param message string formatting message
   * @param args any arguments for the formatting
   * @throws KerberosDiagsFailure containing the formatted text
   *         if the condition was met
   */
  private void failif(boolean condition, String message, Object... args)
    throws KerberosDiagsFailure {
    if (condition) {
      fail(message, args);
    }
  }

  /**
   * Diagnostics failures return the exit code 41, "unauthorized"
   */
  public static class KerberosDiagsFailure extends ExitUtil.ExitException {
    public KerberosDiagsFailure( String message) {
      super(41, message);
    }

    public KerberosDiagsFailure(String message, Object... args) {
      this(String.format(message, args));
    }

    public KerberosDiagsFailure(Throwable throwable,
      String message, Object... args) {
      this(message, args);
      initCause(throwable);
    }
  }
}
