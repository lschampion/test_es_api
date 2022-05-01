## 测试ES7的 JAVA api

参考文章：

简单使用docker搭建ES环境

https://blog.csdn.net/chenweifu365/article/details/124097897

kerberos认证下各种方式连接elasticsearch研究与方案

https://www.cnblogs.com/zhouwenyang/p/14477427.html



JAVA api 参考：

https://www.cnblogs.com/shine-rainbow/p/15500048.html

https://blog.csdn.net/Imflash/article/details/101147730

https://blog.csdn.net/weixin_45321681/article/details/115585128



推荐使用 elasticuve 和 elasticsearch-head 两个浏览器插件 

报错：Fielddata is disabled on text fields by default. Set fielddata=true

参考：https://blog.csdn.net/caizhengwu/article/details/79743499

```json
PUT  /user/_mapping
{
  "properties": {
    "age": {
      "type": "text",
      "fielddata": true
    }
  }
}
```



另外，kerberos 认证连接ES工具类：

```java
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.common.settings.SecureString;
import org.ietf.jgss.*;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class implements {@link HttpClientConfigCallback} which allows for
 * customization of {@link HttpAsyncClientBuilder}.
 * <p>
 * Based on the configuration, configures {@link HttpAsyncClientBuilder} to
 * support spengo auth scheme.<br>
 * It uses configured credentials either password or keytab for authentication.
 */
public class SpnegoHttpClientConfigCallbackHandler implements HttpClientConfigCallback {
    private static final String SUN_KRB5_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";
    private static final String CRED_CONF_NAME = "ESClientLoginConf";
    private static final Oid SPNEGO_OID = getSpnegoOid();

    private static Oid getSpnegoOid() {
        Oid oid = null;
        try {
            oid = new Oid("1.3.6.1.5.5.2");
        } catch (GSSException gsse) {
            throw ExceptionsHelper.convertToRuntime(gsse);
        }
        return oid;
    }

    private final String userPrincipalName;
    private final SecureString password;
    private final String keytabPath;
    private final boolean enableDebugLogs;
    private LoginContext loginContext;

    /**
     * Constructs {@link SpnegoHttpClientConfigCallbackHandler} with given
     * principalName and password.
     *
     * @param userPrincipalName user principal name
     * @param password password for user
     * @param enableDebugLogs if {@code true} enables kerberos debug logs
     */
    public SpnegoHttpClientConfigCallbackHandler(final String userPrincipalName, final SecureString password,
                                                 final boolean enableDebugLogs) {
        this.userPrincipalName = userPrincipalName;
        this.password = password;
        this.keytabPath = null;
        this.enableDebugLogs = enableDebugLogs;
    }

    /**
     * Constructs {@link SpnegoHttpClientConfigCallbackHandler} with given
     * principalName and keytab.
     *
     * @param userPrincipalName User principal name
     * @param keytabPath path to keytab file for user
     * @param enableDebugLogs if {@code true} enables kerberos debug logs
     */
    public SpnegoHttpClientConfigCallbackHandler(final String userPrincipalName, final String keytabPath, final boolean enableDebugLogs) {
        this.userPrincipalName = userPrincipalName;
        this.keytabPath = keytabPath;
        this.password = null;
        this.enableDebugLogs = enableDebugLogs;
    }

    @Override
    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
        setupSpnegoAuthSchemeSupport(httpClientBuilder);
        return httpClientBuilder;
    }

    private void setupSpnegoAuthSchemeSupport(HttpAsyncClientBuilder httpClientBuilder) {
        final Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create()
                .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory()).build();

        final GSSManager gssManager = GSSManager.getInstance();
        try {
            final GSSName gssUserPrincipalName = gssManager.createName(userPrincipalName, GSSName.NT_USER_NAME);
            login();
            final AccessControlContext acc = AccessController.getContext();
            final GSSCredential credential = doAsPrivilegedWrapper(loginContext.getSubject(),
                    (PrivilegedExceptionAction<GSSCredential>) () -> gssManager.createCredential(gssUserPrincipalName,
                            GSSCredential.DEFAULT_LIFETIME, SPNEGO_OID, GSSCredential.INITIATE_ONLY),
                    acc);

            final KerberosCredentialsProvider credentialsProvider = new KerberosCredentialsProvider();
            credentialsProvider.setCredentials(
                    new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthSchemes.SPNEGO),
                    new KerberosCredentials(credential));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        } catch (GSSException e) {
            throw new RuntimeException(e);
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e.getCause());
        }
        httpClientBuilder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
    }

    /**
     * If logged in {@link LoginContext} is not available, it attempts login and
     * returns {@link LoginContext}
     *
     * @return {@link LoginContext}
     * @throws PrivilegedActionException
     */
    public synchronized LoginContext login() throws PrivilegedActionException {
        if (this.loginContext == null) {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                final Subject subject = new Subject(false, Collections.singleton(new KerberosPrincipal(userPrincipalName)),
                        Collections.emptySet(), Collections.emptySet());
                Configuration conf = null;
                final CallbackHandler callback;
                if (password != null) {
                    conf = new PasswordJaasConf(userPrincipalName, enableDebugLogs);
                    callback = new KrbCallbackHandler(userPrincipalName, password);
                } else {
                    conf = new KeytabJaasConf(userPrincipalName, keytabPath, enableDebugLogs);
                    callback = null;
                }
                loginContext = new LoginContext(CRED_CONF_NAME, subject, callback, conf);
                loginContext.login();
                return null;
            });
        }
        return loginContext;
    }

    /**
     * Privileged Wrapper that invokes action with Subject.doAs to perform work as
     * given subject.
     *
     * @param subject {@link Subject} to be used for this work
     * @param action {@link PrivilegedExceptionAction} action for performing inside
     *            Subject.doAs
     * @param acc the {@link AccessControlContext} to be tied to the specified
     *            subject and action see
     *            {@link Subject#doAsPrivileged(Subject, PrivilegedExceptionAction, AccessControlContext)
     * @return the value returned by the PrivilegedExceptionAction's run method
     * @throws PrivilegedActionException
     */
    static <T> T doAsPrivilegedWrapper(final Subject subject, final PrivilegedExceptionAction<T> action, final AccessControlContext acc)
            throws PrivilegedActionException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> Subject.doAsPrivileged(subject, action, acc));
        } catch (PrivilegedActionException pae) {
            if (pae.getCause() instanceof PrivilegedActionException) {
                throw (PrivilegedActionException) pae.getCause();
            }
            throw pae;
        }
    }

    /**
     * This class matches {@link AuthScope} and based on that returns
     * {@link Credentials}. Only supports {@link AuthSchemes#SPNEGO} in
     * {@link AuthScope#getScheme()}
     */
    private static class KerberosCredentialsProvider implements CredentialsProvider {
        private AuthScope authScope;
        private Credentials credentials;

        @Override
        public void setCredentials(AuthScope authscope, Credentials credentials) {
            if (authscope.getScheme().regionMatches(true, 0, AuthSchemes.SPNEGO, 0, AuthSchemes.SPNEGO.length()) == false) {
                throw new IllegalArgumentException("Only " + AuthSchemes.SPNEGO + " auth scheme is supported in AuthScope");
            }
            this.authScope = authscope;
            this.credentials = credentials;
        }

        @Override
        public Credentials getCredentials(AuthScope authscope) {
            assert this.authScope != null && authscope != null;
            return authscope.match(this.authScope) > -1 ? this.credentials : null;
        }

        @Override
        public void clear() {
            this.authScope = null;
            this.credentials = null;
        }
    }

    /**
     * Jaas call back handler to provide credentials.
     */
    private static class KrbCallbackHandler implements CallbackHandler {
        private final String principal;
        private final SecureString password;

        KrbCallbackHandler(final String principal, final SecureString password) {
            this.principal = principal;
            this.password = password;
        }

        public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof PasswordCallback) {
                    PasswordCallback pc = (PasswordCallback) callback;
                    if (pc.getPrompt().contains(principal)) {
                        pc.setPassword(password.getChars());
                        break;
                    }
                }
            }
        }
    }

    /**
     * Usually we would have a JAAS configuration file for login configuration.
     * Instead of an additional file setting as we do not want the options to be
     * customizable we are constructing it in memory.
     * <p>
     * As we are using this instead of jaas.conf, this requires refresh of
     * {@link Configuration} and reqires appropriate security permissions to do so.
     */
    private static class PasswordJaasConf extends AbstractJaasConf {

        PasswordJaasConf(final String userPrincipalName, final boolean enableDebugLogs) {
            super(userPrincipalName, enableDebugLogs);
        }

        public void addOptions(final Map<String, String> options) {
            options.put("useTicketCache", Boolean.FALSE.toString());
            options.put("useKeyTab", Boolean.FALSE.toString());
        }
    }

    /**
     * Usually we would have a JAAS configuration file for login configuration. As
     * we have static configuration except debug flag, we are constructing in
     * memory. This avoids additional configuration required from the user.
     * <p>
     * As we are using this instead of jaas.conf, this requires refresh of
     * {@link Configuration} and requires appropriate security permissions to do so.
     */
    private static class KeytabJaasConf extends AbstractJaasConf {
        private final String keytabFilePath;

        KeytabJaasConf(final String userPrincipalName, final String keytabFilePath, final boolean enableDebugLogs) {
            super(userPrincipalName, enableDebugLogs);
            this.keytabFilePath = keytabFilePath;
        }

        public void addOptions(final Map<String, String> options) {
            options.put("useKeyTab", Boolean.TRUE.toString());
            options.put("keyTab", keytabFilePath);
            options.put("doNotPrompt", Boolean.TRUE.toString());
        }

    }

    private abstract static class AbstractJaasConf extends Configuration {
        private final String userPrincipalName;
        private final boolean enableDebugLogs;

        AbstractJaasConf(final String userPrincipalName, final boolean enableDebugLogs) {
            this.userPrincipalName = userPrincipalName;
            this.enableDebugLogs = enableDebugLogs;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(final String name) {
            final Map<String, String> options = new HashMap<>();
            options.put("principal", userPrincipalName);
            options.put("isInitiator", Boolean.TRUE.toString());
            options.put("storeKey", Boolean.TRUE.toString());
            options.put("debug", Boolean.toString(enableDebugLogs));
            addOptions(options);
            return new AppConfigurationEntry[] { new AppConfigurationEntry(SUN_KRB5_LOGIN_MODULE,
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, Collections.unmodifiableMap(options)) };
        }

        abstract void addOptions(Map<String, String> options);
    }
}
```



ES客户端配置

```java
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.SecureString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

/**
 *
 */
@Configuration
@Slf4j
public class ElasticSearchConfiguration {

    @Value("${elasticsearch.hosts}")
    String clusterHosts;
    @Value("${elasticsearch.port}")
    String elasticHttpPort;
    @Value("${elasticsearch.user}")
    String user;
    @Value("${elasticsearch.password}")
    String password;

    public HttpHost[] buildHosts() {
        HttpHost[] hosts = new HttpHost[0];
        if (clusterHosts != null) {
            String[] split = clusterHosts.split(",");
            hosts = new HttpHost[split.length];
            for (int i = 0; i < split.length; i++) {
                hosts[i] = new HttpHost(split[i], StringUtils.isEmpty(elasticHttpPort) ? 9200 : Integer.parseInt(elasticHttpPort.trim()));
            }
        }
        log.info("init es http {}", Arrays.toString(hosts));
        return hosts;
    }

    @Bean
    RestHighLevelClient getRestHighLevelClient() {
        RestClientBuilder restClientBuilder = RestClient.builder(buildHosts());
        restClientBuilder.setRequestConfigCallback((requestConfigBuilder) -> {
            requestConfigBuilder.setConnectTimeout(10000);
            requestConfigBuilder.setSocketTimeout(40000);
            requestConfigBuilder.setConnectionRequestTimeout(10000);
            return requestConfigBuilder;
        });

        if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
            restClientBuilder.setHttpClientConfigCallback(new SpnegoHttpClientConfigCallbackHandler(user, new SecureString(password), true));
            log.info("built es rest high level client with kerberos password successful");
        } else {
            restClientBuilder.setHttpClientConfigCallback((HttpAsyncClientBuilder httpClientBuilder) -> httpClientBuilder.setDefaultIOReactorConfig(
                    IOReactorConfig.custom()
                            .setIoThreadCount(60)//线程数配置
                            .setConnectTimeout(10000)
                            .setSoTimeout(10000)
                            .build())
            );
        }
        return new RestHighLevelClient(restClientBuilder);
    }
}
```

