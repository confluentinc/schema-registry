package io.confluent.kafka.schemaregistry;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.rest.RestConfig;
import io.confluent.rest.auth.PrincipalNameConverter;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import kafka.security.auth.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.test.TestSslUtils;
import org.eclipse.jetty.server.Server;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SSLClusterRequiringAuthorizationTestHarness extends ClusterTestHarness {

    public static class TestPrincipalConverter implements PrincipalNameConverter {

        private static final Pattern USER_PATTERN = Pattern.compile("OU=([^\\s,]*)");

        @Override
        public String convertPrincipalName(String dn) {
            Matcher matcher = USER_PATTERN.matcher(dn);
            if (matcher.find()) {
                return matcher.group(1);
            }
            return "ANONYMOUS";
        }

    }

    private static final Logger log = LoggerFactory.getLogger(SSLClusterTestHarness.class);

    private static final String SSL_PASSWORD = "test1234";
    private static final boolean DO_NOT_SETUP_REST_APP = false;

    private SchemaRegistryRestApplication schemaRegistryApp;
    private Server schemaRegistryServer;
    private File trustStore;
    private File clientKeystore;
    private File serverKeystore;
    private String clientUserName;

    public SSLClusterRequiringAuthorizationTestHarness(String clientUserName) {
        super(DEFAULT_NUM_BROKERS, DO_NOT_SETUP_REST_APP);
        this.clientUserName = clientUserName;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        setUpCerts();
        setUpSchemaRegistry();
    }

    @Override
    public void tearDown() throws Exception {
        schemaRegistryApp.stop();
        super.tearDown();
    }

    // returns the http response status code.
    public int makePostRequest(String url,
                               String body,
                               String clientKeystoreLocation,
                               String clientKeystorePassword,
                               String clientKeyPassword)
            throws Exception {
        log.debug("Making POST " + url);
        HttpPost request = new HttpPost(url);
        request.setHeader("Content-Type", "application/vnd.schemaregistry.v1+json");
        request.setEntity(new StringEntity(body));

        CloseableHttpClient httpclient;
        if (url.startsWith("http://")) {
            httpclient = HttpClients.createDefault();
        } else {
            // trust all self-signed certs.
            SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                    .loadTrustMaterial(new TrustSelfSignedStrategy());

            // add the client keystore if it's configured.
            if (clientKeystoreLocation != null) {
                sslContextBuilder.loadKeyMaterial(new File(clientKeystoreLocation),
                        clientKeystorePassword.toCharArray(),
                        clientKeyPassword.toCharArray());
            }
            SSLContext sslContext = sslContextBuilder.build();

            SSLConnectionSocketFactory sslSf = new SSLConnectionSocketFactory(sslContext, new String[]{"TLSv1"},
                    null, SSLConnectionSocketFactory.getDefaultHostnameVerifier());

            httpclient = HttpClients.custom()
                    .setSSLSocketFactory(sslSf)
                    .build();
        }

        int statusCode = -1;
        CloseableHttpResponse response = null;
        try {
            response = httpclient.execute(request);
            statusCode = response.getStatusLine().getStatusCode();
        } finally {
            if (response != null) {
                response.close();
            }
            httpclient.close();
        }
        return statusCode;
    }

    public void addWriteAcl(String user, String topic) {
        Set<Acl> aclSet = new HashSet<>();
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user);
        Acl acl = new Acl(principal, Allow$.MODULE$, "*", Write$.MODULE$);
        aclSet.add(acl);
        scala.collection.immutable.Set<Acl> aclSetScala = JavaConversions.asScalaSet(aclSet).toSet();
        SimpleAclAuthorizer authorizer = new SimpleAclAuthorizer();
        authorizer.configure((ImmutableMap.of("zookeeper.connect", zkConnect)));
        authorizer.addAcls(aclSetScala, new Resource(Topic$.MODULE$, topic));
    }

    public int updateSchema(String topic, String schema) throws Exception {
        String uri = "https://localhost:8080";
        return makePostRequest(uri + "/subjects/" + topic + "-value/versions", schema,
                clientKeystore.getAbsolutePath(), SSL_PASSWORD, SSL_PASSWORD);
    }

    private void setUpSchemaRegistry() throws Exception {
        Properties props = new Properties();
        String uri = "https://localhost:8080";
        props.put(RestConfig.LISTENERS_CONFIG, uri);
        configServerKeystore(props);
        configServerTruststore(props);
        enableSslClientAuth(props);
        props.setProperty(SchemaRegistryConfig.PORT_CONFIG, ((Integer) choosePort()).toString());
        props.setProperty(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
        props.setProperty(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://" + brokerList);
        props.put(SchemaRegistryConfig.AUTHORIZATION_PRINCIPAL_CONVERTER_CONFIG, SSLClusterRequiringAuthorizationTestHarness.TestPrincipalConverter.class);
        props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, KAFKASTORE_TOPIC);
        props.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, AvroCompatibilityLevel.NONE.name);
        props.put(SchemaRegistryConfig.MASTER_ELIGIBILITY, true);
        props.put(SchemaRegistryConfig.AUTHORIZATION_ENABLED_CONFIG, true);
        SchemaRegistryConfig config = new SchemaRegistryConfig(props);
        schemaRegistryApp = new SchemaRegistryRestApplication(config);
        schemaRegistryServer = schemaRegistryApp.createServer();
        schemaRegistryServer.start();
    }

    private void setUpCerts() throws Exception {
        try {
            trustStore = File.createTempFile("SslTest-truststore", ".jks");
            clientKeystore = File.createTempFile("SslTest-client-keystore", ".jks");
            serverKeystore = File.createTempFile("SslTest-server-keystore", ".jks");
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to create temporary files for trust stores and keystores.");
        }
        Map<String, X509Certificate> certs = new HashMap<>();
        createKeystoreWithCert(clientKeystore, "client", certs);
        createKeystoreWithCert(serverKeystore, "server", certs);
        TestSslUtils.createTrustStore(trustStore.getAbsolutePath(), new Password(SSL_PASSWORD), certs);
    }

    private void createKeystoreWithCert(File file, String alias, Map<String, X509Certificate> certs) throws Exception {
        KeyPair keypair = TestSslUtils.generateKeyPair("RSA");
        // IMPORTANT: CN must be "localhost" because Jetty expects the server CN to be the FQDN.
        X509Certificate cCert = TestSslUtils.generateCertificate("CN=localhost, O=some-organization, OU=" + clientUserName, keypair, 30, "SHA1withRSA");
        TestSslUtils.createKeyStore(file.getPath(), new Password(SSL_PASSWORD), alias, keypair.getPrivate(), cCert);
        certs.put(alias, cCert);
    }

    private void configServerKeystore(Properties props) {
        props.put(RestConfig.SSL_KEYSTORE_LOCATION_CONFIG, serverKeystore.getAbsolutePath());
        props.put(RestConfig.SSL_KEYSTORE_PASSWORD_CONFIG, SSL_PASSWORD);
        props.put(RestConfig.SSL_KEY_PASSWORD_CONFIG, SSL_PASSWORD);
    }

    private void configServerTruststore(Properties props) {
        props.put(RestConfig.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore.getAbsolutePath());
        props.put(RestConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG, SSL_PASSWORD);
    }

    private void enableSslClientAuth(Properties props) {
        props.put(RestConfig.SSL_CLIENT_AUTH_CONFIG, true);
    }
}
