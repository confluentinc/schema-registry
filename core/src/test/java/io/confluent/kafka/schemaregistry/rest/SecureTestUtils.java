/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rest;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.test.TestSslUtils;

import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SecureTestUtils {

  public static Properties clientSslConfigsWithKeyStore(
      int numberOfCerts,
      File trustStoreFile,
      Password trustPassword,
      List<X509Certificate> clientCerts,
      List<KeyPair> keyPairs
  ) throws GeneralSecurityException, IOException {

    Map<String, X509Certificate> certificateMap = new HashMap<>();

    File clientKSFile = File.createTempFile("CKeystore", ".jks");
    String keyStorePassword = new Password("Client-KS-Password").value();

    for (int i = 0; i < numberOfCerts; i++) {
      KeyPair kp = TestSslUtils.generateKeyPair("RSA");
      X509Certificate cert = TestSslUtils.generateCertificate(
          "CN=localhost, O=Client" + i, kp, 30, "SHA1withRSA");

      clientCerts.add(cert);
      keyPairs.add(kp);
      certificateMap.put("client-" + i, cert);
    }

    createKeyStore(clientKSFile, keyStorePassword, clientCerts, keyPairs);

    TestSslUtils.createTrustStore(trustStoreFile.toString(), trustPassword, certificateMap);

    Properties sslConfigs =
        getClientSslConfigs(trustStoreFile, trustPassword.value(), clientKSFile, keyStorePassword);

    return sslConfigs;
  }

  public static void createKeyStore(
      File keyStoreFile,
      String keyStorePassword,
      List<X509Certificate> clientCerts,
      List<KeyPair> keyPairs
  ) throws CertificateException, NoSuchAlgorithmException, IOException, KeyStoreException {
    KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, null);

    for (int i = 0; i < clientCerts.size(); i++) {
      keyStore.setKeyEntry(
          "client-" + i,
          keyPairs.get(i).getPrivate(),
          keyStorePassword.toCharArray(),
          new Certificate[]{clientCerts.get(i)}
      );
    }

    FileOutputStream out = new FileOutputStream(keyStoreFile);
    keyStore.store(out, keyStorePassword.toCharArray());
    out.close();

  }

  private static Properties getClientSslConfigs(
      File trustStoreFile, String trustPassword,
      File clientKSFile, String keyStorePassword
  ) {
    //makes sure client also runs TLSv1.2
    System.setProperty("https.protocols", "TLSv1.2");
    Properties sslConfigs = new Properties();
    sslConfigs.put("ssl.protocol", "TLSv1.2");
    sslConfigs.put("ssl.keystore.location", clientKSFile.getPath());
    sslConfigs.put("ssl.keystore.type", "JKS");
    sslConfigs.put("ssl.keymanager.algorithm", TrustManagerFactory.getDefaultAlgorithm());
    sslConfigs.put("ssl.keystore.password", keyStorePassword);
    sslConfigs.put("ssl.key.password", keyStorePassword);
    sslConfigs.put("ssl.truststore.location", trustStoreFile.getPath());
    sslConfigs.put("ssl.truststore.password", trustPassword);
    sslConfigs.put("ssl.truststore.type", "JKS");
    sslConfigs.put("ssl.trustmanager.algorithm", TrustManagerFactory.getDefaultAlgorithm());
    sslConfigs.put("ssl.enabled.protocols", "TLSv1.2");
    sslConfigs.put("security.protocol", "SSL");
    return sslConfigs;
  }
}
