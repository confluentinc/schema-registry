/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.client.ssl;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.security.Provider;
import java.security.Security;
import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Strings;
import org.bouncycastle.jsse.BCSSLSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a wrapper class on top of {@link SSLSocketFactory} to address
 * issue where host name is not set on {@link BCSSLSocket} when creating a socket.
 */
public class HostSslSocketFactory extends SSLSocketFactory {

  private static final Logger log = LoggerFactory.getLogger(HostSslSocketFactory.class);

  private final SSLSocketFactory sslSocketFactory;
  private final String peerHost;

  private static final String BC_FIPS_SSL_PROVIDER = "BCJSSE";
  private static final String BC_FIPS_SECURITY_PROVIDER = "BCFIPS";

  public HostSslSocketFactory(SSLSocketFactory sslSocketFactory, String peerHost) {
    this.sslSocketFactory = sslSocketFactory;
    this.peerHost = peerHost;
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return sslSocketFactory.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return sslSocketFactory.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket(Socket socket, String host, int port, boolean autoClose)
          throws IOException {
    return interceptAndSetHost(sslSocketFactory.createSocket(socket, host, port, autoClose));
  }

  @Override
  public Socket createSocket() throws IOException {
    Socket socket = sslSocketFactory.createSocket();
    return interceptAndSetHost(socket);
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    Socket socket = sslSocketFactory.createSocket(host, port);
    return interceptAndSetHost(socket);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localAddress, int localPort)
          throws IOException {
    Socket socket = sslSocketFactory.createSocket(host, port, localAddress, localPort);
    return interceptAndSetHost(socket);
  }

  @Override
  public Socket createSocket(InetAddress address, int port) throws IOException {
    Socket socket = sslSocketFactory.createSocket(address, port);
    return interceptAndSetHost(socket);
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress remoteAddress,
                             int remotePort) throws IOException {
    Socket socket = sslSocketFactory.createSocket(address, port, remoteAddress, remotePort);
    return interceptAndSetHost(socket);
  }

  @Override
  public Socket createSocket(Socket socket, InputStream inputStream, boolean autoClose)
          throws IOException {
    return interceptAndSetHost(sslSocketFactory.createSocket(socket, inputStream, autoClose));
  }

  private Socket interceptAndSetHost(Socket socket) {
    if (peerHost != null && isFipsDeployment() && socket instanceof BCSSLSocket) {
      BCSSLSocket bcsslSocket = (BCSSLSocket) socket;
      if (!Strings.isNullOrEmpty(peerHost)) {
        log.debug("Setting hostname on Bouncy Castle SSL socket: {}", peerHost);
        bcsslSocket.setHost(peerHost);
      }
    }
    return socket;
  }

  public static boolean isFipsDeployment() {
    Provider bcFipsProvider = Security.getProvider(BC_FIPS_SECURITY_PROVIDER);
    Provider bcFipsJsseProvider = Security.getProvider(BC_FIPS_SSL_PROVIDER);
    return (bcFipsProvider != null) && (bcFipsJsseProvider != null);
  }

}