package io.confluent.kafka.schemaregistry.utils;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import javax.net.ssl.SSLException;

public class ExceptionUtils {

  public static boolean IsNetworkConnectionException(IOException e) {
    return e instanceof SocketException ||
        e instanceof SocketTimeoutException ||
        e instanceof SSLException ||
        e instanceof UnknownHostException;
  }
}
