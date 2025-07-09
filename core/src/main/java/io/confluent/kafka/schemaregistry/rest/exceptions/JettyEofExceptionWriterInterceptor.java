/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.exceptions;



import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;
import java.io.IOException;
import org.eclipse.jetty.io.EofException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link WriterInterceptor} to swallow {@link org.eclipse.jetty.io.EofException} which
 * occurs when a client disconnects before the complete response could be sent.
 */
public class JettyEofExceptionWriterInterceptor implements WriterInterceptor {
  private static final Logger LOGGER = LoggerFactory
          .getLogger(JettyEofExceptionWriterInterceptor.class);

  @Override
  public void aroundWriteTo(WriterInterceptorContext context)
          throws IOException, WebApplicationException {
    try {
      context.proceed();
    } catch (EofException e) {
      LOGGER.error("Client disconnected while processing and sending response", e);
    }
  }
}
