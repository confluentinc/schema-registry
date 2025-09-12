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

import io.confluent.rest.entities.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.jetty.io.EofException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * This class is intended to catch Early EOF errors that occur when the client disconnects
 * while the server is reading from the input stream. We catch the org.ecplise.jetty.io.EofException
 * rather than the more generic java.io.EOFException to ensure that we're
 * only catching jetty server based errors where the client disconnects.
 */
@Provider
public class JettyEofExceptionMapper implements ExceptionMapper<EofException> {
  private static final String ERROR_MESSAGE =
            "EOF Exception encountered - client disconnected during stream processing.";
  private static final Logger LOGGER = LoggerFactory.getLogger(JettyEofExceptionMapper.class);

  @Override
  public Response toResponse(EofException exception) {
    LOGGER.error(ERROR_MESSAGE, exception);
    final ErrorMessage message = new ErrorMessage(Response.Status.BAD_REQUEST.getStatusCode(),
            ERROR_MESSAGE);
    return Response.status(Response.Status.BAD_REQUEST)
            .entity(message)
            .build();
  }
}
