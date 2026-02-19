/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.eventfeed.web.rest.exceptionmappers;

import io.cloudevents.rw.CloudEventRWException;
import io.confluent.eventfeed.web.rest.exceptions.Errors;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class CloudEventDeserializationExceptionMapper
        implements ExceptionMapper<CloudEventRWException> {

  @Override
  public Response toResponse(CloudEventRWException exception) {
    // Extract the root cause for a better error message
    Throwable cause = exception.getCause();
    String message = cause != null ? cause.getMessage() : exception.getMessage();

    return Response
            .status(Response.Status.BAD_REQUEST)  // 400
            .entity(new io.confluent.rest.entities.ErrorMessage(
                    Errors.INVALID_CLOUD_EVENT_ERROR_CODE,
                    String.format("cloud event: " + message)
            ))
            .build();
  }
}
