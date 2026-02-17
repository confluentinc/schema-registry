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
