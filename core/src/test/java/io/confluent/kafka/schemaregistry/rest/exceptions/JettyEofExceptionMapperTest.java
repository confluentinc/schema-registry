package io.confluent.kafka.schemaregistry.rest.exceptions;

import io.confluent.rest.entities.ErrorMessage;
import org.eclipse.jetty.io.EofException;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;

public class JettyEofExceptionMapperTest {

    private JettyEofExceptionMapper jettyEofExceptionMapper;

    @Before
    public void setUp() {
        jettyEofExceptionMapper = new JettyEofExceptionMapper();
    }

    @Test
    public void testToResponse() {
        EofException jettyEofException = new EofException("early EOF");

        Response response = jettyEofExceptionMapper.toResponse(jettyEofException);
        assertEquals(400, response.getStatus());
        ErrorMessage out = (ErrorMessage)response.getEntity();
        assertEquals(400, out.getErrorCode());
        assertEquals("EOF Exception encountered - client disconnected during stream processing.", out.getMessage());
    }
}