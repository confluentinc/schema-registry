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
    assertEquals("EOF Exception encountered - client disconnected during stream processing.",
            out.getMessage());
  }
}
