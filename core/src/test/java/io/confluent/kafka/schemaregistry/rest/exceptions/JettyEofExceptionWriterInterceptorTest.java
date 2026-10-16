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

import org.junit.Test;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.WriterInterceptorContext;
import org.eclipse.jetty.io.EofException;
import java.io.IOException;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class JettyEofExceptionWriterInterceptorTest {
  @Test
  public void testAroundWriteToNoExceptionThrown() throws IOException, WebApplicationException {
    WriterInterceptorContext context = mock(WriterInterceptorContext.class);
    JettyEofExceptionWriterInterceptor interceptor = new JettyEofExceptionWriterInterceptor();

    try {
      doThrow(EofException.class).when(context).proceed();
      interceptor.aroundWriteTo(context);
    } catch (Exception e) {
      fail("Exception should not be thrown");
    }
  }
}
