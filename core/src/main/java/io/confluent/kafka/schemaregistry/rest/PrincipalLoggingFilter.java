/*
 * Copyright 2025 Confluent Inc.
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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.Principal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.confluent.kafka.schemaregistry.utils.PrincipalContext;

/**
* This class is a servlet filter that logs the user principal for each incoming request to 
* Schema Registry. It is a necessary step to allow for building resource associations
*/
public class PrincipalLoggingFilter implements Filter {

  private static final Logger log = LoggerFactory.getLogger(PrincipalLoggingFilter.class.getName());

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }
  
  @Override
  public void doFilter(ServletRequest request, ServletResponse servletResponse,
                       FilterChain filterChain) throws IOException, ServletException {
    HttpServletRequest req = (HttpServletRequest) request;
    Principal principal = req.getUserPrincipal();

    if (principal != null) {
      log.info("User Principal: {}", principal.getName());
      PrincipalContext.setPrincipal(principal.getName());
    } else {
      log.info("No User Principal found for the request.");
      PrincipalContext.clear();
    }

    try {
      filterChain.doFilter(request, servletResponse);
    } finally {
      PrincipalContext.clear(); // Clear the principal after the request is processed
    }
  }

  @Override
  public void destroy() {
  }
}
