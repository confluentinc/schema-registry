package io.confluent.kafka.schemaregistry.rest.filters;


import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import java.io.IOException;

@PreMatching
@Priority(Priorities.ENTITY_CODER)
public class PLReqFilter implements ContainerRequestFilter {
    public PLReqFilter() {}
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        System.out.println(requestContext.getUriInfo().getRequestUri().getPort());
        System.out.println(requestContext.getUriInfo().getRequestUri().getHost());
    }
}
