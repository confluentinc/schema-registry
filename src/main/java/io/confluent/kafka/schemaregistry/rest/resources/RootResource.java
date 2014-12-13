package io.confluent.kafka.schemaregistry.rest.resources;

import io.confluent.kafka.schemaregistry.rest.Versions;

import javax.validation.Valid;
import javax.ws.rs.*;
import java.util.HashMap;
import java.util.Map;

@Path("/")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED, Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
    Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON, Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
    Versions.JSON, Versions.GENERIC_REQUEST})
public class RootResource {

    @GET
    public Map<String,String> get() {
        // Currently this just provides an endpoint that's a nop and can be used to check for
        // liveness and can be used for tests that need to test the server setup rather than the
        // functionality of a specific resource. Some APIs provide a listing of endpoints as their
        // root resource; it might be nice to provide that.
        return new HashMap<String,String>();
    }

    @POST
    public Map<String,String> post(@Valid Map<String,String> request) {
        // This version allows testing with posted entities
        return new HashMap<String,String>();
    }

}
