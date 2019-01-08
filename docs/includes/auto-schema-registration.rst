By default, client applications automatically register new schemas.
If they produce new messages to a new topic, then they will automatically try to register new schemas.
This is very convenient in development environments, but in production environments we recommend that client applications do not automatically register new schemas.
Register schemas outside of the client application to control when schemas are registered with |sr-long| and how they evolve.

Within the application, disable automatic schema registration by setting the configuration parameter `auto.register.schemas=false`, as shown in the examples below.

.. sourcecode:: java

   props.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

