package io.confluent.bazel.swagger;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

import io.swagger.v3.core.filter.OpenAPISpecFilter;
import io.swagger.v3.core.filter.SpecFilter;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.core.util.Yaml;
import io.swagger.v3.jaxrs2.integration.JaxrsOpenApiContextBuilder;
import io.swagger.v3.oas.integration.GenericOpenApiContextBuilder;
import io.swagger.v3.oas.integration.OpenApiConfigurationException;
import io.swagger.v3.oas.integration.SwaggerConfiguration;
import io.swagger.v3.oas.integration.api.OpenApiContext;
import io.swagger.v3.oas.models.OpenAPI;
import org.codehaus.plexus.util.FileUtils;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiFunction;

import static java.lang.String.format;

public class BazelSwaggerCoreMain {

    public enum Format {JSON, YAML, JSONANDYAML}

    private static final Logger LOG = LoggerFactory.getLogger(BazelSwaggerCoreMain.class);


    public static void main(String[] args) {
        BazelSwaggerCoreMain main = new BazelSwaggerCoreMain();
        main.setProperties();

        if (args.length > 0) {
            String[] split = args[0].split("/");
            String fileName = split[split.length - 1];

            // output dir is everything before the file name
            String outputDir = args[0].substring(0, args[0].lastIndexOf("/"));

            System.out.println("full path: " + args[0]);
            System.out.println("fileName: " + fileName);
            System.out.println("outputDir: " + outputDir);

            main.outputFileName = fileName;
            main.outputPath = outputDir;
        }
        main.execute();
    }

    public void setProperties() {
        // todo configurable location
        String propertiesFile = "swagger.properties";
        if (propertiesFile != null) {
            try {
                Properties props = new Properties();
                InputStream is = getClass().getClassLoader().getResourceAsStream(propertiesFile);
                props.load(is);
                for (String key : props.stringPropertyNames()) {
                    try {
                        Field field = BazelSwaggerCoreMain.class.getDeclaredField(key);
                        field.setAccessible(true);
                        String property = props.getProperty(key);
                        getLog().info("Setting field: " + key + " to " + property);

                        if (field.getType().equals(Set.class)) {
                            String[] values = property.split(",");
                            Set<String> valueSet = new LinkedHashSet<String>();
                            Collections.addAll(valueSet, values);
                            field.set(this, valueSet);
                        } else if (field.getType().equals(Boolean.class)) {
                            field.set(this, Boolean.parseBoolean(property));
                        } else if (field.getType().equals(Format.class)) {
                            field.set(this, Format.valueOf(property));
                        } else {
                            field.set(this, property);
                        }
                    } catch (NoSuchFieldException e) {
                        // no-op
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("failed to load properties file", e);
            }
        }
    }

    public Logger getLog() {
        return LOG;
    }

    public void execute() {
        if (skip) {
            getLog().info("Skipping OpenAPI specification resolution");
            return;
        }

        if (StringUtils.isBlank(encoding)) {
            encoding = projectEncoding;
        }

        // read swagger configuration if one was provided
        Optional<SwaggerConfiguration> swaggerConfiguration =
                readStructuredDataFromFile(configurationFilePath, SwaggerConfiguration.class, "configurationFilePath");

        // read openApi config, if one was provided
        Optional<OpenAPI> openAPIInput =
                readStructuredDataFromFile(openapiFilePath, OpenAPI.class, "openapiFilePath");

        config = mergeConfig(openAPIInput.orElse(null), swaggerConfiguration.orElse(new SwaggerConfiguration()));

        setDefaultsIfMissing(config);

        try {
            GenericOpenApiContextBuilder builder = new JaxrsOpenApiContextBuilder()
                    .openApiConfiguration(config);
            if (StringUtils.isNotBlank(contextId)) {
                builder.ctxId(contextId);
            }
            OpenApiContext context = builder.buildContext(true);
            OpenAPI openAPI = context.read();

            if (StringUtils.isNotBlank(config.getFilterClass())) {
                try {
                    OpenAPISpecFilter filterImpl = (OpenAPISpecFilter) this.getClass().getClassLoader().loadClass(config.getFilterClass()).newInstance();
                    SpecFilter f = new SpecFilter();
                    openAPI = f.filter(openAPI, filterImpl, new HashMap<>(), new HashMap<>(),
                            new HashMap<>());
                } catch (Exception e) {
                    getLog().error("Error applying filter to API specification", e);
                    throw new RuntimeException("Error applying filter to API specification: " + e.getMessage(), e);
                }
            }

            System.out.println("Output format: " + outputFormat);
//            System.out.println("openApi: " + openAPI);

            String openapiJson = null;
            String openapiYaml = null;
            if (Format.JSON.equals(outputFormat) || Format.JSONANDYAML.equals(outputFormat)) {
                if (config.isPrettyPrint() != null && config.isPrettyPrint()) {
                    openapiJson = context.getOutputJsonMapper().writer(new DefaultPrettyPrinter()).writeValueAsString(openAPI);
                } else {
                    openapiJson = context.getOutputJsonMapper().writeValueAsString(openAPI);
                }
            }
            if (Format.YAML.equals(outputFormat) || Format.JSONANDYAML.equals(outputFormat)) {
                if (config.isPrettyPrint() != null && config.isPrettyPrint()) {
                    openapiYaml = context.getOutputYamlMapper().writer(new DefaultPrettyPrinter()).writeValueAsString(openAPI);
                } else {
                    openapiYaml = context.getOutputYamlMapper().writeValueAsString(openAPI);
                }
            }
            Path path = Paths.get(outputPath);
            final File parentFile = path.toFile().getParentFile();
            if (parentFile != null) {
                parentFile.mkdirs();
            }

            if (openapiJson != null) {
                path = Paths.get(outputPath, outputFileName + ".json");
                Files.write(path, openapiJson.getBytes(Charset.forName(encoding)));
                getLog().info( "JSON output: " + path.toFile().getCanonicalPath());
            }
            if (openapiYaml != null) {
//                path = Paths.get(outputPath, outputFileName + ".yaml");
                path = Paths.get(outputPath, outputFileName); // todo
                System.out.println("Writing to: " + path.toAbsolutePath());
                Files.write(path, openapiYaml.getBytes(Charset.forName(encoding)));
//                getLog().info( "YAML output: " + path.toFile().getCanonicalPath());
            }

        } catch (OpenApiConfigurationException e) {
//            getLog().error( "Error resolving API specification" , e);
//            throw new MojoFailureException(e.getMessage(), e);
        } catch (IOException e) {
//            getLog().error( "Error writing API specification" , e);
//            throw new MojoExecutionException("Failed to write API definition", e);
        } catch (Exception e) {
//            getLog().error( "Error resolving API specification" , e);
//            throw new MojoExecutionException(e.getMessage(), e);
        }
    }

    private void setDefaultsIfMissing(SwaggerConfiguration config) {

        if (prettyPrint == null) {
            prettyPrint = Boolean.FALSE;
        }
        if (readAllResources == null) {
            readAllResources = Boolean.TRUE;
        }
        if (sortOutput == null) {
            sortOutput = Boolean.FALSE;
        }
        if (alwaysResolveAppPath == null) {
            alwaysResolveAppPath = Boolean.FALSE;
        }
        if (openapi31 == null) {
            openapi31 = Boolean.FALSE;
        }
        if (config.isPrettyPrint() == null) {
            config.prettyPrint(prettyPrint);
        }
        if (config.isReadAllResources() == null) {
            config.readAllResources(readAllResources);
        }
        if (config.isSortOutput() == null) {
            config.sortOutput(sortOutput);
        }
        if (config.isAlwaysResolveAppPath() == null) {
            config.alwaysResolveAppPath(alwaysResolveAppPath);
        }
    }

    /**
     * Read the content of given file as either json or yaml and maps it to given class
     *
     * @param filePath    to read content from
     * @param outputClass to map to
     * @param configName  for logging, what user config will be read
     * @param <T>         mapped type
     * @return empty optional if not path was given or the file was empty, read instance otherwis
     */
    private <T> Optional<T> readStructuredDataFromFile(String filePath, Class<T> outputClass, String configName) {
        try {
            // ignore if config is not provided
            if (StringUtils.isBlank(filePath)) {
                return Optional.empty();
            }

            Path pathObj = Paths.get(filePath);
            // read from resources
            InputStream resource = getClass().getClassLoader().getResourceAsStream(filePath);
            String fileContent = null;
            if (resource != null) {
                fileContent = new String(resource.readAllBytes(), StandardCharsets.UTF_8);
                System.out.println("filePath: " + filePath + " fileContent: " + fileContent);

            }

//            // if file does not exist or is not an actual file, finish with error
//            if (!pathObj.toFile().exists() || !pathObj.toFile().isFile()) {
//                throw new IllegalArgumentException(
//                        format("passed path does not exist or is not a file: '%s'", filePath));
//            }
//
//            String fileContent = new String(Files.readAllBytes(pathObj), encoding);

            System.out.println("filePath: " + filePath + " fileContent: " + fileContent);

            // if provided file is empty, log warning and finish
            if (StringUtils.isBlank(fileContent)) {
//                getLog().warn(format("It seems that file '%s' defined in config %s is empty",
//                        pathObj.toString(), configName));
                return Optional.empty();
            }

            // get mappers in the order based on file extension
            List<BiFunction<String, Class<T>, T>> mappers = getSortedMappers(pathObj);

            T instance = null;
            List<Throwable> caughtExs = new ArrayList<>();

            // iterate through mappers and see if one is able to parse
            for (BiFunction<String, Class<T>, T> mapper : mappers) {
                try {
                    instance = mapper.apply(fileContent, outputClass);
                    break;
                } catch (Exception e) {
                    caughtExs.add(e);
                }
            }

            // if no mapper could read the content correctly, finish with error
            if (instance == null) {
                if (caughtExs.isEmpty()) {
                    caughtExs.add(new IllegalStateException("undefined state"));
                }


                // we give more importance to the first exception, it was produced by the preferred mapper
                Throwable caughtEx = caughtExs.get(0);
//                getLog().error(format("Could not read file '%s' for config %s", pathObj, configName), caughtEx);

                if(caughtExs.size() > 1){
                    for (Throwable ex : caughtExs.subList(1, caughtExs.size())) {
//                        getLog().warn(format("Also could not read file '%s' for config %s with alternate mapper", pathObj, configName), ex);
                    }
                }

                throw new IllegalStateException(caughtEx.getMessage(), caughtEx);
            }

            return Optional.of(instance);
        } catch (Exception e) {
//            getLog().error(format("Error reading/deserializing config %s file", configName), e);
//            throw new MojoFailureException(e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Get sorted list of mappers based on given filename.
     * <p>
     * Will sort the 2 supported mappers: json and yaml based on what file extension is used.
     *
     * @param pathObj to get extension from.
     * @param <T>     mapped type
     * @return list of mappers
     */
    private <T> List<BiFunction<String, Class<T>, T>> getSortedMappers(Path pathObj) {
        String ext = FileUtils.extension(pathObj.toString());
        boolean yamlPreferred = false;
        if (ext.equalsIgnoreCase("yaml") || ext.equalsIgnoreCase("yml")) {
            yamlPreferred = true;
        }

        List<BiFunction<String, Class<T>, T>> list = new ArrayList<>(2);

        list.add((content, typeClass) -> {
            try {
                return Json.mapper().readValue(content, typeClass);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        list.add((content, typeClass) -> {
            try {
                return Yaml.mapper().readValue(content, typeClass);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });

        if (yamlPreferred) {
            Collections.reverse(list);
        }

        return Collections.unmodifiableList(list);
    }

    private SwaggerConfiguration mergeConfig(OpenAPI openAPIInput, SwaggerConfiguration config) {
        // todo
        // overwrite all settings provided by other maven config
        if (StringUtils.isNotBlank(filterClass)) {
            config.filterClass(filterClass);
        }
        if (isCollectionNotBlank(ignoredRoutes)) {
            config.ignoredRoutes(ignoredRoutes);
        }
        if (prettyPrint != null) {
            config.prettyPrint(prettyPrint);
        }
        if (sortOutput != null) {
            config.sortOutput(sortOutput);
        }
        if (alwaysResolveAppPath != null) {
            config.alwaysResolveAppPath(alwaysResolveAppPath);
        }
        if (readAllResources != null) {
            config.readAllResources(readAllResources);
        }
        if (StringUtils.isNotBlank(readerClass)) {
            config.readerClass(readerClass);
        }
        if (StringUtils.isNotBlank(scannerClass)) {
            config.scannerClass(scannerClass);
        }
        if (isCollectionNotBlank(resourceClasses)) {
            config.resourceClasses(resourceClasses);
        }
        if (openAPIInput != null) {
            config.openAPI(openAPIInput);
        }
        if (isCollectionNotBlank(resourcePackages)) {
            config.resourcePackages(resourcePackages);
        }
        if (StringUtils.isNotBlank(objectMapperProcessorClass)) {
            config.objectMapperProcessorClass(objectMapperProcessorClass);
        }
        if (isCollectionNotBlank(modelConverterClasses)) {
            config.modelConverterClasses(modelConverterClasses);
        }

        return config;
    }

    private boolean isCollectionNotBlank(Collection<?> collection) {
        return collection != null && !collection.isEmpty();
    }

    private String outputFileName = "openapi";
    private String outputPath;
    private Format outputFormat = Format.JSON;

    private Set<String> resourcePackages;
    private Set<String> resourceClasses;
    private LinkedHashSet<String> modelConverterClasses;
    private String filterClass;
    private String readerClass;
    private String scannerClass;
    private String objectMapperProcessorClass;
    private Boolean prettyPrint;
    private Boolean readAllResources;
    private Collection<String> ignoredRoutes;
    private String contextId;
    private Boolean skip = Boolean.FALSE;

    private String openapiFilePath;

    private String configurationFilePath;

    private String encoding;
    private Boolean sortOutput;

    private Boolean alwaysResolveAppPath;

    private Boolean openapi31;


    private String projectEncoding = "UTF-8";
    private SwaggerConfiguration config;

    public String getOutputPath() {
        return outputPath;
    }

    public String getOpenapiFilePath() {
        return openapiFilePath;
    }

    String getConfigurationFilePath() {
        return configurationFilePath;
    }

    void setContextId(String contextId) {
        this.contextId = contextId;
    }

    SwaggerConfiguration getInternalConfiguration() {
        return config;
    }
}