@echo off
rem
rem Copyright 2018 Confluent Inc.
rem
rem Licensed under the Apache License, Version 2.0 (the "License");
rem you may not use this file except in compliance with the License.
rem You may obtain a copy of the License at
rem
rem http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.
rem


setlocal enabledelayedexpansion
pushd %~dp0..\..
set BASE_DIR=%CD%
popd

for %%i in (%BASE_DIR%/package-schema-registry/target/kafka-schema-registry-package-*-development) do (
	call :concat %%i/share/java/schema-registry/*
)

for %%i in (confluent-common, rest-utils, schema-registry) do (
	call :concat %BASE_DIR%/share/java/%%i/*
)

rem Log4j settings
IF ["%SCHEMA_REGISTRY_LOG4J_OPTS%"] EQU [""] (
	if exist %~dp0../../etc/schema-registry/log4j2.yaml (
		set SCHEMA_REGISTRY_LOG4J_OPTS=-Dlog4j2.configurationFile=%~dp0../../etc/schema-registry/log4j2.yaml
	) else (
		set SCHEMA_REGISTRY_LOG4J_OPTS=-Dlog4j2.configurationFile=%BASE_DIR%/config/log4j2.yaml
	)
)

rem JMX settings
IF ["%SCHEMA_REGISTRY_JMX_OPTS%"] EQU [""] (
	set SCHEMA_REGISTRY_JMX_OPTS=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false
)

rem JMX port to use
IF ["%JMX_PORT%"] NEQ [""] (
	set SCHEMA_REGISTRY_JMX_OPTS=%SCHEMA_REGISTRY_JMX_OPTS% -Dcom.sun.management.jmxremote.port=%JMX_PORT%
)

rem Which java to use
IF ["%JAVA_HOME%"] EQU [""] (
	set JAVA=java
) ELSE (
	set JAVA="%JAVA_HOME%/bin/java"
)

rem Memory options
IF ["%SCHEMA_REGISTRY_HEAP_OPTS%"] EQU [""] (
	set SCHEMA_REGISTRY_HEAP_OPTS=-Xmx512M
)

rem JVM performance options
IF ["%SCHEMA_REGISTRY_JVM_PERFORMANCE_OPTS%"] EQU [""] (
	set SCHEMA_REGISTRY_JVM_PERFORMANCE_OPTS=-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true
)

set COMMAND=%JAVA% %SCHEMA_REGISTRY_HEAP_OPTS% %SCHEMA_REGISTRY_JVM_PERFORMANCE_OPTS% %SCHEMA_REGISTRY_JMX_OPTS% %SCHEMA_REGISTRY_LOG4J_OPTS% -cp %CLASSPATH% %SCHEMA_REGISTRY_OPTS% %*
%COMMAND%

goto :eof
:concat
IF ["%CLASSPATH%"] EQU [""] (
  set CLASSPATH="%1"
) ELSE (
  set CLASSPATH=%CLASSPATH%;"%1"
)
