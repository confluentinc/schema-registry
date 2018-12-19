@echo off
rem
rem Copyright 2018 Confluent Inc.
rem
rem Licensed under the Confluent Community License; you may not use this file
rem except in compliance with the License.  You may obtain a copy of the License at
rem
rem http://www.confluent.io/confluent-community-license
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
rem WARRANTIES OF ANY KIND, either express or implied.  See the License for the
rem specific language governing permissions and limitations under the License.
rem

wmic process where (commandline like "%%io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain%%" and not name="wmic.exe") delete
