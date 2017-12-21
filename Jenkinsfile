#!/usr/bin/env groovy

docker_oraclejdk8 {
  dockerPush = true
  dockerRegistry = '368821881613.dkr.ecr.us-west-2.amazonaws.com/'
  dockerRepos = ['confluentinc/cp-schema-registry']
  mvnPhase = 'package'
  nodeLabel = 'docker-oraclejdk7'
  slackChannel = 'clients-eng'
  upstreamProjects = ['confluentinc/rest-utils', 'confluentinc/confluent-docker-utils']
}
