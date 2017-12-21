#!/usr/bin/env groovy

docker_oraclejdk8 {
  dockerPullDeps = ['confluentinc/cp-base']
  dockerPush = true
  dockerRegistry = '368821881613.dkr.ecr.us-west-2.amazonaws.com/'
  dockerRepos = ['confluentinc/cp-schema-registry']
  dockerUpstreamTag = '4.0.x-latest'
  nodeLabel = 'docker-oraclejdk7'
  slackChannel = 'clients-eng'
  upstreamProjects = ['confluentinc/rest-utils', 'confluentinc/confluent-docker-utils']
}
