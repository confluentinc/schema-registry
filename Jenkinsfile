#!/usr/bin/env groovy

dockerfile {
  dockerPullDeps = ['confluentinc/cp-base']
  dockerRepos = ['confluentinc/cp-schema-registry']
  nodeLabel = 'docker-oraclejdk7'
  slackChannel = 'clients-eng'
  upstreamProjects = ['confluentinc/rest-utils', 'confluentinc/confluent-docker-utils']
}
