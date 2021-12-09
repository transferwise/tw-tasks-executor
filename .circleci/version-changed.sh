#!/bin/bash

 if [ $# -ne 1 ]
  then
      echo "You must provide the libraryName argument"
      exit 1;
  else
    libraryName=$1

    libraryVersion=$( grep '^version=.*$' ${libraryName}/gradle.properties | sed "s/^version=\(.*\)$/\1/")
    if [ "${libraryVersion}" == "" ]; then
      libraryVersion=$( grep '^version=.*$' gradle.properties | sed "s/^version=\(.*\)$/\1/")
    fi

    echo "Checking ${libraryName}:${libraryVersion} in artifactory."

    artifactPath="https://arti.tw.ee/artifactory/libs-release-local/com/transferwise/tasks/${libraryName}/${libraryVersion}/${libraryName}-${libraryVersion}.jar"
    echo "${artifactPath}"
    artifactStatus=$(curl -s -o /dev/null -I -w "%{http_code}" "${artifactPath}")

    if [ "${artifactStatus}" == "404" ]; then
      echo "${libraryName} version ${libraryVersion} does not exist, considering version as changed."
      exit 0
    else
      echo "${libraryName} version ${libraryVersion} exists, considering versions as not changed"
      exit 1
    fi
  fi