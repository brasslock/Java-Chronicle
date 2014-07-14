#!/bin/bash

# Function to print a message and exit
function die {
    [ -n "$1" ] && echo "$1" >&2
    exit 1
}

script=$(basename $0)
cwd=$(cd $(dirname "$0") && pwd)

mvn clean install -DskipTests || die "Failed to build"

cd ${cwd}/chronicle && mvn deploy -DskipTests || die "Failed to deploy chronicle"
#cd ${cwd}/chronicle-sandbox && mvn deploy -DskipTests || die "Failed to deploy chronicle-sandbox"

