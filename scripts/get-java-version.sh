#!/bin/bash
#
# Copyright (C) 2022-2025 Confluent, Inc.
#


# Script to extract Java version from Maven POM
# Usage: ./scripts/get-java-version.sh

# Extract java.major.version from pom.xml
JAVA_VERSION=$(grep -o '<java.major.version>[^<]*</java.major.version>' pom.xml | sed 's/<java.major.version>//g' | sed 's/<\/java.major.version>//g')

# Fallback to maven.compiler.source if java.major.version is not found
if [ -z "$JAVA_VERSION" ]; then
    JAVA_VERSION=$(grep -o '<maven.compiler.source>[^<]*</maven.compiler.source>' pom.xml | sed 's/<maven.compiler.source>//g' | sed 's/<\/maven.compiler.source>//g')
fi

if [ -z "$JAVA_VERSION" ]; then
    echo "Error: Could not find java.major.version or maven.compiler.source in pom.xml"
    exit 1
fi

echo "$JAVA_VERSION"
