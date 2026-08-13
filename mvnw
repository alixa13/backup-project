#!/bin/sh
# ----------------------------------------------------------------------------
# Maven Wrapper startup script.
#
# This delegates to .mvn/wrapper/maven-wrapper.jar, downloading it first if it
# is not already present, using the URL pinned in
# .mvn/wrapper/maven-wrapper.properties. Requires network access on first run
# in a fresh environment (not available in the sandbox that generated this
# skeleton — verify by running this script on a machine with internet access).
# ----------------------------------------------------------------------------

set -e

BASEDIR=$(cd "$(dirname "$0")" && pwd)
WRAPPER_JAR="$BASEDIR/.mvn/wrapper/maven-wrapper.jar"
WRAPPER_PROPERTIES="$BASEDIR/.mvn/wrapper/maven-wrapper.properties"
WRAPPER_LAUNCHER=org.apache.maven.wrapper.MavenWrapperMain

if [ ! -f "$WRAPPER_JAR" ]; then
  WRAPPER_URL=$(grep '^wrapperUrl=' "$WRAPPER_PROPERTIES" | cut -d'=' -f2-)
  echo "Downloading Maven Wrapper from: $WRAPPER_URL"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o "$WRAPPER_JAR" "$WRAPPER_URL"
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "$WRAPPER_JAR" "$WRAPPER_URL"
  else
    echo "ERROR: neither curl nor wget is available to download the wrapper jar." >&2
    exit 1
  fi
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA_CMD="java"
else
  JAVA_CMD="$JAVA_HOME/bin/java"
fi

exec "$JAVA_CMD" \
  -classpath "$WRAPPER_JAR" \
  "-Dmaven.multiModuleProjectDirectory=$BASEDIR" \
  "$WRAPPER_LAUNCHER" "$@"
