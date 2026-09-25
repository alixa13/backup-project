#!/usr/bin/env bash
# deploy.sh build [--with-tests] [--jars-only]: the two job JARs, compiled in
# the pinned Maven/JDK 21 image (no Java on the host), and the Zeek image.

# Maven in a container, as the invoking user so target/ stays theirs; the local
# repository is cached in deploy/.m2 between builds.
maven() {
  docker run --rm --user "$(id -u):$(id -g)" \
    -v "${REPO_ROOT}:/src" -w /src \
    -v "${DEPLOY_DIR}/.m2:/var/maven/.m2" -e MAVEN_CONFIG=/var/maven/.m2 \
    "$(setting MAVEN_IMAGE)" mvn -B -q -Duser.home=/var/maven "$@"
}

# Run a network-heavy step up to ATTEMPTS times. Downloads get cut off (a real
# server's build lost a 52 MB Maven Central transfer after 2.7 MB); Maven and
# Docker keep everything already fetched, so a retry only fetches what failed.
# usage: with_retries ATTEMPTS DESCRIPTION COMMAND [ARGS...]
with_retries() {
  local attempts="$1" what="$2" attempt=1
  shift 2
  until "$@"; do
    if [ "$attempt" -ge "$attempts" ]; then
      warn "${what} failed ${attempts} times; if it is a download that keeps breaking, check this host's connection to the internet"
      return 1
    fi
    attempt=$((attempt + 1))
    warn "${what} failed; retrying (attempt ${attempt} of ${attempts}) in ${NETSEC_RETRY_PAUSE:-10}s"
    sleep "${NETSEC_RETRY_PAUSE:-10}"
  done
}

build_run() {
  local with_tests=0 jars_only=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --with-tests) with_tests=1; shift ;;
      --jars-only) jars_only=1; shift ;;
      *) die "build: unknown option $1" ;;
    esac
  done
  mkdir -p "${DEPLOY_DIR}/.m2" "${DEPLOY_DIR}/jars"

  # Unit suites only: the modules whose tests need no containers (CLAUDE.md,
  # Verification state). The container-backed suites never run here.
  if [ "$with_tests" -eq 1 ]; then
    log "running the unit test suites (domain, application, adapter-kafka, adapter-flink)"
    maven -pl modules/domain,modules/application,modules/adapter-kafka,modules/adapter-flink -am verify
  fi

  # The shaded JARs, copied to the names the supervisor submits.
  log "building the job JARs"
  with_retries 3 "building the job JARs" \
    maven -pl modules/bootstrap-online-job,modules/bootstrap-archive-job -am -DskipTests package
  cp "${REPO_ROOT}"/modules/bootstrap-online-job/target/bootstrap-online-job-*-all.jar "${DEPLOY_DIR}/jars/online-feature-job.jar"
  cp "${REPO_ROOT}"/modules/bootstrap-archive-job/target/bootstrap-archive-job-*-all.jar "${DEPLOY_DIR}/jars/archive-job.jar"
  log "jars: $(cd "${DEPLOY_DIR}/jars" && printf '%s ' *.jar)"

  # The sensor image (compiles two plugins: ~7 minutes the first time).
  if [ "$jars_only" -eq 0 ]; then
    log "building the Zeek sensor image $(setting ZEEK_IMAGE)"
    with_retries 3 "building the Zeek image" \
      docker build --build-arg "ZEEK_BASE_IMAGE=$(setting ZEEK_BASE_IMAGE)" -t "$(setting ZEEK_IMAGE)" "${DEPLOY_DIR}/zeek"
  fi
  log "build complete"
}
