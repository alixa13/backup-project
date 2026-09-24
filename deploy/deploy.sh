#!/usr/bin/env bash
# netsec-ml deployment tool: installs, sizes, builds, runs and checks the
# Modbus + S7comm pipeline on one server
# (docs/superpowers/specs/2026-09-24-server-deployment-design.md).
# Run ./deploy/deploy.sh help for the commands.
set -euo pipefail

# Every lib module, in dependency order.
LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib"
for module in common tune traces doctor install build stack selftest; do
  # shellcheck disable=SC1090
  . "${LIB}/${module}.sh"
done

usage() {
  cat <<'EOF'
Usage: ./deploy/deploy.sh <command> [options]

  doctor                        check this host (read-only)
  install [--interface IF]      install Docker/curl/jq if missing, create deploy/.env,
                                prepare the data folders, size the services
  tune [--dry-run] [--memory-budget SIZE] [--cpus N] [--force]
                                size every service from this host's CPU and RAM
  build [--with-tests] [--jars-only]
                                build the job JARs (in a container) and the Zeek image
  up                            start everything; the jobs resume their saved state
  down                          stop the jobs with a savepoint, then stop everything
  restart                       down + up (after 'build', this is an upgrade)
  status                        health, jobs, traffic, recent ClickHouse rows
  logs [SERVICE]                follow logs: kafka clickhouse flink-jobmanager
                                flink-taskmanager job-submitter zeek
  sql "QUERY"                   run a ClickHouse query
  selftest                      send one Modbus and one S7 pair through the pipeline
  zeek-check [--live N]         check Zeek's records against the platform's parsers
  uninstall [--purge]           remove containers and the Zeek image (--purge: all data)
EOF
}

main() {
  local command="${1:-help}"
  [ $# -gt 0 ] && shift
  case "$command" in
    doctor) doctor_run "$@" ;;
    install) install_run "$@" ;;
    tune) load_env; tune_run "$@" ;;
    build) build_run "$@" ;;
    up) stack_up ;;
    down) stack_down ;;
    restart) stack_down; stack_up ;;
    status) stack_status ;;
    logs) load_env; compose logs -f --tail 200 "$@" ;;
    sql) load_env; ch_query "${1:?sql needs a query}" ;;
    selftest) selftest_run ;;
    zeek-check) zeek_check_run "$@" ;;
    uninstall) uninstall_run "$@" ;;
    help|-h|--help) usage ;;
    *) usage; die "unknown command: ${command}" ;;
  esac
}

main "$@"
