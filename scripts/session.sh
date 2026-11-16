# Sourced by the entry scripts before any container inspection or mutation.
LAB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ "${1:-}" != --lab-lock-held ]; then
    exec "$LAB_ROOT/.venv/bin/python" "$LAB_ROOT/lab_session.py" "$0" "$@"
fi
shift
"$LAB_ROOT/.venv/bin/python" "$LAB_ROOT/lab_session.py" --check

lab_stop_owned() {
    local name="$1"
    if ! docker inspect "$name" >/dev/null 2>&1; then
        local matches
        # An unavailable daemon is a cleanup failure, not proof the container is gone.
        if ! matches=$(docker container ls --all --quiet --filter "name=^/${name}$"); then return 1; fi
        if [ -z "$matches" ]; then return 0; fi
        echo "Cannot inspect lab container: $name" >&2
        return 1
    fi
    if [ "$(docker inspect -f '{{ index .Config.Labels "queue-reliability-lab" }}' "$name")" != true ]; then
        echo "Refusing to stop container without the lab ownership label: $name" >&2
        return 1
    fi
    docker stop "$name"
}

lab_cleanup() {
    local status=$? cleanup_failed=0
    trap - EXIT
    # Attempt both resources even when one stop fails; preserve the primary failure.
    if [ "${LAB_EMULATOR_STARTED:-0}" = 1 ]; then
        if ! lab_stop_owned queue-reliability-emulator; then cleanup_failed=1; fi
    fi
    if [ "${LAB_KAFKA_STARTED:-0}" = 1 ]; then
        if ! lab_stop_owned queue-reliability-kafka; then cleanup_failed=1; fi
    fi
    if [ "$cleanup_failed" = 1 ]; then
        echo 'Lab cleanup failed; inspect the dedicated lab containers.' >&2
        if [ "$status" = 0 ]; then status=1; fi
    fi
    exit "$status"
}
