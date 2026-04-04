#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-fast}"
BASE_REF="${2:-origin/main}"

run_maven() {
  echo ">> mvn $*"
  mvn "$@"
}

to_class_name() {
  local path="$1"
  path="${path#src/test/java/}"
  path="${path%.java}"
  echo "${path//\//.}"
}

changed_files() {
  local range="${BASE_REF}...HEAD"
  if ! git diff --name-only --diff-filter=ACMR "${range}" >/dev/null 2>&1; then
    echo "Cannot diff against ${BASE_REF}, fallback to HEAD~1...HEAD" >&2
    git diff --name-only --diff-filter=ACMR HEAD~1...HEAD
    return
  fi
  git diff --name-only --diff-filter=ACMR "${range}"
}

case "${MODE}" in
  fast)
    run_maven test
    ;;
  balanced)
    run_maven test -Pbalanced-tests
    ;;
  integration)
    run_maven test -Pintegration-tests
    ;;
  full)
    run_maven test -Pfull-tests
    ;;
  changed)
    mapfile -t files < <(changed_files | sed '/^[[:space:]]*$/d')
    if [ "${#files[@]}" -eq 0 ]; then
      echo "No changed files, skip tests."
      exit 0
    fi

    test_files=()
    main_files=()
    for f in "${files[@]}"; do
      if [[ "$f" == src/test/java/*.java ]]; then
        test_files+=("$f")
      elif [[ "$f" == src/main/java/*.java ]]; then
        main_files+=("$f")
      fi
    done

    if [ "${#test_files[@]}" -gt 0 ]; then
      classes=()
      for f in "${test_files[@]}"; do
        classes+=("$(to_class_name "$f")")
      done
      mapfile -t uniq_classes < <(printf '%s\n' "${classes[@]}" | sort -u)
      test_arg="$(IFS=,; echo "${uniq_classes[*]}")"
      echo "Run changed tests only: ${test_arg}"
      run_maven test "-Dtest=${test_arg}"
      exit 0
    fi

    if [ "${#main_files[@]}" -gt 0 ]; then
      echo "Main source changed without direct test changes, fallback to balanced tests."
      run_maven test -Pbalanced-tests
      exit 0
    fi

    echo "Only non-Java files changed, skip tests."
    ;;
  *)
    echo "Unsupported mode: ${MODE}"
    echo "Usage: ./scripts/test.sh [fast|balanced|integration|full|changed] [base-ref]"
    exit 1
    ;;
esac
