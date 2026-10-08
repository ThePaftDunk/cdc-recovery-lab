#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
jar="$PWD/target/scala-2.13/cdc-recovery-lab.jar"
test -f "$jar" || { echo 'Run ./scripts/sbtw assembly first' >&2; exit 1; }
scratch="$(mktemp -d "${TMPDIR:-/tmp}/cdc-reconcile-cli-XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT
export JDBC_URL="jdbc:h2:file:$scratch/sink;MODE=PostgreSQL"
export DB_USER=sa DB_PASSWORD=
java -jar "$jar" file fixtures/changes.jsonl > "$scratch/apply.log"
java -jar "$jar" reconcile fixtures/expected-customers.json > "$scratch/match.json"
python3 - "$scratch" <<'PY'
import json, sys
from pathlib import Path
folder=Path(sys.argv[1])
match=json.loads((folder/'match.json').read_text())
assert match['consistent'] and match['expectedRows']==1 and match['actualRows']==1
(folder/'different.json').write_text(json.dumps([{'id':'customer-a','tier':'platinum'}]))
PY
set +e
java -jar "$jar" reconcile "$scratch/different.json" > "$scratch/drift.json"
status=$?
set -e
test "$status" -eq 2 || { echo "Expected drift exit code 2; received $status" >&2; exit 1; }
python3 - "$scratch/drift.json" <<'PY'
import json,sys
report=json.load(open(sys.argv[1]))
assert not report['consistent']
assert report['changed']['count']==1 and report['changed']['samples']==[{'id':'customer-a','fields':['tier']}]
assert report['missing']['count']==0 and report['unexpected']['count']==0
print('PASS: packaged reconciliation CLI returns 0 for a match and 2 for drift')
PY
