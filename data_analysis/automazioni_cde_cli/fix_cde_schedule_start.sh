#!/usr/bin/env bash
set -uo pipefail
export TZ=UTC

# ==============================================================================
# fix_cde_schedule_start.sh
# AUTOMAZIONE DEFINITIVA CDE SCHEDULE START
#
# Sicurezze:
# - un job alla volta, mai in parallelo;
# - password gestita esclusivamente dalla CDE CLI;
# - pause -> polling pause -> clear -> update completo;
# - attese fisse ridotte al minimo: 1s dopo clear e 1s tra job;
# - update con cron + start + catchup=false + depends-on-past=false;
# - start/flag/paused verificati con cde job describe;
# - nextExecution verificata con cde job list;
# - per il bug noto CDE/Cloudera, nextExecution deve coincidere con PREV_1
#   (marcatore CDE), mentre COMPUTED_NEXT è la prossima run reale;
# - se nextExecution non coincide, pause di sicurezza;
# - se il piano temporale cambia durante il massivo, il job viene saltato;
# - nel massivo, job già paused vengono saltati per non riattivarli per errore.
# ==============================================================================

ONLY_JOB=""
POLL_SECONDS=2
POLL_TIMEOUT=90

# Attese fisse ridotte al minimo.
# CLEAR non espone uno stato direttamente verificabile, quindi manteniamo
# un buffer minimo; tra job manteniamo un solo secondo di separazione.
CLEAR_WAIT_SECONDS=1
BETWEEN_JOBS_SECONDS=1

# Stima iniziale indicativa. Durante l'esecuzione viene ricalcolata
# dinamicamente sulla media reale dei job già completati.
ESTIMATED_SECONDS_PER_JOB=35

is_uint() { [[ "$1" =~ ^[0-9]+$ ]]; }

format_duration() {
  local total="${1:-0}"
  (( total < 0 )) && total=0
  local h=$((total / 3600))
  local m=$(((total % 3600) / 60))
  local s=$((total % 60))

  if (( h > 0 )); then
    printf '%dh %02dm %02ds' "$h" "$m" "$s"
  elif (( m > 0 )); then
    printf '%dm %02ds' "$m" "$s"
  else
    printf '%ds' "$s"
  fi
}

show_job_progress() {
  local result="$1"
  local job="$2"
  local started="$3"
  local now elapsed_job elapsed_total attempted remaining avg eta

  now="$(date +%s)"
  elapsed_job=$((now - started))
  elapsed_total=$((now - EXECUTION_START_EPOCH))
  attempted=$((UPDATED + FAILED))
  remaining=$((PLAN_UPDATE - attempted))
  (( remaining < 0 )) && remaining=0

  if (( attempted > 0 )); then
    avg=$((elapsed_total / attempted))
  else
    avg=$ESTIMATED_SECONDS_PER_JOB
  fi

  eta=$((remaining * avg))

  echo
  echo "  ESITO: $result"
  echo "  Tempo job       : $(format_duration "$elapsed_job")"
  echo "  Avanzamento     : $JOB_INDEX/$PLAN_TOTAL"
  echo "  Tempo trascorso : $(format_duration "$elapsed_total")"
  echo "  Tempo residuo   : ~$(format_duration "$eta")"
  echo
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --job)
      [[ $# -ge 2 ]] || { echo "ERRORE: --job richiede un nome." >&2; exit 2; }
      ONLY_JOB="$2"; shift 2 ;;
    --clear-wait-seconds)
      [[ $# -ge 2 ]] || exit 2
      CLEAR_WAIT_SECONDS="$2"; shift 2 ;;
    --between-jobs-seconds)
      [[ $# -ge 2 ]] || exit 2
      BETWEEN_JOBS_SECONDS="$2"; shift 2 ;;
    --poll-seconds)
      [[ $# -ge 2 ]] || exit 2
      POLL_SECONDS="$2"; shift 2 ;;
    --poll-timeout)
      [[ $# -ge 2 ]] || exit 2
      POLL_TIMEOUT="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,70p' "$0"; exit 0 ;;
    *)
      echo "ERRORE: parametro non riconosciuto: $1" >&2; exit 2 ;;
  esac
done

for x in "$CLEAR_WAIT_SECONDS" "$BETWEEN_JOBS_SECONDS" "$POLL_SECONDS" "$POLL_TIMEOUT"; do
  is_uint "$x" || { echo "ERRORE: parametro temporale non valido: $x" >&2; exit 2; }
done
[[ "$POLL_SECONDS" -gt 0 && "$POLL_TIMEOUT" -gt 0 ]] || exit 2

STAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
WORKDIR="cde_schedule_fix_${STAMP}"
JOBS_JSON="${WORKDIR}/jobs_original.json"
PLAN="${WORKDIR}/schedule_plan.tsv"
LOG="${WORKDIR}/execution.log"
BEFORE_DIR="${WORKDIR}/describe_before"
AFTER_DIR="${WORKDIR}/describe_after"
mkdir -p "$BEFORE_DIR" "$AFTER_DIR"
touch "$LOG"

log() {
  printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" | tee -a "$LOG"
}

cde_tty() {
  # CDE può richiedere nuovamente la password anche durante un massivo.
  # Forziamo stdin sul terminale reale per lasciare il prompt interattivo.
  command cde "$@" </dev/tty
}

fail() { log "ERRORE: $*"; exit 1; }

run_logged() {
  local display="$*"
  display="${display/#cde_tty /cde }"
  log "CMD: $display"
  "$@" 2>&1 | tee -a "$LOG"
  local rc=${PIPESTATUS[0]}
  [[ "$rc" -eq 0 ]] || log "ERRORE: exit code $rc"
  return "$rc"
}

fixed_wait() {
  local sec="$1" why="$2"
  if [[ "$sec" -gt 0 ]]; then
    log "ATTESA ${sec}s - $why"
    sleep "$sec"
  fi
}

validate_json_list() {
  [[ -s "$1" ]] || return 1
  python3 - "$1" <<'PY'
import json,sys
try:
    with open(sys.argv[1],encoding="utf-8") as f:x=json.load(f)
    if not isinstance(x,list):raise ValueError("radice JSON non-list")
except Exception as e:
    print(e,file=sys.stderr);sys.exit(1)
PY
}

validate_json_object() {
  [[ -s "$1" ]] || return 1
  python3 - "$1" <<'PY'
import json,sys
try:
    with open(sys.argv[1],encoding="utf-8") as f:x=json.load(f)
    if not isinstance(x,dict):raise ValueError("radice JSON non-object")
except Exception as e:
    print(e,file=sys.stderr);sys.exit(1)
PY
}

auth_preflight() {
  echo
  echo "============================================================"
  echo "AUTENTICAZIONE CDE"
  echo "============================================================"
  echo ">>> INSERISCI LA WORKLOAD PASSWORD SOLO SE CDE LA RICHIEDE <<<"
  echo
  echo "Se compare un prompt password della CDE CLI:"
  echo "  1. digita la password"
  echo "  2. premi INVIO"
  echo
  echo "Se NON compare alcun prompt, NON digitare nulla."
echo "La password può essere richiesta nuovamente anche più avanti."
  echo "La password NON viene letta né salvata dallo script."
  echo "------------------------------------------------------------"
  if ! cde_tty job list >/dev/null; then
    echo "------------------------------------------------------------"
    fail "autenticazione/connessione CDE fallita"
  fi
  echo "------------------------------------------------------------"
  echo "OK - controllo CDE completato."
  echo "Se NON è comparso alcun prompt, NON era necessario inserire la password."
  echo "============================================================"
}

capture_jobs() {
  local target="$1"
  rm -f "$target"
  log "Acquisizione cde job list"
  cde_tty job list > "$target" || return 1
  validate_json_list "$target" || return 1
  log "OK - JSON acquisito ($(wc -c < "$target") byte)"
}

compare_bool_describe() {
  python3 - "$1" "$2" "$3" <<'PY'
import json,sys
path,field,expected=sys.argv[1:4]
with open(path,encoding="utf-8") as f:o=json.load(f)
actual=bool((o.get("schedule") or {}).get(field,False))
sys.exit(0 if actual==(expected.lower()=="true") else 1)
PY
}

compare_datetime_describe() {
  python3 - "$1" "$2" "$3" <<'PY'
import json,sys
from datetime import datetime,timezone
path,field,expected=sys.argv[1:4]
with open(path,encoding="utf-8") as f:o=json.load(f)
actual=(o.get("schedule") or {}).get(field)
def norm(s):
    if not s:return None
    s=str(s).strip()
    if s.endswith("Z"):s=s[:-1]+"+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc).replace(microsecond=0)
sys.exit(0 if norm(actual)==norm(expected) else 1)
PY
}

poll_describe_bool() {
  local job="$1" field="$2" expected="$3" safe="$4"
  local elapsed=0 f="${WORKDIR}/poll_${safe}_${field}.json"
  while [[ "$elapsed" -le "$POLL_TIMEOUT" ]]; do
    rm -f "$f"
    if cde_tty job describe --name "$job" > "$f" && validate_json_object "$f" &&
       compare_bool_describe "$f" "$field" "$expected"; then
      log "OK [$job]: $field=$expected confermato dopo ${elapsed}s"
      return 0
    fi
    [[ "$elapsed" -ge "$POLL_TIMEOUT" ]] && break
    log "POLL [$job]: $field non ancora $expected; controllo tra ${POLL_SECONDS}s"
    sleep "$POLL_SECONDS"
    elapsed=$((elapsed+POLL_SECONDS))
  done
  return 1
}

poll_describe_datetime() {
  local job="$1" field="$2" expected="$3" safe="$4"
  local elapsed=0 f="${WORKDIR}/poll_${safe}_${field}.json"
  while [[ "$elapsed" -le "$POLL_TIMEOUT" ]]; do
    rm -f "$f"
    if cde_tty job describe --name "$job" > "$f" && validate_json_object "$f" &&
       compare_datetime_describe "$f" "$field" "$expected"; then
      log "OK [$job]: $field=$expected confermato dopo ${elapsed}s"
      return 0
    fi
    [[ "$elapsed" -ge "$POLL_TIMEOUT" ]] && break
    log "POLL [$job]: $field non ancora atteso; controllo tra ${POLL_SECONDS}s"
    sleep "$POLL_SECONDS"
    elapsed=$((elapsed+POLL_SECONDS))
  done
  return 1
}

poll_next_from_list() {
  local job="$1" expected="$2" safe="$3"
  local elapsed=0 f="${WORKDIR}/poll_${safe}_job_list.json"
  while [[ "$elapsed" -le "$POLL_TIMEOUT" ]]; do
    rm -f "$f"
    if cde_tty job list > "$f" && validate_json_list "$f"; then
      RESULT="$(python3 - "$f" "$job" "$expected" <<'PY'
import json,sys
from datetime import datetime,timezone
path,name,expected=sys.argv[1:4]
with open(path,encoding="utf-8") as f:jobs=json.load(f)
j=next((x for x in jobs if x.get("name")==name),None)
if j is None:
    print("JOB_NON_TROVATO");sys.exit(2)
actual=(j.get("schedule") or {}).get("nextExecution")
print(actual or "None")
def norm(s):
    if not s:return None
    s=str(s).strip()
    if s.endswith("Z"):s=s[:-1]+"+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc).replace(microsecond=0)
sys.exit(0 if norm(actual)==norm(expected) else 1)
PY
)"
      rc=$?
      if [[ "$rc" -eq 0 ]]; then
        log "OK [$job]: marker nextExecution CDE=$RESULT confermato da cde job list dopo ${elapsed}s"
        return 0
      fi
      log "POLL [$job]: marker nextExecution CDE=$RESULT | marker atteso=$expected"
    else
      log "POLL [$job]: cde job list non valido"
    fi
    [[ "$elapsed" -ge "$POLL_TIMEOUT" ]] && break
    log "POLL [$job]: nuovo controllo tra ${POLL_SECONDS}s"
    sleep "$POLL_SECONDS"
    elapsed=$((elapsed+POLL_SECONDS))
  done
  return 1
}

safety_pause() {
  local job="$1"
  log "SICUREZZA [$job]: provo a mettere il job in PAUSA."
  if run_logged cde_tty job schedule pause --name "$job"; then
    log "SICUREZZA [$job]: pause eseguito."
  else
    log "ATTENZIONE GRAVE [$job]: pause di sicurezza fallito."
  fi
}

echo "============================================================"
echo "CDE SCHEDULE START - AUTOMAZIONE DEFINITIVA"
echo "NOW UTC: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
[[ -n "$ONLY_JOB" ]] && echo "JOB SELEZIONATO: $ONLY_JOB"
echo "POLL_SECONDS: $POLL_SECONDS"
echo "POLL_TIMEOUT: $POLL_TIMEOUT"
echo "CLEAR_WAIT_SECONDS: $CLEAR_WAIT_SECONDS"
echo "BETWEEN_JOBS_SECONDS: $BETWEEN_JOBS_SECONDS"
echo "CATCHUP TARGET: false"
echo "DEPENDS ON PAST TARGET: false"
echo "============================================================"

for c in bash cde python3 date grep mkdir tee tr wc sleep sed; do
  command -v "$c" >/dev/null 2>&1 || fail "$c non trovato"
done
[[ -f "${CDE_CONFIG:-$HOME/.cde/config.yaml}" ]] || fail "config CDE non trovata"
[[ -r /dev/tty ]] || fail "/dev/tty non disponibile: eseguire lo script da un terminale interattivo"

HELP="$(cde_tty job update --help 2>&1)"
for flag in --schedule-start --cron-expression --catchup --depends-on-past; do
  grep -q -- "$flag" <<<"$HELP" || fail "$flag non supportato dalla CLI"
done
for s in pause clear unpause; do
  cde_tty job schedule "$s" --help >/dev/null 2>&1 || fail "schedule $s non disponibile"
done

auth_preflight
capture_jobs "$JOBS_JSON" || fail "impossibile acquisire cde job list"

python3 - "$JOBS_JSON" "$PLAN" "$ONLY_JOB" <<'PY'

import json, sys
from datetime import datetime, timedelta, timezone

MONTH = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
         "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
DOW = {"SUN":0,"MON":1,"TUE":2,"WED":3,"THU":4,"FRI":5,"SAT":6}

def to_int(x, names=None):
    u = x.strip().upper()
    return names[u] if names and u in names else int(u)

def parse_field(expr, lo, hi, names=None, dow=False):
    vals=set()
    for item in expr.strip().upper().split(","):
        if "/" in item:
            base, ss = item.split("/",1)
            step = int(ss)
        else:
            base, step = item, 1
        if step <= 0:
            raise ValueError("step cron non valido")
        if base == "*":
            a,b=lo,hi
        elif "-" in base:
            aa,bb=base.split("-",1)
            a,b=to_int(aa,names),to_int(bb,names)
        else:
            v=to_int(base,names)
            if dow and v==7:
                v=0
            if not lo <= v <= hi:
                raise ValueError(f"valore cron fuori range: {item}")
            vals.add(v)
            continue
        if a>b or a<lo or b>hi:
            raise ValueError(f"range cron non valido: {item}")
        for v in range(a,b+1,step):
            vals.add(0 if dow and v==7 else v)
    return vals

def compile_cron(expr):
    p=expr.split()
    if len(p)!=5:
        raise ValueError(f"cron non a 5 campi: {expr}")
    mi,hr,dom,mon,dow=p
    return {
        "mi": parse_field(mi,0,59),
        "hr": parse_field(hr,0,23),
        "dom": parse_field(dom,1,31),
        "mon": parse_field(mon,1,12,MONTH),
        "dow": parse_field(dow,0,7,DOW,True),
        "dom_any": dom=="*",
        "dow_any": dow=="*",
    }

def matches(dt,c):
    if dt.minute not in c["mi"] or dt.hour not in c["hr"] or dt.month not in c["mon"]:
        return False
    dm=dt.day in c["dom"]
    dw=((dt.weekday()+1)%7) in c["dow"]
    if c["dom_any"] and c["dow_any"]:
        return True
    if c["dom_any"]:
        return dw
    if c["dow_any"]:
        return dm
    return dm or dw

def prev_occurrence(t,c):
    d=t.replace(second=0,microsecond=0)-timedelta(minutes=1)
    for _ in range(4*366*24*60):
        if matches(d,c):
            return d
        d-=timedelta(minutes=1)
    raise ValueError("occorrenza precedente non trovata entro 4 anni")

def next_occurrence(t,c):
    d=t.replace(second=0,microsecond=0)+timedelta(minutes=1)
    for _ in range(4*366*24*60):
        if matches(d,c):
            return d
        d+=timedelta(minutes=1)
    raise ValueError("occorrenza successiva non trovata entro 4 anni")

def iso(d):
    return d.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def norm(s):
    if not s:
        return None
    s=str(s).strip()
    if s.endswith("Z"):
        s=s[:-1]+"+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc).replace(microsecond=0)

def calculate(cron, now=None):
    now=(now or datetime.now(timezone.utc)).replace(second=0,microsecond=0)
    c=compile_cron(cron)
    p1=prev_occurrence(now,c)
    p2=prev_occurrence(p1,c)
    nx=next_occurrence(now,c)
    ns=p1-timedelta(hours=1)
    if not p2 < ns < p1:
        ns=(p2+(p1-p2)/2).replace(second=0,microsecond=0)
    if not p2 < ns < p1:
        raise ValueError("NEW_START non collocabile tra PREV_2 e PREV_1")
    return now,p2,p1,ns,nx


src,out,only=sys.argv[1:4]
with open(src,encoding="utf-8") as f:jobs=json.load(f)

rows=[]
found=False
for j in jobs:
    name=j.get("name","")
    if only:
        if name!=only:continue
        found=True
    s=j.get("schedule") or {}
    if s.get("enabled") is not True:
        if only and name==only:
            rows.append([name,"",s.get("start",""),"","","","",s.get("nextExecution",""),
                         str(bool(s.get("paused",False))).lower(),
                         str(bool(s.get("catchup",False))).lower(),
                         str(bool(s.get("dependsOnPast",False))).lower(),
                         "SKIP","schedule non enabled"])
        continue

    cron=s.get("cronExpression","")
    paused=bool(s.get("paused",False))
    catch=bool(s.get("catchup",False))
    dep=bool(s.get("dependsOnPast",False))

    if not cron:
        rows.append([name,"",s.get("start",""),"","","","",s.get("nextExecution",""),
                     str(paused).lower(),str(catch).lower(),str(dep).lower(),
                     "SKIP","cron assente"])
        continue

    if catch or dep:
        rows.append([name,cron,s.get("start",""),"","","","",s.get("nextExecution",""),
                     str(paused).lower(),str(catch).lower(),str(dep).lower(),
                     "SKIP","flag speciali già true: verifica manuale"])
        continue

    if paused and not only:
        rows.append([name,cron,s.get("start",""),"","","","",s.get("nextExecution",""),
                     "true","false","false","SKIP","job già paused: non riattivato dal massivo"])
        continue

    try:
        now,p2,p1,ns,nx=calculate(cron)
        rows.append([name,cron,s.get("start",""),iso(p2),iso(p1),iso(ns),iso(nx),s.get("nextExecution",""),
                     str(paused).lower(),"false","false","UPDATE",""])
    except Exception as e:
        rows.append([name,cron,s.get("start",""),"","","","",s.get("nextExecution",""),
                     str(paused).lower(),"false","false","SKIP",str(e)])

if only and not found:
    print("ERRORE: job non trovato: "+only,file=sys.stderr);sys.exit(3)

with open(out,"w",encoding="utf-8") as f:
    f.write("job\tcron\tcurrent_start\tprev_2\tprev_1\tnew_start\tcomputed_next\tcde_next\tpaused\tcatchup\tdepends_on_past\taction\tnote\n")
    for r in rows:f.write("\t".join(map(str,r))+"\n")
PY

[[ $? -eq 0 ]] || fail "generazione piano fallita"

python3 - "$PLAN" <<'PY'
import csv,sys
with open(sys.argv[1],newline="",encoding="utf-8") as f:
    rows=list(csv.DictReader(f,delimiter="\t"))

print()
print("============================================================")
print("PIANO OPERATIVO")
print("============================================================")

u=s=0
total=len(rows)
for i,r in enumerate(rows,1):
    print()
    print(f"[JOB {i}/{total}] {r['job']}")
    if r["action"] == "UPDATE":
        print(f"  Cron           : {r['cron']}")
        print(f"  Start attuale  : {r['current_start']}")
        print(f"  Nuova start    : {r['new_start']}")
        print(f"  Marker atteso  : {r['prev_1']}")
        print(f"  Marker CDE     : {r['cde_next']}")
        print(f"  Prossima run   : {r['computed_next']}")
        print("  Flag target    : catchup=false | dependsOnPast=false")
        print("  Azione         : UPDATE")
        u += 1
    else:
        print(f"  Azione         : SKIP")
        print(f"  Motivo         : {r['note']}")
        s += 1

print()
print("------------------------------------------------------------")
print(f"Totale job       : {total}")
print(f"Da aggiornare    : {u}")
print(f"Da saltare       : {s}")
PY

read -r PLAN_TOTAL PLAN_UPDATE PLAN_SKIP <<<"$(python3 - "$PLAN" <<'PY'
import csv,sys
with open(sys.argv[1],newline="",encoding="utf-8") as f:
    rows=list(csv.DictReader(f,delimiter="\t"))
u=sum(1 for r in rows if r["action"]=="UPDATE")
print(len(rows),u,len(rows)-u)
PY
)"

ESTIMATED_TOTAL_SECONDS=$((PLAN_UPDATE * ESTIMATED_SECONDS_PER_JOB))

echo
echo "Backup/log: $WORKDIR"
echo
echo "============================================================"
echo "PRONTO PER L'APPLICAZIONE"
echo "============================================================"
echo "Job totali       : $PLAN_TOTAL"
echo "Job da aggiornare: $PLAN_UPDATE"
echo "Job da saltare   : $PLAN_SKIP"
echo "Tempo indicativo : ~$(format_duration "$ESTIMATED_TOTAL_SECONDS")"
echo "                   (~${ESTIMATED_SECONDS_PER_JOB}s per job)"
echo
echo "Modalità          : sequenziale, un job completo alla volta"
echo "Attese            : polling sugli stati; sleep fisso solo dopo CLEAR"
echo "                    (${CLEAR_WAIT_SECONDS}s) e tra job (${BETWEEN_JOBS_SECONDS}s)"
echo "Controllo finale  : marker nextExecution da cde job list"
echo "Sicurezza         : in caso di verifica KO il job viene messo in pausa"
echo
echo "IMPORTANTE - AUTENTICAZIONE DURANTE IL MASSIVO"
echo "CDE può richiedere nuovamente la password anche dopo l'avvio."
echo "Se compare: API User Password:"
echo "  -> inserisci la password e premi INVIO."
echo "Il terminale resta interattivo per tutta l'esecuzione."
echo
echo "Per iniziare scrivere ESATTAMENTE: APPLICA"
echo "Qualunque altra risposta annulla l'operazione."
echo "============================================================"
read -r CONFIRM
[[ "$CONFIRM" == "APPLICA" ]] || { echo "Operazione annullata."; exit 0; }

UPDATED=0
FAILED=0
SKIPPED=0
JOB_INDEX=0
EXECUTION_START_EPOCH="$(date +%s)"

exec 3<"$PLAN"
while IFS=$'\t' read -r job cron current_start prev2 prev1 new_start computed_next cde_next paused catchup depends action note <&3; do
  [[ "$job" == "job" ]] && continue
  ((JOB_INDEX+=1))

  echo
  echo "============================================================"
  echo "[JOB $JOB_INDEX/$PLAN_TOTAL] $job"
  echo "============================================================"

  if [[ "$action" != "UPDATE" ]]; then
    echo "  SKIP: $note"
    log "SKIP [$job]: $note"
    ((SKIPPED+=1))
    continue
  fi

  JOB_START_EPOCH="$(date +%s)"
  echo "  Nuova start  : $new_start"
  echo "  Marker atteso: $prev1"
  echo "  Prossima run : $computed_next"
  echo "  Auth          : se compare 'API User Password:', inseriscila e premi INVIO"

  SAFE="$(printf '%s' "$job" | tr '/ ' '__')"

  RECALC="$(python3 - "$cron" <<'PY'

import json, sys
from datetime import datetime, timedelta, timezone

MONTH = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
         "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
DOW = {"SUN":0,"MON":1,"TUE":2,"WED":3,"THU":4,"FRI":5,"SAT":6}

def to_int(x, names=None):
    u = x.strip().upper()
    return names[u] if names and u in names else int(u)

def parse_field(expr, lo, hi, names=None, dow=False):
    vals=set()
    for item in expr.strip().upper().split(","):
        if "/" in item:
            base, ss = item.split("/",1)
            step = int(ss)
        else:
            base, step = item, 1
        if step <= 0:
            raise ValueError("step cron non valido")
        if base == "*":
            a,b=lo,hi
        elif "-" in base:
            aa,bb=base.split("-",1)
            a,b=to_int(aa,names),to_int(bb,names)
        else:
            v=to_int(base,names)
            if dow and v==7:
                v=0
            if not lo <= v <= hi:
                raise ValueError(f"valore cron fuori range: {item}")
            vals.add(v)
            continue
        if a>b or a<lo or b>hi:
            raise ValueError(f"range cron non valido: {item}")
        for v in range(a,b+1,step):
            vals.add(0 if dow and v==7 else v)
    return vals

def compile_cron(expr):
    p=expr.split()
    if len(p)!=5:
        raise ValueError(f"cron non a 5 campi: {expr}")
    mi,hr,dom,mon,dow=p
    return {
        "mi": parse_field(mi,0,59),
        "hr": parse_field(hr,0,23),
        "dom": parse_field(dom,1,31),
        "mon": parse_field(mon,1,12,MONTH),
        "dow": parse_field(dow,0,7,DOW,True),
        "dom_any": dom=="*",
        "dow_any": dow=="*",
    }

def matches(dt,c):
    if dt.minute not in c["mi"] or dt.hour not in c["hr"] or dt.month not in c["mon"]:
        return False
    dm=dt.day in c["dom"]
    dw=((dt.weekday()+1)%7) in c["dow"]
    if c["dom_any"] and c["dow_any"]:
        return True
    if c["dom_any"]:
        return dw
    if c["dow_any"]:
        return dm
    return dm or dw

def prev_occurrence(t,c):
    d=t.replace(second=0,microsecond=0)-timedelta(minutes=1)
    for _ in range(4*366*24*60):
        if matches(d,c):
            return d
        d-=timedelta(minutes=1)
    raise ValueError("occorrenza precedente non trovata entro 4 anni")

def next_occurrence(t,c):
    d=t.replace(second=0,microsecond=0)+timedelta(minutes=1)
    for _ in range(4*366*24*60):
        if matches(d,c):
            return d
        d+=timedelta(minutes=1)
    raise ValueError("occorrenza successiva non trovata entro 4 anni")

def iso(d):
    return d.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def norm(s):
    if not s:
        return None
    s=str(s).strip()
    if s.endswith("Z"):
        s=s[:-1]+"+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc).replace(microsecond=0)

def calculate(cron, now=None):
    now=(now or datetime.now(timezone.utc)).replace(second=0,microsecond=0)
    c=compile_cron(cron)
    p1=prev_occurrence(now,c)
    p2=prev_occurrence(p1,c)
    nx=next_occurrence(now,c)
    ns=p1-timedelta(hours=1)
    if not p2 < ns < p1:
        ns=(p2+(p1-p2)/2).replace(second=0,microsecond=0)
    if not p2 < ns < p1:
        raise ValueError("NEW_START non collocabile tra PREV_2 e PREV_1")
    return now,p2,p1,ns,nx

cron=sys.argv[1]
_,p2,p1,ns,nx=calculate(cron)
print(iso(p2),iso(p1),iso(ns),iso(nx),sep="\t")
PY
)"
  IFS=$'\t' read -r live_prev2 live_prev1 live_start live_next <<<"$RECALC"

  if [[ "$live_prev1" != "$prev1" || "$live_start" != "$new_start" || "$live_next" != "$computed_next" ]]; then
    log "SKIP [$job]: il piano è cambiato mentre il massivo era in corso."
    log "PIANO ORIGINALE: marker=$prev1 start=$new_start next=$computed_next"
    log "PIANO ATTUALE  : marker=$live_prev1 start=$live_start next=$live_next"
    ((SKIPPED+=1))
    show_job_progress "SKIP - piano diventato obsoleto" "$job" "$JOB_START_EPOCH"
    continue
  fi

  log "INIZIO [$job] cron='$cron' new_start=$new_start next=$computed_next"

  echo "  [1/5] Backup e PAUSE (polling fino a paused=true)"
  if ! cde_tty job describe --name "$job" > "${BEFORE_DIR}/${SAFE}.json"; then
    log "ERRORE [$job]: describe iniziale fallito"
    ((FAILED+=1)); show_job_progress "KO" "$job" "$JOB_START_EPOCH"; continue
  fi

  if ! run_logged cde_tty job schedule pause --name "$job"; then
    log "ERRORE [$job]: pause fallito"
    ((FAILED+=1)); show_job_progress "KO" "$job" "$JOB_START_EPOCH"; continue
  fi
  if ! poll_describe_bool "$job" "paused" "true" "$SAFE"; then
    log "ERRORE [$job]: pause non confermato"
    ((FAILED+=1)); show_job_progress "KO" "$job" "$JOB_START_EPOCH"; continue
  fi

  echo "  [2/5] CLEAR schedule precedente (buffer minimo ${CLEAR_WAIT_SECONDS}s)"
  if ! run_logged cde_tty job schedule clear --name "$job"; then
    log "ERRORE [$job]: clear fallito; JOB RESTA IN PAUSA"
    ((FAILED+=1)); show_job_progress "KO" "$job" "$JOB_START_EPOCH"; continue
  fi
  fixed_wait "$CLEAR_WAIT_SECONDS" "buffer minimo dopo CLEAR [$job]"

  echo "  [3/5] UPDATE schedule (polling start + flag)"
  if ! run_logged cde_tty job update \
      --name "$job" \
      --cron-expression "$cron" \
      --schedule-start "$new_start" \
      --catchup false \
      --depends-on-past false; then
    log "ERRORE [$job]: update fallito; JOB RESTA IN PAUSA"
    ((FAILED+=1)); show_job_progress "KO" "$job" "$JOB_START_EPOCH"; continue
  fi

  if ! poll_describe_datetime "$job" "start" "$new_start" "$SAFE"; then
    log "ERRORE [$job]: start non confermata; JOB RESTA IN PAUSA"
    ((FAILED+=1)); show_job_progress "KO" "$job" "$JOB_START_EPOCH"; continue
  fi
  if ! poll_describe_bool "$job" "catchup" "false" "$SAFE"; then
    log "ERRORE [$job]: catchup=false non confermato; JOB RESTA IN PAUSA"
    ((FAILED+=1)); show_job_progress "KO" "$job" "$JOB_START_EPOCH"; continue
  fi
  if ! poll_describe_bool "$job" "dependsOnPast" "false" "$SAFE"; then
    log "ERRORE [$job]: dependsOnPast=false non confermato; JOB RESTA IN PAUSA"
    ((FAILED+=1)); show_job_progress "KO" "$job" "$JOB_START_EPOCH"; continue
  fi

  echo "  [4/5] UNPAUSE (polling fino a paused=false)"
  if ! run_logged cde_tty job schedule unpause --name "$job"; then
    log "ERRORE [$job]: unpause fallito; JOB RESTA IN PAUSA"
    ((FAILED+=1)); show_job_progress "KO" "$job" "$JOB_START_EPOCH"; continue
  fi
  if ! poll_describe_bool "$job" "paused" "false" "$SAFE"; then
    log "ERRORE [$job]: unpause non confermato"
    safety_pause "$job"
    ((FAILED+=1)); show_job_progress "KO" "$job" "$JOB_START_EPOCH"; continue
  fi

  echo "  [5/5] Verifica marker CDE (polling su cde job list)"
  if ! poll_next_from_list "$job" "$prev1" "$SAFE"; then
    log "ERRORE [$job]: marker nextExecution CDE non coincidente con PREV_1"
    safety_pause "$job"
    ((FAILED+=1)); show_job_progress "KO" "$job" "$JOB_START_EPOCH"; continue
  fi


  FINAL="${AFTER_DIR}/${SAFE}.json"
  if ! cde_tty job describe --name "$job" > "$FINAL" || ! validate_json_object "$FINAL"; then
    log "ERRORE [$job]: describe finale fallito"
    safety_pause "$job"
    ((FAILED+=1)); show_job_progress "KO" "$job" "$JOB_START_EPOCH"; continue
  fi

  log "OK [$job]: start, flag, stato e marker nextExecution CDE confermati; prossima run calcolata=$computed_next."
  ((UPDATED+=1))
  fixed_wait "$BETWEEN_JOBS_SECONDS" "separazione minima prima del job successivo"
  show_job_progress "OK" "$job" "$JOB_START_EPOCH"

done
exec 3<&-

EXECUTION_END_EPOCH="$(date +%s)"
TOTAL_ELAPSED=$((EXECUTION_END_EPOCH - EXECUTION_START_EPOCH))

echo
echo "============================================================"
echo "RISULTATO FINALE"
echo "============================================================"
echo "Aggiornati       : $UPDATED"
echo "Saltati          : $SKIPPED"
echo "Falliti          : $FAILED"
echo "Tempo totale     : $(format_duration "$TOTAL_ELAPSED")"
if (( UPDATED + FAILED > 0 )); then
  AVG_SECONDS=$((TOTAL_ELAPSED / (UPDATED + FAILED)))
  echo "Media per job    : ~$(format_duration "$AVG_SECONDS")"
fi
echo "Log              : $LOG"
echo "Backup           : $WORKDIR"
echo "============================================================"

[[ "$FAILED" -eq 0 ]]
