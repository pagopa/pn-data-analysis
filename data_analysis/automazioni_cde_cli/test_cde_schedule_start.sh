#!/usr/bin/env bash
set -uo pipefail
export TZ=UTC

# ==============================================================================
# test_cde_schedule_start.sh
# TEST DEFINITIVO - SOLO LETTURA.
# ==============================================================================

ONLY_JOB=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --job)
      [[ $# -ge 2 ]] || { echo "ERRORE: --job richiede un nome." >&2; exit 2; }
      ONLY_JOB="$2"; shift 2 ;;
    -h|--help)
      echo "Uso: $0 [--job NOME_JOB]"; exit 0 ;;
    *)
      echo "ERRORE: parametro non riconosciuto: $1" >&2; exit 2 ;;
  esac
done

STAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
WORKDIR="cde_schedule_test_${STAMP}"
JOBS_JSON="${WORKDIR}/jobs_original.json"
REPORT="${WORKDIR}/schedule_plan.tsv"
mkdir -p "$WORKDIR"


cde_tty() {
  # CDE può richiedere nuovamente la password anche durante un massivo.
  # Forziamo stdin sul terminale reale per lasciare il prompt interattivo.
  command cde "$@" </dev/tty
}

fail() { echo "ERRORE: $*" >&2; exit 1; }

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

echo "============================================================"
echo "TEST CDE SCHEDULE START - SOLO LETTURA"
echo "NOW UTC: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
[[ -n "$ONLY_JOB" ]] && echo "JOB SELEZIONATO: $ONLY_JOB"
echo "============================================================"

for c in bash cde python3 date mkdir wc; do
  command -v "$c" >/dev/null 2>&1 || fail "$c non trovato"
done
[[ -f "${CDE_CONFIG:-$HOME/.cde/config.yaml}" ]] || fail "config CDE non trovata"
[[ -r /dev/tty ]] || fail "/dev/tty non disponibile: eseguire lo script da un terminale interattivo"

auth_preflight

echo
echo "Acquisizione cde job list..."
cde_tty job list > "$JOBS_JSON" || fail "cde job list fallito"
validate_json_list "$JOBS_JSON" || fail "JSON job list vuoto/non valido"
echo "OK - JSON acquisito ($(wc -c < "$JOBS_JSON") byte)"

python3 - "$JOBS_JSON" "$REPORT" "$ONLY_JOB" <<'PY'

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
with open(src,encoding="utf-8") as f: jobs=json.load(f)

rows=[]
found=False
for j in jobs:
    name=j.get("name","")
    if only:
        if name != only:
            continue
        found=True

    s=j.get("schedule") or {}
    if s.get("enabled") is not True:
        if only and name==only:
            rows.append([name,"",s.get("start",""),"","","","",s.get("nextExecution",""),
                         str(bool(s.get("paused",False))).lower(),
                         str(bool(s.get("catchup",False))).lower(),
                         str(bool(s.get("dependsOnPast",False))).lower(),
                         "SKIP","schedule.enabled != true"])
        continue

    cron=s.get("cronExpression","")
    if not cron:
        rows.append([name,"",s.get("start",""),"","","","",s.get("nextExecution",""),
                     str(bool(s.get("paused",False))).lower(),
                     str(bool(s.get("catchup",False))).lower(),
                     str(bool(s.get("dependsOnPast",False))).lower(),
                     "SKIP","cronExpression assente"])
        continue

    paused=bool(s.get("paused",False))
    catchup=bool(s.get("catchup",False))
    depends=bool(s.get("dependsOnPast",False))

    try:
        now,p2,p1,ns,nx=calculate(cron)
        note=[]
        if norm(s.get("nextExecution","")) != norm(iso(p1)):
            note.append("MARKER_CDE_DIFFERENTE="+str(s.get("nextExecution","")))
        if catchup:
            note.append("catchup=true")
        if depends:
            note.append("dependsOnPast=true")
        rows.append([
            name,cron,s.get("start",""),
            iso(p2),iso(p1),iso(ns),iso(nx),s.get("nextExecution",""),
            str(paused).lower(),str(catchup).lower(),str(depends).lower(),
            "UPDATE","; ".join(note) if note else "OK"
        ])
    except Exception as e:
        rows.append([
            name,cron,s.get("start",""),
            "","","","",s.get("nextExecution",""),
            str(paused).lower(),str(catchup).lower(),str(depends).lower(),
            "SKIP",str(e)
        ])

if only and not found:
    print("ERRORE: job non trovato: "+only,file=sys.stderr)
    sys.exit(3)

with open(out,"w",encoding="utf-8") as f:
    f.write("job\tcron\tcurrent_start\tprev_2\tprev_1\tnew_start\tcomputed_next\tcde_next\tpaused\tcatchup\tdepends_on_past\taction\tnote\n")
    for r in rows:
        f.write("\t".join(map(str,r))+"\n")
PY

[[ $? -eq 0 ]] || fail "calcolo piano fallito"

python3 - "$REPORT" <<'PY'
import csv,sys
with open(sys.argv[1],newline="",encoding="utf-8") as f:
    rows=list(csv.DictReader(f,delimiter="\t"))

print()
print("============================================================")
print("DRY-RUN - ZERO MODIFICHE")
print("============================================================")

updates=skips=0
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
        print(f"  Flag attuali   : catchup={r['catchup']} | dependsOnPast={r['depends_on_past']}")
        print(f"  Esito          : {r['note']}")
        updates += 1
    else:
        print(f"  SKIP           : {r['note']}")
        skips += 1

print()
print("------------------------------------------------------------")
print(f"Job analizzati   : {total}")
print(f"Da aggiornare    : {updates}")
print(f"Da saltare       : {skips}")
PY

echo
echo "============================================================"
echo "DRY-RUN COMPLETATO: nessuna modifica eseguita"
echo "Report: $REPORT"
echo "JSON  : $JOBS_JSON"
echo "============================================================"
