#!/usr/bin/env bash
# CDE Schedule Manager - unico entry point; Bash + Python 3 standard library.
set -euo pipefail
export TZ=UTC
umask 077
HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
[[ -r "$HERE/cde_cli_utils.sh" ]] || { echo 'File cde_cli_utils.sh mancante: salvarlo accanto al manager.' >&2; exit 2; }
command -v python3 >/dev/null || { echo 'Python 3 non disponibile.' >&2; exit 2; }
source "$HERE/cde_cli_utils.sh"
# Il codice viaggia su un file descriptor separato: stdin rimane il terminale.
CDE_MANAGER_DIR="$HERE" python3 <(cde_utils_python; cat <<'PY'
import argparse, copy
from collections import Counter

# Motore cron estratto dal fix collaudato del 02/09/2026, senza cambiare la regola.
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


# Cron con singolo valore/step non era gestita correttamente dal parser storico:
# meglio non modificarla automaticamente anziché produrre un piano ambiguo.
def supported_cron(cron):
    if any(re.fullmatch(r'[A-Za-z0-9]+/\d+', part) for field in cron.split() for part in field.split(',')):
        raise ValueError('cron valore/step non supportata: verifica manuale')
    return compile_cron(cron)

def calc_start(cron, now=None):
    supported_cron(cron)
    _, p2,p1,ns,nx = calculate(cron, now)
    return dict(prev2=iso(p2), marker=iso(p1), target=iso(ns), next=iso(nx))

def parse_date(s):
    for fmt in ('%Y-%m-%d','%d/%m/%Y'):
        try: return datetime.strptime(s,fmt).replace(tzinfo=UTC)
        except ValueError: pass
    raise ValueError('Data non valida; usare GG/MM/AAAA o AAAA-MM-GG')

def parse_month(s):
    for fmt in ('%Y-%m','%m/%Y'):
        try: return datetime.strptime(s,fmt).strftime('%Y-%m')
        except ValueError: pass
    raise ValueError('Mese non valido; usare MM/AAAA o AAAA-MM')

def month_of(j):
    try:
        dt=timestamp(schedule(j).get('end'))
        return dt.strftime('%Y-%m') if dt else None
    except ValueError: return None

def schedule_state(j):
    # Campi funzionali: confronto preciso dei timestamp, flag assenti equivalenti a false.
    s=schedule(j)
    return dict(enabled=bool_field(s,'enabled'), paused=bool_field(s,'paused'),
                catchup=bool_field(s,'catchup'), dependsOnPast=bool_field(s,'dependsOnPast'),
                cronExpression=s.get('cronExpression'), start=timestamp(s.get('start')),
                end=timestamp(s.get('end')), user=s.get('user'))

def plan(jobs, op, new_end=None, force=False, resume_paused=False, now=None, include_missing_end=False):
    now=now or utcnow(); rows=[]
    for j in jobs:
        s=schedule(j); name=j['name']
        r=dict(job=name,cron=s.get('cronExpression') or '',current_start=s.get('start') or '',current_end=s.get('end') or '',cde_next=s.get('nextExecution') or '',paused=s.get('paused',False),catchup=s.get('catchup',False),depends=s.get('dependsOnPast',False),action='SKIP',note='')
        try:
            st=schedule_state(j)
            if not st['enabled']: raise ValueError('schedule non abilitata')
            if not st['cronExpression'] or not st['start']: raise ValueError('cron/start assente')
            if st['end'] is not None and st['end'] <= now: raise ValueError('schedule già scaduta: rinnovo/riattivazione da valutare manualmente')
            if st['end'] is not None and st['end'] <= st['start']: raise ValueError('intervallo start/end incoerente')
            if op=='start':
                if st['catchup'] or st['dependsOnPast']: raise ValueError('flag speciali true: verifica manuale')
                if st['paused'] and not resume_paused: raise ValueError('job già in pausa: escluso')
                r.update(calc_start(r['cron'],now))
                if st['end'] is not None and timestamp(r['target']) >= st['end']: raise ValueError('nuova start non precedente alla end')
                if not force and not st['paused'] and st['start']==timestamp(r['target']) and timestamp(r['cde_next'])==timestamp(r['marker']): raise ValueError('già conforme al piano')
            else:
                if st['end'] is None and not include_missing_end: raise ValueError('endDate assente: inserimento non autorizzato')
                if new_end<=now or new_end<=st['start']: raise ValueError('nuova end non futura o non successiva alla start')
                r['target']=iso(new_end)
                if new_end==st['end']: raise ValueError('endDate già conforme')
                if st['end'] is not None and new_end<st['end']: raise ValueError('rinnovo non consentito: nuova end anticiperebbe quella attuale')
                if st['catchup'] or st['dependsOnPast']: raise ValueError('flag speciali true: rinnovo da valutare manualmente')
                r['end_change']='CREATE' if st['end'] is None else 'RENEW'
            r['action']='UPDATE'
        except (ValueError, TypeError) as e: r['note']=str(e)
        rows.append(r)
    return rows

def show_plan_details(rows,op):
    heading('PIANO OPERATIVO | '+op.upper()+' DATE | nessuna modifica effettuata')
    for i,r in enumerate(rows,1):
        print(f'[{i}/{len(rows)}] {job_name(r["job"])} | {status_text(r["action"])}')
        if r['action']=='SKIP': print('    Motivo: '+r['note']); continue
        print(f'    Cron: {r["cron"]} | Paused: {flag(r["paused"])}')
        print(f'    Attuale: {r["current_start" if op=="start" else "current_end"] or "assente"}')
        if op=='end' and r.get('end_change')=='CREATE':
            print(color('    INSERIMENTO: la schedule acquisirà una scadenza finora assente.','warn'))
        print(f'    Nuova  : {r["target"]}')
        if op=='start': print(f'    Marker atteso: {r["marker"]} | CDE: {r["cde_next"]}\n    Prossima cron teorica UTC: {r["next"]}')
        else: print(f'    Start preservata: {r["current_start"]} | Pausa originale preservata')
    n=sum(r['action']=='UPDATE' for r in rows)
    print(f'\nSelezionati: {len(rows)} | Da aggiornare: {n} | Da saltare: {len(rows)-n}')
    if op=='end':
        created=sum(r['action']=='UPDATE' and r.get('end_change')=='CREATE' for r in rows)
        print(f'  Rinnovi endDate esistenti: {n-created} | Nuove endDate: {created}')
    print(f'Tempo indicativo aggiornamento: ~{duration(n*(35 if op=='start' else 30))} (variabile)')
    return n

def show_plan(rows,op,month=None,missing_only=False):
    heading('PIANO OPERATIVO | '+op.upper()+' DATE | SOLO LETTURA')
    updates=[r for r in rows if r['action']=='UPDATE']
    skips=[r for r in rows if r['action']!='UPDATE']
    if op=='end':
        print('Perimetro: '+('solo senza endDate' if missing_only else ('scadenza UTC '+month if month else 'selezione senza filtro mensile')))
        if updates: print('Nuova scadenza comune:',updates[0]['target'],'(inizio del giorno UTC)')
        print('Cron, start, flag e stato di pausa: da preservare e verificare.')
    else: print('Start calcolata da cron | sequenza PAUSE / CLEAR / UPDATE / UNPAUSE')
    print()
    for r in updates:
        label=('INSERISCI' if r.get('end_change')=='CREATE' else 'RINNOVA') if op=='end' else 'UPDATE'
        old=r['current_end' if op=='end' else 'current_start'] or 'assente'
        target='' if op=='end' else ' -> '+r['target']
        print(f'  {color(label,"warn")} | {job_name(r["job"])} | {old}{target}')
    if skips:
        print(color('ESCLUSI','muted'))
        for r in skips: print(f'  {status_text("SKIP")} | {job_name(r["job"])} | {r["note"]}')
    print(f'\nSelezionati: {len(rows)} | Da aggiornare: {len(updates)} | Da saltare: {len(skips)}')
    if op=='end':
        created=sum(r.get('end_change')=='CREATE' for r in updates)
        print(f'Rinnovi: {len(updates)-created} | Nuove scadenze: {created}')
        if created: print(color('ATTENZIONE: le schedule senza scadenza acquisiranno un termine.','warn'))
    print(f'Tempo indicativo APPLY: ~{duration(len(updates)*(35 if op=='start' else 30))} (stima, non durata dry-run)')
    return len(updates)

def owner_preview(cde, rows, jobs, args, interactive):
    """Forecast, not proof of authenticated identity. Never store credentials."""
    pending=[r for r in rows if r['action']=='UPDATE']
    if not pending: return True
    by={j['name']:j for j in jobs}
    hint=os.environ.get('CDE_USER','').strip()
    # Do not infer an effective profile from YAML. A simple top-level value is only a hint.
    if not hint and not os.environ.get('CDE_CONFIG_PROFILE') and getattr(cde,'config',None):
        matches=re.findall(r'^user:[ \t]*([A-Za-z0-9_.@-]+)[ \t]*(?:#.*)?$',cde.config.read_text(encoding='utf-8'),re.M)
        if len(matches)==1: hint=matches[0]
    identity=getattr(args,'schedule_user',None)
    if interactive:
        hint=identity or hint
        heading('UTENZA SCHEDULE | previsione prima delle modifiche')
        print('  CDE può riassegnare la schedule all’utente che modifica il job.')
        print('  Indica il workload username effettivamente usato dalla CLI (non la password).')
        if hint: print('  Username suggerito da ambiente/config: '+hint+' (non prova del profilo attivo)')
        raw=ask('  Username CDE (Invio = '+(hint+'; da confermare' if hint else 'non determinato')+'; 0 annulla): ').strip()
        if raw=='0': return False
        identity=raw or hint or None
    if identity and (not isinstance(identity,str) or not re.fullmatch(r'[A-Za-z0-9_.@-]+',identity)):
        raise ValueError('Username CDE non valido')
    groups={'same':[],'change':[],'unknown':[]}
    for r in pending:
        owner=schedule(by[r['job']]).get('user')
        kind='unknown' if not identity or not owner else ('same' if owner==identity else 'change')
        groups[kind].append(r)
        r.update(owner_before=owner,owner_expected=identity,owner_forecast=kind)
    print(f'  Da modificare: {len(pending)} | Utenza invariata attesa: {len(groups["same"])} | Cambio atteso: {len(groups["change"])} | Non determinabile: {len(groups["unknown"])}')
    print('  Previsione basata sullo username dichiarato, non su una verifica del token.')
    policy=getattr(args,'owner_policy',None)
    if groups['change'] or groups['unknown']:
        if interactive:
            while True:
                choice=choose('  [1] Solo utenza invariata (default)  [2] Tutti  [3] Mostra elenco  [0] Annulla: ',{'','0','1','2','3'})
                if choice=='0': return False
                if choice=='3':
                    for kind in ('change','unknown','same'):
                        for r in groups[kind]: print(f'    {job_name(r["job"])} | {r["owner_before"] or "non noto"} → {identity or "non noto"} | '+{'same':'invariata attesa','change':'cambio atteso','unknown':'non determinabile'}[kind])
                    continue
                policy='same' if choice in ('','1') else 'all'; break
        elif not policy:
            # CLI dry-run remains noninteractive; apply requires explicit policy.
            if getattr(args,'apply',False): raise ValueError('Specificare --schedule-user e --owner-policy same/all; oppure --owner-policy all per accettare utenze non determinabili')
            print('  Dry-run: nessuna scelta applicata; per apply indicare --owner-policy.')
            policy='all'
    else: policy=policy or 'same'
    for r in pending:
        r['owner_policy']=policy
        if policy=='same' and r['owner_forecast']!='same':
            r.update(action='SKIP',note='utenza diversa o non determinabile: esclusa su richiesta')
    return True

def state_differences(job, expected):
    # CDE può assegnare la schedule all'utente che aggiorna il job.
    # Solo nelle verifiche POST modifica, user è informativo, non bloccante.
    actual=schedule_state(job)
    return [f'schedule.{k}' for k,v in expected.items() if k!='user' and actual[k]!=v]

def differing_paths(before, after, prefix=''):
    # Diagnostica dei soli nomi dei campi, senza esporre valori di configurazione.
    paths=[]
    for k in sorted(before.keys() | after.keys()):
        path=prefix+k
        if k not in before or k not in after: paths.append(path)
        elif before[k]!=after[k]:
            if isinstance(before[k],dict) and isinstance(after[k],dict):
                paths.extend(differing_paths(before[k],after[k],path+'.'))
            else: paths.append(path)
    return paths

def config_differences(before, after, expected):
    diffs=state_differences(after,expected)
    # Stesse esclusioni della v2.2; non estenderle ad altri campi funzionali.
    excluded={'schedule','modified','nextExecution'}
    diffs.extend(differing_paths({k:v for k,v in before.items() if k not in excluded},
                                  {k:v for k,v in after.items() if k not in excluded}))
    skip={'enabled','paused','pausedUponCreation','catchup','dependsOnPast','cronExpression','start','end','user','nextExecution'}
    diffs.extend(differing_paths({k:v for k,v in schedule(before).items() if k not in skip},
                                  {k:v for k,v in schedule(after).items() if k not in skip},'schedule.'))
    return diffs

def compare_config(before, after, expected):
    return not config_differences(before,after,expected)

def checked_poll(cde, label, fetch, differences):
    # Mostra solo quando cambia l'elenco dei campi discordanti; nessun retry di scrittura.
    last=None
    def predicate(job):
        nonlocal last
        fields=tuple(differences(job))
        if fields and fields!=last:
            cde.log('DIAGNOSTICA - campi non conformi: '+', '.join(fields))
        last=fields
        return not fields
    return cde.poll(label,fetch,predicate)

def apply_plan(cde, jobs, rows, op, args):
    by_name={j['name']:j for j in jobs}; pending=[r for r in rows if r['action']=='UPDATE']
    heading('CONTROLLI PRELIMINARI | aggiornamento autorizzato')
    print('  Verifico i comandi CLI disponibili prima di modificare i job.',flush=True)
    cde.preflight(['--cron-expression','--schedule-start','--catchup','--depends-on-past']+(['--schedule-end'] if op in ('end','cron') else []), ['pause','unpause']+(['clear'] if op=='start' else []))
    if op=='cron' and len(pending)!=1:
        raise CDEError('Apply cron: consentito un solo job per operazione.')
    print('  Verifico tutte le run dei job da modificare: se attive o con stato ignoto, blocco il piano prima del backup.',flush=True)
    for r in pending: check_no_active_runs(cde,r['job'])
    try:
        journal=Journal(op, rows)
    except OSError as e:
        raise OSError(f'Impossibile salvare il registro di sicurezza: {e}. Nessun job modificato.') from e
    heading('BACKUP PREVENTIVO | aggiornamento autorizzato')
    print('Registro di sicurezza creato automaticamente (un solo file locale):', flush=True)
    print(f'  {journal.path}', flush=True)
    print('  Conserva la configurazione originale dei job prima di ogni modifica.\n', flush=True)
    results=[dict(job=r['job'],result='SKIP',detail=r['note']) for r in rows if r['action']!='UPDATE']
    begin=time.monotonic(); failed=False
    for i,r in enumerate(pending,1):
        name=r['job']; started=time.monotonic(); mutation=False
        heading(f'AGGIORNAMENTO JOB {i}/{len(pending)} | {name}')
        cde.log(f'[{i}/{len(pending)}] {name}')
        try:
            before=cde.describe(name)
            original=schedule_state(before)
            if original!=schedule_state(by_name[name]):
                if op=='cron': raise CDEError('Configurazione cambiata rispetto al piano cron; interrompo')
                results.append(dict(job=name,result='SKIP',detail='configurazione cambiata rispetto al piano')); continue
            if r.get('owner_policy')=='same' and original['user']!=r.get('owner_expected'):
                if op=='cron': raise CDEError('Utenza non coerente con piano cron; interrompo')
                results.append(dict(job=name,result='SKIP',detail='utenza non coerente con la scelta preventiva')); continue
            if op=='start' and calc_start(r['cron'])!={k:r[k] for k in ('prev2','marker','target','next')}:
                results.append(dict(job=name,result='SKIP',detail='piano temporale cambiato')); continue
            if original['end'] and original['end']<=utcnow():
                if op=='cron': raise CDEError('Schedule nel frattempo scaduta; interrompo')
                results.append(dict(job=name,result='SKIP',detail='schedule nel frattempo scaduta')); continue
            if op=='cron':
                if any(k in by_name[name] and before.get(k)!=by_name[name].get(k) for k in ('spark','mounts','type')):
                    raise CDEError('Risorse/configurazione cambiate rispetto al piano cron')
                check_cron_window(before,r['target'])
            if op=='end' and timestamp(r['target'])<=utcnow():
                results.append(dict(job=name,result='SKIP',detail='target non più futuro')); continue
            baseline=check_no_active_runs(cde,name)
            cde.log('OK - nessuna run attiva rilevata prima della pausa')
            expected=copy.copy(original)
            if op=='cron': expected['cronExpression']=r['target']
            else: expected['start' if op=='start' else 'end']=timestamp(r['target'])
            expected['paused']=True
            # Fail closed: se non si riesce a salvare e sincronizzare il backup, non modificare CDE.
            journal.record('before', job=name, configuration=before, target=r['target'], run_ids_before=sorted(baseline))
            cde.log('[1/5] Configurazione originale salvata; pausa schedule')
            mutation=True
            if not original['paused']: cde.mutate('job','schedule','pause','--name',name)
            cde.poll('paused=true',lambda:cde.describe(name),lambda j:bool_field(schedule(j),'paused'))
            cde.log('[2/5] '+('CLEAR schedule precedente' if op=='start' else 'CLEAR non previsto per '+('cron' if op=='cron' else 'endDate')))
            check_no_active_runs(cde,name,baseline if op=='cron' else None)
            if op=='start':
                cde.mutate('job','schedule','clear','--name',name); time.sleep(1)
            if op=='cron': check_cron_window(before,r['target'])
            check_no_active_runs(cde,name,baseline if op=='cron' else None)
            cde.log('[3/5] UPDATE e verifica configurazione')
            cmd=['job','update','--name',name,'--cron-expression',r['target'] if op=='cron' else r['cron'],'--schedule-start',r['target'] if op=='start' else original['start'].isoformat(timespec='microseconds').replace('+00:00','Z'),'--catchup',flag(original['catchup']),'--depends-on-past',flag(original['dependsOnPast'])]
            if op=='end': cmd+=['--schedule-end',r['target']]
            if op=='cron' and original['end'] is not None:
                cmd+=['--schedule-end',original['end'].isoformat(timespec='microseconds').replace('+00:00','Z')]
            # START mantiene esattamente i flag della procedura già validata.
            cde.mutate(*cmd)
            verified=checked_poll(cde,'schedule e configurazione preservate prima di unpause',lambda:cde.describe(name),lambda j:config_differences(before,j,expected))
            observed_user=schedule(verified).get('user')
            if r.get('owner_policy')=='same' and observed_user!=original['user']:
                raise CDEError('Utenza cambiata nonostante scelta solo invariata; verifica manuale')
            if observed_user!=original['user']:
                cde.log(f'INFO - Utente schedule variato: {original["user"]!r} -> {observed_user!r} (non bloccante)')
            if op=='cron': check_cron_window(before,r['target'])
            check_no_active_runs(cde,name,baseline if op=='cron' else None)
            cde.log('[4/5] Ripristino stato di pausa')
            # La modalità start esplicitamente autorizzata su un singolo job paused lo riattiva.
            final_paused=original['paused'] if op in ('end','cron') else False
            expected['paused']=final_paused
            if not final_paused: cde.mutate('job','schedule','unpause','--name',name)
            if op=='cron':
                # Primo controllo subito dopo UNPAUSE, prima delle verifiche di configurazione.
                observe_cron_runs(cde,name,baseline,journal)
            after=checked_poll(cde,'configurazione finale e stato pausa',lambda:cde.describe(name),lambda j:config_differences(before,j,expected))
            final_user=schedule(after).get('user')
            if r.get('owner_policy')=='same' and final_user!=original['user']:
                raise CDEError('Utenza cambiata dopo unpause nonostante scelta solo invariata')
            if final_user!=observed_user:
                cde.log(f'INFO - Utente schedule dopo ripristino pausa: {observed_user!r} -> {final_user!r} (non bloccante)')
            cde.log('[5/5] Verifica marker CDE' if op=='start' else '[5/5] Lettura marker CDE (informativo)')
            def get_list_job():
                hit=next((j for j in cde.jobs() if j['name']==name),None)
                if hit is None: raise CDEError('Job non trovato in list: '+name)
                return hit
            if op=='start':
                live=checked_poll(cde,'marker PREV_1',get_list_job,lambda j:state_differences(j,expected)+(['schedule.nextExecution'] if timestamp(schedule(j).get('nextExecution'))!=timestamp(r['marker']) else []))
            else:
                live=checked_poll(cde,('cron e stato da job list' if op=='cron' else 'endDate e stato da job list'),get_list_job,lambda j:state_differences(j,expected))
            if r.get('owner_policy')=='same' and schedule(live).get('user')!=original['user']:
                raise CDEError('Utenza da job list non coerente con scelta solo invariata')
            if schedule(live).get('user')!=final_user:
                cde.log(f'INFO - Utente schedule da job list: {schedule(live).get("user")!r} (non bloccante)')
            cde.log(f'Marker CDE osservato: {schedule(live).get("nextExecution")}')
            if op=='cron':
                journal.record('configuration_verified',job=name,configuration=after,marker=schedule(live).get('nextExecution'))
                observe_cron_runs(cde,name,baseline,journal)
                cron_marker_check(before,live,r['target'])
                cde.log('Prossimo avvio cron teorico UTC: '+iso(next_occurrence(utcnow(),supported_cron(r['target'])))+' (non conferma di esecuzione)')
                heading('OSSERVAZIONE POST-UNPAUSE | controllo nuovi avvii')
                print('  Controllo per 60 secondi se compaiono nuove run, anche già concluse. Se una nuova run è running, potrai decidere se terminarla. Nessuna garanzia oltre la finestra osservata.',flush=True)
                deadline=time.monotonic()+60
                while True:
                    observe_cron_runs(cde,name,baseline,journal)
                    remaining=deadline-time.monotonic()
                    if remaining<=0: break
                    print(f'  Nessuna nuova run rilevata; osservazione residua ~{int(remaining+0.999)} s.',flush=True)
                    time.sleep(min(10,remaining))
                cde.log('OK - nessuna nuova run rilevata nei controlli post-unpause')
            journal.record('after', job=name, configuration=after, marker=schedule(live).get('nextExecution'), result='OK')
            results.append(dict(job=name,result='OK',detail='verificato')); mutation=False
        except (Exception,KeyboardInterrupt) as e:
            failed=True
            cde.log(f'KO {name}: {type(e).__name__}: {e}')
            paused_ok=cde.safety_pause(name) if mutation else None
            results.append(dict(job=name,result='KO',detail=f'{type(e).__name__}: {e}',safety_pause_confirmed=paused_ok))
            # Stop conservativo: nessun altro job coinvolto se lo stato è incerto.
            for remaining in pending[i:]: results.append(dict(job=remaining['job'],result='NON ESEGUITO',detail='batch interrotto dopo KO'))
            break
        finally:
            try:
                journal.record('progress', results=results[-1:] if not failed else results)
            except OSError as e:
                # Il backup precedente alla modifica è già su disco; non proseguire senza tracciabilità.
                print(f'ERRORE scrittura backup: {e}. Interrompo il batch.', file=sys.stderr)
                failed=True
                recorded={x['job'] for x in results}
                results.extend(dict(job=x['job'],result='NON ESEGUITO',detail='backup non scrivibile') for x in pending[i:] if x['job'] not in recorded)
                break
            elapsed=time.monotonic()-begin
            print(f'Avanzamento: {i}/{len(pending)} | Job: {duration(time.monotonic()-started)} | Trascorso: {duration(elapsed)} | Residuo stimato: {duration(elapsed/i*(len(pending)-i))}')
        if i<len(pending): time.sleep(1)
    counts=Counter(r['result'] for r in results)
    heading('ESITO AGGIORNAMENTO | riepilogo finale')
    print(f'\nAggiornati: {counts["OK"]} | Saltati: {counts["SKIP"]} | Falliti: {counts["KO"]} | Non eseguiti: {counts["NON ESEGUITO"]}')
    try:
        journal.record('summary', results=results, counts=dict(counts), interrupted=failed)
    except OSError as e:
        print(f'Riepilogo non salvato: {e}', file=sys.stderr); failed=True
    heading('REGISTRO DI SICUREZZA (backup locale)')
    print(f'  File: {journal.path}')
    if failed:
        print(color('  Aggiornamento interrotto: non ripetere il massivo prima delle verifiche.', 'error'))
        print('  Conserva il file e verifica su CDE i job con esito KO o incerto, anche lo stato di pausa.')
        print('  Per consultazione e recupero assistito: README.md, sezione Output e backup locali.')
    else:
        print('  Nessuna azione richiesta sul file se tutto funziona correttamente.')
        print('  Conservalo fino alle verifiche dei job; poi segui le regole di conservazione del team.')
    print('  Non caricare il file su GitHub. Non è uno script di ripristino.\n')
    return 1 if failed else 0

def overview(cde,jobs):
    now=utcnow(); enabled=[j for j in jobs if schedule(j).get('enabled') is True]
    count=Counter(); rows=[]
    for j in jobs:
        s=schedule(j); en=s.get('enabled') is True; paused=s.get('paused') is True
        end=None; notes=[]
        try: end=timestamp(s.get('end')); start=timestamp(s.get('start'))
        except ValueError: notes.append('timestamp non valido'); start=None
        if not en: state='NON SCHEDULATO'
        elif end and end<=now: state='SCADUTO'
        elif paused: state='IN PAUSA'
        elif start and start>now: state='NON ANCORA ATTIVO'
        else: state='ABILITATO'
        count[state]+=1
        if en:
            if not end: count['senza end']+=1
            elif now<end<=now+timedelta(days=30): count['entro 30 giorni']+=1
            if s.get('catchup'): notes.append('catchup=true')
            if s.get('dependsOnPast'): notes.append('dependsOnPast=true')
            if not s.get('cronExpression'): notes.append('cron assente')
            if not s.get('nextExecution'): notes.append('marker assente (da verificare)')
            if start and end and start>=end: notes.append('intervallo incoerente')
        rows.append(dict(job=j['name'],state=state,type=j.get('type'),cron=s.get('cronExpression'),start=s.get('start'),end=s.get('end'),marker=s.get('nextExecution'),paused=paused,note='; '.join(notes)))
    heading('RIEPILOGO VIRTUAL CLUSTER')
    print(f'Job totali: {len(jobs)} | Schedulati: {len(enabled)} | Non schedulati: {len(jobs)-len(enabled)}')
    for k in ('ABILITATO','IN PAUSA','SCADUTO','NON ANCORA ATTIVO','senza end','entro 30 giorni'): print(f'  {k}: {count[k]}')
    issues=[r for r in rows if r['state'] not in ('NON SCHEDULATO','ABILITATO') or r['note']]
    print(f'  Configurazioni con segnalazioni: {len(issues)}')
    for r in issues: print('  '+job_name(r['job'])+' | '+status_text(r['state'])+(' | '+r['note'] if r['note'] else ''))
    return dict(total=len(jobs), scheduled=len(enabled), counts=dict(count), jobs=rows)

def ask_number(prompt, default, lo, hi, integer=True):
    import math
    while True:
        raw=ask(f'{prompt} [Invio = {default}, {lo}-{hi}]: ').strip()
        if not raw: return default
        try:
            value=int(raw) if integer else float(raw.replace(',','.'))
            if math.isfinite(value) and lo<=value<=hi: return value
        except ValueError: pass
        print('Valore non valido, riprova.')

def valid_history(runs, names):
    grouped={name:{} for name in names}
    for r in runs:
        if not isinstance(r,dict) or r.get('job') not in grouped or r.get('status')!='succeeded': continue
        try:
            a,b=timestamp(r.get('started')),timestamp(r.get('ended'))
            if not a or not b or b<=a: continue
        except (ValueError,TypeError,OverflowError): continue
        rid=r.get('id')
        sa=a.isoformat(timespec='microseconds').replace('+00:00','Z'); sb=b.isoformat(timespec='microseconds').replace('+00:00','Z')
        key=str(rid) if rid is not None else (sa,sb)
        grouped[r['job']][key]=dict(id=rid,started=sa,ended=sb,seconds=(b-a).total_seconds())
    return grouped

def load_history(cde,names,n):
    names=list(dict.fromkeys(names)); data={name:{} for name in names}; errors=[]
    if len(names)>1:
        print('Lettura dello storico complessivo delle run positive...',flush=True)
        try: data=valid_history(cde.successful_runs(),names)
        except CDEError as e: errors.append('Storico globale non disponibile: '+str(e))
    for name in names:
        if len(data[name])>=n: continue
        print('Verifica storico individuale: '+job_name(name),flush=True)
        try: data[name].update(valid_history(cde.successful_runs(name),[name])[name])
        except CDEError as e: errors.append(name+': '+str(e))
    stats={}
    for name,records in data.items():
        rows=sorted(records.values(),key=lambda r:(r['started'],r['ended'],str(r['id'])),reverse=True)[:n]
        times=[r['seconds'] for r in rows]
        stats[name]=dict(count=len(rows),requested=n,runs=rows,mean=sum(times)/len(times) if times else None,minimum=min(times) if times else None,maximum=max(times) if times else None)
    return stats,errors

def describe_view(cde,jobs,args,interactive=False):
    details=[]; n=args.history_runs
    if interactive: history,errors={},[]
    else: history,errors=load_history(cde,[j['name'] for j in jobs],n)
    for error in errors: print(color(error,'warn'))
    for i,j in enumerate(jobs,1):
        d=cde.describe(j['name']); s=schedule(d); sp=d.get('spark') or {}
        heading(f'[{i}/{len(jobs)}] '+job_name(d['name']))
        print(f'  Tipo: {d.get("type","n/d")} | File: {sp.get("file","n/d")}')
        print(color('  SCHEDULE'))
        for k in ('enabled','paused','cronExpression','start','end','catchup','dependsOnPast'):
            print(f'  {k:16}: {s.get(k,False if k in ("enabled","paused","catchup","dependsOnPast") else "n/d")}')
        print('  Marker CDE      :',schedule(j).get('nextExecution','n/d'),' (da job list; non necessariamente prossimo avvio)')
        if interactive:
            try:
                st=schedule_state(d)
                if st['enabled'] and not st['paused'] and st['start']:
                    nx=next_occurrence(max(utcnow(),st['start']-timedelta(minutes=1)),supported_cron(st['cronExpression']))
                    if st['end'] is None or nx<st['end']: print('  Prossimo avvio teorico:',iso(nx))
                    else: print('  Nessun prossimo avvio entro endDate.')
            except (ValueError,TypeError): print('  Prossimo avvio teorico non determinabile.')
        print(color('  RISORSE SPARK (stima configurata)')); print_resources(d,args)
        if interactive:
            detail_menu(cde,d,args)
            details.append(dict(configuration=d))
            continue
        h=history[j['name']]
        print(color('  STORICO ULTIME RUN POSITIVE | UTC'))
        print(f'  Run valide: {h["count"]}/{n}')
        for r in h['runs']: print(f'    #{r["id"]} | {r["started"]} | {duration(round(r["seconds"]))}')
        if h['count']:
            print(f'  Media: {duration(round(h["mean"]))} | Min: {duration(round(h["minimum"]))} | Max: {duration(round(h["maximum"]))}')
            if h['count']<n: print(color(f'  Disponibili solo {h["count"]} run valide delle {n} richieste: la stima usa tutte quelle recuperate.','warn'))
        else: print(color('  Nessuna durata valida disponibile.','warn'))
        details.append(dict(configuration=d,history=h))
    return dict(jobs=details,history_errors=errors)

def print_resources(j,args):
    r=resources(j,args.overhead)
    if not r:
        print('  Risorse: non stimabili dai dati disponibili'); return
    print(f'  Driver: {r["driver"]:g} GiB, {r["driver_cores"]} core | Executor: {r["executor"]:g} GiB, {r["executor_cores"]} core | istanze: {r["min_ex"]}-{r["max_ex"]}')
    print(f'  Memoria stimata (+{args.overhead:g}%): {r["min_gib"]:.1f}–{r["max_gib"]:.1f} GiB | max/capacità ipotizzata: {r["max_gib"]/args.capacity:.1%}')

# Analisi read-only: calendario cron teorico, non telemetria né run reali.
DAYS_IT=('Lun','Mar','Mer','Gio','Ven','Sab','Dom')
SCENARIOS=('ordinary','cautious')
SCENARIO_LABEL={'ordinary':'ORDINARIO','cautious':'PRUDENZIALE'}

def duration_model(jobs,history,fallback,fixed=None,overhead=40):
    model={}
    for j in jobs:
        name=j['name']; h=history.get(name,{})
        if fixed is not None:
            a=b=fixed*60; source='fissa'
        elif h.get('count'):
            a=h['mean']; b=h['maximum']; source='storico'
        else:
            a=b=fallback*60; source='ipotizzata'
        # Conservativo: arrotondamento per eccesso al secondo, non al minuto.
        import math
        model[name]=dict(ordinary=max(1,math.ceil(a)),cautious=max(1,math.ceil(b)),source=source,count=h.get('count',0),requested=h.get('requested',0),resources=resources(j,overhead))
    return model

def calendar_runs(jobs,first,last,overhead,model):
    runs=[]; excluded=Counter(); unmodelled=[]; warnings=[]; unknown=set()
    for j in jobs:
        name=j['name']; dur=model[name]
        try:
            st=schedule_state(j)
            if not st['enabled']: excluded['non schedulato']+=1; continue
            if st['paused']: excluded['in pausa']+=1; continue
            window=first-timedelta(seconds=dur['cautious'])
            if st['end'] is not None and st['end']<=window: excluded['scaduto']+=1; continue
            if st['start'] is not None and st['start']>=last: excluded['non ancora attivo nel periodo']+=1; continue
            if not st['start'] or not st['cronExpression']: raise ValueError('start/cron assente')
            if st['end'] is not None and st['end']<=st['start']: raise ValueError('intervallo non valido')
            c=supported_cron(st['cronExpression'])
            if st['catchup'] or st['dependsOnPast']: warnings.append(name+': flag speciali; possibili run non modellate')
        except (ValueError,TypeError,KeyError,OverflowError) as e:
            unmodelled.append(name+': '+str(e)); continue
        r=resources(j,overhead)
        # Ogni run può attraversare più giorni. Non fermarla alla endDate: quella limita gli avvii.
        day=max(window,st['start']).replace(hour=0,minute=0,second=0,microsecond=0)
        hours=sorted(c['hr']); minutes=sorted(c['mi'])
        while day<last:
            if matches(day.replace(hour=hours[0],minute=minutes[0]),c):
                for h in hours:
                    for m in minutes:
                        t=day.replace(hour=h,minute=m)
                        if t<window or t>=last or t<st['start'] or (st['end'] and t>=st['end']): continue
                        if r is None: unknown.add(name)
                        runs.append((t,name,r['min_gib'] if r else None,r['max_gib'] if r else None))
                        if len(runs)>100000: raise ValueError('Oltre 100.000 occorrenze: ridurre l’orizzonte di analisi')
            day+=timedelta(days=1)
    return runs,excluded,unmodelled,warnings,sorted(unknown)

def simulate_day(runs,day,scenario,capacity,model):
    end=day+timedelta(days=1); events={}
    for i,(t,name,lo,hi) in enumerate(runs):
        a=max(t,day); b=min(t+timedelta(seconds=model[name][scenario]),end)
        if a>=b: continue
        events.setdefault(a,[[],[]])[1].append((i,name,lo,hi))
        events.setdefault(b,[[],[]])[0].append(i)
    times=sorted(events); active={}; peak_min=peak_max=0.; concurrency=0; incomplete=False; intervals=[]
    for k,t in enumerate(times[:-1]):
        for i in events[t][0]: active.pop(i,None)
        for i,name,lo,hi in events[t][1]: active[i]=(name,lo,hi)
        if not active: continue
        b=times[k+1]
        lo=sum(x[1] for x in active.values() if x[1] is not None)
        hi=sum(x[2] for x in active.values() if x[2] is not None)
        missing=any(x[1] is None or x[2] is None for x in active.values())
        incomplete|=missing; concurrency=max(concurrency,len(active)); peak_min=max(peak_min,lo); peak_max=max(peak_max,hi)
        if hi>=.8*capacity or missing or len(active)>1:
            names=Counter(x[0] for x in active.values())
            row=dict(start=iso(t),end=iso(b),min_gib=lo,max_gib=hi,unknown=missing,jobs=dict(sorted(names.items())))
            if intervals and intervals[-1]['end']==row['start'] and all(intervals[-1][key]==row[key] for key in ('min_gib','max_gib','unknown','jobs')):
                intervals[-1]['end']=row['end']
            else: intervals.append(row)
    return dict(peak_min_gib=peak_min,peak_max_gib=peak_max,concurrency=concurrency,incomplete=incomplete,intervals=intervals)

def saturation_report(jobs,first,days,capacity,overhead,model):
    last=first+timedelta(days=days)
    runs,excluded,unmodelled,warnings,unknown=calendar_runs(jobs,first,last,overhead,model)
    buckets={}; starts_by_job=Counter(); bucket_entries=0
    for run in runs:
        t,name,*_=run
        if t>=first: starts_by_job[(t.date().isoformat(),name)]+=1
        d=max(first,t.replace(hour=0,minute=0,second=0,microsecond=0))
        finish=t+timedelta(seconds=model[name]['cautious'])
        while d<last and d<finish:
            buckets.setdefault(d,[]).append(run); bucket_entries+=1
            if bucket_entries>500000: raise ValueError('Troppe sovrapposizioni teoriche: ridurre l’orizzonte')
            d+=timedelta(days=1)
    rows=[]
    for i in range(days):
        d=first+timedelta(days=i); relevant=buckets.get(d,[])
        a,b=(simulate_day(relevant,d,k,capacity,model) for k in SCENARIOS)
        starts=[x for x in relevant if x[0]>=d and x[0]<d+timedelta(days=1)]
        partial=bool(unmodelled or warnings or a['incomplete'] or b['incomplete'])
        if a['peak_max_gib']>capacity+1e-9: status='CRITICO'
        elif b['peak_max_gib']>capacity+1e-9: status='ATTENZIONE'
        elif partial: status='PARZIALE'
        elif b['peak_max_gib']>=capacity*.8: status='VICINO AL LIMITE'
        elif not b['concurrency']: status='NESSUNA RUN'
        else: status='REGOLARE'
        rows.append(dict(date=d.date().isoformat(),weekday=DAYS_IT[d.weekday()],starts=len(starts),distinct_jobs=len({x[1] for x in starts}),status=status,partial=partial,ordinary=a,cautious=b))
    return dict(capacity_gib=capacity,overhead_percent=overhead,days=rows,excluded=dict(excluded),unmodelled=unmodelled,warnings=warnings,unknown_resources=unknown,occurrences=len(runs),starts_by_job=[[d,n,c] for (d,n),c in sorted(starts_by_job.items())])

def conflicts(report,scenario):
    cap=report['capacity_gib']
    return [x for r in report['days'] for x in r[scenario]['intervals'] if x['max_gib']>cap+1e-9]

def conflict_metrics(report):
    result={}
    for key in SCENARIOS:
        items=conflicts(report,key); cap=report['capacity_gib']
        secs=[(timestamp(x['end'])-timestamp(x['start'])).total_seconds() for x in items]
        result[key]=dict(peak=max((r[key]['peak_max_gib'] for r in report['days']),default=0),over_seconds=sum(secs),excess_gib_seconds=sum((x['max_gib']-cap)*s for x,s in zip(items,secs)),days=len({x['start'][:10] for x in items}))
    return result

def no_new_conflicts(before,after):
    # Verifica esatta per ciascuno scenario: ogni intervallo critico nuovo deve
    # essere interamente coperto da intervalli già critici nel piano originale.
    for key in SCENARIOS:
        baseline=[]
        for x in conflicts(before,key):
            a,b=x['start'],x['end']
            if baseline and a<=baseline[-1][1]: baseline[-1][1]=max(b,baseline[-1][1])
            else: baseline.append([a,b])
        i=0
        for x in conflicts(after,key):
            while i<len(baseline) and baseline[i][1]<=x['start']: i+=1
            if i==len(baseline) or baseline[i][0]>x['start'] or baseline[i][1]<x['end']: return False
    return True

def shift_cron(expr,delta):
    # Solo un orario giornaliero esplicito. Non cambiare giorno o frequenza.
    p=expr.split()
    if len(p)!=5 or not all(re.fullmatch(r'\d+',v) for v in p[:2]): return None
    t=int(p[1])*60+int(p[0])+delta
    if not 0<=t<1440: return None
    return ' '.join([str(t%60),str(t//60),*p[2:]])

def suggestions(jobs,first,days,capacity,overhead,model,base,exclude=()):
    old=conflict_metrics(base)
    if not any(old[k]['over_seconds'] for k in SCENARIOS): return dict(items=[],tested=0,note='Nessun superamento noto da risolvere.')
    if base['occurrences']>10000: return dict(items=[],tested=0,note='Calendario molto fitto: ridurre l’orizzonte per calcolare i suggerimenti.')
    involved=Counter()
    for k in SCENARIOS:
        for x in conflicts(base,k):
            seconds=(timestamp(x['end'])-timestamp(x['start'])).total_seconds()
            for name in x['jobs']: involved[name]+=seconds
    eligible=[]
    for j in jobs:
        if j['name'] not in involved or j['name'] in exclude: continue
        try:
            st=schedule_state(j)
            if st['catchup'] or st['dependsOnPast']: continue
            expr=st['cronExpression']
            if not expr or not shift_cron(expr,0) or resources(j,overhead) is None: continue
            eligible.append(j)
        except (ValueError,TypeError): continue
    eligible.sort(key=lambda j:(-involved[j['name']],j['name']))
    # Limite esplicito per mantenere il tool interattivo: ricerca locale, non ottimo globale.
    tested=0; items=[]
    for j in eligible[:6]:
        name=j['name']; expr=schedule(j)['cronExpression']
        for delta in (5,-5,10,-10,15,-15,20,-20,30,-30,45,-45,60,-60):
            new=shift_cron(expr,delta)
            if not new: continue
            changed=copy.deepcopy(j); changed['schedule']['cronExpression']=new
            try: candidate=saturation_report([changed if x['name']==name else x for x in jobs],first,days,capacity,overhead,model)
            except (ValueError,OverflowError): continue
            tested+=1
            # Non ottenere un miglioramento eliminando esecuzioni vicino ai limiti start/end.
            if candidate['starts_by_job']!=base['starts_by_job'] or not no_new_conflicts(base,candidate): continue
            if any(candidate[k]!=base[k] for k in ('unmodelled','warnings','unknown_resources')): continue
            m=conflict_metrics(candidate)
            if any(m[k]['peak']>old[k]['peak']+1e-9 or m[k]['excess_gib_seconds']>old[k]['excess_gib_seconds']+1e-6 or m[k]['over_seconds']>old[k]['over_seconds']+1e-6 for k in SCENARIOS): continue
            if not any(m[k]['excess_gib_seconds']<old[k]['excess_gib_seconds']-1e-6 for k in SCENARIOS): continue
            resolved=not any(m[k]['over_seconds'] for k in SCENARIOS)
            items.append(dict(job=name,delta_minutes=delta,cron_before=expr,cron_after=new,before=old,after=m,resolved=resolved))
    def ranking(x):
        m=x['after']; shift=abs(x['delta_minutes'])
        return (not x['resolved'],m['ordinary']['excess_gib_seconds'],m['cautious']['excess_gib_seconds'],m['cautious']['peak'],sum(c for _,name,c in base['starts_by_job'] if name==x['job']),shift,x['job'])
    items.sort(key=ranking)
    # Tre alternative, al massimo una per job. Non sono cumulabili senza nuova simulazione.
    unique=[]; seen=set()
    for x in items:
        if x['job'] not in seen: unique.append(x); seen.add(x['job'])
    note='Ricerca locale: un solo job per alternativa, +/- 5–60 min (stesso giorno), fino a 6 job coinvolti; cron con un solo orario esplicito.'
    if not unique: note+=' Nessuna soluzione migliorativa senza nuovi conflitti trovata: valutare più spostamenti coordinati o le risorse.'
    return dict(items=unique[:3],tested=tested,note=note)

def coordinated_suggestions(jobs,first,days,capacity,overhead,model,base):
    # Ricerca greedy limitata: ogni passo viene simulato sull'intero calendario.
    # Non sommare benefici di alternative indipendenti; rivalutare il piano cumulativo.
    initial=suggestions(jobs,first,days,capacity,overhead,model,base)
    current=base; working=copy.deepcopy(jobs); moves=[]; tested=initial['tested']
    step=initial
    for i in range(3):
        if not step['items']: break
        x=step['items'][0]
        name=x['job']
        for j in working:
            if j['name']==name: j['schedule']['cronExpression']=x['cron_after']
        current=saturation_report(working,first,days,capacity,overhead,model)
        moves.append(x)
        if x['resolved'] or i==2: break
        step=suggestions(working,first,days,capacity,overhead,model,current,exclude={m['job'] for m in moves})
        tested+=step['tested']
    # Controllo anche rispetto alla baseline originale, non solo al passo precedente.
    old=conflict_metrics(base); new=conflict_metrics(current)
    if current['starts_by_job']!=base['starts_by_job'] or not no_new_conflicts(base,current) or any(current[k]!=base[k] for k in ('unmodelled','warnings','unknown_resources')) or any(new[k][m]>old[k][m]+1e-6 for k in SCENARIOS for m in ('peak','over_seconds','excess_gib_seconds')):
        raise ValueError('Piano cumulativo non coerente: nessun suggerimento applicato')
    return dict(**initial,coordinated=dict(moves=moves,before=old,after=new,tested=tested,
        resolved=not any(new[k]['over_seconds'] for k in SCENARIOS),
        residual=[dict(date=r['date'],ordinary=r['ordinary']['peak_max_gib'],cautious=r['cautious']['peak_max_gib']) for r in current['days'] if r['status'] in ('CRITICO','ATTENZIONE')],
        residual_groups=critical_groups(current)))

def clock_text(value,day):
    return '24:00' if value[:10]!=day else value[11:16]+(':'+value[17:19] if value[17:19]!='00' else '')

def range_text(start,end,day):
    return clock_text(start,day)+'–'+clock_text(end,day)

def critical_groups(report):
    # Un blocco per fascia contigua, accorpando date con profili identici.
    # Non usare la concorrenza giornaliera per raggruppare i conflitti.
    cap=report['capacity_gib']; groups={}
    for row in report['days']:
        day=row['date']; entries=sorted((x['start'],x['end'],k,x) for k in SCENARIOS for x in row[k]['intervals'] if x['max_gib']>cap+1e-9)
        blocks=[]
        for a,b,k,x in entries:
            if not blocks or a>blocks[-1]['end']:
                blocks.append(dict(start=a,end=b,ordinary=[],cautious=[]))
            block=blocks[-1]; block['end']=max(block['end'],b); block[k].append(x)
        for block in blocks:
            def signature(x):
                return (range_text(x['start'],x['end'],day),round(x['min_gib'],8),round(x['max_gib'],8),x['unknown'],tuple(x['jobs'].items()))
            key=(row['weekday'],tuple(tuple(signature(x) for x in block[k]) for k in SCENARIOS))
            if key not in groups: groups[key]=dict(block=block,dates=[],weekday=row['weekday'])
            groups[key]['dates'].append(day)
    return list(groups.values())

def show_critical(report,model,runs=()):
    cap=report['capacity_gib']; groups=critical_groups(report)
    heading('CRITICITÀ | solo fasce oltre capacità')
    if not groups: print('  Nessun superamento noto.'); return
    for i,g in enumerate(groups,1):
        b=g['block']; dates=g['dates']; day=dates[0]
        peak=max(x['max_gib'] for k in SCENARIOS for x in b[k])
        min_over=any(x['min_gib']>cap+1e-9 for k in SCENARIOS for x in b[k])
        status='CRITICO' if b['ordinary'] else 'ATTENZIONE'
        print(); print(color(f'  [{i}] {g["weekday"]} {range_text(b["start"],b["end"],day)} UTC | {status}', 'error' if b['ordinary'] else 'warn'))
        print('      Date: '+', '.join(datetime.fromisoformat(d).strftime('%d/%m/%Y') for d in dates))
        print('      '+color(f'Picco stimato: {peak:.1f} / {cap:g} GiB (+{peak-cap:.1f} GiB) | {peak/cap:.0%} della capacità','error' if b['ordinary'] else 'warn'))
        print('      '+('Superamento anche con risorse minime.' if min_over else 'Superamento con risorse massime, non con le minime note.'))
        names=sorted({name for k in SCENARIOS for x in b[k] for name in x['jobs']})
        labels={name:str(i+1) for i,name in enumerate(names)}
        print('      '+color('JOB COINVOLTI | avvio e fine stimata'))
        if len(dates)>1:
            print('      Orari riferiti alla prima data del gruppo; le fasce oltre soglia coincidono.')
        for name in names:
            h=model[name]; r=h['resources']; mem=f'{r["min_gib"]:.1f}–{r["max_gib"]:.1f} GiB' if r else 'non stimabile'
            source=f'{h["count"]}/{h["requested"]} run' if h['source']=='storico' else h['source']
            print('\n        '+job_name(f'({labels[name]}) {name}'))
            # Ricaviamo le vere partenze dal calendario, non dall'inizio dell'intervallo critico.
            relevant=[]
            for t,jn,_,_ in runs:
                if jn!=name: continue
                if any(name in x['jobs'] and t<timestamp(x['end']) and t+timedelta(seconds=h[k])>timestamp(x['start']) for k in SCENARIOS for x in b[k]):
                    relevant.append(t)
            for t in sorted(set(relevant)):
                def fmt(dt):
                    return dt.strftime('%H:%M:%S' if dt.second else '%H:%M') if dt.date().isoformat()==day else dt.strftime('%d/%m %H:%M:%S' if dt.second else '%d/%m %H:%M')
                print(f'            Avvio {fmt(t)} | Fine media {fmt(t+timedelta(seconds=h["ordinary"]))} | Fine prud. {fmt(t+timedelta(seconds=h["cautious"]))} UTC')
            print(f'            RAM {mem} | Durata media {duration(h["ordinary"])} / max {duration(h["cautious"])} ({source})')
        print('      '+color('SOVRAPPOSIZIONI OLTRE SOGLIA | RAM massima','error' if b['ordinary'] else 'warn'))
        scenarios=[('ordinary','Media'),('cautious','Prudenziale')]
        if b['ordinary']==b['cautious']: scenarios=[('ordinary','Entrambi')]
        for k,label in scenarios:
            if not b[k]: print(f'        {label}: nessun superamento in questa fascia'); continue
            for x in b[k]:
                active=' + '.join('('+labels[name]+')'+(f' x{count}' if count>1 else '') for name,count in x['jobs'].items())
                print(f'        {label:11} {range_text(x["start"],x["end"],day):17} | {x["max_gib"]:5.1f} GiB | {active}'+(' + RAM ignota' if x['unknown'] else ''))
        # Indicare anche le coppie che da sole eccedono la capacità, utile per scegliere chi spostare.
        pairs=set()
        for k in SCENARIOS:
            for x in b[k]:
                active=list(x['jobs'])
                for i,name in enumerate(active):
                    r=model[name]['resources']
                    if not r: continue
                    if x['jobs'][name]>1 and 2*r['max_gib']>cap+1e-9: pairs.add((name,name))
                    for other in active[i+1:]:
                        rr=model[other]['resources']
                        if rr and r['max_gib']+rr['max_gib']>cap+1e-9: pairs.add(tuple(sorted((name,other))))
        if pairs:
            print('      Coppie da separare per evitare il superamento con RAM massima:')
            for a,z in sorted(pairs):
                print('        '+job_name(a)+' + '+job_name(z)+f' = {model[a]["resources"]["max_gib"]+model[z]["resources"]["max_gib"]:.1f} GiB')
        else:
            print('      Valutare lo spostamento di uno o più job coinvolti.')

def show_calendar(report):
    heading('CALENDARIO COMPLETO | UTC')
    print('Data UTC         Avvii   Picco medio   Picco prud.   Utilizzo max   Run simult.   Esito')
    cap=report['capacity_gib']
    for r in report['days']:
        a,b=r['ordinary'],r['cautious']; st=r['status']; kind='error' if st=='CRITICO' else 'warn' if st in ('ATTENZIONE','PARZIALE','VICINO AL LIMITE') else 'ok'
        print(f'{r["date"]} {r["weekday"]}  {r["starts"]:5} {a["peak_max_gib"]:9.1f} GiB {b["peak_max_gib"]:9.1f} GiB {b["peak_max_gib"]/cap:12.0%} {b["concurrency"]:12}   '+color(st+(' *' if r['partial'] else ''),kind))
    print('Picchi di RAM massima configurata; concorrenza massima prudenziale del giorno. * Analisi parziale.')

def schedule_label(expr):
    """Etichetta informativa; non interviene nella validazione cron."""
    try:
        mi,hr,dom,mon,dow=expr.split()
        clock=cron_clock(expr)
        if dom==mon=='*':
            names={'0':'Dom','1':'Lun','2':'Mar','3':'Mer','4':'Gio','5':'Ven','6':'Sab','7':'Dom'}
            freq='Ogni giorno' if dow=='*' else ('Lun–Ven' if dow=='1-5' else names.get(dow,'giorni '+dow))
        elif dow=='*': freq=f'giorno {dom}, mesi {mon}' if mon!='*' else f'Mensile, giorno {dom}'
        else: freq=f'giorni {dom}/{dow}, mesi {mon}'
        return f'{clock} | {freq}'
    except (ValueError,TypeError,AttributeError,IndexError): return str(expr or 'cron assente')


def show_impact(old,new,capacity):
    """Solo presentazione: usa le metriche già calcolate dal simulatore."""
    heading('IMPATTO STIMATO | calendario attuale → calendario proposto')
    print(f'  Soglia RAM ipotizzata: {capacity:g} GiB; confronto sullo stesso periodo e storico.')
    for k,label in [('ordinary','Medio'),('cautious','Prudenziale')]:
        a,b=old[k],new[k]; delta=a['peak']-b['peak']
        pct=f' ({abs(delta)/a["peak"]*100:.1f}%)' if a['peak'] else ''
        change=('riduzione' if delta>1e-9 else 'aumento' if delta<-1e-9 else 'invariato')
        print('  '+color(f'{label}: picco {a["peak"]:.1f} → {b["peak"]:.1f} GiB | {change} {abs(delta):.1f} GiB{pct}', 'ok' if delta>1e-9 else 'warn' if delta<-1e-9 else 'job'))
        print(f'    Giorni oltre soglia: {a["days"]} → {b["days"]} | Tempo oltre soglia: {duration(a["over_seconds"])} → {duration(b["over_seconds"])}')
        margin=capacity-b['peak']
        print(color(f'    {"Margine residuo stimato" if margin>=0 else "Superamento residuo stimato"}: {abs(margin):.1f} GiB','ok' if margin>=0 else 'warn'))
    print('  La riduzione riguarda il picco di RAM configurata stimata, non il consumo effettivo o il costo.')


def cron_clock(expr):
    p=expr.split(); return f'{int(p[1]):02}:{int(p[0]):02} UTC'

def show_advice(advice,capacity):
    heading('ESEMPIO DI PIANO AUTOMATICO | nessuna modifica')
    print('  Ricerca locale: non conosce priorità operative, dipendenze o SLA.')
    plan=advice['coordinated']; moves=plan['moves']
    if not moves:
        print('  '+('Nessun intervento necessario per i superamenti noti.' if plan['resolved'] else 'Nessuno spostamento migliorativo trovato nei limiti della ricerca.'));
        if not plan['resolved']: print('  Valutare orari, risorse e dipendenze manualmente. '+advice['note'])
        return
    print(color('  Piano individuato: '+('elimina i superamenti noti' if plan['resolved'] else 'riduce i superamenti, ma non li elimina tutti'),'ok' if plan['resolved'] else 'warn'))
    for i,x in enumerate(moves,1):
        print('\n  '+job_name(f'{i}. {x["job"]}'))
        print(f'     {cron_clock(x["cron_before"])} → {cron_clock(x["cron_after"])}  ({"posticipo" if x["delta_minutes"]>0 else "anticipo"} di {abs(x["delta_minutes"])} min)')
        print(f'     Cron: {x["cron_before"]}  →  {x["cron_after"]}')
    show_impact(plan['before'],plan['after'],capacity)
    if plan['residual']:
        print(color('  Criticità residue dopo il piano:', 'warn'))
        for g in plan['residual_groups']:
            b=g['block']; peak=max(x['max_gib'] for k in SCENARIOS for x in b[k]); names=sorted({name for k in SCENARIOS for x in b[k] for name in x['jobs']})
            print(f'    {g["weekday"]} {range_text(b["start"],b["end"],g["dates"][0])} UTC | picco {peak:.1f} GiB | '+', '.join(g['dates']))
            print('      Job: '+', '.join(names))
    print('  Nessuna nuova fascia oltre soglia nota; numero di avvii per job/giorno preservato.')
    print(f'  Simulazioni: {plan["tested"]} | ricerca locale, max 3 job, spostamenti entro 60 min.')
    print('  Verificare dipendenze e SLA prima di modificare gli orari. Nessuna modifica automatica.')
    alternatives=[x for x in advice['items'] if (x['job'],x['cron_after']) not in {(m['job'],m['cron_after']) for m in moves}]
    if alternatives:
        print('\n  Altri spostamenti individuali validi al primo passo (non cumulabili):')
        for x in alternatives:
            print(f'    {job_name(x["job"])}: {cron_clock(x["cron_before"])} → {cron_clock(x["cron_after"])} | giorni oltre soglia (prud.) {x["after"]["cautious"]["days"]}')

def manual_candidate(jobs,first,days,capacity,overhead,model,base,moves):
    # Una copia in memoria del calendario originale. Non esiste alcuna chiamata CDE.
    working=copy.deepcopy(jobs); by_name={j['name']:j for j in working}; result=[]
    for name,delta in moves.items():
        j=by_name[name]; st=schedule_state(j)
        if not st['enabled'] or st['paused'] or st['catchup'] or st['dependsOnPast']:
            raise ValueError('Schedule non ammissibile: '+name)
        expr=st['cronExpression']; supported_cron(expr)
        new=shift_cron(expr,delta)
        if not new: raise ValueError('Spostamento non supportato (orario unico, stesso giorno): '+name)
        j['schedule']['cronExpression']=new
        result.append(dict(job=name,delta_minutes=delta,cron_before=expr,cron_after=new))
    report=saturation_report(working,first,days,capacity,overhead,model)
    return working,report,result

def manual_view(jobs,first,days,capacity,overhead,model,base):
    heading('SIMULA I TUOI SPOSTAMENTI | sola lettura')
    print('  Scegli uno o più job e indica di quanti minuti anticiparli o posticiparli.')
    print('  Ogni prova riparte dal calendario originale, senza modificare CDE.')
    involved={name for k in SCENARIOS for x in conflicts(base,k) for name in x['jobs']}
    eligible=[]
    for j in jobs:
        try:
            st=schedule_state(j); expr=st['cronExpression']
            if not st['enabled'] or st['paused'] or st['catchup'] or st['dependsOnPast'] or not expr: continue
            supported_cron(expr)
            if not shift_cron(expr,0): continue
            eligible.append(j)
        except (ValueError,TypeError): continue
    eligible.sort(key=lambda j:(j['name'] not in involved,j['name'].casefold()))
    if not eligible: print('  Nessuna cron a orario unico disponibile.'); return
    pool=[j for j in eligible if j['name'] in involved] or eligible
    if len(pool)<len(eligible):
        print('  [1] Solo job coinvolti nelle criticità   [2] Tutti i job compatibili   [0] Annulla')
        choice=choose('Scelta [1]: ',{'','1','2','0'})
        if choice=='0': return
        if choice=='2': pool=eligible
    heading('SELEZIONE JOB DA SPOSTARE | simulazione')
    for i,j in enumerate(pool,1): print(f'  {i:2} | {schedule_label(schedule(j)["cronExpression"])} | {job_name(j["name"])}')
    lookup={j['name']:i for i,j in enumerate(pool,1)}
    while True:
        raw=ask('Numeri o nomi separati da virgola (es. 1,3; Invio annulla): ').strip()
        if not raw or raw=='0': return
        try:
            indices=list(dict.fromkeys(lookup[t.strip()] if t.strip() in lookup else int(t.strip()) for t in raw.split(',')))
            if not all(1<=i<=len(pool) for i in indices): raise ValueError()
            break
        except ValueError: print('  Selezione non valida. Riprova.')
    heading('SPOSTAMENTI PROPOSTI | minuti UTC')
    moves={}
    for i in indices:
        j=pool[i-1]; expr=schedule(j)['cronExpression']; name=j['name']
        while True:
            raw=ask(f'  {name} ({cron_clock(expr)}): minuti +/- (es. -10, +15; 0 salta; Invio annulla): ').strip()
            if not raw: return
            try:
                delta=int(raw)
                if not shift_cron(expr,delta): raise ValueError()
                if delta: moves[name]=delta
                break
            except ValueError: print('  Inserire minuti interi che mantengano l’orario nello stesso giorno UTC.')
    if not moves: print('  Nessuno spostamento selezionato.'); return
    heading('CALCOLO IMPATTO | simulazione in memoria')
    print('  Ricalcolo il calendario completo per confrontare picchi, durata dei conflitti e nuovi superamenti.',flush=True)
    working,after,changes=manual_candidate(jobs,first,days,capacity,overhead,model,base,moves)
    heading('RISULTATO DELLA TUA SIMULAZIONE')
    for x in changes:
        print(f'  {job_name(x["job"])}: {cron_clock(x["cron_before"])} → {cron_clock(x["cron_after"])} ({x["delta_minutes"]:+d} min)')
        print(f'    Cron simulata: {x["cron_after"]}')
    old,new=conflict_metrics(base),conflict_metrics(after)
    show_impact(old,new,capacity)
    same=after['starts_by_job']==base['starts_by_job']; no_new=no_new_conflicts(base,after)
    comparable=all(after[k]==base[k] for k in ('unmodelled','warnings','unknown_resources'))
    print(color('  Numero di avvii per job/giorno preservato.' if same else '  ATTENZIONE: numero di avvii modificato (verificare start/end e confini del periodo).','ok' if same else 'warn'))
    print(color('  Nessuna nuova fascia oltre soglia nota.' if no_new else '  ATTENZIONE: introdotte nuove fasce oltre soglia.','ok' if no_new else 'warn'))
    # Il modello contiene anche job esclusi (non schedulati, in pausa, ecc.).
    # Non attribuire al piano durate ipotizzate di job che non vi compaiono.
    before_runs=calendar_runs(jobs,first,first+timedelta(days=days),overhead,model)[0]
    after_runs=calendar_runs(working,first,first+timedelta(days=days),overhead,model)[0]
    involved_names={x[1] for x in before_runs+after_runs}
    fallback_names=sorted(name for name in involved_names if model[name]['source']=='ipotizzata')
    reduced_names=sorted(name for name in involved_names if model[name]['source']=='storico' and model[name]['count']<model[name]['requested'])
    if not comparable or any(r['partial'] for r in after['days']):
        print(color('  ATTENZIONE: calendario parziale o informazioni di configurazione mancanti.','warn'))
    if fallback_names:
        print(color('  Durata ipotizzata per i job presenti nel calendario: '+', '.join(fallback_names),'warn'))
    if reduced_names:
        print(color('  Durate basate su meno run valide di quelle richieste: '+', '.join(reduced_names),'warn'))
    if any(model[name]['source']=='fissa' for name in involved_names):
        print('  Durate fisse scelte dall’utente, non misurate.')
    if same and no_new and comparable and not any(new[k]['over_seconds'] for k in SCENARIOS):
        print(color('  Nessun superamento noto nel piano simulato.','ok'))
    else:
        show_critical(after,model,after_runs)
    print(f'  Margine minimo stimato nel piano prudenziale: {capacity-new["cautious"]["peak"]:.1f} GiB')
    for day in after['days']:
        if day['status']=='VICINO AL LIMITE': print(f'    Vicino al limite: {day["date"]} | {day["cautious"]["peak_max_gib"]:.1f} GiB')
    print('  Nessuna modifica su CDE. Verificare dipendenze, SLA e capacità effettiva.')
    return changes

def validate_clock_change(old,new):
    # Only change the clock, not the calendar/frequency or cron dialect.
    a,b=old.split(),new.split()
    if len(a)!=5 or len(b)!=5 or a[2:]!=b[2:] or not shift_cron(old,0) or not shift_cron(new,0):
        raise ValueError('Consentito un solo orario UTC, stessi campi giorno/mese/giorno settimana')
    supported_cron(old); supported_cron(new)
    return ' '.join([str(int(b[0])),str(int(b[1])),*a[2:]])


def check_cron_window(job,new,now=None):
    # Guard against moving a run across now, imminent runs and schedule boundaries.
    now=now or utcnow(); st=schedule_state(job); old=st['cronExpression']
    validate_clock_change(old,new)
    if job.get('type')!='spark' or not st['enabled'] or st['paused'] or st['catchup'] or st['dependsOnPast']:
        raise ValueError('Cron: ammessi solo Spark abilitati, non in pausa, flag speciali false')
    if not st['start'] or st['start']>=now or (st['end'] and st['end']<=now):
        raise ValueError('Cron: schedule non attiva o già scaduta')
    a,b=(supported_cron(x) for x in (old,new))
    p1,p2=(prev_occurrence(now,c) for c in (a,b)); n1,n2=(next_occurrence(now,c) for c in (a,b))
    if p1.date()!=p2.date() or n1.date()!=n2.date():
        raise ValueError('Cron: lo spostamento attraverserebbe un avvio già trascorso; riprovare lontano dalla fascia')
    if min((now-p1).total_seconds(),(now-p2).total_seconds(),(n1-now).total_seconds(),(n2-now).total_seconds())<=3600:
        raise ValueError('Cron: avvio vecchio o nuovo entro 60 minuti; scegliere una finestra di manutenzione')
    if min(p1,p2)<st['start'] or (st['end'] and max(n1,n2)>=st['end']):
        raise ValueError('Cron: avvio vicino al limite start/end; verifica manuale')
    marker=timestamp(schedule(job).get('nextExecution'))
    if marker not in (p1,n1):
        raise ValueError('Cron: marker CDE di partenza non coerente con le occorrenze teoriche; verifica manuale')
    return iso(n1),iso(n2)


def cron_marker_check(before,live,new):
    # CDE observed marker may refer to last, not next theoretical execution.
    # Old/new adjacent occurrences only; do not force a marker via CLEAR.
    now=utcnow(); c1=supported_cron(schedule(before)['cronExpression']); c2=supported_cron(new)
    allowed={f(now,c) for c in (c1,c2) for f in (prev_occurrence,next_occurrence)}
    if timestamp(schedule(live).get('nextExecution')) not in allowed:
        raise CDEError('Cron: marker CDE finale inatteso; controllare manualmente la schedule')


RUN_TERMINAL={'succeeded','failed','killed','cancelled','canceled','timedout','timed_out'}


def run_snapshot(cde,name):
    runs=cde.json_command(list,'run','list','--filter',f'job[eq]{name}')
    result={}
    for r in runs:
        if not isinstance(r,dict) or r.get('job')!=name or r.get('id') is None or not str(r['id']).strip():
            raise CDEError('Storico run non verificabile per '+name+'; interrompo')
        rid=str(r['id'])
        if rid in result: raise CDEError('ID run duplicato; interrompo')
        result[rid]=r
    return result


def observe_cron_runs(cde,name,baseline,journal):
    # Solo dopo UNPAUSE. Nessuna attribuzione automatica del trigger alla modifica.
    runs=run_snapshot(cde,name)
    new={rid:r for rid,r in runs.items() if rid not in baseline}
    if not new:
        check_no_active_runs(cde,name,baseline, snapshot=runs)
        return
    heading('NUOVO AVVIO RILEVATO | decisione operatore')
    print(color('  Possibile avvio inatteso dopo modifica cron: il trigger non è identificato.','warn'))
    for rid,r in new.items():
        print(f'  Job: {name} | Run ID: {rid} | Stato: {r.get("status")} | Avvio UTC: {r.get("started") or "non disponibile"} | Utente: {r.get("user") or "non disponibile"}')
    journal.record('unexpected_runs',job=name,runs=list(new.values()))
    print('  La schedule verrà lasciata in pausa di sicurezza, se confermata, indipendentemente dalla decisione sulla run.')
    print('  KILL non annulla scritture o notifiche già prodotte; verificare che non sia una run manuale autorizzata.')
    # Fail closed: solo una nuova run con ID numerico e stato running riconosciuto.
    if len(new)==1:
        rid,r=next(iter(new.items()))
        if re.fullmatch(r'[0-9]+',rid) and str(r.get('status','')).lower()=='running':
            decision=choose('  [1] Lascia in esecuzione (default)  [2] Richiedi KILL della sola run '+rid+' : ',{'','1','2'})
            journal.record('unexpected_run_decision',job=name,run_id=rid,decision='request_kill' if decision=='2' else 'keep')
            if decision=='2':
                if ask('  Per terminare SOLO la run '+rid+', scrivi KILL '+rid+': ').strip()=='KILL '+rid:
                    # Controllo compatibilità CLI e identità/stato immediatamente prima del comando.
                    helptext=cde.command('run','kill','--help')
                    if not re.search(r'(?<![\w-])--id(?![\w-])',helptext):
                        raise CDEError('Flag --id non verificato per run kill: nessun kill eseguito')
                    fresh=cde.json_command(dict,'run','describe','--id',rid)
                    if str(fresh.get('id'))!=rid or fresh.get('job')!=name or str(fresh.get('status','')).lower()!='running':
                        raise CDEError('Run non più in stato running o identità diversa: nessun kill eseguito')
                    journal.record('kill_authorized',job=name,run_id=rid)
                    cde.mutate('run','kill','--id',rid)
                    def fetch():
                        current=cde.json_command(dict,'run','describe','--id',rid)
                        if str(current.get('id'))!=rid or current.get('job')!=name: raise CDEError('Identità run non verificabile dopo kill')
                        return current
                    final=cde.poll('stato terminale run '+rid,fetch,lambda r:str(r.get('status','')).lower() in RUN_TERMINAL)
                    journal.record('kill_result',job=name,run_id=rid,status=final.get('status'))
                    print('  Stato finale osservato della run '+rid+': '+str(final.get('status')))
                else:
                    print('  Conferma non valida: nessun kill eseguito.')
            else: print('  Run lasciata in esecuzione; nessun kill.')
        else: print('  Kill non proposto: ID o stato non compatibile; verifica manuale.')
    else: print('  Kill non proposto: più run nuove; verifica manuale.')
    raise CDEError('Nuova run rilevata durante il cambio cron per '+name+': '+', '.join(new)+'; esito anomalo, verifica manuale necessaria')


def check_no_active_runs(cde,name,baseline=None,snapshot=None):
    runs=(snapshot if snapshot is not None else run_snapshot(cde,name)).values()
    # A snapshot, not a lock. Unknown state or missing id is a blocking error.
    terminal=RUN_TERMINAL
    ids=set(); active=[]; new=[]
    for r in runs:
        if not isinstance(r,dict) or r.get('job')!=name or r.get('id') is None or not str(r['id']).strip():
            raise CDEError('Storico run non verificabile per '+name+'; interrompo')
        rid=str(r['id']); ids.add(rid); status=str(r.get('status','')).lower()
        if status not in terminal: active.append(f'{rid} ({status or "ignoto"})')
        if baseline is not None and rid not in baseline: new.append(f'{rid} ({status or "ignoto"})')
    if new: raise CDEError('Nuova run rilevata durante il cambio cron per '+name+': '+', '.join(new)+'; nessun kill automatico')
    if active: raise CDEError('Run attiva o stato non riconosciuto per '+name+': '+', '.join(active)+'; interrompo')
    return ids


def maintenance_warning():
    print(color('  AVVERTENZA OPERATIVA: concordare una finestra di manutenzione e non avviare run manuali durante l’aggiornamento.','warn'))
    print('  Il manager blocca run attive o stati ignoti rilevati; la lettura non è un lock e non esclude avvii concorrenti.')
    print('  In caso di anomalia dopo una modifica, tenta la pausa di sicurezza; nessun kill o rollback automatico. Solo durante il cambio cron è possibile autorizzare il kill di una singola nuova run running.')

def cron_snapshot(jobs):
    # Check schedule and resource inputs across the cluster, excluding volatile marker and metadata.
    return {j['name']:(schedule_state(j),j.get('type'),j.get('spark'),j.get('mounts')) for j in jobs}


def cron_rows(jobs,changes,now=None):
    by={j['name']:j for j in jobs}; rows=[]
    for m in changes:
        j=by[m['job']]; st=schedule_state(j); old=st['cronExpression']; new=validate_clock_change(old,m['cron_after'])
        if old!=m['cron_before']: raise ValueError('Cron cambiata rispetto al piano: '+j['name'])
        same=old.split()[2:]==new.split()[2:] and supported_cron(old)==supported_cron(new)
        r=dict(job=j['name'],cron=old,target=new,action='SKIP' if same else 'UPDATE',note='cron già conforme' if same else '',current_start=schedule(j).get('start'),current_end=schedule(j).get('end'),paused=st['paused'])
        if not same:
            clock=now or utcnow()
            r['next_before']=iso(next_occurrence(clock,supported_cron(old)))
            r['next_after']=iso(next_occurrence(clock,supported_cron(new)))
            try: check_cron_window(j,new,clock)
            except ValueError as e: r['window_warning']=str(e)
        rows.append(r)
    return rows


def cron_simulation(jobs,rows):
    # Historical simulation stays based on same sample before/after.
    first=utcnow().replace(hour=0,minute=0,second=0,microsecond=0)
    working=copy.deepcopy(jobs); by={j['name']:j for j in working}
    for r in rows:
        if r['action']=='UPDATE': by[r['job']]['schedule']['cronExpression']=r['target']
    return first,working


def cron_operation(cde,args,jobs,changes=None,settings=None):
    # Dedicated interactive path. No mutation until explicit APPLICA, no automatic apply.
    heading('MODIFICA ORARI | preparazione piano, nessuna modifica')
    settings=settings or (args.days,args.capacity,args.overhead,args.history_runs,args.fallback_minutes,None)
    if changes is None:
        eligible=[]
        for j in jobs:
            try:
                st=schedule_state(j)
                if j.get('type')=='spark' and st['enabled'] and not st['paused'] and not st['catchup'] and not st['dependsOnPast'] and st['cronExpression'] and shift_cron(st['cronExpression'],0):
                    supported_cron(st['cronExpression']); eligible.append(j)
            except (ValueError,TypeError): continue
        if not eligible: print('Nessun job compatibile'); return 0
        heading('SELEZIONA UN SOLO JOB | aggiornamento orari')
        for i,j in enumerate(eligible,1): print(f'  {i:3} | {schedule_label(schedule(j).get("cronExpression"))} | {job_name(j["name"])}')
        while True:
            selection=ask('Numero o nome esatto del job (0 annulla): ').strip()
            if selection in ('','0'): return 0
            chosen=[j for i,j in enumerate(eligible,1) if selection in (str(i),j['name'])]
            if len(chosen)==1:
                action_boundary(); break
            print('  Selezionare un solo job valido.')
        heading('MODALITÀ DI MODIFICA | nessuna modifica su CDE')
        print('[1] Nuovo orario HH:MM UTC  [2] Spostamento minuti +/-  [3] Cron completa (stessa frequenza)  [0] Annulla')
        mode=choose('Scelta [1]: ',{'','0','1','2','3'})
        if mode=='0': return 0
        changes=[]
        for j in chosen:
            old=schedule(j)['cronExpression']
            heading('IMPOSTA ORARIO | '+j['name'])
            print('  Attuale: '+schedule_label(old)+' | Cron: '+old)
            prompt={'2':'Minuti di spostamento (-25 = anticipo, +45 = posticipo, 0 = invariato)',
                    '3':'Nuova cron a 5 campi (stessi giorni e frequenza)',
                    '1':'Nuovo orario UTC (HH:MM, es. 16:45)',
                    '':'Nuovo orario UTC (HH:MM, es. 16:45)'}[mode]
            while True:
                raw=ask('  '+prompt+' (Invio annulla): ').strip()
                if not raw: return 0
                try:
                    if mode=='2': new=shift_cron(old,int(raw)); new=validate_clock_change(old,new or '')
                    elif mode=='3': new=validate_clock_change(old,raw)
                    else:
                        if not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d',raw): raise ValueError('Orario HH:MM non valido')
                        h,m=map(int,raw.split(':')); new=validate_clock_change(old,' '.join([str(m),str(h),*old.split()[2:]]))
                    break
                except ValueError as e: print('  '+str(e))
            action_boundary()
            print('  Anteprima: '+cron_clock(old)+' → '+cron_clock(new)+' | Cron: '+new)
            changes.append(dict(job=j['name'],cron_before=old,cron_after=new))
    else:
        print('  Piano trasferito dalla simulazione: verrà ricalcolato con dati aggiornati.')
        if len(changes)>1:
            heading('SELEZIONA UN SOLO SPOSTAMENTO DAL PIANO')
            print('  Gli altri spostamenti restano simulati. Ricalcolo gli effetti del solo job scelto.')
            for i,m in enumerate(changes,1): print(f'  {i:3} | {job_name(m["job"])} | {m["cron_before"]} → {m["cron_after"]}')
            selected=choose('Numero (0 annulla): ',{'0'}|{str(i) for i in range(1,len(changes)+1)})
            if selected=='0': return 0
            changes=[changes[int(selected)-1]]
    heading('VERIFICA CONFIGURAZIONE | sola lettura')
    print('  Rileggo il cluster per verificare che le schedule non siano cambiate.',flush=True)
    fresh=cde.jobs()
    if cron_snapshot(jobs)!=cron_snapshot(fresh):
        raise ValueError('Calendario/configurazione cambiati dalla selezione: ripetere la simulazione')
    rows=cron_rows(fresh,changes)
    heading('MODIFICHE PROPOSTE | nessun aggiornamento ancora eseguito')
    for r in rows: print(f'  {r["action"]} | {job_name(r["job"])} | {r["cron"]} → {r["target"]}')
    if not any(r['action']=='UPDATE' for r in rows): print('Nessuna modifica necessaria.'); return 0
    if not owner_preview(cde,rows,fresh,args,True): print('Operazione annullata.'); return 0
    pending=[r for r in rows if r['action']=='UPDATE']
    if not pending: print('Nessun aggiornamento dopo il filtro utenza.'); return 0
    days,capacity,overhead,n,fallback,fixed=settings
    names=[j['name'] for j in fresh if schedule(j).get('enabled') is True and schedule(j).get('paused') is not True]
    heading('STIMA DELLE DURATE | sola lettura')
    print(f'  Recupero le ultime {n} run riuscite di ogni job abilitato per stimare le sovrapposizioni.',flush=True) if fixed is None else print(f'  Durata fissa scelta: {fixed} min per job.')
    history,errors=load_history(cde,names,n) if fixed is None else ({},[])
    model=duration_model(fresh,history,fallback,fixed,overhead)
    first,working=cron_simulation(fresh,rows)
    heading('SIMULAZIONE DEL PIANO DEFINITIVO | sola lettura')
    print(f'  Confronto {days} giorni UTC, dal {first:%d/%m/%Y} al {first+timedelta(days=days-1):%d/%m/%Y}, prima e dopo le modifiche selezionate.',flush=True)
    print(f'  Obiettivo: misurare riduzione o aumento dei picchi, tempo oltre soglia e nuove sovrapposizioni. Overhead RAM: +{overhead:g}%.',flush=True)
    before=saturation_report(fresh,first,days,capacity,overhead,model); after=saturation_report(working,first,days,capacity,overhead,model)
    old,new=conflict_metrics(before),conflict_metrics(after)
    heading('PIANO CRON DEFINITIVO | UTC | SOLO LETTURA')
    print(f'  Selezionati: {len(rows)} | Da aggiornare: {len(pending)} | Da saltare: {len(rows)-len(pending)}')
    for r in pending:
        print(f'  {job_name(r["job"])}: {r["cron"]} → {r["target"]} | prossimo avvio teorico {r["next_before"]} → {r["next_after"]}')
        print(f'    Start/end preservate: {r["current_start"]} / {r["current_end"] or "assente"}')
        if r.get('window_warning'): print(color('    '+r['window_warning'],'warn'))
    show_impact(old,new,capacity)
    same=before['starts_by_job']==after['starts_by_job']; no_new=no_new_conflicts(before,after)
    complete=not any(r['partial'] for r in before['days']+after['days']) and not errors and fixed is None and all(h['count'] for h in history.values())
    no_worse=all(new[k][m]<=old[k][m]+1e-6 for k in SCENARIOS for m in ('peak','over_seconds','excess_gib_seconds'))
    heading('VALUTAZIONE DEL PIANO | prima dell’apply')
    for label,result in [('Avvii per job/giorno preservati',same),('Nessuna nuova fascia oltre soglia nota',no_new),('Indicatori non peggiorati',no_worse),('Stima completa (senza fallback o durata fissa)',complete),('Finestra temporale ammissibile',not any(r.get('window_warning') for r in pending))]:
        print(color(f'  {label}: '+('Sì' if result else 'No'),'ok' if result else 'warn'))
    print(f'  Storico disponibile: {sum(bool(h["count"]) for h in history.values())}/{len(names)} job; durata fissa: {fixed if fixed is not None else "no"}.')
    for error in errors: print(color('  '+error,'warn'))
    print('  Stime, non consumo reale. Verificare dipendenze e SLA; nessun CLEAR, start/end preservate.')
    if len(pending)!=1 or not same or any(r.get('window_warning') for r in pending):
        heading('AGGIORNAMENTO BLOCCATO','error')
        if not same: print('  Numero di avvii teorici per job/giorno alterato.')
        if len(pending)!=1: print('  Selezionare un solo job.')
        for r in pending:
            if r.get('window_warning'): print('  '+r['window_warning'])
        print('  Nessuna modifica.'); return 0
    risk=not (no_new and complete and no_worse)
    if risk:
        heading('AVVERTENZE SULLA SIMULAZIONE','warn')
        if not no_new: print(color('  Il piano introduce nuove fasce oltre soglia.','warn'))
        if not no_worse: print(color('  Uno o più indicatori di saturazione peggiorano.','warn'))
        if not complete: print(color('  Stima parziale o basata su ipotesi: non certifica la capacità disponibile.','warn'))
        print('  Queste avvertenze non bloccano l’apply: la scelta operativa resta tua.')
    heading('DECISIONE FINALE | nessuna modifica fino alla conferma APPLICA')
    print(color('  Il cambio cron può causare un avvio inatteso anche senza CLEAR e con catchup=false.','warn'))
    print('  Monitoraggio: primo controllo dopo UNPAUSE, poi almeno 60 s dopo le verifiche (intervallo fino a 10 s tra letture); kill solo su richiesta esplicita.')
    decision=choose('[1] Dry-run / torna al menu (default)  [2] Applica  [0] Annulla: ',{'','0','1','2'})
    if decision!='2': print('Nessuna modifica.'); return 0
    heading('CONFERMA AGGIORNAMENTO REALE | modifica su CDE')
    cde.context()
    print('  Conferma un aggiornamento cron; backup locale; nessun rollback automatico.')
    maintenance_warning()
    print(color('  Possibile avvio inatteso: il kill non annulla scritture o notifiche già prodotte.','warn'))
    print('  In caso di nuova run rilevata il manager tenta la pausa di sicurezza; verificare prima di riattivare.')
    if risk: print(color('  Confermando APPLICA accetti anche le avvertenze sulla simulazione sopra riportate.','warn'))
    if ask('  Scrivi APPLICA per modificare l’orario: ').strip()!='APPLICA': print('Operazione annullata.'); return 0
    latest=cde.jobs()
    if utcnow().date()!=first.date(): raise ValueError('Giorno UTC cambiato: ricalcolare il piano')
    if cron_snapshot(latest)!=cron_snapshot(fresh): raise ValueError('Calendario cambiato dopo il piano. Nessuna modifica.')
    # Recheck all targets before the first mutation, then each target in apply_plan.
    by={j['name']:j for j in latest}
    for r in pending:
        check_cron_window(by[r['job']],r['target']); check_no_active_runs(cde,r['job'])
    return apply_plan(cde,latest,rows,'cron',args)

def show_history_coverage(history, total, n, fallback):
    # Conteggi per job, non campionamento dei job. Nessun cambiamento al modello.
    full=sum(h['count']>=n for h in history.values())
    reduced=sum(0<h['count']<n for h in history.values())
    missing=sum(not h['count'] for h in history.values())
    heading('STORICO DELLE ESECUZIONI | copertura per job')
    print(f'    Job abilitati non in pausa: {total}')
    print(f'    Run positive richieste per ciascun job: {n}')
    print(f'    Job con durate reali disponibili: {full+reduced}/{total}')
    print(f'    Job con {n} run utilizzate: {full}')
    print(color(f'    Job con meno di {n} run valide disponibili (almeno una): {reduced}', 'warn' if reduced else 'ok'))
    for name,h in history.items():
        if 0<h['count']<n: print(f'      - {name}: {h["count"]}/{n} run; utilizzate tutte quelle recuperate')
    print(color(f'    Job senza run valide recuperate: {missing}', 'warn' if missing else 'ok'))
    print(f'    Job con durata ipotizzata: {missing}'+(f' ({fallback} min per job)' if missing else ' (nessuna durata di fallback utilizzata)'))
    for name,h in history.items():
        if not h['count']: print(f'      - {name}: durata ipotizzata {fallback} min')
    print('    Scenario medio: media delle durate effettive per ciascun job.')
    print('    Scenario prudenziale: massima durata osservata per ciascun job.')
    print('    Durate future stimate, arrotondate per eccesso al secondo; massimo storico non garantito.')


def saturation_view(cde,jobs,args,interactive=False):
    days=args.days; capacity=args.capacity; overhead=args.overhead; n=args.history_runs; fallback=args.fallback_minutes; fixed=args.fixed_minutes
    if interactive:
        heading('ANALISI SATURAZIONE | sola lettura')
        print('  [1] Automatica (consigliata) - ultime 3 run, 15 min se assenti; solo criticità')
        print(f'      Orizzonte {days} giorni UTC | RAM ipotizzata {capacity:g} GiB | margine memoria +{overhead:g}%')
        print('  [2] Personalizzata - scegli periodo, capacità e numero di run')
        print('  [3] Durata fissa - simula tutti i job con la stessa durata')
        print('  [0] Torna al menu')
        mode=choose('Scelta [1]: ',{'','1','2','3','0'})
        if mode=='0': return None
        if mode in ('','1'): n=3; fallback=15; fixed=None
        else:
            days=ask_number('Giorni UTC da analizzare',days,7,90)
            capacity=ask_number('Capacità RAM ipotizzata per i workload (GiB)',capacity,1,100000,False)
            if mode=='3': fixed=ask_number('Durata fissa (minuti)',15,1,10080)
            else:
                fixed=None; n=ask_number('Ultime run positive per job',n,3,5)
                fallback=ask_number('Durata ipotizzata se non ci sono run valide (minuti)',fallback,1,10080)
    first=utcnow().replace(hour=0,minute=0,second=0,microsecond=0)
    eligible=[j for j in jobs if schedule(j).get('enabled') is True and schedule(j).get('paused') is not True]
    heading('STIMA DELLE DURATE | sola lettura')
    print(f'  Analizzo {len(eligible)} job abilitati: ultime {n} run riuscite per ciascuno.' if fixed is None else f'  Durata fissa scelta: {fixed} min per job.',flush=True)
    history,errors=load_history(cde,[j['name'] for j in eligible],n) if fixed is None else ({},[])
    model=duration_model(jobs,history,fallback,fixed,overhead)
    heading('CALCOLO DEL CALENDARIO | sola lettura')
    print(f'  Ricostruisco {days} giorni di avvii teorici e sommo la RAM configurata dei job sovrapposti; soglia {capacity:g} GiB.',flush=True)
    report=saturation_report(jobs,first,days,capacity,overhead,model)
    report.update(history=history,history_errors=errors,duration_model=model,fixed_minutes=fixed,history_runs=n,fallback_minutes=fallback)
    counts=Counter(r['status'] for r in report['days']); metrics=conflict_metrics(report)
    critical=counts['CRITICO']+counts['ATTENZIONE']
    heading('RISULTATO ANALISI | UTC | nessuna modifica a CDE')
    print(f'  Periodo: {first:%d/%m/%Y} – {first+timedelta(days=days-1):%d/%m/%Y} | {days} giorni interi, incluso oggi')
    print(f'  Capacità ipotizzata: {capacity:g} GiB | margine memoria +{overhead:g}% | job abilitati non in pausa: {len(eligible)}')
    if fixed is None:
        if interactive: print(f'  Copertura storico: {sum(bool(h["count"]) for h in history.values())}/{len(eligible)} job; {sum(h["count"]==n for h in history.values())} con {n} run, {sum(not h["count"] for h in history.values())} con fallback.')
        else: show_history_coverage(history, len(eligible), n, fallback)
    else: print(f'  Durata fissa: {fixed} min per job; nessuna lettura storico.')
    print(f'\n  Giorni oltre soglia: {critical}/{days} (media: {counts["CRITICO"]}, solo prudenziale: {counts["ATTENZIONE"]})')
    print(f'  Vicino al limite (80%): {counts["VICINO AL LIMITE"]} | Regolari: {counts["REGOLARE"]} | Senza run: {counts["NESSUNA RUN"]} | Parziali: {sum(r["partial"] for r in report["days"])}')
    print(f'  Picco globale: medio {metrics["ordinary"]["peak"]:.1f} GiB | prudenziale {metrics["cautious"]["peak"]:.1f} GiB')
    print('  RAM configurata stimata, non consumo reale. Escluse run manuali, retry e altri workload.')
    print('  Esclusioni: '+(', '.join(f'{k}={v}' for k,v in report['excluded'].items()) or 'nessuna'))
    expiring=[]; last=first+timedelta(days=days)
    for j in eligible:
        try:
            end=timestamp(schedule(j).get('end'))
            if end and first<=end<last: expiring.append(j['name'])
        except (ValueError,TypeError): pass
    if expiring: print(color(f'  Nota: {len(expiring)} schedule scadono nel periodo. Ripetere l’analisi dopo eventuali rinnovi.','warn'))
    for key,label in [('unmodelled','Schedule non modellate'),('warnings','Avvertenze'),('unknown_resources','Risorse non stimabili')]:
        if report[key]: print(color('  '+label+': '+'; '.join(report[key]),'warn'))
    for e in errors: print(color('  '+e,'warn'))
    if errors or any(r['partial'] for r in report['days']) or any(not h['count'] for h in history.values()):
        print(color('  Risultati condizionati da dati mancanti o ipotesi.','warn'))
    for row in report['days']:
        if row['status']=='VICINO AL LIMITE':
            peak=row['cautious']['peak_max_gib']; print(f'  Vicino al limite: {row["date"]} | {peak:.1f} GiB | margine {capacity-peak:.1f} GiB')
    runs=calendar_runs(jobs,first,last,overhead,model)[0]
    if not interactive: show_critical(report,model,runs)
    else:
        heading('CRITICITÀ | riepilogo')
        groups=critical_groups(report)
        if not groups: print('  Nessun superamento noto.')
        for g in groups:
            b=g['block']; names=sorted({name for k in SCENARIOS for x in b[k] for name in x['jobs']})
            peak=max(x['max_gib'] for k in SCENARIOS for x in b[k])
            print(color(f'  {g["weekday"]} {range_text(b["start"],b["end"],g["dates"][0])} UTC | {peak:.1f} GiB | {len(g["dates"])} date','warn'))
            print('    '+', '.join(job_name(name) for name in names))
    def advice_view():
        if 'suggestions' not in report:
            heading('RICERCA AUTOMATICA DI ORARI ALTERNATIVI | sola lettura')
            if critical: print('  Provo combinazioni di spostamenti per ridurre picchi e tempo oltre soglia senza creare nuovi conflitti.',flush=True)
            report['suggestions']=coordinated_suggestions(jobs,first,days,capacity,overhead,model,report)
        show_advice(report['suggestions'],capacity)
    # L'interfaccia interattiva lascia la decisione all'utente. La CLI esplicita
    # mantiene il report completo e la struttura JSON precedente.
    if not interactive: advice_view()
    if interactive:
        last_manual=None; last_auto=None; last_plan=None
        while True:
            heading('APPROFONDIMENTI | simulazioni in sola lettura')
            print('  [1] Soluzioni suggerite automaticamente\n  [2] Simula uno o più spostamenti a tua scelta\n  [3] Calendario dei picchi RAM\n  [4] Dettaglio di una data\n  [5] Storico e durate utilizzate\n  [6] Dettaglio delle criticità')
            if last_plan: print('  [7] Prepara modifica di un job dell’ultimo piano (con conferma)')
            if last_manual and last_auto: print('  [8] Confronta piano manuale e automatico')
            print('  [0] Torna al menu')
            allowed={'','0','1','2','3','4','5','6'}
            if last_plan: allowed.add('7')
            if last_manual and last_auto: allowed.add('8')
            choice=choose('Scelta [0]: ',allowed)
            if choice in ('','0'): break
            if choice=='1':
                advice_view(); last_auto=report['suggestions']['coordinated']['moves']; last_plan=last_auto
            elif choice=='2':
                try:
                    changes=manual_view(jobs,first,days,capacity,overhead,model,report)
                    if changes: last_manual=changes; last_plan=changes
                except (ValueError,OverflowError) as e: print(color('  Simulazione non disponibile: '+str(e),'warn'))
            elif choice=='7':
                if not last_plan: print('  Prima eseguire una simulazione con spostamenti.'); continue
                print('  Ultimo piano: '+', '.join(m['job'] for m in last_plan))
                rc=cron_operation(cde,args,jobs,changes=last_plan,settings=(days,capacity,overhead,n,fallback,fixed))
                if rc: raise CDEError('Aggiornamento cron interrotto: verificare backup e stato CDE')
                # Anche dopo dry-run rientrare al menu per non riusare un calendario potenzialmente obsoleto.
                return report
            elif choice=='8':
                heading('CONFRONTO PIANI MANUALE E AUTOMATICO | sola lettura')
                for label,moves in [('Manuale',last_manual),('Automatico',last_auto)]:
                    if not moves: print(label+': nessun piano disponibile'); continue
                    _,candidate,_=manual_candidate(jobs,first,days,capacity,overhead,model,report,{m['job']:m['delta_minutes'] for m in moves})
                    met=conflict_metrics(candidate)
                    print(f'  {label}: {len(moves)} job | avvii interessati nel periodo: {sum(c for _,name,c in report["starts_by_job"] if name in {m["job"] for m in moves})} | picco prud. {met["cautious"]["peak"]:.1f} GiB | giorni oltre soglia {met["cautious"]["days"]}')
            elif choice=='6': show_critical(report,model,runs)
            elif choice=='3': show_calendar(report)
            elif choice=='4':
                raw=ask('Data (GG/MM/AAAA o AAAA-MM-GG, Invio annulla): ').strip()
                if not raw: continue
                try: day=parse_date(raw).date().isoformat()
                except ValueError as e: print(e); continue
                rows=[r for r in report['days'] if r['date']==day]
                if not rows: print('Data fuori dal periodo analizzato.'); continue
                r=rows[0]; heading(day+' | '+r['status'])
                print(f'  Avvii: {r["starts"]} | Job distinti in avvio: {r["distinct_jobs"]} | Picco medio/prudenziale: {r["ordinary"]["peak_max_gib"]:.1f}/{r["cautious"]["peak_max_gib"]:.1f} GiB')
                show_critical(dict(report,days=rows),model,runs)
                print('  Altre sovrapposizioni e fasce vicine al limite (scenario prudenziale):')
                for x in r['cautious']['intervals']:
                    if x['max_gib']<=capacity+1e-9: print(f'    {range_text(x["start"],x["end"],day)} | {x["max_gib"]:.1f} GiB | '+', '.join(x['jobs']))
            else:
                if fixed is None: show_history_coverage(history,len(eligible),n,fallback)
                heading('DURATE UTILIZZATE NELLA SIMULAZIONE')
                for j in eligible:
                    h=model[j['name']]; source=f'{h["count"]}/{n} run positive utilizzate' if h['source']=='storico' else ('durata ipotizzata: nessuna run valida' if h['source']=='ipotizzata' else 'durata fissa scelta')
                    print(f'  {job_name(j["name"])} | media {duration(h["ordinary"])} | max {duration(h["cautious"])} | {source}')
                if fixed is None:
                    print('  Per consultare ID, inizio, fine e durata delle run effettivamente utilizzate:')
                    for i,j in enumerate(eligible,1): print(f'    {i:2} | {job_name(j["name"])}')
                    raw=ask('Numero del job (Invio torna agli approfondimenti): ').strip()
                    if raw:
                        if raw.isdigit() and 1<=int(raw)<=len(eligible):
                            name=eligible[int(raw)-1]['name']; h=history[name]
                            heading('RUN POSITIVE UTILIZZATE | '+name+' | UTC')
                            if not h['runs']: print('  Nessuna run valida recuperata.')
                            for r in h['runs']:
                                print(f'  #{r["id"]} | Inizio {r["started"]} | Fine {r["ended"]} | Durata {duration(round(r["seconds"]))}')
                            print('  Durate delle run arrotondate al secondo per la visualizzazione; simulazione arrotondata per eccesso.')
                        else: print('  Numero non valido; nessuna modifica.')
    return report

def expiry_view(cde,jobs,month):
    heading('SCADENZE UTC'+(' | '+month if month else ' | tutte'))
    rows=[]; now=utcnow()
    for j in jobs:
        s=schedule(j)
        if not s.get('enabled'): continue
        if month and month_of(j)!=month: continue
        try: dt=timestamp(s.get('end')); status='SENZA END' if not dt else ('SCADUTO' if dt<=now else 'FUTURA')
        except ValueError: status='DATA NON VALIDA'
        rows.append(dict(job=j['name'],end=s.get('end'),status=status,paused=s.get('paused',False)))
    rows.sort(key=lambda r:(r['end'] is None,str(r['end']),r['job']))
    group=None
    for r in rows:
        try:
            dt=timestamp(r['end']); key=dt.strftime('%Y-%m') if dt else 'SENZA END'
        except ValueError: key='DATA NON VALIDA'
        if key!=group:
            print(color('  '+key)); group=key
        print(f'    {job_name(r["job"])} | {r["end"] or "assente"} | '+status_text(r['status'])+f' | paused={flag(r["paused"])}')
    print(f'Totale: {len(rows)}')
    return rows

def ask_date():
    heading('NUOVA SCADENZA')
    print('La data indica l’inizio del giorno (00:00 UTC). Per includere tutto il 31/12, inserire 01/01 dell’anno successivo.')
    while True:
        a=ask('Data [GG/MM/AAAA, es. 01/01/2031; G = guidata; 0 = annulla]: ').strip()
        if a.upper()=='G':
            day=ask('  Giorno (1-31, 0 annulla): ').strip()
            if day=='0': return None
            month=ask('  Mese (1-12, 0 annulla): ').strip()
            if month=='0': return None
            year=ask('  Anno (AAAA, 0 annulla): ').strip()
            if year=='0': return None
            a=f'{day}/{month}/{year}'
        if a=='0': return None
        try:
            dt=parse_date(a)
            if dt<=utcnow(): raise ValueError('La data deve essere futura')
            return dt
        except ValueError as e: print(e)

def ask_month(allow_all=True):
    while True:
        a=ask('Mese scadenza [MM/AAAA, es. 12/2026; G = guidata; '+('TUTTE = nessun filtro; ' if allow_all else '')+'0 = annulla]: ').strip()
        if a=='0': return False
        if a.upper()=='TUTTE' and allow_all: return None
        if a.upper()=='G':
            mm=ask('  Mese (1-12, 0 annulla): ').strip()
            if mm=='0': return False
            yyyy=ask('  Anno (AAAA, 0 annulla): ').strip()
            if yyyy=='0': return False
            a=f'{mm}/{yyyy}'
        try: return parse_month(a)
        except ValueError as e: print(e)

def overview_menu(cde,jobs,report):
    while True:
        heading('APPROFONDIMENTI | stato delle configurazioni')
        print('  [1] Job schedulati\n  [2] Job non schedulati\n  [3] Scadenze (tutte o per mese)\n  [4] Configurazioni con segnalazioni\n  [0] Torna al menu')
        choice=choose('Scelta [0]: ',{'','0','1','2','3','4'})
        if choice in ('','0'): return
        if choice=='3':
            month=ask_month()
            if month is not False: expiry_view(cde,jobs,month)
            continue
        rows=[r for r in report['jobs'] if (r['state']!='NON SCHEDULATO' if choice=='1' else r['state']=='NON SCHEDULATO' if choice=='2' else r['state'] not in ('NON SCHEDULATO','ABILITATO') or r['note'])]
        heading('ELENCO JOB | '+str(len(rows)))
        for r in rows:
            print('  '+job_name(r['job'])+' | '+status_text(r['state'])+(' | '+schedule_label(r['cron']) if r['cron'] else '')+(' | '+r['note'] if r['note'] else ''))


def detail_menu(cde,job,args):
    name=job['name']
    while True:
        heading('APPROFONDIMENTI | '+name)
        print('  [1] Ultime run (tutti gli stati)\n  [2] Durate ultime run positive\n  [3] Configurazione completa JSON\n  [0] Torna / prosegui con il prossimo job')
        choice=choose('Scelta [0]: ',{'','0','1','2','3'})
        if choice in ('','0'): return
        if choice=='3':
            print(json.dumps(job,ensure_ascii=False,indent=2)); continue
        if choice=='1':
            rows=run_snapshot(cde,name).values()
            def sort_key(r):
                for field in ('created','started','ended'):
                    try:
                        dt=timestamp(r.get(field))
                        if dt: return dt
                    except (ValueError,TypeError): pass
                return datetime.min.replace(tzinfo=UTC)
            rows=sorted(rows,key=sort_key,reverse=True)[:10]
            heading('ULTIME 10 RUN RECUPERATE | UTC')
            if not rows: print('  Nessuna run recuperata.')
            for r in rows:
                status=str(r.get('status') or 'ignoto'); kind='ok' if status=='succeeded' else 'error' if status in ('failed','killed') else 'warn'
                try:
                    a,b=timestamp(r.get('started')),timestamp(r.get('ended'))
                    d=duration((b-a).total_seconds()) if a and b and b>=a else 'n/d'
                except (ValueError,TypeError): d='n/d'
                print(f'  #{r["id"]} | '+color(status,kind)+f' | Inizio {r.get("started") or "n/d"} | Durata {d}')
            continue
        n=ask_number('Ultime run positive da considerare',args.history_runs,3,5)
        history,errors=load_history(cde,[name],n); h=history[name]
        heading('DURATE STORICHE | UTC')
        for e in errors: print(color(e,'warn'))
        print(f'  Run valide recuperate: {h["count"]}/{n}')
        for r in h['runs']: print(f'  #{r["id"]} | {r["started"]} → {r["ended"]} | {duration(round(r["seconds"]))}')
        if h['count']: print(f'  Media: {duration(round(h["mean"]))} | Min: {duration(round(h["minimum"]))} | Max: {duration(round(h["maximum"]))}')
        else: print(color('  Nessuna durata valida disponibile.','warn'))


def calendar_starts(jobs,first,days,overhead=40):
    # Stesso enumeratore del simulatore: durata zero = solo avvii, nessun lookback.
    model={j['name']:{'cautious':0} for j in jobs}
    runs,excluded,missing,warnings,_=calendar_runs(jobs,first,first+timedelta(days=days),overhead,model)
    return sorted(runs,key=lambda x:(x[0],x[1].casefold())),excluded,missing,warnings


def show_operational_calendar(jobs,first,days,args):
    runs,excluded,missing,warnings=calendar_starts(jobs,first,days,args.overhead)
    now=utcnow(); heading('CALENDARIO OPERATIVO | avvii teorici UTC')
    print(f'  {first:%d/%m/%Y} – {first+timedelta(days=days-1):%d/%m/%Y} | {len(runs)} avvii teorici')
    if first.date()<=now.date()<(first+timedelta(days=days)).date():
        upcoming=next((r for r in runs if r[0]>=now),None)
        print('  Prossimo orario nel periodo: '+(upcoming[0].strftime('%d/%m %H:%M UTC') if upcoming else 'nessuno'))
    for i in range(days):
        day=first+timedelta(days=i); rows=[r for r in runs if r[0].date()==day.date()]
        print('\n'+color(f'  {DAYS_IT[day.weekday()]} {day:%d/%m/%Y} | {len(rows)} avvii'))
        for t,name,*_ in rows:
            past=t<now
            print('    '+color(t.strftime('%H:%M'),'muted' if past else 'heading')+' | '+job_name(name)+(' | '+color('orario trascorso','muted') if past else ''))
        if not rows: print('    Nessun avvio teorico modellato.')
    for e in missing+warnings: print(color('  '+e,'warn'))
    print('  Esclusioni: '+(', '.join(f'{k}={v}' for k,v in excluded.items()) or 'nessuna'))
    print('  Gli orari trascorsi non indicano run riuscite; escluse run manuali, retry e altri workload.')
    return runs


def active_runs_view(cde):
    # Una sola lettura globale senza filtro succeeded: stati non riconosciuti restano visibili.
    rows=cde.json_command(list,'run','list'); seen=set(); active=[]
    for r in rows:
        if not isinstance(r,dict) or r.get('id') is None or not r.get('job'):
            raise CDEError('Elenco run globale non verificabile')
        rid=str(r['id'])
        if rid in seen: raise CDEError('ID run duplicato nella risposta globale')
        seen.add(rid)
        if str(r.get('status') or '').lower() not in RUN_TERMINAL: active.append(r)
    heading('RUN ATTIVE O CON STATO NON TERMINALE | istantanea UTC')
    print(f'  Rilevazione {iso(utcnow())} | {len(active)} run non terminali nella risposta CLI')
    for r in active:
        print('  '+job_name(r['job'])+f' | #{r["id"]} | '+color(str(r.get('status') or 'ignoto'),'warn')+f' | Inizio {r.get("started") or "n/d"}')
    if not active: print('  Nessuna run non terminale rilevata nella risposta.')
    print('  Istantanea, non lock né misura della RAM libera; la visibilità dipende dalle autorizzazioni e dai risultati CLI.')
    return active


def free_windows(runs,model,first,last,buffer_seconds=0):
    # Unione degli intervalli prudenziali, inclusi job partiti prima di first.
    occupied=[]
    for t,name,*_ in runs:
        a=max(first,t-timedelta(seconds=buffer_seconds)); b=min(last,t+timedelta(seconds=model[name]['cautious']+buffer_seconds))
        if a<b: occupied.append((a,b))
    gaps=[]; cursor=first
    for a,b in sorted(occupied):
        if a>cursor: gaps.append((cursor,a))
        cursor=max(cursor,b)
    if cursor<last: gaps.append((cursor,last))
    return gaps


def test_windows_view(cde,jobs,first,days,args):
    minutes=ask_number('Durata prevista del test (minuti)',60,1,1440)
    buffer=ask_number('Margine prima e dopo ogni attività (minuti)',10,0,120)
    now=utcnow(); last=first+timedelta(days=days); begin=max(first,now)
    if begin>=last: print('  Il periodo selezionato è già trascorso.'); return
    # Anche job di giorni precedenti possono occupare il periodo: storico di tutti gli abilitati.
    eligible=[j['name'] for j in jobs if schedule(j).get('enabled') is True and schedule(j).get('paused') is not True]
    history,errors=load_history(cde,eligible,args.history_runs)
    model=duration_model(jobs,history,args.fallback_minutes,None,args.overhead)
    runs,_,missing,warnings,_=calendar_runs(jobs,first-timedelta(minutes=buffer),last+timedelta(minutes=buffer),args.overhead,model)
    begin=max(first,utcnow())
    gaps=free_windows(runs,model,begin,last,buffer*60) if begin<last else []
    heading('FINESTRE CANDIDATE PER TEST | stima UTC')
    print(f'  Durata richiesta: {minutes} min | Margine: {buffer} min su entrambi i lati | Scenario: massimo storico per job.')
    print(f'  Storico valido: {sum(bool(h["count"]) for h in history.values())}/{len(eligible)} | Fallback: {args.fallback_minutes} min se assente.')
    partial=bool(errors or missing or warnings or any(not h['count'] for h in history.values()))
    for e in errors+missing+warnings: print(color('  '+e,'warn'))
    if partial: print(color('  Calendario parziale o durate ipotizzate: affidabilità ridotta.','warn'))
    candidates=[(a,b) for a,b in gaps if (b-a).total_seconds()>=minutes*60]
    for a,b in candidates:
        print('  '+color(f'{a:%d/%m %H:%M:%S} → {b:%d/%m %H:%M:%S} UTC','warn' if partial else 'heading')+f' | {duration((b-a).total_seconds())}')
    if not candidates: print('  Nessuna finestra modellata abbastanza lunga.')
    print('  Intervalli senza sovrapposizioni schedulate stimate, non disponibilità reale della RAM. Non includono run manuali, retry o altri workload.')
    if first.date()<=now.date()<(first+timedelta(days=days)).date():
        try: active_runs_view(cde)
        except CDEError as e: print(color('  Run attive non verificabili: '+str(e),'warn'))
    print(color('  Prima del test verificare di nuovo run attive, risorse effettive e accordi operativi; nessuna finestra è una garanzia.','warn'))


def operational_calendar(cde,jobs,args):
    first=utcnow().replace(hour=0,minute=0,second=0,microsecond=0); days=1
    while True:
        show_operational_calendar(jobs,first,days,args)
        while True:
            heading('APPROFONDIMENTI | calendario operativo')
            print('  [1] Oggi  [2] Domani  [3] Data a scelta  [4] Prossimi 7 giorni\n  [5] Run attualmente attive  [6] Stima finestre per test  [7] Aggiorna configurazioni\n  [0] Torna al menu')
            choice=choose('Scelta [0]: ',{'','0','1','2','3','4','5','6','7'})
            if choice in ('','0'): return 0
            if choice=='5': active_runs_view(cde); continue
            if choice=='6': test_windows_view(cde,jobs,first,days,args); continue
            if choice=='7': jobs=cde.jobs(); break
            today=utcnow().replace(hour=0,minute=0,second=0,microsecond=0)
            if choice=='3':
                raw=ask('Data UTC (GG/MM/AAAA o AAAA-MM-GG, Invio annulla): ').strip()
                if not raw: continue
                try: first=parse_date(raw); days=1
                except ValueError as e: print(e); continue
            else: first=today+timedelta(days=choice=='2'); days=7 if choice=='4' else 1
            # Una nuova data usa una nuova istantanea delle schedule.
            jobs=cde.jobs(); break


def operation(cde,args,op,interactive=False):
    if interactive:
        labels={'start':'AGGIORNA START DATE','end':'AGGIORNA END DATE','cron':'MODIFICA ORARI','overview':'RIEPILOGO CLUSTER','describe':'DETTAGLIO JOB','expiries':'ANALISI SCADENZE','saturation':'ANALISI SATURAZIONE','calendar':'CALENDARIO OPERATIVO'}
        heading(labels[op]+' | '+('preparazione piano, sola lettura' if op in ('start','end','cron') else 'sola lettura'))
    if not getattr(cde,'context_shown',False):
        cde.context()
        heading('AUTENTICAZIONE CDE')
        print(color('Password: solo se richiesta dalla CLI (API User Password).','prompt'),flush=True)
        print('  Se la sessione è già valida non occorre digitare nulla.',flush=True)
        print('  Verifica dell’accesso e acquisizione dei dati CDE in corso...',flush=True)
        jobs=cde.jobs()
        print(color('  Accesso al Virtual Cluster verificato: risposta CDE valida.','ok'))
        cde.context_shown=True
    else:
        print('  Rileggo le configurazioni correnti del Virtual Cluster.',flush=True)
        jobs=cde.jobs()
    if op=='cron': return cron_operation(cde,args,jobs)
    if op=='calendar': return operational_calendar(cde,jobs,args)
    month=args.expiring_month; missing_only=args.missing_end_only
    if op=='end' and interactive:
        heading('PERIMETRO END DATE')
        print('[1] Job in scadenza in un mese (predefinito)\n[2] Tutti i job schedulati (nessun filtro mensile)\n[3] Solo job senza endDate\n[0] Annulla')
        scope=choose('Scelta [1]: ',{'','0','1','2','3'})
        if scope=='0': return 0
        if scope in ('','1'):
            month=ask_month(allow_all=False)
            if month is False: return 0
        elif scope=='3': missing_only=True
    elif op=='expiries' and interactive:
        month=ask_month()
        if month is False: return 0
    if month: month=parse_month(month)
    if op=='end' and not interactive and not month and not args.all_expiries and not missing_only and not (args.job or args.jobs):
        raise ValueError('Per rinnovi senza nomi espliciti usare --expiring-month oppure --all-expiries')
    candidates=jobs
    if op in ('start','end','expiries'): candidates=[j for j in jobs if schedule(j).get('enabled') is True]
    if args.job or args.jobs: candidates=select_jobs(candidates,names=args.job or args.jobs)
    if month: candidates=[j for j in candidates if month_of(j)==month]
    if missing_only: candidates=[j for j in candidates if schedule(j).get('end') in (None,'')]
    if op=='describe' and interactive:
        heading('SELEZIONA UN JOB | dettaglio')
        for i,j in enumerate(candidates,1): print(f'  {i:3} | '+job_name(j['name']))
        while True:
            raw=ask('Numero o nome esatto (0 annulla): ').strip()
            if raw in ('','0'): return 0
            selected=[j for i,j in enumerate(candidates,1) if raw in (str(i),j['name'])]
            if len(selected)==1: candidates=selected; action_boundary(); break
            print('  Selezione non valida.')
    elif op!='overview':
        candidates=select_jobs(candidates,prefix=args.prefix,interactive=interactive and op in ('start','end','describe'),compact=op in ('start','end','describe'))
    if not candidates and op!='overview': print('Nessun job selezionato. Nessuna modifica.'); return 0
    if op=='overview':
        report=overview(cde,jobs)
        if interactive: overview_menu(cde,jobs,report)
    elif op=='describe': report=describe_view(cde,candidates,args,interactive)
    elif op=='expiries': report=expiry_view(cde,candidates,month)
    elif op=='saturation': report=saturation_view(cde,candidates,args,interactive)
    else:
        new_end=None; resume=args.resume_paused; include_missing=args.include_missing_end or missing_only
        if op=='end':
            missing=sum(schedule(j).get('end') in (None,'') for j in candidates)
            if interactive and missing and not month and not missing_only:
                print(f'Job selezionati senza endDate: {missing}')
                print(color('Inserire una endDate significa introdurre una scadenza prima assente.', 'warn'))
                selection=choose('[1] Solo rinnovi (default prudenziale)  [2] Includi anche senza endDate  [0] Annulla: ',{'','0','1','2'})
                if selection=='0': return 0
                include_missing=selection=='2'
            new_end=ask_date() if interactive else parse_date(args.new_date or '')
            if new_end is None: return 0
            if new_end<=utcnow(): raise ValueError('La nuova endDate deve essere futura')
            print('Timestamp CDE:',iso(new_end),'(inizio del giorno UTC, non fine giornata)')
        elif interactive and len(candidates)==1 and schedule(candidates[0]).get('paused') is True:
            resume=choose('Job in pausa. Vuoi riattivarlo dopo il fix? [s/n]: ',{'s','n'})=='s'
        if resume and (op!='start' or len(candidates)!=1): raise ValueError('--resume-paused ammesso solo per un singolo job startDate')
        heading('CALCOLO DEL PIANO | sola lettura')
        print('  Calcolo le nuove startDate dalle occorrenze cron e confronto i marker CDE.' if op=='start' else '  Confronto le scadenze attuali con la data richiesta e individuo rinnovi, inserimenti ed esclusioni.',flush=True)
        rows=plan(candidates,op,new_end,args.force,resume,include_missing_end=include_missing)
        n=show_plan(rows,op,month,missing_only)
        if n:
            if not owner_preview(cde,rows,candidates,args,interactive): print('Operazione annullata. Zero modifiche.'); return 0
            if sum(r['action']=='UPDATE' for r in rows)!=n: n=show_plan(rows,op,month,missing_only)
        if args.details: show_plan_details(rows,op)
        if args.export: export_json(args.export, dict(operation=op,plan=rows))
        if not n: return 0
        apply=args.apply
        if interactive:
            heading('DECISIONE FINALE | nessuna modifica fino alla conferma APPLICA')
            while True:
                decision=choose('[1] Dry-run / torna al menu (default)  [2] Applica  [3] Dettagli  [0] Annulla: ',{'','0','1','2','3'})
                if decision=='3': show_plan_details(rows,op); continue
                if decision=='0': print('Operazione annullata. Zero modifiche.'); return 0
                apply=decision=='2'; break
        if not apply: print('DRY-RUN COMPLETATO - zero modifiche su CDE'); return 0
        heading('CONFERMA AGGIORNAMENTO REALE | modifica su CDE')
        cde.context()
        print(f'CONFERMA: {op.upper()} DATE | {n} job da modificare'+(f' | nuova scadenza {iso(new_end)}' if op=='end' else ' | start calcolate da cron'))
        print('Verrà creato un solo registro di sicurezza locale per conservare le configurazioni originali.')
        print('Non serve gestirlo durante l’aggiornamento; nessun file se annulli.\n')
        maintenance_warning()
        if ask(f'Per aggiornare {n} job sul Virtual Cluster configurato, scrivi APPLICA: ').strip()!='APPLICA': print('Operazione annullata.'); return 0
        return apply_plan(cde,candidates,rows,op,args)
    if args.export: export_json(args.export, report)
    return 0

def main():
    p=argparse.ArgumentParser(prog='cde_schedule_manager.sh',description='CDE Schedule Manager | dry-run predefinito | UTC')
    modes=p.add_mutually_exclusive_group()
    for op in ('start','end','overview','describe','expiries','saturation'): modes.add_argument('--'+op,action='store_const',const=op,dest='operation')
    scopes=p.add_mutually_exclusive_group(); scopes.add_argument('--all',action='store_true'); scopes.add_argument('--job'); scopes.add_argument('--jobs'); scopes.add_argument('--prefix')
    apply=p.add_mutually_exclusive_group(); apply.add_argument('--dry-run',action='store_true'); apply.add_argument('--apply',action='store_true')
    expiry=p.add_mutually_exclusive_group(); expiry.add_argument('--expiring-month'); expiry.add_argument('--all-expiries',action='store_true'); expiry.add_argument('--missing-end-only',action='store_true',help='end: seleziona e autorizza solo schedule senza endDate')
    p.add_argument('--include-missing-end',action='store_true',help='end: autorizza inserimento endDate assente (senza filtro mensile)')
    p.add_argument('--details',action='store_true',help='mostra anche il piano dettagliato start/end'); p.add_argument('--new-date'); p.add_argument('--force',action='store_true',help='start: ripeti anche se già conforme'); p.add_argument('--resume-paused',action='store_true',help='start: consenti riattivazione esplicita di un solo job in pausa')
    p.add_argument('--experimental-cron-apply',action='store_true',help='compatibilità v2.4.3: non più necessario; cambio cron disponibile nel menu con conferma')
    p.add_argument('--schedule-user',help='workload username dichiarato per previsione cambio utenza (non password)')
    p.add_argument('--owner-policy',choices=('same','all'),help='apply start/end: solo utenza invariata oppure tutti')
    p.add_argument('--export',metavar='FILE.json',help='salva il report JSON di una consultazione o dry-run (facoltativo)')
    p.add_argument('--capacity',type=float,default=80,help='capacità ipotizzata workload in GiB'); p.add_argument('--overhead',type=float,default=40,help='overhead ipotizzato percentuale')
    p.add_argument('--history-runs',type=int,choices=(3,4,5),default=3,help='ultime run positive da usare (3-5)')
    p.add_argument('--fallback-minutes',type=int,default=15,help='durata ipotizzata senza storico')
    p.add_argument('--fixed-minutes',type=int,help='saturazione: durata fissa per tutti i job (minuti)')
    p.add_argument('--days',type=int,default=30,help='saturazione: giorni UTC da analizzare (7-90)')
    p.add_argument('--poll-seconds',type=float,default=2); p.add_argument('--poll-timeout',type=float,default=90)
    args=p.parse_args()
    if args.experimental_cron_apply and args.operation: p.error('--experimental-cron-apply richiede il menu interattivo')
    import math
    if not all(math.isfinite(v) for v in (args.capacity,args.overhead,args.poll_seconds,args.poll_timeout)) or args.capacity<=0 or args.overhead<0 or args.poll_seconds<=0 or args.poll_timeout<=0: p.error('Parametri numerici non validi')
    if not 1<=args.fallback_minutes<=10080 or (args.fixed_minutes is not None and not 1<=args.fixed_minutes<=10080): p.error('Durata in minuti: 1-10080')
    if args.fixed_minutes is not None and args.operation!='saturation': p.error('--fixed-minutes richiede --saturation')
    if not 7<=args.days<=90: p.error('--days deve essere tra 7 e 90')
    if args.operation=='saturation' and (args.job or args.jobs or args.prefix): p.error('--saturation analizza l’intero cluster')
    if args.job and ',' in args.job: p.error('Per più nomi utilizzare --jobs')
    if args.prefix is not None and not args.prefix.strip(): p.error('Prefisso vuoto')
    if args.operation=='overview' and (args.job or args.jobs or args.prefix): p.error('--overview mostra l’intero cluster')
    if args.operation and args.operation not in ('start','end') and (args.schedule_user or args.owner_policy): p.error('Opzioni utenza solo per --start / --end o menu interattivo')
    if args.apply and args.operation not in ('start','end'): p.error('--apply richiede --start o --end')
    if args.force or args.resume_paused:
        if args.operation!='start': p.error('--force e --resume-paused richiedono --start')
    if (args.expiring_month or args.all_expiries) and args.operation not in ('end','expiries'): p.error('Filtro scadenza solo per --end / --expiries')
    if args.new_date and args.operation!='end': p.error('--new-date richiede --end')
    if args.missing_end_only and args.operation!='end': p.error('--missing-end-only richiede --end')
    if args.details and args.operation not in ('start','end'): p.error('--details richiede --start o --end')
    if args.include_missing_end and (args.operation!='end' or args.expiring_month): p.error('--include-missing-end richiede --end e non è compatibile con --expiring-month')
    if not args.operation and (args.all or args.job or args.jobs or args.prefix or args.dry_run): p.error('Specificare anche una operazione')
    if args.export and (args.apply or not args.operation): p.error('--export richiede una consultazione o dry-run esplicito, senza --apply')
    cde=CDE(args.poll_seconds,args.poll_timeout)
    if args.operation:
        return operation(cde,args,args.operation)
    menu={'1':'start','2':'end','3':'cron','4':'overview','5':'describe','6':'calendar','7':'saturation'}
    while True:
        action_boundary()
        heading('CDE SCHEDULE MANAGER | v2.4.5', 'title')
        print(color('GESTIONE SCHEDULE')+'\n  [1] Aggiorna startDate - corregge le date di partenza\n  [2] Aggiorna endDate - rinnova o inserisce le scadenze\n  [3] Modifica orario - un job, monitoraggio post-apply\n\n'+color('CONSULTAZIONE RAPIDA (sola lettura)')+'\n  [4] Riepilogo Virtual Cluster - stato, anomalie e scadenze\n  [5] Dettaglio job - configurazione, risorse e run\n  [6] Calendario operativo - oggi, altre date e finestre per test\n\n'+color('ANALISI E OTTIMIZZAZIONE (sola lettura)')+'\n  [7] Saturazione e conflitti - stime e simulazioni\n\n  [0] Esci')
        choice=choose('Scelta: ',set(menu)|{'0'})
        if choice=='0': return 0
        try:
            rc=operation(cde,args,menu[choice],True)
            if rc: return rc
        except (CDEError,ValueError) as e: print('ERRORE:',e,file=sys.stderr)

if __name__=='__main__':
    try: sys.exit(main())
    except (CDEError,ValueError,EOFError,KeyboardInterrupt,OSError) as e:
        print('Operazione terminata:',e or type(e).__name__,file=sys.stderr); sys.exit(1)
PY
) "$@"
