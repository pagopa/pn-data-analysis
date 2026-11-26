#!/usr/bin/env bash
# Libreria condivisa: nessuna azione CDE al caricamento. Solo Python standard library.
# Il manager concatena questo modulo al proprio codice Python in memoria.
cde_utils_python() {
cat <<'PY'
import json, os, re, shlex, shutil, subprocess, sys, time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit

UTC = timezone.utc

# ANSI standard: nessuna libreria esterna, nessun colore in pipe o NO_COLOR.
def color(text, kind='heading'):
    codes={'heading':'1;36','title':'1;95','prompt':'1;95','ok':'32','warn':'33','error':'31','muted':'2','job':'1;97'}
    if not sys.stdout.isatty() or os.environ.get('TERM')=='dumb' or 'NO_COLOR' in os.environ:
        return str(text)
    return '\033['+codes[kind]+'m'+str(text)+'\033[0m'

def use_unicode():
    return sys.stdout.isatty() and 'NO_COLOR' not in os.environ and os.environ.get('TERM')!='dumb' and 'UTF' in (sys.stdout.encoding or '').upper()

def action_boundary():
    print('\n'+color(('━' if use_unicode() else '=')*72,'title'))

def job_name(name):
    return color(name,'job')

def heading(text, kind='heading'):
    # Separatore compatto e leggibile anche senza colori. Il titolo principale ha già le sue righe.
    if kind != 'title':
        print('\n'+color(('─' if use_unicode() else '-')*72,'muted'))
        print(color(text,kind))
    else: print('\n'+color(text,kind))

def ask(prompt):
    # Coloriamo solo la domanda: password e input CLI restano gestiti dalla CLI.
    return input(color(prompt,'prompt'))

def status_text(text):
    kind={'OK':'ok','UPDATE':'warn','SKIP':'warn','KO':'error','IN PAUSA':'warn',
          'SCADUTO':'error','ABILITATO':'ok','SENZA END':'muted','FUTURA':'ok'}.get(text,'muted')
    return color(text,kind)


def utcnow():
    return datetime.now(UTC)

def timestamp(value):
    if value in (None, ''):
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except ValueError:
        # CDE restituisce anche date HTTP/RFC 1123; non inventare il fuso.
        dt = parsedate_to_datetime(str(value))
    if dt.tzinfo is None:
        raise ValueError('Timestamp senza timezone')
    return dt.astimezone(UTC)

def iso(dt):
    return dt.astimezone(UTC).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def duration(seconds):
    m, s = divmod(max(0, int(seconds)), 60)
    h, m = divmod(m, 60)
    return (f'{h}h ' if h else '') + (f'{m}m ' if m or h else '') + (f'{s:02}s' if m or h else f'{s}s')

def flag(value):
    return str(value).lower()

def export_json(path, obj):
    # Esportazione solo su richiesta; non sovrascrivere file esistenti.
    path = Path(path).expanduser()
    with path.open('x', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write('\n')
    print('Esportazione JSON:', path.resolve())

class Journal:
    # Unico file permanente per apply, scritto prima di ogni modifica.
    # JSON Lines: ogni riga è un record indipendente, recuperabile anche dopo un'interruzione.
    def __init__(self, operation, plan):
        # Directory accanto agli script, indipendente dalla directory di lancio.
        # Creata solo all'apply autorizzato; non spostare o cancellare backup preesistenti.
        backup_dir = Path(os.environ['CDE_MANAGER_DIR']) / 'backups'
        if backup_dir.is_symlink():
            raise OSError(f'La directory dei backup non può essere un link simbolico: {backup_dir}')
        backup_dir.mkdir(mode=0o700, exist_ok=True)
        if backup_dir.is_symlink():
            raise OSError(f'La directory dei backup non può essere un link simbolico: {backup_dir}')
        self.path = backup_dir / ('cde_schedule_backup_'+operation+'_'+utcnow().strftime('%Y%m%dT%H%M%S_%fZ')+'.jsonl')
        with self.path.open('x', encoding='utf-8') as f:
            self._write(f, 'plan', operation=operation, rows=plan)

    @staticmethod
    def _write(f, event, **data):
        f.write(json.dumps(dict(timestamp=iso(utcnow()), event=event, **data), ensure_ascii=False)+'\n')
        f.flush()
        os.fsync(f.fileno())

    def record(self, event, **data):
        with self.path.open('a', encoding='utf-8') as f:
            self._write(f, event, **data)

def schedule(job):
    s = job.get('schedule') or {}
    if not isinstance(s, dict):
        raise ValueError('schedule non valida')
    return s

def bool_field(s, key):
    v = s.get(key, False)
    if not isinstance(v, bool):
        raise ValueError(f'{key}: booleano non valido')
    return v

def choose(prompt, allowed):
    while True:
        answer = ask(prompt).strip()
        if answer in allowed:
            action_boundary()
            return answer
        print('Scelta non valida. Riprova.')

def select_jobs(jobs, names=None, prefix=None, interactive=False, compact=False, show_schedule=False):
    by_name = {j['name']: j for j in jobs}
    if names is not None:
        wanted = list(dict.fromkeys(n.strip() for n in names.split(',') if n.strip()))
        if not wanted or any(n not in by_name for n in wanted):
            raise ValueError('Nomi inesistenti o selezione vuota: '+', '.join(n for n in wanted if n not in by_name))
        return [by_name[n] for n in wanted]
    if prefix is not None:
        return [j for j in jobs if j['name'].startswith(prefix)]
    if not interactive or not jobs:
        return jobs
    def display(pool):
        for i,j in enumerate(pool,1):
            if show_schedule:
                print(f'  {i:3} | {schedule_label(schedule(j).get("cronExpression"))} | {job_name(j["name"])}')
            else: print(f'  {i:3} | {job_name(j["name"])}')
    heading(f'QUALI JOB VUOI SELEZIONARE? ({len(jobs)} disponibili)')
    print('[1] Tutti - seleziona l’intero elenco\n[2] Uno solo - scegli un job\n[3] Più job - scegli numeri o nomi\n[4] Cerca per nome - filtra e scegli\n[0] Annulla')
    mode=choose('Scelta: ',{'0','1','2','3','4','TUTTI','tutti'})
    if mode=='0': return []
    if mode in ('1','TUTTI','tutti'): return jobs
    pool=jobs
    if mode=='4':
        while True:
            term=ask('Parte del nome (0 annulla): ').strip()
            if term=='0': return []
            pool=[j for j in jobs if term and term.casefold() in j['name'].casefold()]
            if pool: break
            print('Nessun risultato, riprova.')
    display(pool)
    lookup={j['name']:i for i,j in enumerate(pool,1)}
    while True:
        prompt='Numero o nome del job (0 annulla): ' if mode=='2' else 'Numeri (es. 1,3-5), nomi separati da virgola, TUTTI oppure 0: '
        a=ask(prompt).strip()
        if a=='0': return []
        if a.upper()=='LISTA': display(pool); continue
        if a.upper()=='TUTTI' and mode!='2': return pool
        try:
            indices=[]
            for token in a.split(','):
                token=token.strip()
                if token in lookup: indices.append(lookup[token])
                elif re.fullmatch(r'\d+',token): indices.append(int(token))
                elif re.fullmatch(r'\d+-\d+',token) and mode!='2':
                    lo,hi=map(int,token.split('-'))
                    if not 1<=lo<=hi<=len(pool): raise ValueError()
                    indices.extend(range(lo,hi+1))
                else: raise ValueError()
            indices=list(dict.fromkeys(indices))
            if not indices or not all(1<=i<=len(pool) for i in indices) or (mode=='2' and len(indices)!=1): raise ValueError()
            return [pool[i-1] for i in indices]
        except ValueError: print('Selezione non valida, riprova.')

class CDEError(RuntimeError): pass

class CDE:
    def __init__(self, poll_seconds=2, poll_timeout=90):
        if not shutil.which('cde'): raise CDEError('CDE CLI non trovata nel PATH')
        self.config = Path(os.environ.get('CDE_CONFIG', str(Path.home()/'.cde/config.yaml'))).expanduser()
        if not self.config.is_file(): raise CDEError(f'Configurazione non trovata: {self.config}')
        # Stesso requisito del fix collaudato: la CLI gestisce direttamente la password.
        try: self.tty = open('/dev/tty', 'r')
        except OSError as e: raise CDEError('Avviare da un terminale interattivo (WSL Ubuntu)') from e
        self.poll_seconds, self.poll_timeout = poll_seconds, poll_timeout

    def context(self):
        print(f'Configurazione CDE: {self.config}')
        # Solo host degli endpoint, mai credenziali o contenuto completo del config.
        hosts = set()
        for line in self.config.read_text(encoding='utf-8').splitlines():
            m = re.match(r'^\s*(?:vcluster-endpoint|vclusterEndpoint|endpoint)\s*:\s*[\'\"]?(https?://[^\s\'\"#]+)', line)
            if m:
                host = urlsplit(m.group(1)).hostname
                if host: hosts.add(host)
        print('Host nel config (non prova del profilo attivo): '+(', '.join(sorted(hosts)) or 'non individuati'))
        print('Verificare che la configurazione/profilo CLI punti al Virtual Cluster desiderato.')

    def log(self, msg):
        print(f'{utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")} {msg}', flush=True)

    def command(self, *args, capture=True):
        # Nessuna password nel codice, negli argomenti o nei log. Stderr resta visibile.
        try:
            result = subprocess.run(['cde', *args], stdin=self.tty,
                                    stdout=subprocess.PIPE if capture else None,
                                    text=True, check=False)
        except OSError as e: raise CDEError('Impossibile avviare CDE CLI') from e
        if result.returncode:
            raise CDEError(f'CDE exit {result.returncode}: '+shlex.join(args))
        return result.stdout if capture else ''

    def json_command(self, expected, *args):
        # Hotfix 1.1: dopo il login CDE può restituire una risposta non JSON
        # anche con exit code 0. Ripetiamo una sola volta le letture note,
        # senza estrarre JSON parziale né stampare l'output potenzialmente sensibile.
        # I comandi di modifica e gli exit code non zero non vengono ritentati.
        readonly = args == ('job', 'list') or (
            len(args) == 4 and args[:3] == ('job', 'describe', '--name')) or args[:2] in (('run', 'list'), ('run', 'describe'))
        for attempt in range(2 if readonly else 1):
            raw = self.command(*args)
            try:
                obj = json.loads(raw)
            except (ValueError, TypeError) as e:
                if readonly and attempt == 0:
                    print('  Acquisizione della risposta CDE in corso; nuovo tentativo di lettura...', flush=True)
                    time.sleep(3)
                    continue
                raise CDEError('Risposta CDE non JSON; verificare autenticazione/CLI '
                               '(nessun dato utilizzato)') from e
            if not isinstance(obj, expected):
                raise CDEError('Struttura JSON CDE inattesa')
            return obj

    def jobs(self):
        jobs = self.json_command(list, 'job', 'list')
        if any(not isinstance(j, dict) or not isinstance(j.get('name'), str) or not j['name'] for j in jobs):
            raise CDEError('Elenco job non valido')
        if len({j['name'] for j in jobs}) != len(jobs): raise CDEError('Nomi duplicati nella risposta CDE')
        return sorted(jobs, key=lambda j:j['name'].casefold())

    def describe(self, name):
        obj = self.json_command(dict, 'job', 'describe', '--name', name)
        if obj.get('name') != name: raise CDEError('Describe restituisce un job differente')
        return obj

    def successful_runs(self, name=None):
        args=['run','list','--filter','status[eq]succeeded']
        if name is not None: args+=['--filter',f'job[eq]{name}']
        return self.json_command(list,*args)

    def preflight(self, flags, subcommands):
        helptext = self.command('job', 'update', '--help')
        for f in flags:
            if not re.search(r'(?<![\w-])'+re.escape(f)+r'(?![\w-])', helptext):
                raise CDEError('Flag non supportato dalla CLI: '+f)
        for sub in subcommands: self.command('job', 'schedule', sub, '--help')

    def mutate(self, *args):
        self.log('CMD: '+shlex.join(['cde', *args]))
        self.command(*args, capture=False)

    def poll(self, label, fetch, predicate):
        # Timeout monotonic del polling (non interrompe una chiamata CLI o un prompt password).
        start = time.monotonic()
        while True:
            obj = fetch()
            if predicate(obj):
                self.log(f'OK - {label} ({duration(time.monotonic()-start)})')
                return obj
            remaining = self.poll_timeout-(time.monotonic()-start)
            if remaining <= 0: raise CDEError('Verifica non confermata: '+label)
            self.log('POLL - '+label)
            time.sleep(min(self.poll_seconds, remaining))

    def safety_pause(self, name):
        try:
            self.mutate('job','schedule','pause','--name',name)
            self.poll('pause di sicurezza '+name, lambda:self.describe(name), lambda j:bool_field(schedule(j),'paused'))
            self.log('ATTENZIONE: job lasciato in pausa; verificare manualmente.')
            return True
        except (Exception, KeyboardInterrupt) as e:
            self.log(f'ATTENZIONE: pause di sicurezza NON confermato per {name}: {e}. Verifica manuale urgente.')
            return False

# Stima delle risorse configurate: non è una metrica osservata.
def memory_gib(value):
    m = re.fullmatch(r'(\d+(?:\.\d+)?)\s*([kmgt])(?:i?b)?', str(value).strip(), re.I)
    if not m: raise ValueError('unità memoria non riconosciuta')
    return float(m.group(1))*{'k':1/1048576,'m':1/1024,'g':1,'t':1024}[m.group(2).lower()]

def resources(job, overhead=40):
    if job.get('type') != 'spark': return None
    sp = job.get('spark') or {}
    if not isinstance(sp,dict): return None
    conf = sp.get('conf') or {}
    if not isinstance(conf,dict): return None
    try:
        d,e = memory_gib(sp['driverMemory']), memory_gib(sp['executorMemory'])
        if str(conf.get('spark.dynamicAllocation.enabled','')).lower() == 'false':
            lo=hi=int(conf.get('spark.executor.instances', sp.get('numExecutors')))
        elif 'spark.dynamicAllocation.minExecutors' in conf and 'spark.dynamicAllocation.maxExecutors' in conf:
            lo=int(conf['spark.dynamicAllocation.minExecutors']); hi=int(conf['spark.dynamicAllocation.maxExecutors'])
        elif str(conf.get('spark.dynamicAllocation.enabled','')).lower() == 'true' or any(k in conf for k in ('spark.dynamicAllocation.minExecutors','spark.dynamicAllocation.maxExecutors')):
            return None  # dynamic allocation incompleta: non ipotizzare un numero fisso
        elif 'numExecutors' in sp or 'spark.executor.instances' in conf:
            lo=hi=int(conf.get('spark.executor.instances',sp.get('numExecutors')))
        else: return None
        if not 0 <= lo <= hi or d<=0 or e<=0: return None
        # Overhead uniforme ipotetico; se esistono override la stima resta uno scenario.
        return dict(driver=d,executor=e,min_ex=lo,max_ex=hi,min_gib=(d+lo*e)*(1+overhead/100),max_gib=(d+hi*e)*(1+overhead/100),driver_cores=sp.get('driverCores'),executor_cores=sp.get('executorCores'))
    except (ValueError, TypeError, KeyError): return None
PY
}
