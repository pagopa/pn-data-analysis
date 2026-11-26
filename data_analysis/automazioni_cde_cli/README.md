# CDE Schedule Manager

Utility per consultare e gestire le schedulazioni dei job Spark di Cloudera Data Engineering (CDE) tramite CLI. Versione: **2.4.5**.

Il manager si esegue localmente, non come job CDE. Integra in un unico menu le automazioni di aggiornamento delle schedule e le funzionalità di consultazione e analisi del Virtual Cluster.

## Requisiti

- Ubuntu/WSL, Bash e Python 3 (sola libreria standard).
- CDE CLI installata e configurata per il Virtual Cluster desiderato.
- Terminale interattivo e autorizzazioni CDE adeguate alle operazioni richieste.

Per configurare la CLI consultare [0. Setup CDE CLI](https://pagopa.atlassian.net/wiki/spaces/SSE/pages/2656862307/0.+Setup+CDE+CLI). Per i comandi di gestione manuale consultare [1. Creazione e Gestione Job CDE da CLI](https://pagopa.atlassian.net/wiki/spaces/SSE/pages/2656895078/1.+Creazione+e+Gestione+Job+CDE+da+CLI).

## Avvio

Scaricare `cde_schedule_manager.sh` e `cde_cli_utils.sh` nella stessa directory. Da un terminale Ubuntu, entrare nella directory ed eseguire:

```bash
chmod +x cde_schedule_manager.sh
./cde_schedule_manager.sh
```

La password workload, se richiesta, viene gestita direttamente dalla CDE CLI. Verificare sempre il Virtual Cluster e il profilo attivi prima di operare.

## Funzionalità

| Menu | Funzione |
| --- | --- |
| 1 – Aggiorna startDate | Calcola e corregge le date di partenza delle schedule. |
| 2 – Aggiorna endDate | Rinnova le scadenze o inserisce quelle mancanti, se autorizzato. |
| 3 – Modifica orario | Cambia l'orario UTC di un job senza modificarne la frequenza; stima l'impatto e osserva eventuali nuovi avvii. |
| 4 – Riepilogo Virtual Cluster | Stato delle schedule, scadenze e anomalie. |
| 5 – Dettaglio job | Configurazione, risorse Spark e storico delle run. |
| 6 – Calendario operativo | Avvii teorici, run attive e finestre candidate per i test. |
| 7 – Saturazione e conflitti | Stime delle sovrapposizioni e simulazioni di orari alternativi. |

Le opzioni 4–7 sono di consultazione; il trasferimento di una simulazione apre una procedura di modifica separata. Nessun aggiornamento viene applicato automaticamente.

## Utilizzo da riga di comando

Oltre al menu interattivo sono disponibili comandi diretti. Ad esempio:

```bash
./cde_schedule_manager.sh --help
./cde_schedule_manager.sh --overview
./cde_schedule_manager.sh --start --all --dry-run
./cde_schedule_manager.sh --end --all --expiring-month 2026-12 --new-date 2031-01-01 --dry-run
./cde_schedule_manager.sh --describe --job NOME_JOB
./cde_schedule_manager.sh --saturation
```

Per il cambio di orario utilizzare il menu interattivo. Tutte le date e gli orari sono UTC; la endDate corrisponde alle 00:00 del giorno inserito. Il dry-run è la modalità predefinita per le modifiche da CLI.

## Sicurezza e limiti

- Verificare il piano in dry-run prima di qualsiasi apply; l'aggiornamento richiede la conferma `APPLICA`.
- Il manager controlla configurazioni e run attive, salva lo stato originale e aggiorna i job sequenzialmente. I controlli non costituiscono un lock del cluster. CDE può cambiare l'utenza della schedule durante un update.
- I cambi cron possono generare avvii inattesi anche senza `CLEAR` e con `catchup=false`. Il monitoraggio successivo rileva nuovi ID nella finestra osservata; nessun kill o rollback è automatico.
- La saturazione rappresenta una stima della memoria configurata, non il consumo effettivo. Verificare capacità reale, dipendenze e SLA prima di applicare un piano.
- Consultazioni e dry-run non producono file persistenti, salvo esportazione esplicita. Gli apply producono un registro JSONL in `backups/` accanto agli script. Non pubblicare backup, configurazioni, credenziali o esportazioni interne nel repository.

## Stato di validazione

Il 25–26 novembre 2026 sono stati collaudati il rinnovo delle endDate (versione precedente del manager), le consultazioni, le simulazioni e un apply cron reale con la v2.4.5. La correzione startDate è stata validata con gli script originari; l'apply nel manager unificato e il kill assistito restano da verificare su CDE reale quando necessario. Il successo delle verifiche di configurazione non certifica le esecuzioni schedulate successive.

Per la procedura operativa completa fare riferimento alla pagina Confluence **CDE Schedule Manager** del Team Data Analysis.
