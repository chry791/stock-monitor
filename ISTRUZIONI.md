# Stock Monitor v3.0 — Istruzioni di installazione

Il tuo scanner di ribassi, online e apribile da qualsiasi dispositivo.
Tempo stimato: 15 minuti, una volta sola. Tutto gratuito.

## Cosa otterrai

- Un **URL personale** tipo `https://TUONOME.github.io/stock-monitor/`
  apribile da telefono, tablet, PC, ovunque nel mondo
- **Scansione automatica ogni mattina** (06:45 UTC, mar-sab) sui server
  di GitHub: il tuo PC può restare spento
- Pulsante **"Run workflow"** per lanciare una scansione a mano quando vuoi
- Stessa logica di sempre: 738 titoli, soglia -5%, CSV identico
- Ma con l'endpoint **bulk** di EODHD: ~20 secondi invece di 5 minuti

## Passo 1 — Account GitHub (se non ce l'hai)

1. Vai su https://github.com e clicca **Sign up**
2. Registrati con la tua email (gratis)

## Passo 2 — Crea il repository

1. In alto a destra clicca **+** → **New repository**
2. Nome: `stock-monitor`
3. Scegli **Private** (così solo tu vedi il codice; la pagina web
   funzionerà comunque*)
4. Clicca **Create repository**

*Nota: con repository Private, GitHub Pages richiede un piano Pro
(circa 4$/mese). Se vuoi tutto gratis scegli **Public**: il codice
sarà visibile ma NON contiene la tua API key (sta nei Secrets).

## Passo 3 — Carica i file

1. Nel repository appena creato clicca **uploading an existing file**
2. Trascina TUTTO il contenuto di questa cartella (scan.py, tickers.csv,
   ISTRUZIONI.md e le cartelle docs e .github)
   - ATTENZIONE: se il trascinamento non carica le cartelle nascoste,
     usa il metodo alternativo: **Add file → Create new file**, scrivi
     come nome `.github/workflows/scan.yml` e incolla dentro il
     contenuto del file scan.yml
3. Clicca **Commit changes**

## Passo 4 — Metti la API key al sicuro

1. Nel repository: **Settings → Secrets and variables → Actions**
2. Clicca **New repository secret**
3. Name: `EOD_API_KEY`
4. Secret: incolla la tua API key di EODHD (la trovi su
   https://eodhd.com nella tua dashboard)
5. **Add secret**

Consiglio: già che ci sei, rigenera la chiave dalla dashboard EODHD
(quella vecchia è scritta in chiaro nei vecchi file sul tuo PC).

## Passo 5 — Attiva la pagina web (GitHub Pages)

1. Nel repository: **Settings → Pages**
2. Sotto "Build and deployment":
   - Source: **Deploy from a branch**
   - Branch: **main** — cartella: **/docs**
3. **Save**
4. Dopo 1-2 minuti la pagina sarà su
   `https://TUONOME.github.io/stock-monitor/`
   (l'indirizzo esatto appare in cima alla stessa schermata Pages)

## Passo 6 — Prima scansione

1. Nel repository: scheda **Actions**
2. Se chiede di abilitare i workflow, clicca **I understand... enable**
3. A sinistra clicca **Scansione giornaliera** → **Run workflow** → **Run workflow**
4. Aspetta ~1 minuto: pallino verde = fatto
5. Apri il tuo URL: vedrai la dashboard con i ribassi del giorno

Da domani la scansione parte da sola ogni mattina.

## Uso quotidiano

- Apri l'URL da qualsiasi dispositivo: dati già pronti, zero attese
- Cursore "soglia" per vedere anche i ribassi sotto il 3%, 4%...
- Ricerca per nome o ticker, filtro per mercato
- "Scarica CSV" per il report del giorno (stesso formato di sempre)
- In basso, gli ultimi 30 giorni: tocca un giorno per rivedere i suoi alert

## Modificare la watchlist

Modifica `tickers.csv` direttamente su GitHub (matita in alto a destra
sul file) o chiedimelo in chat: formato `ticker,name,exchange,index`,
es. `ENEL.MI,Enel,MI,FTSE MIB`. Alla scansione successiva è attivo.

## Modificare la soglia

La soglia predefinita (-5%) si cambia nel file
`.github/workflows/scan.yml` aggiungendo sotto `EOD_API_KEY`:
`ALERT_THRESHOLD: "-4.0"` — ma di solito non serve: dalla dashboard
puoi già filtrare a qualsiasi soglia col cursore.

## Consumo API

Ogni scansione: circa 1.500-2.000 "crediti" API su 100.000 giornalieri
del tuo piano. Puoi lanciarla anche 50 volte al giorno senza problemi.

## Problemi?

Ricarica questa cartella in chat con Claude e descrivi cosa vedi:
sistemiamo insieme.
