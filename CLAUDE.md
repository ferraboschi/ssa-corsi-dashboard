# SSA Gestione Corsi — Technical Summary

## Project Scope

Dashboard gestionale per la **Sake Sommelier Association (SSA)** che permette di gestire corsi di formazione sul sake: monitorare iscrizioni, costi, educator, programmi didattici (sake da degustare), e generare report Excel. La piattaforma aggrega dati da Shopify (prodotti/ordini), Airtable (configurazioni persistenti e registrazioni studenti), e Twilio (verifica numeri WhatsApp).

**URL produzione:** `https://ssa-corsi-dashboard.onrender.com`
**Hosting:** Render (Web Service, manual deploy)
**Repository:** `https://github.com/ferraboschi/ssa-corsi-dashboard`
**Credenziali accesso:** definite via env vars `AUTH_USERNAME` e `AUTH_PASSWORD` su Render

---

## Stack Tecnico

### Backend — `server.js` (~2500 righe)

- **Runtime:** Node.js >= 18
- **Framework:** Express 4.18
- **Dipendenze npm:** `express`, `cors`, `dotenv`, `cookie-session`, `exceljs`
- **Nessun database relazionale** — persistenza via file JSON locale (`data/course-costs.json`, `data/share-tokens.json`) + sync asincrono su Airtable per sopravvivere ai wipe del filesystem Render

### Frontend — `public/index.html` (~4500 righe)

- **Single Page Application (SPA)** in vanilla JavaScript (zero framework)
- **CSS inline** nel file HTML
- **Font Awesome** per icone (CDN)
- **Navigazione** gestita via `navigateTo(page)` che chiama `renderCurrentPage()` — le pagine sono: `dashboard`, `corsi`, `corsisti`, `passati`, `educator`
- **Cache client-side:** `sessionStorage` con chiave `ssa_courses`, TTL 30 minuti

### Persistenza dati

Il pattern è "file locale + Airtable backup":
1. All'avvio il server carica `course-costs.json` e `share-tokens.json` dal filesystem
2. Poi arricchisce/sovrascrive con i dati da Airtable (tabella `SSA_CourseConfig`)
3. Ad ogni salvataggio: scrive su file + fire-and-forget `airtableConfigSet()`
4. Questo garantisce persistenza anche dopo i redeploy di Render che cancellano il filesystem efimero

---

## Variabili d'Ambiente (Render)

| Variabile | Descrizione |
|---|---|
| `SHOPIFY_STORE` | Store Shopify SSA (default: `sakesommelierassociation.myshopify.com`) |
| `SHOPIFY_ACCESS_TOKEN` | Token Admin REST API Shopify |
| `AIRTABLE_API_KEY` | API key Airtable (PAT) |
| `AIRTABLE_BASE_ID` | Base ID per config corsi (default: `appwCWGRd0jXOCxMA`) |
| `AIRTABLE_TABLE_ID` | Table ID sake products (default: `tblnJO5Mf7EVmteRk`) |
| `TWILIO_ACCOUNT_SID` | Account SID Twilio (opzionale) |
| `TWILIO_AUTH_TOKEN` | Auth Token Twilio (opzionale) |
| `SAKE_COMPANY_STORE` | Store Shopify Sake Company (default: `sake-company.myshopify.com`) |
| `SAKE_COMPANY_STOREFRONT_TOKEN` | Token Storefront API Sake Company |
| `AUTH_USERNAME` | Username login dashboard |
| `AUTH_PASSWORD` | Password login dashboard |
| `SESSION_SECRET` | Secret per cookie-session |
| `PORT` | Porta server (default: 3000) |

---

## API Esterne Utilizzate

### 1. Shopify Admin REST API (SSA Store)
- **Base URL:** `https://{SHOPIFY_STORE}/admin/api/2024-01/`
- **Auth:** Header `X-Shopify-Access-Token`
- **Endpoint usati:**
  - `GET /products.json?limit=250` — tutti i prodotti (paginazione via `since_id`)
  - `GET /orders.json?limit=250&status=any&created_at_min=2024-01-01` — tutti gli ordini dal 2024
  - `GET /customers.json?limit=250` — lista clienti (per export)
  - `GET /products/{id}/metafields.json` — metafield per ogni corso (educator name, location)
  - `GET /metaobject_definitions.json` — definizioni metaobject
  - `GET /metaobjects.json?type={type}` — metaobject (educator profiles da "Chi Siamo")
  - `GET /pages.json`, `GET /blogs.json`, `GET /collections.json` — endpoint debug
- **Rate limiting:** retry automatico su HTTP 429 con `Retry-After` header, max 3 tentativi
- **Caching:** prodotti 15 min, ordini 10 min, metafield 30 min

### 2. Shopify Storefront API (Sake Company Store)
- **Base URL:** `https://{SAKE_COMPANY_STORE}/api/2024-01/graphql.json`
- **Auth:** Header `X-Shopify-Storefront-Access-Token`
- **Query GraphQL:** recupera tutti i prodotti sake con immagini, SKU, prezzi
- **Scopo:** database dei sake disponibili per il programma didattico dei corsi
- **Caching:** 10 min

### 3. Airtable REST API
Usata per tre scopi distinti:

**a) Persistenza configurazioni (Base `appwCWGRd0jXOCxMA`)**
- Tabella `SSA_CourseConfig` — salva `course_costs` e `share_tokens` come record JSON
- CRUD via `GET/POST/PATCH` su `https://api.airtable.com/v0/{baseId}/{tableName}`

**b) Tabella sake prodotti (Table `tblnJO5Mf7EVmteRk`)**
- Catalogo sake con codice, nome, tipo, sakagura, dimensione, prezzo
- Usata dal frontend per la ricerca sake nel programma corsi

**c) Registrazione studenti QR (Base `app8OYdmX32x7Frjk`, Table `tblmHWvzfar6Wf0hw`)**
- Form compilato dagli studenti via QR code durante il corso
- Cross-reference con dati Shopify per verificare corrispondenza nomi
- Caching: 15 min

### 4. Twilio Lookup API v2
- **Endpoint:** `https://lookups.twilio.com/v2/PhoneNumbers/{phone}?Fields=line_type_intelligence`
- **Auth:** Basic (account_sid:auth_token)
- **Scopo:** verifica se un numero è mobile/VoIP (proxy per "ha WhatsApp")
- **Pattern non-bloccante:** la response HTTP viene inviata con dati cached; l'arricchimento Twilio avviene in background e aggiorna la cache
- **Caching:** 30 giorni in-memory (`twilioLookupCache`)

---

## Endpoint API del Server

### Autenticazione
| Metodo | Endpoint | Descrizione |
|---|---|---|
| GET | `/login` | Pagina login |
| POST | `/auth/login` | Verifica credenziali |
| POST/GET | `/auth/logout` | Logout |

### Dati principali
| Metodo | Endpoint | Descrizione |
|---|---|---|
| GET | `/api/health` | Health check |
| GET | `/api/courses` | **Endpoint principale** — ritorna tutti i corsi con iscritti, revenue, educator. Supporta `?nocache=1` |
| GET | `/api/costs` | Tutti i costi/configurazioni salvati per ogni corso |
| GET | `/api/costs/:courseId` | Costi di un singolo corso |
| POST | `/api/costs/:courseId` | Salva costi/programma/configurazione corso |

### Shopify proxy
| Metodo | Endpoint | Descrizione |
|---|---|---|
| GET | `/api/shopify/products` | Proxy prodotti Shopify |
| GET | `/api/shopify/orders` | Proxy ordini Shopify |
| GET | `/api/shopify/customers` | Proxy clienti Shopify |

### Airtable / Sake
| Metodo | Endpoint | Descrizione |
|---|---|---|
| GET | `/api/airtable/sake` | Catalogo sake da Airtable |
| GET | `/api/sakecompany/products` | Prodotti Sake Company (immagini sake) |

### Educator
| Metodo | Endpoint | Descrizione |
|---|---|---|
| GET | `/api/educator-profiles` | Profili educator (da Shopify metaobject "Chi Siamo") |
| GET | `/api/educator/:id` | Dettaglio educator con corsi e studenti |

### Export
| Metodo | Endpoint | Descrizione |
|---|---|---|
| GET | `/api/export/corsisti` | Export CSV di tutti i clienti Shopify |
| GET | `/api/export/course/:handle` | Export Excel iscritti di un corso |
| GET | `/api/export/sake/:handle` | Export Excel sake programma di un corso |

### Override dati
| Metodo | Endpoint | Descrizione |
|---|---|---|
| POST | `/api/phone-overrides/:courseId` | Override telefono studente (per-order) |
| POST | `/api/name-overrides/:courseId` | Override nome studente |
| POST | `/api/fatturato/:courseId` | Segna corso come fatturato |

### Link condivisione (Educator Share)
| Metodo | Endpoint | Descrizione |
|---|---|---|
| GET | `/api/share-link/:courseHandle` | Auto-genera o ritorna link condivisione |
| POST | `/api/share/:courseHandle` | Crea nuovo token condivisione |
| DELETE | `/api/share/:token` | Elimina token |
| GET | `/api/share-tokens/:courseHandle` | Lista token per corso |
| GET | `/api/shared/:token` | Dati corso per link condiviso (no auth) |
| GET | `/share/:token` | Pagina pubblica corso condiviso (render server-side) |

### Debug
| Metodo | Endpoint | Descrizione |
|---|---|---|
| GET | `/api/debug/metafields/:handle` | Metafield di un prodotto |
| GET | `/api/debug/metaobject-definitions` | Definizioni metaobject Shopify |
| GET | `/api/debug/metaobjects/:type` | Metaobject per tipo |
| GET | `/api/debug/shopify-pages` | Pagine Shopify |
| GET | `/api/debug/shopify-blogs` | Blog Shopify |
| GET | `/api/debug/shop-metafields` | Metafield shop-level |
| GET | `/api/debug/shopify-collections` | Collezioni Shopify |

---

## Struttura Dati Chiave

### Corso (oggetto ritornato da `/api/courses`)
```json
{
  "shopifyId": 10487499915601,
  "title": "Corso di Sake Sommelier Certificato - Maggio 2026, Milano",
  "handle": "corso-sake-sommelier-certificato-maggio-2026-milano",
  "tags": "Certificato, In Presenza",
  "status": "active",
  "created_at": "2026-01-15T...",
  "variants": [],
  "educatorName": "Lorenzo Ferraboschi",
  "educatorPhoto": "https://...",
  "educatorBio": "...",
  "enrollmentCount": 4,
  "revenue": 1670.00,
  "students": [
    {
      "name": "Nome Cognome",
      "email": "email@example.com",
      "phone": "+39...",
      "orderId": 123,
      "orderNumber": "SSA3217",
      "orderDate": "2026-03-09T...",
      "financialStatus": "paid",
      "amount": 490.00,
      "grossAmount": 590.00,
      "discountCode": "KITSUNE100",
      "hasWhatsApp": true,
      "registrationName": "Nome dal QR",
      "nameMismatch": false
    }
  ]
}
```

### COURSE_COSTS (salvato in Airtable e file locale, chiave = handle)
```json
{
  "corso-sake-sommelier-certificato-maggio-2026-milano": {
    "educator": 600,
    "gestione": 900,
    "diplomi": 460,
    "libri": 36,
    "location": 0,
    "food": 0,
    "adv": 0,
    "customLines": [],
    "educatorName": "Lorenzo Ferraboschi",
    "fatturato": false,
    "whatsappLink": "https://chat.whatsapp.com/...",
    "phoneOverrides": { "email@example.com": "+39..." },
    "phoneOverridesByOrder": { "SSA3217": "+39..." },
    "nameOverrides": { "SSA3217": "Nome Corretto" },
    "program": [
      {
        "name": "Assaggi",
        "day": 1,
        "sakes": [
          {
            "id": "rec123",
            "code": "SAK001",
            "name": "Junmai Daiginjo",
            "nameJp": "純米大吟醸",
            "type": "Junmai Daiginjo",
            "sakagura": "Asahi Shuzo",
            "size": 720,
            "cost": 45.00,
            "qty": 1,
            "note": "",
            "image": "https://..."
          }
        ]
      }
    ]
  }
}
```

---

## Logica Frontend Importante

### Parsing corso dal handle Shopify
I dati del corso (tipo, data, città) vengono estratti dall'handle del prodotto Shopify:

- **`parseCourseType(handle, title)`** — Restituisce tipo corso: `certificato`, `introduttivo`, `masterclass`, `shochu`, `mixology`, `bartending`. Include livello, colore badge, modalità (presenza/online).
- **`parseCourseDate(handle, courseObj)`** — Estrae mese/anno italiano dall'handle (es. `maggio-2026`). Se non trovato, fallback a `created_at` di Shopify.
- **`parseCourseCity(handle, title, tags)`** — Estrae città dall'handle. Controlla anche i tags per "ONLINE".

### Filtro prodotti corso
`isCourseProduct(product)` — filtra i prodotti Shopify che sono corsi (non poster, gift card, bundle, etc.). Verifica che l'handle contenga uno dei pattern: `certificato`, `introduttivo`, `shochu`, `masterclass`, `mixology`, `bartending`, `spirits-of-japan`.

### Pagine SPA
1. **Dashboard** — KPI (corsi attivi, corsisti totali, fatturato, corsi a rischio), tabella corsi attivi, sezioni Educator e Ultime Iscrizioni
2. **Corsi** — Cards dei corsi attivi con ricerca e filtro per tipo. Click apre dettaglio corso.
3. **Dettaglio Corso** — Conto economico, Programma & Prodotti (sake con drag/copy), lista iscritti con edit inline telefono/nome, link WhatsApp, link Educator condivisibile, export Excel iscritti e sake
4. **Corsisti** — Tabella aggregata di tutti gli studenti con ricerca e modal dettaglio
5. **Passati (Storico Corsi)** — Tutti i corsi (attivi + passati) con filtri per città/educator/mese/anno, ordinamento colonne, badge tipo corso, checkbox fatturato
6. **Educator** — Lista educator con statistiche corsi e studenti

### Sistema di Caching (multi-livello)

**Server-side (in-memory Map):**
- Prodotti Shopify: 15 min
- Ordini Shopify: 10 min
- Metafield corsi: 30 min
- Full response `/api/courses`: 10 min
- Registrazioni Airtable: 15 min
- Prodotti Sake Company: 10 min
- Twilio lookup: 30 giorni

**Client-side (sessionStorage):**
- Dati corsi (`ssa_courses`): 30 min
- Prodotti Sake Company: 30 min

**Background auto-refresh:** ogni 8 minuti il server fa una self-request a `/api/courses?nocache=1` per mantenere la cache calda.

**Warm-up all'avvio:** al boot il server pre-fetcha tutti i dati Shopify/Airtable e popola la cache, poi triggera una request `/api/courses` completa via `http.get` locale.

### Pattern Twilio Non-Bloccante
1. La response HTTP viene inviata immediatamente con i dati Twilio **cached** (`applyCachedTwilioData`)
2. Dopo la response, `enrichStudentsWithWhatsApp()` fa le lookup API in background (batch da 5)
3. Al completamento, aggiorna la cache del full response
4. Alla prossima request, il client riceve i dati arricchiti

---

## Flusso Deploy

1. Push su `main` del repo GitHub
2. Su Render: **Manual Deploy** (auto-deploy non attivo)
3. Il server si avvia, carica config da file + Airtable, esegue warm-up in background
4. Il primo utente che accede dopo il deploy troverà i dati già in cache (grazie al warm-up)

---

## File e Cartelle

```
/
├── server.js                    # Backend Express (~2500 righe)
├── package.json                 # Dipendenze npm
├── .env                         # Variabili ambiente (locale, non committato)
├── CLAUDE.md                    # Questo file — contesto tecnico per Claude Code
├── data/
│   ├── course-costs.json        # Persistenza locale costi/configurazioni
│   └── share-tokens.json        # Persistenza locale token condivisione
└── public/
    └── index.html               # Frontend SPA completo (~4500 righe)
```

---

## Note Operative per Claude Code

- **Non esiste un build step** — il progetto si avvia con `node server.js`
- **Non ci sono test automatizzati** — il testing è manuale sulla piattaforma live
- **Il frontend è monolitico** — tutto in un unico `index.html` (HTML + CSS + JS)
- **I dati dei sake nel programma usano il campo `code`** (non `sku`) come codice prodotto
- **L'handle Shopify è l'identificatore primario** dei corsi (`course.handle` = chiave in `COURSE_COSTS`)
- **Render ha filesystem efimero** — i file in `data/` vengono persi ad ogni deploy, per questo Airtable funge da backup persistente
- **Le cache hanno TTL differenziati** — modificarle impatta performance vs freshness dei dati
- **Il pattern non-bloccante Twilio** è critico per la velocità — non rendere Twilio sincrono
- **`parseCourseDate` ha un fallback** a `created_at` di Shopify per corsi senza data nel handle (es. masterclass)
- **`parseCourseCity` accetta un parametro `tags`** per rilevare corsi "ONLINE" dai tag Shopify
- **Il corso "attivo/futuro" viene determinato dal frontend** confrontando la data parsata con oggi — non c'è un flag Shopify
- **Dopo ogni modifica, l'utente fa manual deploy su Render** — il push su GitHub non triggera deploy automatico

---

## Changelog — Sessione 5 Maggio 2026

### ssa-corsi-dashboard (questo repo)

**1. Fix maxStudents per corsi con varianti multiple**
- Il conteggio `maxStudents` sommava l'inventario di tutte le varianti, incluse quelle "Early Bird" esaurite
- Fix: usa solo la variante con più inventory, oppure il metafield `custom.posti_disponibili` se presente
- File: `server.js` (funzione calcolo maxStudents dentro `/api/courses`)

**2. Fix ordini duplicati**
- Ordini con più line_item dello stesso corso (es. quantity > 1) venivano contati come studenti separati
- Fix: deduplica per `orderId` e usa `line_item.quantity` per contare i ticket correttamente
- File: `server.js` (aggregazione ordini), `public/index.html` (colonna "Ticket" nella tabella iscritti)

**3. Link diretto Shopify per ogni corso**
- Aggiunto bottone "Apri su Shopify" nella pagina dettaglio corso con link diretto al prodotto in admin Shopify
- File: `public/index.html` (template dettaglio corso)

**4. Colonna quantità ticket nella tabella iscritti**
- Aggiunta colonna "Ticket" che mostra la quantità acquistata per ordine (campo `quantity` dalla API)
- File: `server.js` (aggiunto `quantity` alla risposta API), `public/index.html` (colonna tabella)

**5. Contatti educator da Shopify Customers API**
- Nuova funzionalità: email e telefono degli educator recuperati dalla Shopify Customers API (`/customers/search.json?query=name:NomeEducator`)
- Cache server-side 24h per i contatti
- Visualizzazione: icone email/telefono cliccabili nella pagina dettaglio corso + contatti completi nelle card della pagina Educator
- File: `server.js` (funzioni `fetchEducatorContact`, `fetchAllEducatorContacts`, integrazione in `/api/courses`), `public/index.html` (4 punti: info grid, educator card, educator page cards, educator map building)

**6. Fix date parsing per usare date specifiche da Shopify**
- Le date dei corsi ora vengono estratte dal campo `variants[].title` di Shopify (es. "11-13 Mag 2026") invece che dall'handle
- Supporta date multi-giorno e range (es. "27-29 Mag 2026")
- File: `server.js` (parsing date da varianti)

### sake-menu-platform (repo separato: `ferraboschi/sake-menu-platform`)

**7. Fix build TypeScript fallito — campi mancanti in ProposalItem**
- Il commit `1bb8915` aveva aggiunto 3 campi al tipo `ProposalItem` (`price_box`, `custom_price_bottle`, `custom_price_box`) senza aggiornare le funzioni che costruiscono oggetti di quel tipo
- Fix: aggiunto i 3 campi in `productToProposalItem()`, fallback di `claudePairingToItem()`, e `itemsForInsert()`
- File: `lib/proposal-helpers.ts`
- Commit: `ff55e5a`
- Deploy: auto-deploy su Render riuscito, servizio live su `https://menu.sakecompany.com`

## Changelog — Sessione 7 Maggio 2026

### ssa-corsi-dashboard

**1. Fix perdita dati locali dopo salvataggio costi/programma**
- Bug: `saveCourseDetail()` nel client sovrascriveva `COURSE_COSTS[handle]` con solo il payload parziale (location, educator, food, sake, adv, lines, program), perdendo notebook, phoneOverrides, nameOverrides, educatorName, whatsappGroupLink etc. dalla memoria del browser
- Fix: il client ora usa la risposta del server (che contiene i dati completi post-merge) per aggiornare la cache locale. Fallback: merge invece di overwrite
- File: `public/index.html` (funzione `saveCourseDetail`, riga ~4672)

**2. Fix persistenza Airtable — await nel POST /api/costs**
- Bug: `saveCostsToFile()` scriveva su Airtable in modalità fire-and-forget. Se Render riavviava il servizio subito dopo il salvataggio, il filesystem efimero veniva cancellato e i dati Airtable potevano essere ancora quelli vecchi
- Fix: il POST `/api/costs/:courseId` ora fa `await saveCostsToFile()` per assicurarsi che Airtable sia aggiornato prima di rispondere al client. Aggiunto anche warning di dimensione quando i dati si avvicinano al limite di 100k caratteri di Airtable
- File: `server.js` (handler POST costs + funzione saveCostsToFile)

**3. saveCostsToFile() ora restituisce Promise**
- La funzione ora fa `return airtableConfigSet(...)` invece di fire-and-forget, permettendo ai chiamanti critici di await-are la persistenza
- File: `server.js`

### Note tecniche — Cache layers
Il sistema ha **3 livelli di cache** che vanno considerati dopo ogni deploy:
1. **Server full response cache** (`api_courses_full_response`): 10 min — bypassare con `?nocache=1`
2. **Browser sessionStorage** (`ssa_courses`): 30 min — l'utente deve fare hard refresh o aspettare
3. **Cache specifiche server** (prodotti 15min, ordini 10min, metafield 30min, educator contacts 24h)
