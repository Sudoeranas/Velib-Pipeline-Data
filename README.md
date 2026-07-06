# VélibData — Plateforme Big Data Vélib' Métropole


## Contexte

Vélib' Métropole exploite 1 400 stations et 20 000 vélos à Paris. Ce projet conçoit, déploie et administre une plateforme Big Data permettant :
- D'optimiser la redistribution physique des vélos (Direction Mobilité)
- D'entraîner des modèles ML de prédiction de la demande (Data Scientists)
- De superviser la plateforme et maîtriser les coûts cloud (DSI, budget ~500 €/mois)

---

## Architecture réelle déployée

```
API Vélib OpenData Paris (opendata.paris.fr)
  │
  ▼
Airflow (VM IaaS GCP — e2-medium, europe-west1-c)
  │  DAG availability  → toutes les 6h  → GCS raw/availability/YYYY/MM/DD/HH/
  │  DAG stations      → toutes les 6h  → GCS raw/stations/YYYY/MM/DD/
  │  DAG dbt           → 30 min après   → dbt run + dbt test
  │
  ▼
GCS bucket_velib_paris (Google Cloud Storage)
  │  raw/availability/  →  JSON { ingested_at, total, records: [...1511 stations] }
  │  raw/stations/      →  JSON { ingested_at, total, records: [...1510 stations] }
  │
  ▼  Snowpipe auto-ingest (GCS Pub/Sub → Snowflake)
  │  Notification GCS → Pub/Sub topic: velib-snowpipe-topic
  │  Subscription: velib-snowpipe-topic-sub
  │  Storage Integration: gcs_velib_integration
  │
  ▼
Snowflake (VELIB_RAW)
  │  RAW_AVAILABILITY  : 1 row/fichier (VARIANT JSON brut)
  │  RAW_STATIONS      : 1 row/fichier (VARIANT JSON brut)
  │
  ▼  dbt (dbt-snowflake 1.9.0 — via Airflow DAG sur VM)
  │  LATERAL FLATTEN sur data:records pour dépiler le JSON
  │
  ▼
Snowflake (VELIB_STAGING)
  │  stg_availability  : ~35 000+ lignes — vue — 1 ligne/station/ingestion
  │  stg_stations      : ~1 510 lignes  — vue — 1 ligne/station (dernière version)
  │    └─ city_district récupéré depuis raw_availability (nom_arrondissement_communes)
  │    └─ is_installed/is_renting/is_returning : "OUI"/"NON" → TRUE/FALSE
  │
  ▼
Snowflake (VELIB_MARTS)
  │  mart_station_availability : agrégats horaires par station (BI / Direction Mobilité)
  │  mart_demand_forecast      : agrégats jour/heure pour les modèles ML
```

---

## Stack technique réelle

| Composant | Choix retenu | Justification |
|---|---|---|
| Orchestration | Apache Airflow 2.10 (Docker, VM IaaS GCP) | Cloud Composer abandonné après 3 échecs (quotas GKE, droits SA) — VM e2-medium à 7,53$/mois vs 85$/mois |
| Transformation | dbt-snowflake 1.9.0 | SQL versionné, tests natifs, lineage, DAG Airflow automatisé |
| Entrepôt | Snowflake (3 databases + RBAC) | Serverless, Time Travel, colonnes VARIANT pour JSON |
| Stockage brut | GCS bucket_velib_paris | Zone Raw, partitionnement YYYY/MM/DD/HH |
| Ingestion auto | Snowpipe + Pub/Sub GCP | Auto-ingest GCS → Snowflake en < 1 min |
| Monitoring | Prometheus + Grafana + StatsD + cAdvisor (VM e2-micro) | Métriques Airflow + containers Docker |
| Orchestration K8s (démo) | k3s (installé sur VM airflow) | Liveness/readiness probes pour la démo jury |
| CI/CD | GitHub Actions | dbt test automatique à chaque push sur velib_dbt/ |

---

## Infrastructure GCP

| VM | Type | Zone | IP externe | Rôle |
|---|---|---|---|---|
| velib-airflow | e2-medium (2 vCPU, 4 GB) | europe-west1-c | 34.22.120.14 | Airflow + dbt + k3s |
| velib-monitoring | e2-micro (2 vCPU, 1 GB) | europe-west1-c | 34.62.7.39 | Prometheus + Grafana |

**Firewall rules** :
- `allow-airflow` : tcp:8080 → tag `velib-airflow`
- `allow-monitoring` : tcp:3000,9090,9102 → tag `velib-monitoring`

---

## Snowflake — Configuration

**Account** : ZZHNGGJ-MPB61332
**Warehouse** : VELIB_WH (X-SMALL, auto-suspend 60s)

### Databases et schemas

| Database | Schema | Contenu |
|---|---|---|
| VELIB_RAW | RAW | raw_availability, raw_stations (VARIANT) |
| VELIB_STAGING | STAGING | stg_availability (vue), stg_stations (vue) |
| VELIB_MARTS | MARTS | mart_station_availability (table), mart_demand_forecast (table) |

### RBAC

| Rôle | Accès |
|---|---|
| ACCOUNTADMIN | Administration complète |
| DATA_ENGINEER | Toutes les couches — utilisé par dbt (DBT_USER) |
| DATA_SCIENTIST | STAGING + MARTS lecture |
| DATA_ANALYST | MARTS lecture seule |

---

## Snowpipe — Configuration

- **Storage Integration** : `gcs_velib_integration` (SA Snowflake)
- **Notification Integration** : `velib_pubsub_integration` (SA Pub/Sub )
- **Stage** : `@VELIB_RAW.RAW.gcs_velib_stage` → `gcs://bucket_velib_paris/`
- **Pipe availability** : `VELIB_RAW.RAW.PIPE_RAW_AVAILABILITY` → `raw/availability/`
- **Pipe stations** : `VELIB_RAW.RAW.PIPE_RAW_STATIONS` → `raw/stations/`

---

## DAGs Airflow

| DAG | Schedule | Description |
|---|---|---|
| `velib_availability_ingestion` | `0 * * * *` | Fetch API disponibilité → GCS |
| `velib_stations_ingestion` | `0 * * * *` | Fetch API stations → GCS |
| `velib_dbt_transform` | `30 * * * *` | dbt run + dbt test (après ingestion) |

Paramètres communs : retry=3, retry_delay=30s, retry_exponential_backoff, on_failure_callback, max_active_runs=1

---

## dbt — Modèles et tests

### Staging (vues — VELIB_STAGING.STAGING)

**stg_availability** : LATERAL FLATTEN sur `data:records`, conversion "OUI"/"NON" → BOOLEAN, déduplication par station_id + ingested_at

**stg_stations** : LATERAL FLATTEN sur `data:records`, city_district récupéré depuis raw_availability via LEFT JOIN

### Marts (tables — VELIB_MARTS.MARTS)

**mart_station_availability** : agrégats horaires (avg/min/max vélos par station par heure)

**mart_demand_forecast** : agrégats par jour_semaine + heure (pour ML)

### Tests qualité (17 tests)
`not_null`, `unique`, `accepted_values` (day_of_week 0-6, hour_of_day 0-23) sur tous les modèles

---

## CI/CD — GitHub Actions

Fichier : `.github/workflows/dbt-ci.yml`
Déclencheur : push/PR sur `main` si fichiers `velib_dbt/**` modifiés
Jobs : `dbt debug` → `dbt run` → `dbt test`
Secrets requis : `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`

---

## Sources de données

| API | Fréquence | Lignes |
|---|---|---|
| Disponibilité (velib-disponibilite-en-temps-reel) | Toutes les 6h | ~1 511 stations |
| Stations (velib-emplacement-des-stations) | Toutes les 6h | ~1 510 stations |

Pagination : 100 résultats/page, boucle jusqu'à `total_count`. SSL verify=False (réseau entreprise).

---

## Sécurité

- Credentials GCS : `credentials/gcp-service-account.json` — gitignored, copié manuellement sur la VM
- Secret key Airflow : variable d'environnement `${AIRFLOW_SECRET_KEY}` via `.env`
- Credentials Snowflake : variables d'environnement `SNOWFLAKE_ACCOUNT/USER/PASSWORD` via `.env`
- `.env` gitignored — `.env.example` versionné
- Aucune PII collectée (données d'infrastructure publiques)

---

## Choix architecturaux importants

### Pourquoi VM IaaS plutôt que Cloud Composer (PaaS) ?
Cloud Composer v2 a échoué 3 fois sur ~3h pour des raisons de droits IAM sur les Service Accounts Google-managed (cloudservices, cloudcomposer-accounts, GKE nodes). Les quotas GKE et les permissions sur les SA système sont non-modifiables sans support Google. Décision : VM e2-medium à 7,53$/mois (vs 85$/mois pour Composer). Économie : ~77$/mois, crédits GCP durent 7+ mois au lieu de 3. Le choix IaaS vs PaaS est aussi explicite.

### Pourquoi VARIANT dans Snowflake pour le RAW ?
Le JSON des DAGs est structuré `{"ingested_at": ..., "records": [...]}`. Charger directement les colonnes via COPY INTO ne fonctionnait pas (1 seule ligne par fichier au lieu de 1511). Solution : 1 ligne par fichier en VARIANT, puis LATERAL FLATTEN dans dbt. Cela préserve le JSON brut intact (principe Raw = données non modifiées) et respecte l'architecture médaillon.

### Pourquoi "OUI"/"NON" et non TRUE/FALSE ?
L'API OpenData Paris retourne les booléens en français textuel. La conversion est faite dans stg_availability : `f.value:is_installed::STRING = 'OUI' AS is_installed`.

### Pourquoi city_district dans stg_stations vient de raw_availability ?
Le fichier `raw/stations/` de l'API ne contient pas le champ `nom_arrondissement_communes` — ce champ n'existe que dans le fichier de disponibilité. Un LEFT JOIN dans stg_stations récupère le district depuis la vue stg_availability.

### k3s pour les liveness probes
k3s est installé sur la VM airflow pour démontrer la connaissance Kubernetes (liveness/readiness probes, deployments, kubectl). Le pipeline réel tourne sur Docker Compose avec `restart: always` + healthchecks équivalents côté Docker.

---

## Budget estimé

| Service | Coût estimé |
|---|---|
| VM velib-airflow (e2-medium) | ~5,5$/mois |
| VM velib-monitoring (e2-micro) | ~2$/mois |
| GCS + Pub/Sub | < 1$/mois |
| Snowflake (S warehouse, auto-suspend 60s) | ~15-20$/mois |
| **Total** | **~25-30$/mois** |

Crédits GCP disponibles : 250$ → durée estimée 8+ mois.
