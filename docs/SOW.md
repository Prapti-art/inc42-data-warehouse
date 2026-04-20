# Statement of Work — Inc42 Data Warehouse

**Client:** Inc42 Media
**Vendor:** [Vendor Name]
**Reference Architecture:** [architecture-detailed.md](./architecture-detailed.md)
**Effective Date:** 2026-04-20
**Version:** 1.0

---

## 1. Background & Objectives

Inc42 operates 7 disconnected source systems (Gravity Forms, Tally, Inc42 DB, Customer.io, WooCommerce, HubSpot, Datalabs DB). This SOW governs the delivery of a unified BigQuery-based warehouse with three tracks:

1. **ETL & Warehouse** — ingest and transform all 7 sources into a Bronze → Silver → Gold medallion model. *(Largely delivered — see §3.)*
2. **Reverse ETL** — activate Gold-layer segments into Customer.io, HubSpot, and PostHog. *(Customer.io partial; HubSpot and PostHog new.)*
3. **Conversational Data Access (Chatbot)** — natural-language questions return results as an Excel file or Google Sheet. *(New build.)*

**Business outcome:** single source of truth for 330K+ unified contacts and 75K companies, with self-serve analytics across marketing, sales, events, revenue, and product teams.

**Status legend used below:** ✅ Live · 🚧 Partial / in progress · 📋 Planned (new scope under this SOW)

---

## 2. Current State (April 2026)

The warehouse is already operational. Metrics below are from the live pipeline and establish the baseline this SOW extends.

| Metric | Current |
|---|---|
| Source systems connected | 7 of 7 ✅ |
| Bronze tables ingested | 49 (25.3M rows) ✅ |
| Identity resolution (unique people) | 330K across 1M+ records (68% dedup) ✅ |
| Silver cleaned contacts | 328K ✅ |
| Gold `contact_360` / `company_360` | Live (328K / 75K rows) ✅ |
| Orchestration (Airflow, daily midnight IST) | Live ✅ |
| dbt Core tests & docs | Live ✅ |
| Streamlit dashboards (7 pages) | Live ✅ |
| Looker Studio | Not started 📋 |
| Reverse ETL → Customer.io | Partial 🚧 |
| Reverse ETL → HubSpot | Not started 📋 |
| Reverse ETL → PostHog | Not started 📋 |
| Chatbot (NL → SQL → Excel/Sheets) | Not started 📋 |

---

## 3. Scope of Work

### 3.1 ETL & Warehouse (Foundation)

| # | Deliverable | Status | Detail |
|---|---|---|---|
| A1 | Ingestion pipelines (7 connectors) | ✅ Live | MySQL direct (Inc42 DB, Gravity Forms, WooCommerce via Azure), PostgreSQL (Datalabs), native BigQuery connector (Customer.io → GCS parquet), REST APIs (Tally, HubSpot). Cadence 10 min – daily per source. |
| A2 | Bronze layer | ✅ Live | 49 append-only raw tables with `_ingested_at`, `_source_file`, `_ingestion_id` lineage tags. Duplicate-file detection on load. |
| A3 | Silver layer + identity resolution | ✅ Live | 30+ dbt Core models. PySpark on Dataproc → `unified_contact_id`, company fuzzy matching, revenue parser, phone/email/geo standardization. |
| A4 | Gold layer (star schema + 360 views) | ✅ Live | 11 dimensions, 19 facts, 5 "360" wide views. Max-2-join design rule. |
| A5 | Orchestration (Airflow) | ✅ Live | GCP VM, midnight IST. Phases: ingestion → PySpark → dbt → DQ tests → Reverse ETL. 3-retry, Slack alerts. |
| A6 | Data quality (dbt tests) | ✅ Live | Uniqueness, not-null, referential integrity, freshness. Pipeline halts on any failure. |
| A7 | Documentation (dbt docs, runbook) | 🚧 Partial | dbt docs live. Runbook and on-call playbook to be completed under this SOW. |
| A8 | Looker Studio integration | 📋 Planned | Free-tier dashboards for leadership; 3–4 boards to complement Streamlit. |
| A9 | Hardening & SLA instrumentation | 📋 Planned | Alerting dashboards, SLA tracking, cost guardrails (BigQuery slot/byte caps), credential rotation policy. |

### 3.2 Reverse ETL (Activation)

**Destinations:** Customer.io (marketing automation), HubSpot (CRM/sales), PostHog (product analytics).

| # | Deliverable | Status | Detail |
|---|---|---|---|
| B1 | Customer.io sync | 🚧 Partial | Extend existing sync to include engagement score, reachability flags, Plus-tier status, and segment membership deltas. |
| B2 | HubSpot sync | 📋 Planned | Upsert contacts and companies via CRM v3 API — unified identity, lifecycle stage, lead score, sector/city, Plus/Datalabs flags, last-engagement date. Batching respects 100 req/10s rate limit. |
| B3 | PostHog sync | 📋 Planned | `identify` and `group` calls to enrich user/company profiles with warehouse attributes (cohort tags, LTV buckets, Plus tier, engagement score). |
| B4 | Automation segments | 📋 Planned | Churn-risk (Plus expiring ≤30d), upsell (high engagement + non-Plus), channel-switch (bounced email → WhatsApp), newsletter-level suppression, sales-ready leads (HubSpot), power-user cohorts (PostHog). |
| B5 | Destination-agnostic framework | 📋 Planned | One dbt model per segment, one Airflow operator per destination. Adding a 4th destination = config + credentials. |
| B6 | Audit log | 📋 Planned | `reverse_etl_audit` table: destination, row count, payload hash, API response, latency, error. Per-destination monitoring dashboard. |
| B7 | Rollback | 📋 Planned | Last-known-good snapshot per segment per destination; one-command revert DAG scoped to a single destination. |
| B8 | Rate-limit & retry | 📋 Planned | Exponential backoff per destination's API contract; dead-letter queue in GCS for persistent failures. |

#### 3.2.1 Field catalog — what gets pushed to each destination

All fields are sourced from the Gold layer (`contact_360`, `company_360`, lifecycle and fact tables). The catalog below is the target scope; additions/removals follow the change-control process in §10.

**Customer.io (Person attributes + tracked events)**

| Group | Fields |
|---|---|
| Identity | `email` (id), `unified_contact_id`, `phone_e164`, `first_name`, `last_name` |
| Professional | `company_name`, `designation`, `sector`, `city`, `state` |
| Engagement | `engagement_score` (0–100), `engagement_tier`, `email_reachable`, `phone_reachable`, `last_open_at`, `last_click_at`, `last_event_attended_at`, `newsletter_subscriptions[]`, `channel_preference` |
| Commerce | `total_orders`, `total_spend_inr`, `first_order_at`, `last_order_at`, `avg_order_value_inr` |
| Plus membership | `plus_tier`, `plus_started_at`, `plus_expiry_date`, `plus_renewal_status`, `plus_ltv_inr` |
| Lead lifecycle | `lead_stage`, `lead_score`, `lead_source` |
| Segment flags | `is_churn_risk`, `is_upsell_candidate`, `is_power_user`, `is_founder_alumni`, `is_plus_member` |
| Tracked events | `event_registered` (event_id, attended), `order_placed`, `plus_purchased`, `plus_renewed`, `plus_expired`, `newsletter_subscribed`, `newsletter_unsubscribed`, `email_bounced` |

**HubSpot (Contacts + Companies via CRM v3)**

| Object | Fields |
|---|---|
| Contact (standard) | `email`, `firstname`, `lastname`, `phone`, `mobilephone`, `jobtitle`, `lifecyclestage` (mapped from `lead_stage`), `hs_lead_status`, `city`, `state`, `country` |
| Contact (custom) | `unified_contact_id`, `lead_score`, `engagement_score`, `engagement_tier`, `plus_tier`, `plus_expiry_date`, `last_event_attended`, `last_order_date`, `total_spend_inr`, `sector_interest` (multi), `channel_preference`, `founder_alumni` |
| Company (standard) | `name`, `domain`, `industry`, `numberofemployees`, `annualrevenue`, `city`, `state`, `country` |
| Company (custom, from Datalabs) | `funding_stage`, `total_funding_raised_inr`, `last_funding_date`, `lead_investor`, `inc42_tags` (multi), `company_360_score` |
| Associations | Contact ↔ Company via `unified_contact_id` ↔ `datalabs_company_id` |
| Optional (Phase 2) | Deal creation on Plus purchase (closed-won), with amount and term |

**PostHog (Persons + Groups + Cohorts)**

| Call | Fields |
|---|---|
| `$identify` (person) | `distinct_id` = `unified_contact_id`, `email`, `name`, `plus_tier`, `plus_started_at`, `plus_expiry_date`, `engagement_score`, `engagement_tier`, `sector`, `role`, `city`, `state`, `signup_source`, `lead_stage`, `newsletter_count`, `event_count_lifetime`, `order_count_lifetime`, `ltv_bucket`, `is_churn_risk`, `is_upsell_candidate`, `is_power_user` |
| `$groupidentify` (company) | group type `company`, `group_key` = `datalabs_company_id`, `name`, `industry`, `funding_stage`, `total_funding_raised_inr`, `employee_count`, `employee_bucket`, `hq_city`, `hq_state`, `inc42_tags`, `first_seen_at` |
| Materialized cohorts (for feature flags + analytics) | Plus active, Churn risk (Plus expiring ≤30d), Upsell (high engagement + non-Plus), Founder alumni, Power users (top 10% engagement), Recent event attendees |

> PostHog receives warehouse-derived **attributes and cohorts** only. Product event streams continue to originate from the Inc42 app → PostHog directly; the warehouse does not replay raw events.

**Not synced to any destination** (intentional): raw PII beyond standardized email/phone (e.g., full postal address, government IDs), financial line-item data (orders table rows), raw Datalabs scraped fields, any field marked sensitive in the Gold-layer column catalog.

**Write semantics — how each field is written to the destination:**

| Field class | Write rule |
|---|---|
| `email` (Customer.io, HubSpot) / `distinct_id` (PostHog) | **Lookup key only** — used to identify the target record for upsert. Never overwritten. |
| `unified_contact_id` | **Always written.** This is warehouse-native information the destination doesn't have; it's what enables cross-system joins and is the core reason reverse ETL runs. |
| `phone_e164`, `first_name`, `last_name`, `company_name`, `designation`, `city`, `state` | **Upgrade-only (append-if-absent).** Written only when (a) the destination field is null/empty, or (b) the warehouse value is the standardized form of an existing value (E.164 phone normalization, trimmed/proper-cased name). Never overwrites a non-null destination value with a conflicting warehouse value. |
| Engagement scores, Plus tier, lifecycle, segment flags, cohort membership, custom fields | **Always written (authoritative).** Warehouse is the source of truth for these derived fields; they're overwritten on every sync. |
| Tracked events (Customer.io) | **Append-only.** Each event is a new record; never mutates history. |

#### 3.2.2 Phased rollout of data points per destination

Each destination is rolled out in three phases — Foundation, Activation, Enrichment — so marketing/sales/product teams can start using the sync before the full catalog lands. Week windows align with §5.2.

**Customer.io**

| Phase | Week | Data points pushed |
|---|---|---|
| P1 — Foundation | W2–W3 | Identity (`email`, `unified_contact_id`, `phone_e164`, `first_name`, `last_name`), basic profile (`company_name`, `designation`, `city`, `state`), Plus core (`plus_tier`, `plus_started_at`, `plus_expiry_date`), flags (`is_plus_member`, `is_churn_risk`) |
| P2 — Activation | W3–W4 | Engagement (`engagement_score`, `engagement_tier`, `email_reachable`, `phone_reachable`, `last_open_at`, `last_click_at`), newsletter subs + `channel_preference`, commerce (`total_orders`, `total_spend_inr`, `last_order_at`, `avg_order_value_inr`), tracked events (`event_registered`, `order_placed`, `plus_purchased/renewed/expired`, `newsletter_subscribed/unsubscribed`, `email_bounced`), flags (`is_upsell_candidate`, `is_power_user`) |
| P3 — Enrichment | W4–W5 | Sector, `plus_renewal_status`, `plus_ltv_inr`, lead lifecycle (`lead_stage`, `lead_score`, `lead_source`), `first_order_at`, `last_event_attended_at`, `is_founder_alumni`, event enrichment (event_id, attended flag) |

**HubSpot**

| Phase | Week | Data points pushed |
|---|---|---|
| P1 — Contacts foundation | W4–W5 | Standard contact (`email`, `firstname`, `lastname`, `phone`, `mobilephone`, `jobtitle`, `city`, `state`, `country`), custom (`unified_contact_id`), `lifecyclestage` mapped from `lead_stage`, `lead_score` |
| P2 — Engagement + Companies foundation | W5–W6 | Custom contact (`engagement_score`, `engagement_tier`, `plus_tier`, `plus_expiry_date`, `last_event_attended`, `last_order_date`, `total_spend_inr`), standard company (`name`, `domain`, `industry`, `numberofemployees`, `annualrevenue`, `city`, `state`, `country`), Contact↔Company association |
| P3 — Datalabs enrichment + Deals | W6 | Company custom (`funding_stage`, `total_funding_raised_inr`, `last_funding_date`, `lead_investor`, `inc42_tags`, `company_360_score`), contact custom (`sector_interest`, `channel_preference`, `founder_alumni`, `hs_lead_status`), optional Deal creation on Plus purchase (closed-won) |

**PostHog**

| Phase | Week | Data points pushed |
|---|---|---|
| P1 — Person identity | W5–W6 | `$identify`: `distinct_id` (= `unified_contact_id`), `email`, `name`, `plus_tier`, `sector`, `role`, `city`, `state` |
| P2 — Engagement + cohorts | W6 | `$identify` adds: `engagement_score`, `engagement_tier`, `plus_started_at`, `plus_expiry_date`, `lead_stage`, `newsletter_count`, `event_count_lifetime`, `order_count_lifetime`, `ltv_bucket`, `is_churn_risk`, `is_upsell_candidate`, `is_power_user`. Materialized cohorts: Plus active, Churn risk, Upsell, Power users |
| P3 — Company groups + advanced cohorts | W7 | `$groupidentify` (group type `company`): `group_key` (= `datalabs_company_id`), `name`, `industry`, `funding_stage`, `total_funding_raised_inr`, `employee_count`, `employee_bucket`, `hq_city`, `hq_state`, `inc42_tags`, `first_seen_at`. Additional cohorts: Founder alumni, Recent event attendees. `$identify` adds `signup_source` |

**Phase gates:** each phase must pass (a) schema validation in staging, (b) ≤2% payload error rate on a 24h dry-run, and (c) stakeholder sign-off (marketing for Customer.io, sales for HubSpot, product for PostHog) before the next phase starts.

#### 3.2.3 Audience eligibility — who gets synced

Not every contact in the warehouse is synced to every destination. Each destination has its own eligible-audience view in the Gold layer (`reverse_etl_audience_customerio`, `_hubspot`, `_posthog`), and only rows in that view flow to that destination. This keeps destinations focused, avoids per-seat/per-contact costs on irrelevant records, and reduces noise for the teams using each tool.

| Destination | Eligibility rule | Excluded | Est. volume* |
|---|---|---|---|
| **Customer.io** | Active marketing universe: contact has a reachable email or phone **AND** (any marketing engagement in last 18 months OR Plus member OR registered for an event in last 12 months OR subscribed to any newsletter). | Global unsubscribers, hard-bounced emails with no valid phone, internal/test accounts, accounts marked `do_not_contact`. | ~200K of 330K |
| **HubSpot** | Sales-relevant universe: `lead_stage` in (MQL, SQL, customer) **OR** Plus member **OR** associated with a target-account company (from Datalabs target list) **OR** had a direct sales touch in last 24 months. | Newsletter-only signups with no other engagement, consumer contacts outside India without B2B signal, internal/test accounts. | ~30K–50K of 330K |
| **PostHog** | Enrich existing PostHog persons only: warehouse does not create new persons. A person is synced if a PostHog `distinct_id` already exists for the `unified_contact_id` (via email or login event match). Companies as groups: companies where ≥1 user has PostHog activity **OR** companies on the Datalabs target-account list. | Contacts who have never used the product (no PostHog presence to enrich). | All identified product users |

*Volumes are planning estimates; exact numbers are confirmed from Gold audience views during W2.

**Governance of eligibility:**
- Audience views are defined in dbt and reviewed/approved in a PR by the destination's owning team (marketing / sales / product).
- Any change to eligibility criteria follows the change-control process in §10.
- Opt-outs, unsubscribes, and `do_not_contact` flags from any source system are honored across **all** destinations within 24 hours.
- A contact leaving the eligible audience triggers a **suppress** action in the destination (archive in HubSpot, suppress in Customer.io, remove from cohort in PostHog) — not a delete, so audit history is preserved.

### 3.3 Conversational Data Access (Chatbot)

**End-user flow:** User asks *"Give me all Plus members in Bangalore who attended an event in the last 6 months"* in Slack or a web UI and receives a downloadable Excel file or Google Sheet link with the results.

| # | Deliverable | Status | Detail |
|---|---|---|---|
| C1 | Interface | 📋 Planned | Slack slash command `/ask` + lightweight Streamlit web form. Both accept free-text questions. |
| C2 | NL→SQL engine | 📋 Planned | Claude API (Sonnet 4.6) with prompt caching of the Gold-layer schema, column descriptions, and example query pairs. Only Gold-layer tables exposed. |
| C3 | Semantic guardrails | 📋 Planned | Allowlist of queryable tables/columns, banned-column list (raw PII), mandatory `LIMIT 50000` injection, BigQuery `maximumBytesBilled` cap per query. |
| C4 | Execution layer | 📋 Planned | Python service runs generated SQL against BigQuery using a read-only service account scoped to Gold datasets only. |
| C5 | Export module | 📋 Planned | (a) Excel via `openpyxl` (formatted headers, frozen pane, auto-width); (b) Google Sheet via Sheets API, shared with requester's email. |
| C6 | Delivery | 📋 Planned | Slack: file upload as thread reply. Web: signed download URL (24h TTL) or Sheet link. Email fallback. |
| C7 | Audit & cost control | 📋 Planned | Every request logged (user, question, generated SQL, bytes scanned, row count, output URL). Per-user daily query budget. |
| C8 | Human review loop | 📋 Planned | Queries scanning >10 GB or touching sensitive columns return a SQL preview and wait for user confirmation. |
| C9 | Accuracy feedback | 📋 Planned | Thumbs-up/down in Slack appends to a fine-tuning log; weekly review of failed queries expands the example library. |

**Out of scope for Phase 1:** free-form writes, cross-workspace sharing, multi-turn memory beyond current thread, non-English questions, chart generation (tabular export only).

---

## 4. Architecture Summary

```
Sources (7) → GCS Landing → BigQuery Bronze → dbt/PySpark → Silver → dbt → Gold
                                                                          │
           ┌──────────────────────────────────────────────────────────────┼──────────────────┐
           ▼                                                              ▼                  ▼
   Reverse ETL                                                  Streamlit BI         Chatbot (Claude)
   Customer.io · HubSpot · PostHog                              + Looker Studio      → Excel / GSheet
```

---

## 5. Timeline

**Engagement duration for remaining scope:** ~12 weeks from kickoff. Tracks run partially in parallel.

### 5.1 Already delivered (pre-SOW, April 2026 baseline)
- Bronze ingestion for all 7 sources
- Silver identity and company resolution
- Gold star schema and 360 views
- Airflow orchestration + dbt tests
- Streamlit analytics (7 pages)
- Customer.io reverse ETL (partial)

### 5.2 Remaining scope schedule

| Phase | Weeks | Track | Milestone |
|---|---|---|---|
| 0. Kickoff & access provisioning | W0–W1 | All | Credentials (PostHog, HubSpot private app, Slack bot, Anthropic API), runbook kickoff, golden-question set drafted |
| 1. Hardening & docs | W1–W3 | ETL | Runbook, on-call playbook, SLA dashboards, cost guardrails |
| 2. Looker Studio | W2–W4 | ETL | 3–4 leadership boards on Gold |
| 3. Reverse ETL framework + Customer.io completion | W2–W5 | Reverse ETL | Framework, audit log, rollback DAG, Customer.io full sync |
| 4. HubSpot reverse ETL | W4–W6 | Reverse ETL | Contacts + companies upsert, lifecycle + lead score |
| 5. PostHog reverse ETL | W5–W7 | Reverse ETL | `identify` + `group` enrichment, cohort tags |
| 6. Automation segments (all destinations) | W6–W8 | Reverse ETL | Churn, upsell, channel-switch, sales-ready, power-user |
| 7. Chatbot — NL→SQL core | W5–W8 | Chatbot | Claude prompt, schema cache, allowlist, execution layer |
| 8. Chatbot — export + delivery | W8–W10 | Chatbot | Excel + Google Sheets export, Slack `/ask`, web form |
| 9. Chatbot — guardrails + audit | W9–W11 | Chatbot | Cost caps, human review loop, audit log, feedback |
| 10. UAT + training + handover | W11–W12 | All | Golden-set validation, stakeholder training, 2-week hypercare |

---

## 6. Acceptance Criteria

- **ETL hardening:** runbook and on-call playbook signed off; SLA dashboard live; 99% daily pipeline success over a rolling 14-day window (already observed, to be formally tracked).
- **Reverse ETL:** ≤2% payload error rate per destination (Customer.io, HubSpot, PostHog); rollback tested end-to-end on all three.
- **Chatbot:** ≥85% first-shot SQL correctness on a 50-question golden set; zero queries executed outside Gold allowlist; zero queries exceeding budget cap.

---

## 7. Assumptions & Dependencies

- PostHog project API key and HubSpot private-app token provisioned by W1.
- Anthropic API key (Claude Sonnet 4.6) provisioned by W4 for the chatbot track.
- Slack admin approval for the bot app by W5.
- Legal sign-off on sending Inc42 schema/column names (not row data) to the Claude API for NL→SQL prompting by W4.
- Existing GCP project, BigQuery datasets, Airflow VM, and dbt repo access are already in place.

---

## 8. Pricing

| Item | One-time | Monthly |
|---|---|---|
| ETL hardening + Looker Studio | ₹[TBD] | — |
| Reverse ETL (3 destinations + framework) | ₹[TBD] | — |
| Chatbot build | ₹[TBD] | — |
| Existing infrastructure (BigQuery, GCS, VMs) | — | ~$23 (unchanged) |
| Claude API (Sonnet 4.6, cached schema prompt) | — | $[TBD — usage-based] |
| Support & hypercare (first 30 days) | Included | — |
| Post-launch SLA (optional) | — | ₹[TBD] |

---

## 9. Security & Governance

- All source credentials stored in Airflow Variables, never in code.
- BigQuery datasets use role-based access; chatbot service account is read-only, scoped to Gold datasets.
- Reverse-ETL payloads hashed and logged for replay/audit.
- Chatbot audit log retains every prompt, generated SQL, and output URL for 12 months.
- dbt models version-controlled in GitHub; PR review required for Gold-layer changes.

---

## 10. Change Control

Any change to scope, sources, destinations, or output formats requires a written Change Request signed by both parties. Chatbot allowlist changes follow the lightweight process in the runbook (PR + 1 reviewer).

---

## 11. Sign-off

| Party | Name | Role | Date |
|---|---|---|---|
| Inc42 | | | |
| Vendor | | | |
