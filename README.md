# DevPulse — Engineering Intelligence Platform

> **More context. Better insights. Healthier engineering teams.**
>
> DevPulse turns fragmented engineering and organizational data into a contextual, unified view of how engineering work is progressing — powered by Databricks.

---

## Table of Contents

- [Overview](#overview)
- [The Core Idea](#the-core-idea)
- [Features](#features)
- [Architecture](#architecture)
- [Data Model — Medallion Architecture](#data-model--medallion-architecture)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [API Reference](#api-reference)
- [Data Sources](#data-sources)
- [Roadmap](#roadmap)
- [Contributing](#contributing)

---

## Overview

DevPulse is an internal Engineering Intelligence Platform built for software organizations. It aggregates signals from engineering tools (GitHub, Jira, CI/CD, Monitoring) and combines them with organizational context from **Vyaguta** — Leapfrog's internal company platform — to give engineering managers and leads a single, coherent view of their organization.

The product is **not** an employee productivity scoring or surveillance system. It focuses on:

- Project health and delivery risk
- Engineering system health (CI/CD, incidents, cycle times)
- Delivery bottlenecks and flow efficiency
- Organizational context that explains metric changes
- Team workload distribution and focus areas
- Qualitative signals from feedback and worklogs

---

## The Core Idea

### Before Vyaguta

```
GitHub + Jira + CI/CD + Monitoring
                ↓
       Engineering Metrics
                ↓
          "What happened?"
```

### After Vyaguta Integration

```
GitHub + Jira + CI/CD + Monitoring
                 +
               Vyaguta
         (organizational context)
                 ↓
     Engineering Intelligence
                 ↓
      Metrics + Context + Signals
                 ↓
        "What happened?"
                 +
        "What context surrounds it?"
                 +
        "Where should we investigate?"
```

Each data source answers a different question:

| Source     | Question answered                              |
|------------|------------------------------------------------|
| GitHub     | What are we building?                          |
| Jira       | What work is planned?                          |
| CI/CD      | How is software moving to production?          |
| Monitoring | What happens in production?                    |
| Vyaguta    | What organizational context surrounds the work? |

---

## Features

### Organization Pulse Dashboard
The primary screen — an at-a-glance view of the entire engineering organization.

- **KPI Summary Cards** — Total Commits, Pull Requests, Deployment Frequency, CI Failure Rate, Avg. Incident Recovery
- **Engineering Health Score** — Composite health gauge (0–100) with individual metric breakdowns
- **Team Performance Table** — Members, open tickets, PRs, and incidents per team
- **Project Health Table** — PR cycle time, deployment frequency, and incident count per project

### Engineering Delivery Intelligence
- **Where Engineering Time Goes** — Donut chart breaking down hours across Feature Work, Bug Fixing, Code Review, CI/CD, Meetings, and Other (sourced from Vyaguta worklogs)
- **Delivery Flow: Idea → Production** — Visual pipeline showing elapsed time at each stage: Jira → Development → PR → Code Review → CI Build → Deployment → Production

### Contextual Intelligence (Vyaguta Layer)
- **Vyaguta Insights Panel** — Active Projects, Total Members, Attendance, and Recent Feedback counts
- **Key Insights** — Automatically surfaced observations that link metric changes to organizational events (releases, holidays, leave, company events)
- Insights use correlation language ("coincided with"), never causal claims

### Cross-Platform Activity Timeline
A unified feed combining events from GitHub, Jira, CI/CD, Monitoring, Slack, and Vyaguta — filterable by source.

### Navigation Sections
- Overview (Organization Pulse)
- Projects
- Teams
- Members
- Code Activity
- Pull Requests
- CI/CD
- Incidents
- Meetings
- Reports

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                             │
│  GitHub    Jira    CI/CD    Monitoring    Slack    Vyaguta      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              Ingestion Layer                                     │
│        Lakeflow Connect / APIs / Auto Loader                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│    Bronze    │  │    Silver    │  │     Gold     │
│  Raw JSON    │  │  Cleaned &   │  │  Business    │
│  Ingestion   │→ │  Correlated  │→ │ Aggregations │
└──────────────┘  └──────────────┘  └──────────────┘
                                           │
                                           ▼
                              ┌────────────────────────┐
                              │  Unity Catalog /        │
                              │  Governance Layer        │
                              └────────────┬────────────┘
                                           │
                                           ▼
                              ┌────────────────────────┐
                              │  DevPulse Dashboard     │
                              │  (Node.js + HTML/CSS)   │
                              └────────────────────────┘
```

### Current Implementation

| Layer | Status | Notes |
|---|---|---|
| Bronze ingestion | ✅ Phase 1 + 2 | Databricks Delta tables via Spark |
| Silver cleansing | ✅ Phase 2 | Typed, correlated, deduped |
| Gold aggregations | ✅ Phase 2 | `developer_productivity`, `team_summary` |
| Dashboard (mock) | ✅ Phase 3 | Node.js + Express, mock JSON data |
| Live Databricks API | 🔜 Planned | Replace mock endpoints with Unity Catalog queries |

---

## Data Model — Medallion Architecture

### Bronze — Raw Ingestion
Exact source data, no transformations applied.

```
bronze.vyaguta_users
bronze.vyaguta_worklogs
bronze.vyaguta_projects
bronze.vyaguta_feedback
bronze.vyaguta_events
bronze.vyaguta_holidays

bronze.github_commits
bronze.github_prs

bronze.jira_tickets

bronze.ci_builds
bronze.deployments

bronze.incidents
```

### Silver — Cleaned & Correlated
Type-cast, null-filtered, and cross-source relationships established.

```
silver.developers          ← vyaguta_users (canonical identity)
silver.github_commits      ← author_email joined to developers
silver.jira_tickets        ← assignee_email joined to developers
silver.worklogs            ← emp_id joined to developers
silver.feedback            ← sender/receiver joined to developers
silver.events              ← org events with team/project linkage
silver.pull_requests
silver.builds
silver.deployments
silver.incidents
```

### Gold — Business Aggregations
Analytical datasets consumed directly by the dashboard.

```
gold.developer_productivity   ← commits, PRs, tickets, feedback, hours per developer
gold.team_summary             ← team-level KPI rollup
gold.project_health           ← cycle time, deploy freq, incidents per project
gold.team_delivery            ← delivery velocity and trend per team
gold.team_workload            ← worklog category distribution per team
gold.delivery_bottlenecks     ← stage-level elapsed time (Jira → Production)
gold.incident_metrics         ← MTTR, frequency, severity per team/project
gold.feedback_themes          ← theme classification counts per team
gold.organizational_calendar  ← holidays, events, milestones
gold.engineering_signals      ← cross-source correlated signals for Insights panel
gold.weekly_summary           ← narrative data for Engineering Summary section
```

**Key join path across all layers:**

```
Project ─── Team ─── Member ─── Repository
                         │
              Jira Ticket ─── Pull Request ─── Build ─── Deployment
                         │
                      Incident ─── Event ─── Worklog ─── Feedback
```

---

## Project Structure

```
capstone/
│
├── Phase 1.py              # Databricks: catalog + schema setup, mock data generation
├── Phase-2.py              # Databricks: Bronze → Silver → Gold pipeline
│
├── server.js               # Express server — serves dashboard + mock API
├── package.json
│
└── public/
    └── index.html          # DevPulse dashboard (self-contained HTML/CSS/JS)
```

---

## Getting Started

### Prerequisites

- [Node.js](https://nodejs.org/) v16 or later
- npm (bundled with Node.js)
- For the data pipeline: a Databricks workspace with Unity Catalog enabled

### 1. Clone the repository

```bash
git clone <repo-url>
cd capstone
```

### 2. Install dependencies

```bash
npm install
```

### 3. Start the development server

```bash
node server.js
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

### 4. (Optional) Run the Databricks pipeline

Upload and run the notebooks in order inside your Databricks workspace:

1. **Phase 1.py** — Creates the `leapfrog_pulse` catalog, `bronze`/`silver`/`gold` schemas, a Volume for raw files, and writes mock JSON payloads.
2. **Phase-2.py** — Ingests raw JSON into Bronze Delta tables, transforms to Silver, and aggregates to Gold.

> The mock JSON files are written to `/Volumes/leapfrog_pulse/bronze/raw_files/` and include Vyaguta users, worklogs, GitHub commits, and Jira tickets.

---

## API Reference

All endpoints are served by `server.js` at `http://localhost:3000`.

| Endpoint | Description |
|---|---|
| `GET /api/kpis` | Top-level KPI card values (commits, PRs, deploys, CI failure rate, MTTR) |
| `GET /api/health` | Engineering health score + per-metric breakdown |
| `GET /api/teams` | Team list with member count, tickets, PRs, and incidents |
| `GET /api/projects` | Project list with cycle time, deploy frequency, and incidents |
| `GET /api/time-distribution` | Worklog hour breakdown by category (Feature, Bug Fix, etc.) |
| `GET /api/activity` | Cross-platform activity timeline items |
| `GET /api/vyaguta` | Vyaguta org context KPIs (attendance, feedback, member count) |
| `GET /api/insights` | Key insight strings for the Insights panel |

All endpoints currently return static mock data. The data shapes are designed to be drop-in replaceable with Databricks SQL queries against the Gold layer.

---

## Data Sources

| Source | Data Provided | Connection |
|---|---|---|
| **Vyaguta** | Projects, teams, members, worklogs, attendance, feedback, events, holidays | Internal API |
| **GitHub** | Commits, pull requests, code review activity, repositories | GitHub API / Webhooks |
| **Jira** | Tickets, story points, status transitions, assignees, sprints | Jira REST API |
| **CI/CD** | Build results, deployment events, pipeline duration | CI platform webhooks |
| **Monitoring** | Incidents, alerts, MTTR, severity | Monitoring platform API |
| **Slack / Teams** | Team communication signals (optional) | Slack Events API |

---

## Roadmap

### Phase 3 (Current) — Dashboard Prototype
- [x] Databricks Medallion architecture (Bronze → Silver → Gold)
- [x] Mock JSON data generation
- [x] Node.js/Express mock API server
- [x] DevPulse dashboard — Organization Pulse view

### Phase 4 — Expanded Data Model
- [ ] Add CI/CD, incidents, and monitoring bronze tables
- [ ] Add Vyaguta feedback, events, and holiday tables
- [ ] Complete all Gold aggregation tables
- [ ] Delivery Flow (Idea → Production) pipeline calculation

### Phase 5 — Live Databricks Integration
- [ ] Replace mock API endpoints with Databricks SQL Connector queries
- [ ] Unity Catalog governance and row-level security
- [ ] Lakeflow Connect for automated ingestion from Vyaguta API

### Phase 6 — Advanced Features
- [ ] Contextual Insights engine (metric change + org event correlation)
- [ ] Engineering Weekly Summary (template-generated narrative)
- [ ] Project detail page with delivery flow visualization
- [ ] Team and Member detail pages
- [ ] Calendar & Events view
- [ ] Feedback Themes classification

### Phase 7 — AI Layer (Future)
- [ ] AI-generated engineering summaries using Claude API
- [ ] Natural language query interface ("Which team has the most deployment risk?")
- [ ] Anomaly detection on engineering metrics

---

## Contributing

This is a capstone project. Contributions follow the standard fork-and-PR model.

When replacing mock data with real API calls:
1. Keep the same response shape as the mock endpoints in `server.js`
2. Add the Databricks connection logic in a new `src/db/` directory
3. Keep mock data available as a fallback for local development

---

*Powered by Databricks · Built at Leapfrog Technology*
