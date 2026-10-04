const { Router } = require("express");
const router = Router();
const vy = require("../data/vyaguta");
const db = require("../data/databricks");

// ─── helpers ────────────────────────────────────────────────────────────────

/** Run a DB query; return null on failure (dashboard falls back to stubs). */
async function dbQuery(sql, cacheKey) {
  try {
    return await db.query(sql, cacheKey);
  } catch (err) {
    console.warn(`[gold] query failed (${cacheKey}):`, err.message);
    return null;
  }
}

function num(v) { return v == null ? null : Number(v); }
function fmt(v) { return v == null ? "—" : Number(v).toLocaleString(); }

function selectedProject(req) {
  const project = req.query.project;
  return typeof project === "string" && project.trim() ? project.trim() : null;
}

function projectWhere(project) {
  return project ? ` WHERE project_name = '${project.replace(/'/g, "''")}'` : "";
}

async function getProjectRows(project, cacheKey) {
  return dbQuery(
    `SELECT * FROM ${db.tbl("dashboard_gold_project_summary")}${projectWhere(project)} ORDER BY project_name`,
    `${cacheKey}:${project || "all"}`,
  );
}

function summarizeProjectMetrics(rows) {
  const totalIssues = rows.reduce((sum, row) =>
    sum + (num(row.open_issues) || 0) + (num(row.in_progress_issues) || 0) + (num(row.done_issues) || 0), 0);
  const totalBugs = rows.reduce((sum, row) => sum + (num(row.bugs_count) || 0), 0);
  const totalChecks = rows.reduce((sum, row) => sum + (num(row.ci_checks_total) || 0), 0);
  const failedChecks = rows.reduce((sum, row) => sum + (num(row.ci_checks_failed) || 0), 0);
  let mergedPrs = 0;
  let weightedLeadTime = 0;
  let resolvedIncidents = 0;
  let weightedRecoveryTime = 0;

  rows.forEach((row) => {
    const projectMergedPrs = num(row.merged_prs_last_30d) || 0;
    const projectLeadTime = num(row.avg_lead_time_days);
    if (projectLeadTime != null && projectMergedPrs > 0) {
      mergedPrs += projectMergedPrs;
      weightedLeadTime += projectLeadTime * projectMergedPrs;
    }
    const incidentCount = num(row.resolved_incidents) || 0;
    const recoveryTime = num(row.avg_incident_recovery_hrs);
    if (recoveryTime != null && incidentCount > 0) {
      resolvedIncidents += incidentCount;
      weightedRecoveryTime += recoveryTime * incidentCount;
    }
  });

  return {
    bugRate: totalIssues > 0 ? (totalBugs / totalIssues) * 100 : null,
    leadTimeDays: mergedPrs > 0 ? weightedLeadTime / mergedPrs : null,
    ciFailureRate: totalChecks > 0 ? (failedChecks / totalChecks) * 100 : null,
    incidentRecoveryHours: resolvedIncidents > 0 ? weightedRecoveryTime / resolvedIncidents : null,
  };
}

const TEAM_COLORS = {
  HealthTech:               "#3b82f6",
  FinTech:                  "#f59e0b",
  EdTech:                   "#8b5cf6",
  Enterprise:               "#10b981",
  "Engineering Intelligence": "#06b6d4",
};
function teamColor(name) {
  if (TEAM_COLORS[name]) return TEAM_COLORS[name];
  const palette = ["#f59e0b","#3b82f6","#8b5cf6","#10b981","#06b6d4","#ef4444"];
  let h = 0;
  for (let i = 0; i < (name || "").length; i++) h = (h * 31 + name.charCodeAt(i)) % palette.length;
  return palette[h];
}

// ─── /api/kpis ───────────────────────────────────────────────────────────────

router.get("/kpis", async (req, res) => {
  const project = selectedProject(req);
  const [rows, projectRows] = await Promise.all([
    dbQuery(
      `SELECT * FROM ${db.tbl("dashboard_gold_kpis")} ORDER BY kpi_date DESC LIMIT 1`,
      "kpis",
    ),
    getProjectRows(project, "project_kpis"),
  ]);
  const r = rows && rows[0];
  const scopedRows = projectRows || [];
  const projectMetrics = summarizeProjectMetrics(scopedRows);
  const scopedProject = scopedRows[0];

  res.json({
    totalCommits:     { value: project ? fmt(num(scopedProject && scopedProject.commits_last_30d)) : (r ? fmt(r.total_commits_30d) : "—"), change: null, up: true },
    pullRequests:     { value: "—",  change: null, up: true  },
    bugRate:          { value: projectMetrics.bugRate != null ? `${projectMetrics.bugRate.toFixed(1)}%` : "—", change: null, up: false },
    leadTime:         { value: projectMetrics.leadTimeDays != null ? `${projectMetrics.leadTimeDays.toFixed(1)} days` : "—", change: null, up: false },
    deployFrequency:  { value: project && num(scopedProject && scopedProject.deploy_frequency_30d) != null
      ? `${num(scopedProject.deploy_frequency_30d).toFixed(2)}/day`
      : (r && num(r.deploy_frequency_30d) != null ? `${num(r.deploy_frequency_30d).toFixed(2)}/day` : "—"), change: null, up: true },
    ciFailureRate:    { value: projectMetrics.ciFailureRate != null ? `${projectMetrics.ciFailureRate.toFixed(1)}%` : "—", change: null, up: false },
    incidentRecovery: { value: projectMetrics.incidentRecoveryHours != null ? `${projectMetrics.incidentRecoveryHours.toFixed(1)}h` : "—", change: null, up: false },
    // Extra gold fields for new KPI tiles
    activeProjects:   r ? num(r.active_projects)   : null,
    activeDevelopers: r ? num(r.active_developers)  : null,
    openIssues:       r ? num(r.total_open_issues)  : null,
    doneIssues30d:    r ? num(r.done_issues_30d)    : null,
  });
});

// ─── /api/health ─────────────────────────────────────────────────────────────

router.get("/health", async (req, res) => {
  const project = selectedProject(req);
  const [rows, projectRows] = await Promise.all([
    dbQuery(
      `SELECT * FROM ${db.tbl("dashboard_gold_kpis")} ORDER BY kpi_date DESC LIMIT 1`,
      "kpis",
    ),
    getProjectRows(project, "project_health"),
  ]);
  const r = rows && rows[0];
  const scopedRows = projectRows || [];
  const projectMetrics = summarizeProjectMetrics(scopedRows);
  const scopedProject = scopedRows[0];

  const score = project
    ? (num(scopedProject && scopedProject.health_score) || 0)
    : (r ? (num(r.org_health_score) || 0) : 0);

  // Compute derived health metrics from gold data
  const bugRate = projectMetrics.bugRate;
  const prReviewTime = project
    ? num(scopedProject && scopedProject.avg_pr_review_time_hrs)
    : (r ? num(r.avg_pr_review_time_hrs) : null);
  const incidentRecovery = projectMetrics.incidentRecoveryHours;
  const deployFreq = project
    ? num(scopedProject && scopedProject.deploy_frequency_30d)
    : (r ? num(r.deploy_frequency_30d) : null);
  const velocitySP = project ? null : (r ? num(r.velocity_story_points_30d) : null);
  const velocityIssues = project
    ? num(scopedProject && scopedProject.done_issues_last_30d)
    : (r ? num(r.done_issues_30d) : null);

  const velocityValue = velocitySP != null && velocitySP > 0
    ? `${velocitySP} SP/30d`
    : (velocityIssues != null ? `${velocityIssues} issues/30d` : "—");

  res.json({
    score:  score,
    change: null,
    metrics: [
      { label: "PR Review Time",         value: prReviewTime != null ? `${prReviewTime.toFixed(1)}h avg` : "—", change: null, goodDown: true  },
      { label: "Deployment Frequency",   value: deployFreq != null ? `${deployFreq.toFixed(2)}/day` : "—",      change: null, goodDown: false },
      { label: "Bug Rate",               value: bugRate != null ? `${Number(bugRate).toFixed(1)}%` : "—",        change: null, goodDown: true  },
      { label: "Lead Time",              value: projectMetrics.leadTimeDays != null ? `${projectMetrics.leadTimeDays.toFixed(1)} days` : "—", change: null, goodDown: true },
      { label: "CI Failure Rate",        value: projectMetrics.ciFailureRate != null ? `${projectMetrics.ciFailureRate.toFixed(1)}%` : "—", change: null, goodDown: true },
      { label: "Avg. Incident Recovery", value: incidentRecovery != null ? `${incidentRecovery.toFixed(1)}h avg` : "—", change: null, goodDown: true },
      { label: "Project Velocity",       value: velocityValue,                                                   change: null, goodDown: false },
    ],
  });
});

// ─── /api/teams ──────────────────────────────────────────────────────────────

router.get("/teams", async (_req, res) => {
  const rows = await dbQuery(
    `SELECT team,
            SUM(commits_last_30d)    AS commits_30d,
            SUM(active_developers)   AS active_devs,
            SUM(open_issues)         AS open_issues,
            SUM(in_progress_issues)  AS in_progress
     FROM ${db.tbl("dashboard_gold_project_summary")}
     GROUP BY team
     ORDER BY commits_30d DESC`,
    "teams",
  );

  if (!rows || rows.length === 0) {
    // Static fallback
    return res.json([
      { name: "HealthTech",               color: "#3b82f6", activeDevelopers: "—", openIssues: "—", commits30d: "—", inProgress: "—" },
      { name: "FinTech",                  color: "#f59e0b", activeDevelopers: "—", openIssues: "—", commits30d: "—", inProgress: "—" },
      { name: "EdTech",                   color: "#8b5cf6", activeDevelopers: "—", openIssues: "—", commits30d: "—", inProgress: "—" },
      { name: "Enterprise",               color: "#10b981", activeDevelopers: "—", openIssues: "—", commits30d: "—", inProgress: "—" },
      { name: "Engineering Intelligence", color: "#06b6d4", activeDevelopers: "—", openIssues: "—", commits30d: "—", inProgress: "—" },
    ]);
  }

  res.json(rows.map((r) => ({
    name:             r.team,
    color:            teamColor(r.team),
    activeDevelopers: num(r.active_devs),
    openIssues:       num(r.open_issues),
    commits30d:       num(r.commits_30d),
    inProgress:       num(r.in_progress),
  })));
});

// ─── /api/projects ───────────────────────────────────────────────────────────

router.get("/projects", async (req, res) => {
  const project = selectedProject(req);
  const rows = await dbQuery(
    `SELECT project_name, team, health_score,
            commits_last_30d, open_issues, bugs_count,
            in_progress_issues, done_issues_last_30d
     FROM ${db.tbl("dashboard_gold_project_summary")}${projectWhere(project)}
     ORDER BY health_score DESC`,
    `projects:${project || "all"}`,
  );

  if (!rows || rows.length === 0) {
    return res.json([
      { name: "HealthTrack Platform",   team: "HealthTech", color: "#3b82f6", healthScore: null, commits30d: null, openIssues: null, bugs: null },
      { name: "FinEdge Analytics",      team: "FinTech",    color: "#f59e0b", healthScore: null, commits30d: null, openIssues: null, bugs: null },
      { name: "EduConnect LMS",         team: "EdTech",     color: "#8b5cf6", healthScore: null, commits30d: null, openIssues: null, bugs: null },
      { name: "LogiTrack Supply Chain", team: "Enterprise", color: "#10b981", healthScore: null, commits30d: null, openIssues: null, bugs: null },
      { name: "DevPulse Internal",      team: "Engineering Intelligence", color: "#06b6d4", healthScore: null, commits30d: null, openIssues: null, bugs: null },
    ].filter((entry) => !project || entry.name === project));
  }

  res.json(rows.map((r) => ({
    name:        r.project_name,
    team:        r.team,
    color:       teamColor(r.team),
    healthScore: num(r.health_score),
    commits30d:  num(r.commits_last_30d),
    openIssues:  num(r.open_issues),
    bugs:        num(r.bugs_count),
    inProgress:  num(r.in_progress_issues),
  })));
});

// ─── /api/velocity ───────────────────────────────────────────────────────────

router.get("/velocity", async (req, res) => {
  const project = selectedProject(req);
  const rows = await dbQuery(
    `SELECT project_name, team, sprint_name,
            done_issues_last_30d, in_progress_issues, open_issues,
            avg_story_points, lines_added_30d, lines_removed_30d,
            health_score
     FROM ${db.tbl("dashboard_gold_project_summary")}${projectWhere(project)}
     ORDER BY done_issues_last_30d DESC`,
    `velocity:${project || "all"}`,
  );

  if (!rows) return res.json([]);

  res.json(rows.map((r) => ({
    projectName:      r.project_name,
    team:             r.team,
    sprintName:       r.sprint_name || "—",
    doneIssues30d:    num(r.done_issues_last_30d),
    inProgress:       num(r.in_progress_issues),
    openIssues:       num(r.open_issues),
    avgStoryPoints:   r.avg_story_points != null ? Number(r.avg_story_points).toFixed(1) : null,
    linesAdded:       num(r.lines_added_30d),
    linesRemoved:     num(r.lines_removed_30d),
    healthScore:      num(r.health_score),
  })));
});

// ─── /api/time-distribution ──────────────────────────────────────────────────

router.get("/time-distribution", async (_req, res) => {
  let rows = null;
  try {
    rows = await dbQuery(
      `SELECT * FROM ${db.tbl("dashboard_gold_time_allocation")} ORDER BY allocation_date DESC LIMIT 1`,
      "time_allocation",
    );
  } catch (_) {}

  const r = rows && rows[0];
  if (!r) {
    // Static fallback with hours-based data
    return res.json({
      total: "—",
      allocated: "—",
      coverage: null,
      breakdown: [
        { label: "Feature Work",  color: "#3b82f6", pct: 38, hours: null },
        { label: "Code Reviews",  color: "#8b5cf6", pct: 12, hours: null },
        { label: "Bug Fixing",    color: "#ef4444", pct: 15, hours: null },
        { label: "Meetings",      color: "#f59e0b", pct: 20, hours: null },
        { label: "Others",        color: "#d1d5db", pct: 15, hours: null },
      ],
    });
  }

  const eligible  = num(r.eligible_working_hours) || 0;
  const feature   = num(r.feature_work_hours) || 0;
  const review    = num(r.code_review_hours)  || 0;
  const bugs      = num(r.bug_fixing_hours)   || 0;
  const meetings  = num(r.meeting_hours)      || 0;
  const others    = num(r.other_hours)        || 0;
  const allocated = num(r.total_allocated_hours) || 0;

  const pct = (h) => allocated > 0 ? Math.round((h / allocated) * 100) : 0;

  res.json({
    total:     eligible > 0 ? `${Math.round(eligible)}h eligible` : "—",
    allocated: allocated > 0 ? `${Math.round(allocated)}h allocated` : "—",
    coverage:  num(r.coverage_score),
    featureSource: r.feature_source || null,
    breakdown: [
      { label: "Feature Work",  color: "#3b82f6", pct: pct(feature),  hours: Math.round(feature)  },
      { label: "Code Reviews",  color: "#8b5cf6", pct: pct(review),   hours: Math.round(review)   },
      { label: "Bug Fixing",    color: "#ef4444", pct: pct(bugs),     hours: Math.round(bugs)     },
      { label: "Meetings",      color: "#f59e0b", pct: pct(meetings), hours: Math.round(meetings) },
      { label: "Others",        color: "#d1d5db", pct: pct(others),   hours: Math.round(others)   },
    ].filter((b) => b.pct > 0 || b.hours > 0),
  });
});

// ─── /api/activity ───────────────────────────────────────────────────────────

router.get("/activity", async (req, res) => {
  const project = selectedProject(req);
  const rows = await dbQuery(
    `SELECT event_id, event_type, event_time, project_name, repository,
            actor, description, source, priority, status
     FROM ${db.tbl("dashboard_gold_recent_activity")}${projectWhere(project)}
     ORDER BY event_time DESC
     LIMIT 20`,
    `activity:${project || "all"}`,
  );

  if (!rows || rows.length === 0) {
    return res.json([
      { source: "GitHub", color: "#24292e", icon: "GH", text: "Waiting for first pipeline run…", time: "—" },
    ]);
  }

  const SOURCE_META = {
    GitHub:     { color: "#24292e", icon: "GH" },
    Jira:       { color: "#0052cc", icon: "JI" },
    "CI/CD":    { color: "#f59e0b", icon: "CI" },
    Monitoring: { color: "#ef4444", icon: "MO" },
    Vyaguta:    { color: "#8b5cf6", icon: "VY" },
  };

  function timeAgo(ts) {
    if (!ts) return "—";
    const secs = Math.floor((Date.now() - new Date(ts).getTime()) / 1000);
    if (secs < 60)   return `${secs}s ago`;
    if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
    if (secs < 86400)return `${Math.floor(secs / 3600)}h ago`;
    return `${Math.floor(secs / 86400)}d ago`;
  }

  res.json(rows.map((r) => {
    const meta  = SOURCE_META[r.source] || { color: "#6b7280", icon: "??" };
    const label = r.event_type === "commit"
      ? `${r.actor || "Someone"} pushed to ${r.repository || r.project_name}: ${(r.description || "").slice(0, 60)}`
      : `${r.issue_key || ""} ${r.description || "JIRA issue updated"} [${r.status || ""}]`;
    return {
      source: r.source,
      color:  meta.color,
      icon:   meta.icon,
      text:   label,
      time:   timeAgo(r.event_time),
      projectName: r.project_name,
    };
  }));
});

// ─── /api/vyaguta ────────────────────────────────────────────────────────────

router.get("/vyaguta", async (_req, res) => {
  const rows = await dbQuery(
    `SELECT * FROM ${db.tbl("dashboard_gold_kpis")} ORDER BY kpi_date DESC LIMIT 1`,
    "kpis",
  );
  const r = rows && rows[0];

  // Jira projects and archived count from static Vyaguta mirror (gold doesn't track these yet)
  const active   = vy.activeProjects();
  const archived = vy.archivedProjects();
  const jiraKeys = [...new Set(active.map((p) => p.jiraProjectKey))];

  res.json([
    { label: "Active Projects", value: r ? String(num(r.active_projects))  : String(active.length)  },
    { label: "Tracked Repos",   value: r ? String(num(r.active_repos))     : String(vy.activeProjectRepos().length) },
    { label: "Jira Projects",   value: String(jiraKeys.length) },
    { label: "Archived",        value: String(archived.length) },
  ]);
});

// ─── /api/insights ───────────────────────────────────────────────────────────

router.get("/insights", async (req, res) => {
  const project = selectedProject(req);
  const [kpiRows, projRows] = await Promise.all([
    dbQuery(`SELECT * FROM ${db.tbl("dashboard_gold_kpis")} ORDER BY kpi_date DESC LIMIT 1`, "kpis"),
    getProjectRows(project, "insights_proj"),
  ]);

  const kpi  = kpiRows  && kpiRows[0];
  const proj = projRows && (project
    ? projRows[0]
    : [...projRows].sort((a, b) => (num(a.health_score) || 0) - (num(b.health_score) || 0))[0]);

  const insights = [];

  if (project && proj) {
    const metrics = summarizeProjectMetrics([proj]);
    insights.push(`"${proj.project_name}" recorded ${fmt(proj.commits_last_30d)} commits in the last 30 days.`);
    insights.push(`${fmt(proj.open_issues)} issues remain open${metrics.bugRate != null ? `, with a ${metrics.bugRate.toFixed(1)}% bug rate` : ""}.`);
    insights.push(`Project health is ${fmt(proj.health_score)}/100.`);
  } else if (kpi) {
    insights.push(`${fmt(kpi.total_commits_30d)} commits across ${fmt(kpi.active_projects)} active projects in the last 30 days.`);
    insights.push(`${fmt(kpi.done_issues_30d)} JIRA issues resolved in the last 30 days (${fmt(kpi.total_open_issues)} still open).`);
    if (num(kpi.total_bugs) > 0) insights.push(`${fmt(kpi.total_bugs)} bugs tracked across all projects.`);
    if (num(kpi.active_developers) > 0) insights.push(`${fmt(kpi.active_developers)} developers made commits in the last 30 days.`);
  }

  if (!project && proj) insights.push(`"${proj.project_name}" has the lowest health score (${proj.health_score}/100) — ${proj.bugs_count} open bugs.`);

  if (insights.length === 0) {
    insights.push(
      "Run the daily_ingest_pipeline job to populate live data.",
      "Dashboard gold tables are currently empty.",
    );
  }

  res.json(insights);
});

module.exports = router;
