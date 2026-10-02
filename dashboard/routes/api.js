const { Router } = require("express");
const router = Router();
const vy = require("../data/vyaguta");

// Shapes here mirror what the render functions in public/index.html expect.
// Replace stub values with real Databricks / Gold-layer queries when ready.

router.get("/kpis", (_req, res) => {
  res.json({
    totalCommits:     { value: "12,842", change: 14,  up: true  },
    pullRequests:     { value: "3,102",  change: 22,  up: true  },
    deployFrequency:  { value: "18 / week", change: 31, up: true },
    ciFailureRate:    { value: "5.6%",   change: 28,  up: false },
    incidentRecovery: { value: "22 min", change: 52,  up: false },
  });
});

router.get("/health", (_req, res) => {
  res.json({
    score: 82,
    change: 12,
    metrics: [
      { label: "PR Review Time",        value: "3.6 hrs",      change: 34, goodDown: true  },
      { label: "Deployment Frequency",  value: "18 / week",    change: 31, goodDown: false },
      { label: "CI Failure Rate",       value: "5.6%",         change: 28, goodDown: true  },
      { label: "Avg. Incident Recovery",value: "22 min",       change: 52, goodDown: true  },
      { label: "Team Velocity",         value: "28 story pts", change: 21, goodDown: false },
    ],
  });
});

router.get("/teams", (_req, res) => {
  res.json([
    { name: "Payments",       color: "#f59e0b", members: 8, tickets: 23, prs: 42, incidents: 7 },
    { name: "Mobile App",     color: "#3b82f6", members: 5, tickets: 18, prs: 31, incidents: 3 },
    { name: "Analytics",      color: "#8b5cf6", members: 4, tickets:  9, prs: 15, incidents: 1 },
    { name: "Internal Tools", color: "#10b981", members: 3, tickets: 21, prs: 12, incidents: 4 },
    { name: "Platform",       color: "#06b6d4", members: 6, tickets: 16, prs: 28, incidents: 5 },
  ]);
});

router.get("/projects", (_req, res) => {
  res.json([
    { name: "Payment Gateway", team: "Payments",  color: "#f59e0b", cycleTime: "18h", deployFreq: "1.8/day", incidents: 7 },
    { name: "Mobile App",      team: "Mobile",    color: "#3b82f6", cycleTime: "12h", deployFreq: "2.4/day", incidents: 3 },
    { name: "Analytics",       team: "Analytics", color: "#8b5cf6", cycleTime: "26h", deployFreq: "1.1/day", incidents: 1 },
    { name: "Internal Tools",  team: "Tools",     color: "#10b981", cycleTime: "32h", deployFreq: "0.8/day", incidents: 4 },
    { name: "User Service",    team: "Platform",  color: "#06b6d4", cycleTime: "15h", deployFreq: "1.9/day", incidents: 5 },
  ]);
});

router.get("/time-distribution", (_req, res) => {
  res.json({
    total: "2,480 hrs",
    breakdown: [
      { label: "Feature work",  color: "#3b82f6", pct: 38 },
      { label: "Code reviews",  color: "#8b5cf6", pct: 16 },
      { label: "Bug fixing",    color: "#ef4444", pct: 15 },
      { label: "CI/CD",         color: "#f59e0b", pct: 12 },
      { label: "Meetings",      color: "#10b981", pct:  8 },
      { label: "Other",         color: "#d1d5db", pct: 11 },
    ],
  });
});

router.get("/activity", (_req, res) => {
  res.json([
    { source: "GitHub",     color: "#24292e", icon: "GH", text: "PR #452 merged in payment-service",      time: "2h ago" },
    { source: "Jira",       color: "#0052cc", icon: "JI", text: "Jira ticket DEV-312 moved to In Progress", time: "3h ago" },
    { source: "CI/CD",      color: "#f59e0b", icon: "CI", text: "Deployment to prod succeeded (v2.4.1)",  time: "4h ago" },
    { source: "Monitoring", color: "#ef4444", icon: "MO", text: "Incident resolved — api-gateway",        time: "5h ago" },
    { source: "Vyaguta",    color: "#8b5cf6", icon: "VY", text: "New feedback received for team Payments", time: "6h ago" },
  ]);
});

router.get("/vyaguta", (_req, res) => {
  const active   = vy.activeProjects();
  const repos    = vy.activeProjectRepos();
  const teams    = vy.activeTeams();
  const archived = vy.archivedProjects();
  res.json([
    { label: "Active Projects", value: String(active.length),   change: "",  up: null },
    { label: "Tracked Repos",   value: String(repos.length),    change: "",  up: null },
    { label: "Jira Projects",   value: String(active.length),   change: "",  up: null },
    { label: "Archived",        value: String(archived.length), change: "",  up: null },
  ]);
});

router.get("/insights", (_req, res) => {
  res.json([
    "Payment Gateway has the highest incident rate (7 in last 30 days).",
    "PR review time improved by 34% after the new review process.",
    "Team Analytics has the highest cycle time (26h avg).",
    "Overall engineering health is up by 12%.",
  ]);
});

module.exports = router;
