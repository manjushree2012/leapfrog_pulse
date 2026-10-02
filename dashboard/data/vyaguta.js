/**
 * JavaScript mirror of src/leapfrog_pulse/vyaguta/mock_source.py.
 *
 * This is the integration boundary for the dashboard layer.
 * When the real Vyaguta API is available, replace the PROJECTS array below
 * with a live fetch and keep the helper functions as-is.
 */

const COMPANY = "leapfrog";

const PROJECTS = [
  {
    projectId:   "proj-001",
    projectName: "HealthTrack Platform",
    team:        "HealthTech",
    status:      "active",
    githubRepos: [
      "https://github.com/leapfrogonline/healthtrack-api",
      "https://github.com/leapfrogonline/healthtrack-web",
    ],
    jiraProjectKey: "HTP",
  },
  {
    projectId:   "proj-002",
    projectName: "FinEdge Analytics",
    team:        "FinTech",
    status:      "active",
    githubRepos: [
      "https://github.com/leapfrogonline/finedge-analytics",
      "https://github.com/leapfrogonline/finedge-mobile",
    ],
    jiraProjectKey: "FEA",
  },
  {
    projectId:   "proj-003",
    projectName: "EduConnect LMS",
    team:        "EdTech",
    status:      "active",
    githubRepos: [
      "https://github.com/leapfrogonline/educonnect-backend",
      "https://github.com/leapfrogonline/educonnect-frontend",
      "https://github.com/leapfrogonline/educonnect-mobile",
    ],
    jiraProjectKey: "ECL",
  },
  {
    projectId:   "proj-004",
    projectName: "LogiTrack Supply Chain",
    team:        "Enterprise",
    status:      "active",
    githubRepos: [
      "https://github.com/leapfrogonline/logitrack-core",
      "https://github.com/leapfrogonline/logitrack-dashboard",
    ],
    jiraProjectKey: "LSC",
  },
  {
    projectId:   "proj-005",
    projectName: "DevPulse Internal",
    team:        "Engineering Intelligence",
    status:      "active",
    githubRepos: [
      "https://github.com/leapfrogonline/leapfrog_pulse",
    ],
    jiraProjectKey: "DEVP",
  },
  {
    projectId:   "proj-006",
    projectName: "RetailPro POS",
    team:        "Retail",
    status:      "archived",
    githubRepos: [
      "https://github.com/leapfrogonline/retailpro-pos",
    ],
    jiraProjectKey: "RPP",
  },
];

function activeProjects() {
  return PROJECTS.filter((p) => p.status === "active");
}

function archivedProjects() {
  return PROJECTS.filter((p) => p.status === "archived");
}

/** All (project, repo) rows for active projects — mirrors gold table rows. */
function activeProjectRepos() {
  const rows = [];
  for (const p of activeProjects()) {
    for (const url of p.githubRepos) {
      const parts = url.replace(/https?:\/\/github\.com\//, "").split("/");
      rows.push({
        projectId:      p.projectId,
        projectName:    p.projectName,
        team:           p.team,
        githubRepoUrl:  url,
        githubOwner:    parts[0],
        githubRepoName: parts[1],
        jiraProjectKey: p.jiraProjectKey,
        company:        COMPANY,
      });
    }
  }
  return rows;
}

/** Unique teams across active projects. */
function activeTeams() {
  return [...new Set(activeProjects().map((p) => p.team))];
}

module.exports = { PROJECTS, activeProjects, archivedProjects, activeProjectRepos, activeTeams };
