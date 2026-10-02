/**
 * Databricks SQL connector for the DevPulse dashboard.
 *
 * Queries the dashboard_gold Delta tables via the Databricks SQL HTTP API.
 * Results are cached for CACHE_TTL_MS to avoid hammering the warehouse on
 * every page load.
 *
 * Required env vars (dashboard will fall back to static stubs if missing):
 *   DATABRICKS_HOST       — workspace hostname, e.g. dbc-199c9ed4-1fd6.cloud.databricks.com
 *   DATABRICKS_TOKEN      — personal access token
 *   DATABRICKS_HTTP_PATH  — SQL warehouse HTTP path, e.g. /sql/1.0/warehouses/abc123
 *
 * Optional:
 *   DATABRICKS_CATALOG    — default: workspace
 *   DATABRICKS_SCHEMA     — default: prod
 */

const { DBSQLClient } = require("@databricks/sql");

const HOST      = (process.env.DATABRICKS_HOST || "").replace(/^https?:\/\//, "");
const TOKEN     = process.env.DATABRICKS_TOKEN;
const HTTP_PATH = process.env.DATABRICKS_HTTP_PATH;

const CATALOG = process.env.DATABRICKS_CATALOG || "workspace";
const SCHEMA  = process.env.DATABRICKS_SCHEMA  || "prod";

const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
const _cache = new Map();

let _clientPromise = null;

function _getClient() {
  if (_clientPromise) return _clientPromise;
  if (!HOST || !TOKEN || !HTTP_PATH) return null;

  _clientPromise = (async () => {
    const client = new DBSQLClient();
    await client.connect({ host: HOST, path: HTTP_PATH, token: TOKEN });
    return client;
  })().catch((err) => {
    _clientPromise = null; // allow retry on next call
    throw err;
  });

  return _clientPromise;
}

/**
 * Run a SQL query against the Databricks warehouse.
 * @param {string} sql
 * @param {string|null} cacheKey  — omit to skip caching
 * @returns {Promise<Object[]>}   — array of plain row objects (column names are keys)
 */
async function query(sql, cacheKey = null) {
  if (cacheKey) {
    const hit = _cache.get(cacheKey);
    if (hit && Date.now() - hit.ts < CACHE_TTL_MS) return hit.rows;
  }

  const clientPromise = _getClient();
  if (!clientPromise) throw new Error("Databricks credentials not configured");

  const client  = await clientPromise;
  const session = await client.openSession({ initialNamespace: { catalogName: CATALOG, schemaName: SCHEMA } });
  try {
    const op   = await session.executeStatement(sql, { runAsync: false });
    const rows = await op.fetchAll();
    await op.close();
    if (cacheKey) _cache.set(cacheKey, { rows, ts: Date.now() });
    return rows;
  } finally {
    await session.close().catch(() => {});
  }
}

/** Fully-qualified backtick-quoted table reference. */
function tbl(name) {
  return `\`${CATALOG}\`.\`${SCHEMA}\`.\`${name}\``;
}

module.exports = { query, tbl, CATALOG, SCHEMA };
