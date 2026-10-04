const assert = require("node:assert/strict");
const { test } = require("node:test");
const { generateInsights } = require("../services/gemini");

function restoreEnv(t) {
  const originalKey = process.env.GEMINI_API_KEY;
  const originalModel = process.env.GEMINI_MODEL;
  const originalFetch = global.fetch;
  t.after(() => {
    if (originalKey === undefined) delete process.env.GEMINI_API_KEY;
    else process.env.GEMINI_API_KEY = originalKey;
    if (originalModel === undefined) delete process.env.GEMINI_MODEL;
    else process.env.GEMINI_MODEL = originalModel;
    global.fetch = originalFetch;
  });
}

test("generates at most six trimmed insights from Gemini's JSON response", async (t) => {
  restoreEnv(t);
  process.env.GEMINI_API_KEY = "test-key";
  process.env.GEMINI_MODEL = "test-model";
  let requestUrl;
  global.fetch = async (url, options) => {
    requestUrl = new URL(url);
    assert.equal(options.method, "POST");
    assert.equal(JSON.parse(options.body).generationConfig.responseMimeType, "application/json");
    return new Response(JSON.stringify({
      candidates: [{
        content: {
          parts: [{ text: JSON.stringify({ insights: ["  First insight  ", "Second", "Third", "Fourth", "Fifth", "Sixth", "Seventh"] }) }],
        },
      }],
    }), { status: 200 });
  };

  const insights = await generateInsights({ scope: "whole organization" });

  assert.equal(requestUrl.pathname, "/v1beta/models/test-model:generateContent");
  assert.equal(requestUrl.searchParams.get("key"), "test-key");
  assert.deepEqual(insights, ["First insight", "Second", "Third", "Fourth", "Fifth", "Sixth"]);
});

test("requires a Gemini API key", async (t) => {
  restoreEnv(t);
  delete process.env.GEMINI_API_KEY;

  await assert.rejects(generateInsights({}), /GEMINI_API_KEY is not configured/);
});

test("reports Gemini API failures", async (t) => {
  restoreEnv(t);
  process.env.GEMINI_API_KEY = "test-key";
  global.fetch = async () => new Response("quota exceeded", { status: 429 });

  await assert.rejects(generateInsights({}), /HTTP 429/);
});
