/**
 * Google Sheets Integration for Playwright Image Automation API
 *
 * Updated for async queue-based API (v2).
 * The API now returns a jobId immediately (202), and we poll /api/job/:id
 * until the job completes, then write the final image URL to Column C.
 *
 * Two ways to use:
 *   1. Custom Formula: =REGENERATE_IMAGE(texte, image, apiLink)
 *   2. Custom Menu:   🤖 Automation > ...
 */

// =========================================================================
// SOLUTION 1: Custom Function (Formula)
// =========================================================================

/**
 * Regenerates an image using the Playwright automation API.
 *
 * @param {string} texte The text prompt to pass to the API.
 * @param {string} image The original image URL to pass to the API (or first image).
 * @param {string} apiLink The API base URL or the full API execution link from Column D.
 * @param {string} [image2] Optional second image URL.
 * @return {string} The public URL of the regenerated image.
 * @customfunction
 */
function REGENERATE_IMAGE(texte, image, apiLink, image2) {
  if (!apiLink) return "Error: API URL or Link (Column D) is required.";

  var apiConfig = parseApiLink(apiLink);
  if (apiConfig.error) return "Error: " + apiConfig.error;

  if (!texte && !image && !image2) return "Error: Missing input parameters (texte or image).";

  var cacheKey = getCacheKey((texte || "") + "|" + (image2 || ""), image, apiConfig.baseUrl);
  var cache = CacheService.getScriptCache();
  var cachedJobId = cache.get(cacheKey);

  var url = apiConfig.baseUrl + "/api/execute/" + encodeURIComponent(apiConfig.scenarioName) +
            "?texte=" + encodeURIComponent(texte || "") +
            "&image=" + encodeURIComponent(image || "");
  if (image2) {
    url += "&image2=" + encodeURIComponent(image2);
  }

  var jobId = cachedJobId;

  try {
    if (!jobId) {
      // ── Step 1: Enqueue if not already cached ──────────────────────────────
      var headers = { "X-Pinggy-No-Screen": "true", "pinggy-bypass": "true" };
      var response = UrlFetchApp.fetch(url, { method: "get", muteHttpExceptions: true, followRedirects: true, headers: headers });
      var code     = response.getResponseCode();
      var text     = response.getContentText();

      if (code !== 202) return "Error: Enqueue failed (" + code + "): " + text;

      try {
        jobId = JSON.parse(text).jobId;
      } catch (_) {}
      if (!jobId) return "Error: No jobId in response: " + text;

      // Cache the jobId for 20 minutes (1200 seconds) to prevent duplicate runs
      cache.put(cacheKey, jobId, 1200);
    }

    // ── Step 2: Poll (Custom functions timeout at 30s; poll for max 28s) ────
    var result = pollJob(apiConfig.baseUrl, jobId, 28, 3000);
    
    // If successfully returned an image URL, remove from cache and return it
    if (result.indexOf("http") === 0) {
      cache.remove(cacheKey);
      return result;
    }
    
    // If it is still processing, throw a Google Sheets error so it doesn't return a text status
    throw new Error("Job is still processing on the server. Please wait a moment and recalculate.");
  } catch (e) {
    // Throw error so Google Sheets displays #ERROR! with the message
    throw new Error(e.message || e.toString());
  }
}

// Helper to generate a cache-safe key
function getCacheKey(texte, image, apiLink) {
  var str = (texte || "") + "|" + (image || "") + "|" + (apiLink || "");
  var hash = 0;
  for (var i = 0; i < str.length; i++) {
    var char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return "img_job_" + Math.abs(hash);
}

// Polling helper specifically for the custom formula (returns status instead of throwing)
function pollJob(baseUrl, jobId, maxWaitSec, pollIntervalMs) {
  var headers = { "X-Pinggy-No-Screen": "true", "pinggy-bypass": "true" };
  var maxAttempts = Math.ceil((maxWaitSec * 1000) / pollIntervalMs);
  var pollUrl     = baseUrl.replace(/\/+$/, "") + "/api/job/" + jobId;

  for (var i = 0; i < maxAttempts; i++) {
    Utilities.sleep(pollIntervalMs);

    try {
      var pollRes  = UrlFetchApp.fetch(pollUrl, { method: "get", muteHttpExceptions: true, headers: headers });
      var pollCode = pollRes.getResponseCode();
      var pollText = pollRes.getContentText();

      if (pollCode !== 200) continue;

      var status = JSON.parse(pollText);
      if (status.status === "completed") {
        return (status.result && status.result.imageUrl) ? status.result.imageUrl : "Done (no image URL)";
      }
      if (status.status === "failed") {
        return "Failed: " + (status.error || "unknown error");
      }
    } catch (e) {
      // Ignore temporary network drops
    }
  }

  return "Processing (Job ID: " + jobId + ")";
}

// =========================================================================
// SOLUTION 2: Custom Menu (Recommended — no timeout problems)
// =========================================================================

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('🤖 Automation')
    .addItem('Regenerate Image (Selected Row)',        'regenerateActiveRow')
    .addItem('Regenerate Image (All Empty Rows)',      'regenerateEmptyRows')
    .addItem('▶ Run ALL Rows in Parallel (Up to 50 Rows)', 'regenerateAllRowsParallel')
    .addToUi();
}

function regenerateActiveRow() {
  var sheet = SpreadsheetApp.getActiveSheet();
  var row   = sheet.getActiveCell().getRow();
  if (row === 1) {
    SpreadsheetApp.getUi().alert("Please select a data row (Row 2 or below).");
    return;
  }
  processRow(sheet, row);
}

function regenerateEmptyRows() {
  var sheet   = SpreadsheetApp.getActiveSheet();
  var lastRow = sheet.getLastRow();
  if (lastRow < 2) { SpreadsheetApp.getUi().alert("No data found."); return; }

  var values = sheet.getRange(2, 1, lastRow - 1, 4).getValues();
  var count  = 0;
  for (var i = 0; i < values.length; i++) {
    if ((values[i][0] || values[i][1]) && !values[i][2]) {
      processRow(sheet, i + 2);
      count++;
    }
  }
  if (count === 0) SpreadsheetApp.getUi().alert("All rows already have results!");
  else SpreadsheetApp.getActiveSpreadsheet().toast("Finished " + count + " rows.", "Success", 5);
}

/**
 * Parallel batch: enqueues ALL rows at once, then polls all job IDs until done.
 * This avoids the 30-second Apps Script timeout since UrlFetchApp.fetchAll is fast.
 */
function regenerateAllRowsParallel() {
  var sheet   = SpreadsheetApp.getActiveSheet();
  var lastRow = Math.min(sheet.getLastRow(), 50);

  if (lastRow < 2) { SpreadsheetApp.getUi().alert("No data found."); return; }

  var fallbackApiLink = sheet.getRange(2, 4).getValue().toString();
  var fallbackConfig  = parseApiLink(fallbackApiLink);
  if (fallbackConfig.error) {
    SpreadsheetApp.getUi().alert("Error: " + fallbackConfig.error + "\n\nPlease set a valid API URL in Column D.");
    return;
  }

  // ── Step 1: Enqueue all jobs in one fetchAll ─────────────────────────────
  var enqueueRequests = [];
  var rowMeta         = [];

  for (var row = 2; row <= lastRow; row++) {
    var texte  = sheet.getRange(row, 1).getValue().toString();
    var image  = sheet.getRange(row, 2).getValue().toString();
    if (!texte && !image) continue;

    var rowApiLink = sheet.getRange(row, 4).getValue().toString() || fallbackApiLink;
    var cfg        = parseApiLink(rowApiLink);
    var baseUrl    = cfg.error ? fallbackConfig.baseUrl    : cfg.baseUrl;
    var scenario   = cfg.error ? fallbackConfig.scenarioName : cfg.scenarioName;

    var url = baseUrl + "/api/execute/" + encodeURIComponent(scenario) +
              "?texte=" + encodeURIComponent(texte) +
              "&image=" + encodeURIComponent(image);

    enqueueRequests.push({ url: url, method: "get", muteHttpExceptions: true, followRedirects: true, headers: { "X-Pinggy-No-Screen": "true", "pinggy-bypass": "true" } });
    rowMeta.push({ row: row, baseUrl: baseUrl });
  }

  if (enqueueRequests.length === 0) {
    SpreadsheetApp.getUi().alert("No rows with data found.");
    return;
  }

  SpreadsheetApp.getActiveSpreadsheet().toast(
    "Enqueueing " + enqueueRequests.length + " jobs...", "Parallel Queue", -1
  );

  var enqueueResponses = UrlFetchApp.fetchAll(enqueueRequests);

  // ── Step 2: Collect all jobIds ────────────────────────────────────────────
  var jobs = []; // [{ row, baseUrl, jobId }]
  for (var i = 0; i < enqueueResponses.length; i++) {
    var code = enqueueResponses[i].getResponseCode();
    var text = enqueueResponses[i].getContentText();
    var meta = rowMeta[i];

    if (code === 202) {
      try {
        var parsed = JSON.parse(text);
        if (parsed.jobId) {
          jobs.push({ row: meta.row, baseUrl: meta.baseUrl, jobId: parsed.jobId });
        } else {
          sheet.getRange(meta.row, 3).setValue("Error: No jobId returned — " + text);
        }
      } catch (_) {
        sheet.getRange(meta.row, 3).setValue("Error: Unexpected response — " + text);
      }
    } else {
      sheet.getRange(meta.row, 3).setValue("Enqueue Error " + code + ": " + text);
    }
  }

  if (jobs.length === 0) {
    SpreadsheetApp.getActiveSpreadsheet().toast("All rows failed to enqueue.", "Error", 5);
    return;
  }

  SpreadsheetApp.getActiveSpreadsheet().toast(
    "Polling " + jobs.length + " jobs for results...", "Waiting", -1
  );

  // ── Step 3: Poll until all jobs are done ──────────────────────────────────
  var maxWaitMs    = 300000; // 5 minutes max
  var pollInterval = 4000;  // poll every 4 seconds
  var elapsed      = 0;
  var pending      = jobs.slice(); // copy

  while (pending.length > 0 && elapsed < maxWaitMs) {
    Utilities.sleep(pollInterval);
    elapsed += pollInterval;

    var pollRequests = pending.map(function(j) {
      return { url: j.baseUrl + "/api/job/" + j.jobId, method: "get", muteHttpExceptions: true, headers: { "X-Pinggy-No-Screen": "true", "pinggy-bypass": "true" } };
    });

    var pollResponses;
    try {
      pollResponses = UrlFetchApp.fetchAll(pollRequests);
    } catch (e) {
      // If fetching fails due to network/tunnel limit congestion, ignore and retry in next cycle
      continue;
    }

    var stillPending  = [];

    for (var p = 0; p < pollResponses.length; p++) {
      var job    = pending[p];
      var pCode  = pollResponses[p].getResponseCode();
      var pText  = pollResponses[p].getContentText();

      if (pCode !== 200) {
        // If it's a temporary network/server error, keep in pending so it retries
        stillPending.push(job);
        continue;
      }

      try {
        var status = JSON.parse(pText);
        if (status.status === "completed") {
          var imageUrl = (status.result && status.result.imageUrl) ? status.result.imageUrl : "";
          sheet.getRange(job.row, 3).setValue(imageUrl || "Done (no URL)");
        } else if (status.status === "failed") {
          sheet.getRange(job.row, 3).setValue("Failed: " + (status.error || "unknown error"));
        } else {
          // still waiting or active
          stillPending.push(job);
        }
      } catch (_) {
        // Parse error (could be temporary HTML response from proxy/tunnel); retry
        stillPending.push(job);
      }
    }

    pending = stillPending;
  }

  // Mark any remaining as timed out
  for (var t = 0; t < pending.length; t++) {
    sheet.getRange(pending[t].row, 3).setValue("Timeout: job " + pending[t].jobId + " did not complete in time.");
  }

  SpreadsheetApp.getActiveSpreadsheet().toast("All " + jobs.length + " jobs finished!", "Done", 5);
}

// =========================================================================
// HELPER: Process a single row (enqueue + poll)
// =========================================================================

function processRow(sheet, row) {
  var texte  = sheet.getRange(row, 1).getValue().toString();
  var image  = sheet.getRange(row, 2).getValue().toString();
  var apiLink = sheet.getRange(row, 4).getValue().toString();

  if (!apiLink) apiLink = sheet.getRange(2, 4).getValue().toString();
  if (!apiLink) { sheet.getRange(row, 3).setValue("Error: Missing API Link in Column D"); return; }

  var apiConfig = parseApiLink(apiLink);
  if (apiConfig.error) { sheet.getRange(row, 3).setValue("Error: " + apiConfig.error); return; }

  SpreadsheetApp.getActiveSpreadsheet().toast("Enqueueing row " + row + "...", "Running", -1);

  var url = apiConfig.baseUrl + "/api/execute/" + encodeURIComponent(apiConfig.scenarioName) +
            "?texte=" + encodeURIComponent(texte) +
            "&image=" + encodeURIComponent(image);

  try {
    var result = enqueueAndPoll(url, apiConfig.baseUrl, 300, 4000);
    sheet.getRange(row, 3).setValue(result);
    SpreadsheetApp.getActiveSpreadsheet().toast("Row " + row + " completed!", "Success", 5);
  } catch (e) {
    sheet.getRange(row, 3).setValue("Error: " + e.toString());
    SpreadsheetApp.getActiveSpreadsheet().toast("Row " + row + " failed.", "Error", 5);
  }
}

// =========================================================================
// HELPER: Enqueue job then poll until done, returns final image URL
// =========================================================================

/**
 * Sends a request to /api/execute/... , gets a jobId (202),
 * then polls /api/job/:id every `pollIntervalMs` ms until completed or timeout.
 *
 * @param {string} executeUrl  Full URL to call (e.g. http://host/api/execute/Nano?texte=...)
 * @param {string} baseUrl     Base URL of the server (e.g. http://host)
 * @param {number} maxWaitSec  Max seconds to wait for completion (default 300)
 * @param {number} pollIntervalMs  How often to poll in ms (default 4000)
 * @returns {string} Final image URL
 */
function enqueueAndPoll(executeUrl, baseUrl, maxWaitSec, pollIntervalMs) {
  maxWaitSec    = maxWaitSec    || 300;
  pollIntervalMs = pollIntervalMs || 4000;

  var headers = { "X-Pinggy-No-Screen": "true", "pinggy-bypass": "true" };

  // ── Step 1: Enqueue ───────────────────────────────────────────────────────
  var response = UrlFetchApp.fetch(executeUrl, { method: "get", muteHttpExceptions: true, followRedirects: true, headers: headers });
  var code     = response.getResponseCode();
  var text     = response.getContentText();

  if (code !== 202) throw new Error("Enqueue failed (" + code + "): " + text);

  var jobId;
  try {
    jobId = JSON.parse(text).jobId;
  } catch (_) {}
  if (!jobId) throw new Error("No jobId in response: " + text);

  // ── Step 2: Poll ──────────────────────────────────────────────────────────
  var maxAttempts = Math.ceil((maxWaitSec * 1000) / pollIntervalMs);
  var pollUrl     = baseUrl.replace(/\/+$/, "") + "/api/job/" + jobId;

  for (var i = 0; i < maxAttempts; i++) {
    Utilities.sleep(pollIntervalMs);

    try {
      var pollRes  = UrlFetchApp.fetch(pollUrl, { method: "get", muteHttpExceptions: true, headers: headers });
      var pollCode = pollRes.getResponseCode();
      var pollText = pollRes.getContentText();

      if (pollCode !== 200) {
        // If it's a temporary network/server error, keep polling/retrying
        continue;
      }

      var status;
      try { 
        status = JSON.parse(pollText); 
      } catch (_) { 
        // Bad JSON response (e.g. proxy HTML); retry
        continue;
      }

      if (status.status === "completed") {
        return (status.result && status.result.imageUrl) ? status.result.imageUrl : "Done (no image URL)";
      }
      if (status.status === "failed") {
        throw new Error("Job failed: " + (status.error || "unknown error"));
      }
      // status is 'waiting' or 'active' — keep polling
    } catch (e) {
      // If it's a real job execution failure error, propagate it
      if (e.message && e.message.indexOf("Job failed:") !== -1) {
        throw e;
      }
      // Otherwise, ignore temporary network issues (like "Address unavailable") and keep polling
    }
  }

  throw new Error("Timeout: job " + jobId + " did not complete in " + maxWaitSec + " seconds.");
}

// =========================================================================
// HELPER: Parse API link
// =========================================================================

function parseApiLink(link) {
  if (!link || link.trim() === "") {
    return { error: "No API link provided. Please set a valid URL in Column D." };
  }

  link = link.trim();
  var baseUrl      = "";
  var scenarioName = "Nano";

  if (link.indexOf("/api/execute/") !== -1) {
    var parts    = link.split("/api/execute/");
    baseUrl      = parts[0].trim();
    scenarioName = decodeURIComponent(parts[1].split("?")[0].trim());
  } else {
    baseUrl = link;
  }

  baseUrl = baseUrl.replace(/\/+$/, "");

  if (baseUrl.indexOf("http") !== 0) {
    return { error: "Invalid URL: '" + baseUrl + "'. Must start with http:// or https://" };
  }

  return { baseUrl: baseUrl, scenarioName: scenarioName };
}
