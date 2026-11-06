// ──────────────────────────────────────────────────────────────────────────────
// worker.js  ─  BullMQ Worker process (all Playwright automation lives here)
//
// Run with:    node worker.js
// Scale up:    CONCURRENCY=5 node worker.js
//
// The worker pulls jobs from Redis and runs them with a configurable
// concurrency limit (default: 3 simultaneous browsers per worker process).
// You can run multiple worker.js processes across multiple machines.
// ──────────────────────────────────────────────────────────────────────────────

'use strict';

let chromium;
try {
    chromium = require('patchright').chromium;
} catch (_) {
    chromium = require('playwright').chromium;
}
const path         = require('path');
const fs           = require('fs');
const os           = require('os');
const { execSync } = require('child_process');

const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { Worker }    = require('bullmq');
const Redis         = require('ioredis');
const lockfile      = require('proper-lockfile');

// ─── Redis connection ─────────────────────────────────────────────────────────
const redisConnection = new Redis({
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: parseInt(process.env.REDIS_PORT, 10) || 6379,
    maxRetriesPerRequest: null,
    enableReadyCheck: false
});

redisConnection.on('connect', () => console.log('[Redis] Worker connected ✓'));
redisConnection.on('error',   (err) => console.error('[Redis] Error:', err.message));

// ─── Concurrency (tune this per machine RAM) ──────────────────────────────────
//  Each Chrome instance ≈ 1.5–2 GB RAM.
//  Default: 3  →  change via env:  CONCURRENCY=5 node worker.js
const CONCURRENCY = parseInt(process.env.CONCURRENCY, 10) || 3;

/**
 * Helper to remove a directory with retry logic (handles Windows file locks)
 */
async function removeDirWithRetry(dirPath, retries = 5, delay = 600) {
    if (!dirPath || !fs.existsSync(dirPath)) return;
    for (let i = 0; i < retries; i++) {
        try {
            await new Promise(r => setTimeout(r, delay));
            fs.rmSync(dirPath, { recursive: true, force: true });
            console.log(`  [Clean] Removed temp dir: ${path.basename(dirPath)}`);
            return;
        } catch (err) {
            if (i === retries - 1) {
                console.warn(`  [Clean] Cleanup warning for ${path.basename(dirPath)}: ${err.message}`);
            }
        }
    }
}

/**
 * Scans local_profiles/ and removes any leftover temporary _run_ directories.
 */
function cleanTempProfiles(activeProfileDir = null) {
    const profilesDir = path.join(__dirname, 'local_profiles');
    if (!fs.existsSync(profilesDir)) return;
    try {
        const entries = fs.readdirSync(profilesDir, { withFileTypes: true });
        let cleanedCount = 0;
        for (const entry of entries) {
            if (entry.isDirectory() && entry.name.includes('_run_')) {
                const fullPath = path.join(profilesDir, entry.name);
                if (activeProfileDir && path.resolve(fullPath) === path.resolve(activeProfileDir)) {
                    continue;
                }
                try {
                    fs.rmSync(fullPath, { recursive: true, force: true });
                    cleanedCount++;
                } catch (_) {}
            }
        }
        if (cleanedCount > 0) {
            console.log(`  [Clean] Freed disk space: removed ${cleanedCount} leftover profile directories`);
        }
    } catch (e) {
        console.warn('  [Clean] Profile cleanup error:', e.message);
    }
}

/**
 * Removes old downloads and temporary error files to save disk space.
 */
function cleanOldDownloads(maxAgeMs = 3600000) {
    const downloadsDir = path.join(__dirname, 'downloads');
    if (!fs.existsSync(downloadsDir)) return;
    try {
        const now = Date.now();
        const files = fs.readdirSync(downloadsDir);
        for (const file of files) {
            const filePath = path.join(downloadsDir, file);
            try {
                const stat = fs.statSync(filePath);
                const isErrorFile = file.startsWith('error_');
                const cutoff = isErrorFile ? 600000 : maxAgeMs; // 10m for error files, 1h for normal downloads
                if (now - stat.mtimeMs > cutoff) {
                    fs.unlinkSync(filePath);
                }
            } catch (_) {}
        }
    } catch (_) {}
}

/**
 * Recursively copies a folder from src to dest.
 */
function copyFolderSync(src, dest) {
    if (!fs.existsSync(src)) { fs.mkdirSync(dest, { recursive: true }); return; }
    fs.mkdirSync(dest, { recursive: true });
    for (const entry of fs.readdirSync(src, { withFileTypes: true })) {
        const srcPath  = path.join(src, entry.name);
        const destPath = path.join(dest, entry.name);
        if (entry.isDirectory()) {
            copyFolderSync(srcPath, destPath);
        } else {
            try { fs.copyFileSync(srcPath, destPath); } catch (e) { /* skip locked files */ }
        }
    }
}

/**
 * Thread-safe profile cloning using proper-lockfile.
 * Retries up to 15 times with exponential back-off.
 */
async function safeCopyProfile(src, dest) {
    if (!fs.existsSync(src)) fs.mkdirSync(src, { recursive: true });
    let release;
    try {
        release = await lockfile.lock(src, {
            retries: { retries: 15, factor: 2, minTimeout: 100, maxTimeout: 3000 }
        });
        copyFolderSync(src, dest);
    } catch (err) {
        console.error(`[Lock] Could not lock ${src}:`, err.message);
        copyFolderSync(src, dest);   // fallback
    } finally {
        if (release) { try { await release(); } catch (_) {} }
    }
}

/**
 * Global clipboard mutex — only one browser at a time may perform the
 * canvas → clipboard → paste cycle.
 */
let _clipboardChain = Promise.resolve();
function acquireClipboardLock() {
    let releaseFn;
    const next = new Promise(resolve => { releaseFn = resolve; });
    const waitPromise = _clipboardChain.then(() => releaseFn);
    _clipboardChain = _clipboardChain.then(() => next);
    return waitPromise;
}

/**
 * Auto-dismiss common Gemini overlays (cookie consent, disclaimers).
 * Specifically avoids clicking any exit / redirect buttons like "Try Gemini".
 */
async function dismissCommonOverlays(page, timeoutPerBtn = 500) {
    const selectors = [
        'mat-dialog-actions button.confirm-button',
        '.mat-mdc-dialog-actions button.confirm-button',
        'mat-dialog-actions button:has-text("Continue")',
        'mat-dialog-actions button:has-text("Continuer")',
        'button.confirm-button',
        'button:has-text("Continue")',
        'button:has-text("Continuer")',
        'immersive-share-disclaimer-dialog button:has-text("Ok")',
        'immersive-share-disclaimer-dialog button:has-text("OK")',
        'immersive-share-disclaimer-dialog button:has-text("Continuer")',
        'immersive-share-disclaimer-dialog button:has-text("Continue")',
        'button:has-text("Continue this chat")',
        'button:has-text("Continuer cette discussion")',
        'button:has-text("Use prompt")',
        'button:has-text("Utiliser cette invite")',
        'button:has-text("Remix this")',
        'button:has-text("Remixer")',
        'button:has-text("Tout accepter")',
        'button:has-text("Accept all")',
        'button:has-text("J\'accepte")',
        'button:has-text("I agree")',
        'button:has-text("Got it")',
        'button:has-text("Compris")',
        'button:has-text("Stay signed out")',
        'button:has-text("No thanks")',
        'button:has-text("Not now")',
        'button:has-text("Dismiss")',
        'button[aria-label="Close"]',
        'button[aria-label="Dismiss"]'
    ];

    for (const selector of selectors) {
        try {
            for (const frame of [page, ...page.frames()]) {
                const btn = frame.locator(selector).first();
                if (await btn.isVisible({ timeout: timeoutPerBtn }).catch(() => false)) {
                    // Safety check: NEVER click an exit button or "Try Gemini" which redirects away
                    const isExitOrRedirect = await btn.evaluate(el => {
                        const cls = (el.className || '').toLowerCase();
                        const txt = (el.innerText || '').toLowerCase();
                        return cls.includes('exit') || txt.includes('try gemini') || txt.includes('essayer gemini');
                    }).catch(() => false);
                    if (isExitOrRedirect) continue;

                    await btn.click({ force: true }).catch(() => {});
                    await page.waitForTimeout(500);
                    break;
                }
            }
        } catch (_) {}
    }
}

/**
 * Handles third-party shared Gemini Canvas apps (gemini.google.com/share/*).
 * Confirms the security disclaimer modal so the app stays on the share page and loads the iframe.
 * Also handles dialog errors like "Le lien n'existe pas" and recovers from redirects to /app.
 */
async function handleGeminiSharePage(page, targetShareUrl = '') {
    console.log('[SharePage] Handling Gemini Share Page disclaimer & waiting for canvas iframe...');

    // Helper to dismiss error/alert dialogs (e.g. "Le lien n'existe pas", "Something went wrong")
    async function dismissErrorDialogs() {
        const dialogSelectors = [
            'mat-dialog-container button:has-text("OK")',
            '[role="dialog"] button:has-text("OK")',
            '.cdk-overlay-container button:has-text("OK")',
            'button:has-text("OK")'
        ];
        for (const sel of dialogSelectors) {
            try {
                const btn = page.locator(sel).first();
                if (await btn.isVisible({ timeout: 1200 }).catch(() => false)) {
                    console.log(`[SharePage] Found dialog button (${sel}). Clicking OK to dismiss...`);
                    await btn.click({ force: true }).catch(() => {});
                    await page.waitForTimeout(1000);
                    return true;
                }
            } catch (_) {}
        }
        return false;
    }

    // Dismiss any immediate error dialog
    await dismissErrorDialogs();

    // If Gemini redirected away from /share/ to /app, try reloading targetShareUrl
    const currentUrl = page.url() || '';
    if (targetShareUrl && targetShareUrl.includes('/share/') && currentUrl.includes('/app') && !currentUrl.includes('/share/')) {
        console.warn(`[SharePage] Gemini redirected from ${targetShareUrl} to /app. Attempting reload to target share URL...`);
        try {
            await page.goto(targetShareUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
            await page.waitForTimeout(2000);
            await dismissErrorDialogs();
        } catch (_) {}
    }

    const confirmSelectors = [
        'button.confirm-button',
        'mat-dialog-actions button.confirm-button',
        '.mat-mdc-dialog-actions button.confirm-button',
        'mat-dialog-actions button:has-text("Continue")',
        'mat-dialog-actions button:has-text("Continuer")',
        'button:has-text("Continue")',
        'button:has-text("Continuer")',
        'button:has-text("Accéder")',
        'button:has-text("Try Gemini Canvas")'
    ];

    async function tryClickConfirmButton() {
        for (const sel of confirmSelectors) {
            try {
                const btn = page.locator(sel).first();
                if (await btn.isVisible({ timeout: 500 }).catch(() => false)) {
                    const txt = (await btn.innerText().catch(() => '')).trim().toLowerCase();
                    if (txt === 'try gemini' || txt === 'essayer gemini') continue;
                    console.log(`[SharePage] Found confirm button (${sel}: "${txt}"). Clicking to proceed...`);
                    await btn.click({ force: true }).catch(() => {});
                    await page.waitForTimeout(1500);
                    return true;
                }
            } catch (_) {}
        }
        return false;
    }

    // Try clicking immediately
    let confirmed = await tryClickConfirmButton();

    // Wait for the immersive child iframe (blob: or scf.usercontent.goog or gemini-code-immersive)
    const deadline = Date.now() + 30000;
    while (Date.now() < deadline) {
        const hasImmersiveFrame = page.frames().some(f => {
            const u = (f.url() || '').toLowerCase();
            return u.startsWith('blob:') || u.includes('scf.usercontent.goog') || u.includes('gemini-code-immersive');
        });
        if (hasImmersiveFrame) {
            console.log('[SharePage] ✅ Canvas app iframe attached and ready!');
            return true;
        }

        // Check if confirm button appeared
        if (!confirmed) {
            confirmed = await tryClickConfirmButton();
        }

        await dismissErrorDialogs();
        await page.waitForTimeout(500);
    }
    console.warn('[SharePage] ⚠️ Canvas app iframe not attached within 30s. Current URL:', page.url());
    return false;
}

/**
 * Automatically detects, clicks, and waits for Google reCAPTCHA checkbox resolution
 * on google.com/sorry/index and submits/redirects back to the destination.
 */
async function solveOrBypassGoogleCaptcha(page, timeoutMs = 60000, targetRedirectUrl = '') {
    const isCaptchaPage = () => {
        try {
            const url = (page.url() || '').toLowerCase();
            const hasRecaptcha = page.frames().some(f => (f.url() || '').includes('recaptcha'));
            return url.includes('google.com/sorry') || url.includes('/sorry/') || url.includes('recaptcha') || hasRecaptcha;
        } catch (_) {
            return false;
        }
    };

    if (!isCaptchaPage()) return false;

    console.warn(`\n⚠️  [Google CAPTCHA Detected] ${page.url()}`);
    console.log(`  [reCAPTCHA] Attempting automatic checkbox click & verification (timeout: ${timeoutMs}ms)...`);

    const deadline = Date.now() + timeoutMs;
    let lastClickAttempt = 0;

    while (Date.now() < deadline) {
        // 1. Check if redirected away from CAPTCHA page
        if (!isCaptchaPage()) {
            console.log(`  [reCAPTCHA] ✅ Page redirected to: ${page.url().slice(0, 100)}`);
            await page.waitForTimeout(2000);
            return true;
        }

        if (targetRedirectUrl && page.url().includes(targetRedirectUrl)) {
            console.log(`  [reCAPTCHA] ✅ Reached target redirect URL: ${page.url().slice(0, 100)}`);
            await page.waitForTimeout(2000);
            return true;
        }

        // 2. Check if the checkbox has been checked (aria-checked="true")
        let isChecked = false;
        for (const frame of [page, ...page.frames()]) {
            try {
                const checkedLoc = frame.locator('#recaptcha-anchor[aria-checked="true"], .recaptcha-checkbox[aria-checked="true"], [role="checkbox"][aria-checked="true"], .recaptcha-checkbox-checked').first();
                if (await checkedLoc.isVisible({ timeout: 200 }).catch(() => false)) {
                    isChecked = true;
                    break;
                }
            } catch (_) {}
        }

        if (isChecked) {
            console.log(`  [reCAPTCHA] Checkbox is verified (checked)! Submitting form / waiting for redirect...`);
            try {
                const submitted = await page.evaluate(() => {
                    const form = document.querySelector('form#captcha-form') || document.querySelector('form[action*="index"]') || document.querySelector('form');
                    if (form) {
                        const submitBtn = form.querySelector('input[type="submit"]') || form.querySelector('button[type="submit"]');
                        if (submitBtn) { submitBtn.click(); return true; }
                        form.submit();
                        return true;
                    }
                    return false;
                });
                if (submitted) {
                    console.log(`  [reCAPTCHA] Form submit triggered.`);
                }
            } catch (_) {}

            await page.waitForTimeout(2000);
            continue;
        }

        const now = Date.now();
        // 3. Attempt to click the reCAPTCHA checkbox every 3.5 seconds
        if (now - lastClickAttempt > 3500) {
            lastClickAttempt = now;
            let clicked = false;

            // Strategy A: Iterate all frames
            for (const frame of page.frames()) {
                const fUrl = (frame.url() || '').toLowerCase();
                if (fUrl.includes('recaptcha') || fUrl.includes('anchor') || fUrl.includes('google.com/sorry')) {
                    const selectors = [
                        '#recaptcha-anchor',
                        '.recaptcha-checkbox-border',
                        '.recaptcha-checkbox',
                        'div[role="checkbox"]',
                        'span#recaptcha-anchor',
                        '.rc-anchor-checkbox',
                        'label#recaptcha-anchor-label'
                    ];
                    for (const sel of selectors) {
                        try {
                            const el = frame.locator(sel).first();
                            if (await el.isVisible({ timeout: 400 }).catch(() => false)) {
                                console.log(`  [reCAPTCHA] Found checkbox (${sel}) in frame: ${frame.url().slice(0, 50)}. Clicking...`);
                                await el.scrollIntoViewIfNeeded().catch(() => {});
                                await page.waitForTimeout(250);

                                const box = await el.boundingBox().catch(() => null);
                                if (box && box.width > 0 && box.height > 0) {
                                    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2, { steps: 5 });
                                    await page.waitForTimeout(150);
                                    await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
                                } else {
                                    await el.click({ force: true }).catch(() => {});
                                }
                                clicked = true;
                                console.log(`  [reCAPTCHA] Clicked checkbox.`);
                                break;
                            }
                        } catch (_) {}
                    }
                    if (clicked) break;
                }
            }

            // Strategy B: Playwright frameLocator
            if (!clicked) {
                try {
                    const fl = page.frameLocator('iframe[src*="recaptcha"], iframe[title*="reCAPTCHA" i], iframe[title*="recaptcha" i]').first();
                    const checkEl = fl.locator('#recaptcha-anchor, .recaptcha-checkbox-border, [role="checkbox"]').first();
                    if (await checkEl.isVisible({ timeout: 600 }).catch(() => false)) {
                        console.log(`  [reCAPTCHA] Found checkbox via frameLocator. Clicking...`);
                        const box = await checkEl.boundingBox().catch(() => null);
                        if (box && box.width > 0 && box.height > 0) {
                            await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
                        } else {
                            await checkEl.click({ force: true }).catch(() => {});
                        }
                        clicked = true;
                    }
                } catch (_) {}
            }
        }

        await page.waitForTimeout(1000);
    }

    if (isCaptchaPage()) {
        console.warn(`  [reCAPTCHA] ⚠️ Still on CAPTCHA page after ${timeoutMs}ms.`);
        return false;
    }

    return true;
}

async function handleCaptchaIfPresent(page) {
    const url = (page.url() || '').toLowerCase();
    const hasRecaptcha = page.frames().some(f => (f.url() || '').includes('recaptcha'));
    if (url.includes('google.com/sorry') || url.includes('recaptcha') || hasRecaptcha) {
        const solved = await solveOrBypassGoogleCaptcha(page, 60000);
        if (!solved && (page.url().includes('google.com/sorry') || page.url().includes('recaptcha') || page.frames().some(f => (f.url() || '').includes('recaptcha')))) {
            console.warn(`\n⚠️  [CAPTCHA] Automatic click pending / challenge active. Waiting up to 60s for verification...`);
            const deadline = Date.now() + 60000;
            while (Date.now() < deadline) {
                await page.waitForTimeout(2000);
                if (!page.url().includes('google.com/sorry') && !page.url().includes('recaptcha') && !page.frames().some(f => (f.url() || '').includes('recaptcha'))) {
                    console.log(`✅ [CAPTCHA Solved] Verification completed! Resuming automation...`);
                    await page.waitForTimeout(2000);
                    return true;
                }
            }
            throw new Error(`CAPTCHA was not solved within timeout. Please reduce concurrency or check your browser.`);
        }
        return true;
    }
    return false;
}

function normalizeSelector(selector, searchKind = null) {
    if (!selector || typeof selector !== 'string') return selector;
    let s = selector.trim();
    if (searchKind === 'ID') {
        if (!s.startsWith('#')) return `#${s}`;
        return s;
    }
    if (searchKind === 'XPath') {
        if (!s.startsWith('xpath=')) return `xpath=${s}`;
        return s;
    }
    // Auto-detect XPath: starts with / or (// or contains xpath= or ./
    if (s.startsWith('/') || s.startsWith('((') || s.startsWith('(/') || s.startsWith('./')) {
        if (!s.startsWith('xpath=')) return `xpath=${s}`;
        return s;
    }
    return s;
}

function getCandidateSelectors(rawSelector) {
    if (!rawSelector) return [];
    const selector = normalizeSelector(rawSelector);
    const list = [selector];

    // If it starts with xpath=//, also add bare // version (Playwright supports both)
    if (selector.startsWith('xpath=//')) {
        list.push(selector.slice(6));
    }

    // If it's an XPath looking for an ID: //*[@id="foo"] or //div[@id='foo']
    const idMatch = selector.match(/@id=["']([^"']+)["']/);
    if (idMatch) {
        list.push(`#${idMatch[1]}`);
        list.push(`[id="${idMatch[1]}"]`);
    }

    // Common Gemini Input Fallbacks (handles Shadow DOM <rich-textarea>)
    if (selector.includes('prompt-input') || selector.includes('prompt') || selector.includes('input')) {
        list.push('#prompt-input');
        list.push('rich-textarea [contenteditable="true"]');
        list.push('rich-textarea p');
        list.push('div[contenteditable="true"]');
        list.push('textarea');
        list.push('[aria-label*="prompt" i]');
        list.push('[aria-label*="Enter a prompt" i]');
        list.push('[data-placeholder]');
    }

    // Common Gemini Send Button Fallbacks
    if (selector.includes('send-btn') || selector.includes('send')) {
        list.push('#send-btn');
        list.push('button[aria-label*="Send" i]');
        list.push('button[aria-label*="Envoyer" i]');
        list.push('button.send-button');
        list.push('mat-icon:has-text("send")');
        list.push('button:has(mat-icon:has-text("send"))');
        list.push('button[mattooltip*="Send" i]');
        list.push('.send-button-container button');
        list.push('div[aria-label*="Send" i]');
    }

    // Common Gemini Share / Canvas / Upload targets
    if (selector.includes('share-landing-page') || selector.includes('web-preview') || selector.includes('bard-sidenav')) {
        list.push('rich-textarea [contenteditable="true"]');
        list.push('div[contenteditable="true"]');
        list.push('#prompt-input');
        list.push('rich-textarea');
        list.push('immersive-share-landing-page');
    }

    // Common Gemini Image Download Fallbacks
    if (selector.includes('chat-container') || selector.includes('/a') || selector.includes('download')) {
        list.push('#chat-container a');
        list.push('#chat-container img');
        list.push('div.message-content img');
        list.push('div[data-message-id] img');
        list.push('img[src*="googleusercontent"]');
        list.push('img[src*="blob:"]');
        list.push('img.generated-image');
    }

    return [...new Set(list)];
}

/**
 * Find a locator across all frames with retry loop.
 */
async function findLocatorWithRetry(page, selector, timeoutMs = 15000) {
    const deadline = Date.now() + timeoutMs;
    const candidateSelectors = getCandidateSelectors(selector);
    let attempts = 0;

    while (Date.now() < deadline) {
        attempts++;
        if (page.url().includes('google.com/sorry') || page.url().includes('recaptcha')) {
            await handleCaptchaIfPresent(page);
        }

        try {
            const frames = [page, ...page.frames()];
            for (const sel of candidateSelectors) {
                try {
                    const locator = await Promise.any(frames.map(async f => {
                        const loc = f.locator(sel).first();
                        await loc.waitFor({ state: 'attached', timeout: 1200 });
                        return loc;
                    }));
                    if (locator && (await locator.count().catch(() => 0)) > 0) {
                        return locator;
                    }
                } catch (_) {}
            }
        } catch (_) {}

        if (attempts % 4 === 0) {
            await dismissCommonOverlays(page, 300).catch(() => {});
            await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => {});
        }
        await page.waitForTimeout(400);
    }
    throw new Error(`Locator not found after ${timeoutMs}ms: ${selector}`);
}

// ═════════════════════════════════════════════════════════════════════════════
//  CORE PLAYWRIGHT AUTOMATION ENGINE
// ═════════════════════════════════════════════════════════════════════════════

/**
 * Launches a browser, executes all steps, uploads result to R2, and returns
 * the final image URL.
 *
 * @param {object} scenarioConfig  – scenario-level config (profile, headless, R2 creds, …)
 * @param {Array}  steps           – array of automation step objects
 * @param {string} hostUrl         – base URL of the server (for local fallback URLs)
 * @returns {{ success, imageUrl, r2UploadError }}
 */

async function injectClipboardHook(frameOrPage) {
    try {
        await frameOrPage.evaluate(() => {
            if (window.__clipboardHookInstalled) return;
            window.__clipboardHookInstalled = true;
            window.__lastCopiedText = window.__lastCopiedText || '';

            try {
                if (navigator.clipboard && navigator.clipboard.writeText) {
                    const origWrite = navigator.clipboard.writeText.bind(navigator.clipboard);
                    navigator.clipboard.writeText = async (text) => {
                        window.__lastCopiedText = String(text || '');
                        try { return await origWrite(text); } catch (_) { return Promise.resolve(); }
                    };
                }
            } catch (_) {}

            try {
                const origExec = document.execCommand ? document.execCommand.bind(document) : null;
                document.execCommand = function(cmd, ...args) {
                    if (cmd && String(cmd).toLowerCase() === 'copy') {
                        try {
                            const focused = document.activeElement;
                            if (focused && (focused.tagName === 'TEXTAREA' || focused.tagName === 'INPUT') && typeof focused.value === 'string' && focused.value.length > 0) {
                                window.__lastCopiedText = focused.value;
                            } else {
                                const sel = window.getSelection() ? window.getSelection().toString() : '';
                                if (sel) window.__lastCopiedText = sel;
                            }
                        } catch (_) {}
                    }
                    return origExec ? origExec(cmd, ...args) : true;
                };
            } catch (_) {}

            try {
                document.addEventListener('copy', (e) => {
                    try {
                        const focused = document.activeElement;
                        if (focused && (focused.tagName === 'TEXTAREA' || focused.tagName === 'INPUT') && typeof focused.value === 'string' && focused.value.length > 0) {
                            window.__lastCopiedText = focused.value;
                        } else if (e.clipboardData) {
                            const h = e.clipboardData.getData('text/html');
                            const t = e.clipboardData.getData('text');
                            if (h) window.__lastCopiedText = h;
                            else if (t) window.__lastCopiedText = t;
                        }
                    } catch (_) {}
                }, true);
            } catch (_) {}
        }).catch(() => {});
    } catch (_) {}
}

function cleanupProfileLocks(dir, profileName) {
    if (!dir) return;
    try {
        const locks = ['SingletonLock', 'SingletonSocket', 'SingletonCookie', 'lockfile'];
        for (const l of locks) {
            const p = path.join(dir, l);
            if (fs.existsSync(p)) {
                try { fs.unlinkSync(p); } catch (_) {}
            }
        }
        // Remove old crashed session tabs so Chromium does not restore stale pages
        const sessionsDir = path.join(dir, 'Default', 'Sessions');
        if (fs.existsSync(sessionsDir)) {
            try { fs.rmSync(sessionsDir, { recursive: true, force: true }); } catch (_) {}
        }
        // Mark exit_type as Normal to suppress the "Restore pages" popup
        const prefPath = path.join(dir, 'Default', 'Preferences');
        if (fs.existsSync(prefPath)) {
            try {
                const raw = fs.readFileSync(prefPath, 'utf8');
                const prefs = JSON.parse(raw);
                if (prefs.profile) {
                    prefs.profile.exit_type = 'Normal';
                    prefs.profile.exited_cleanly = true;
                }
                fs.writeFileSync(prefPath, JSON.stringify(prefs));
            } catch (_) {}
        }
    } catch (_) {}
}

async function safeLaunchPersistentContext(userDataDir, launchOptions, profileName, forceClose = false) {
    if (forceClose) {
        try {
            if (process.platform === 'win32') {
                execSync('taskkill /F /IM chrome.exe /T', { stdio: 'ignore' });
            } else {
                if (profileName) {
                    execSync(`pkill -f "${profileName}" || true`, { stdio: 'ignore' });
                } else {
                    execSync('pkill -f chrome || pkill -f chromium || true', { stdio: 'ignore' });
                }
            }
            await new Promise(r => setTimeout(r, 600));
        } catch (_) {}
    }
    cleanupProfileLocks(userDataDir, profileName);

    try {
        return await chromium.launchPersistentContext(userDataDir, launchOptions);
    } catch (err) {
        if (err.message && (err.message.includes('already in use') || err.message.includes('Opening in existing browser session') || err.message.includes('ProcessSingleton'))) {
            console.warn(`[Worker] Profile '${profileName || userDataDir}' is locked by another instance. Auto-recovering...`);
            try {
                if (process.platform === 'win32') {
                    execSync('taskkill /F /IM chrome.exe /T', { stdio: 'ignore' });
                } else {
                    if (profileName) {
                        execSync(`pkill -9 -f "${profileName}" || true`, { stdio: 'ignore' });
                    }
                    execSync('pkill -9 -f chrome || pkill -9 -f chromium || true', { stdio: 'ignore' });
                }
                await new Promise(r => setTimeout(r, 1000));
            } catch (_) {}
            cleanupProfileLocks(userDataDir, profileName);
            return await chromium.launchPersistentContext(userDataDir, launchOptions);
        }
        throw err;
    }
}

async function runAutomation(scenarioConfig, steps, hostUrl = 'http://localhost:3000') {
    const { chromeProfile, browserType, userAgent, headless, forceCloseChrome,
            r2Endpoint, r2Bucket, r2KeyId, r2Secret, r2PublicDomain } = scenarioConfig;

    let browserContext;
    let tempUserDataDir = null;
    let finalDownloadedImageUrl = null;
    let localDownloadedImageUrl = null;
    let r2UploadError = null;
    let returnedClipboardText = null;

    try {
        // ── optional: kill system Chrome ─────────────────────────────────────
        if (forceCloseChrome && browserType !== 'Isolated') {
            try { execSync('taskkill /F /IM chrome.exe /T', { stdio: 'ignore' }); await new Promise(r => setTimeout(r, 1000)); } catch (_) {}
        }

        const contextOptions = userAgent ? { userAgent } : {};

        const isLinux = process.platform === 'linux';
        const hasDisplay = Boolean(process.env.DISPLAY);
        const shouldBeHeadless = headless === true || (isLinux && !hasDisplay);
        const linuxFlags = isLinux ? [
            '--no-sandbox', '--disable-setuid-sandbox',
            '--disable-dev-shm-usage', '--disable-gpu',
            '--password-store=basic',
            '--disable-session-crashed-bubble',
            '--hide-crash-restore-bubble',
            '--no-recovery-component'
        ] : [
            '--password-store=basic',
            '--disable-session-crashed-bubble',
            '--hide-crash-restore-bubble',
            '--no-recovery-component'
        ];

        // ── launch browser ────────────────────────────────────────────────────
        if (chromeProfile && chromeProfile.trim()) {
            let userDataDir, launchOptions;

            if (browserType === 'Isolated') {
                const baseDir = path.join(__dirname, 'local_profiles', chromeProfile);
                const runnerId = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                tempUserDataDir = path.join(__dirname, 'local_profiles', `${chromeProfile}_run_${runnerId}`);

                console.log(`  [Profile] Cloning ${baseDir} → ${tempUserDataDir}`);
                await safeCopyProfile(baseDir, tempUserDataDir);
                userDataDir = tempUserDataDir;

                launchOptions = {
                    headless: shouldBeHeadless,
                    args: ['--disable-blink-features=AutomationControlled', '--password-store=basic', ...linuxFlags],
                    ignoreDefaultArgs: ['--enable-automation'],
                    ignoreHTTPSErrors: true,
                    ...contextOptions
                };
            } else {
                userDataDir = isLinux
                    ? path.join(os.homedir(), '.config', 'google-chrome')
                    : path.join(os.homedir(), 'AppData', 'Local', 'Google', 'Chrome', 'User Data');
                launchOptions = {
                    channel: isLinux ? undefined : 'chrome',
                    headless: shouldBeHeadless,
                    args: [`--profile-directory=${chromeProfile}`, '--disable-blink-features=AutomationControlled', '--password-store=basic', ...linuxFlags],
                    ignoreDefaultArgs: ['--enable-automation'],
                    ignoreHTTPSErrors: true,
                    ...contextOptions
                };
            }

            browserContext = await safeLaunchPersistentContext(userDataDir, launchOptions, chromeProfile, forceCloseChrome);
            if (typeof browserContext.grantPermissions === 'function') {
                try { await browserContext.grantPermissions(['clipboard-read', 'clipboard-write']).catch(() => {}); } catch (_) {}
            }
        } else {
            const browser = await chromium.launch({
                headless: headless === true,
                args: ['--disable-blink-features=AutomationControlled', '--password-store=basic', ...linuxFlags]
            });
            browserContext = await browser.newContext({ ignoreHTTPSErrors: true, ...contextOptions });
            browserContext._browser = browser;
        }

        // ── get a usable page ──────────────────────────────────────────────────
        let page = browserContext.pages().find(p => !p.url().startsWith('chrome-extension://'));
        if (!page) page = await browserContext.newPage();

        page.on('framenavigated', f => { injectClipboardHook(f).catch(() => {}); });
        page.on('frameattached', f => { injectClipboardHook(f).catch(() => {}); });
        
        await page.addInitScript(() => {
            window.chrome = { runtime: {}, app: {}, csi: () => {}, loadTimes: () => {} };
            window.__lastCopiedText = '';

            try {
                if (navigator.clipboard) {
                    const origWriteText = typeof navigator.clipboard.writeText === 'function' ? navigator.clipboard.writeText.bind(navigator.clipboard) : null;
                    navigator.clipboard.writeText = async (text) => {
                        window.__lastCopiedText = (text !== undefined && text !== null) ? String(text) : '';
                        if (origWriteText) {
                            try { return await origWriteText(text); } catch (_) { return Promise.resolve(); }
                        }
                        return Promise.resolve();
                    };
                }
            } catch (_) {}

            try {
                const origExecCommand = typeof document.execCommand === 'function' ? document.execCommand.bind(document) : null;
                document.execCommand = function(cmd, ...args) {
                    if (cmd && String(cmd).toLowerCase() === 'copy') {
                        try {
                            const sel = window.getSelection() ? window.getSelection().toString() : '';
                            if (sel) window.__lastCopiedText = sel;
                        } catch (_) {}
                    }
                    return origExecCommand ? origExecCommand(cmd, ...args) : true;
                };
            } catch (_) {}

            try {
                document.addEventListener('copy', (e) => {
                    try {
                        if (e.clipboardData) {
                            const txt = e.clipboardData.getData('text');
                            if (txt) window.__lastCopiedText = txt;
                        }
                        const sel = window.getSelection() ? window.getSelection().toString() : '';
                        if (sel) window.__lastCopiedText = sel;
                    } catch (_) {}
                }, true);
            } catch (_) {}
        });

        await page.bringToFront();

        // ── execute steps ──────────────────────────────────────────────────────
        for (let i = 0; i < steps.length; i++) {
            const step = steps[i];
            console.log(`  [Step ${i + 1}/${steps.length}] ${step.type}`);

            // ── Navigate ───────────────────────────────────────────────────────
            if (step.type === 'Navigate') {
                const navUrl = step.url || 'https://example.com';
                try {
                    await page.goto(navUrl, { waitUntil: 'domcontentloaded', timeout: 90000 });
                } catch (navErr) {
                    console.warn(`[Navigate] First attempt failed (${navErr.message}), retrying...`);
                    await page.waitForTimeout(2000);
                    await page.goto(navUrl, { waitUntil: 'domcontentloaded', timeout: 90000 });
                }
                await page.waitForLoadState('domcontentloaded', { timeout: 15000 }).catch(() => {});
                await handleCaptchaIfPresent(page);
                await page.waitForTimeout(1000);
                if (navUrl.includes('/share/')) {
                    await handleGeminiSharePage(page, navUrl);
                } else {
                    await dismissCommonOverlays(page);
                }
            }

            // ── Click ──────────────────────────────────────────────────────────
            else if (step.type === 'Click') {
                if (!step.searchValue) throw new Error('Click: missing searchValue');
                let selector = normalizeSelector(step.searchValue, step.searchKind);
                const locator = await findLocatorWithRetry(page, selector);
                await locator.scrollIntoViewIfNeeded().catch(() => {});
                const isBtnDisabled = await locator.evaluate(el => Boolean(el.disabled || el.hasAttribute('disabled') || el.getAttribute('aria-disabled') === 'true')).catch(() => false);
                if (isBtnDisabled) {
                    console.log(`[Click] Target "${selector}" is currently disabled. Waiting up to 20s for it to become enabled...`);
                    const enableDeadline = Date.now() + 20000;
                    while (Date.now() < enableDeadline) {
                        const stillDisabled = await locator.evaluate(el => Boolean(el.disabled || el.hasAttribute('disabled') || el.getAttribute('aria-disabled') === 'true')).catch(() => false);
                        if (!stillDisabled) {
                            console.log(`[Click] Target "${selector}" is now enabled.`);
                            break;
                        }
                        await page.waitForTimeout(300);
                    }
                }

                for (const f of [page, ...page.frames()]) {
                    await injectClipboardHook(f).catch(() => {});
                }

                if (step.clickType === 'Real Mouse Click') {
                    const box = await locator.boundingBox();
                    if (!box) throw new Error('Click: could not get bounding box');
                    await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
                } else {
                    await locator.click({ force: true });
                }

                await page.waitForTimeout(400);
                for (const f of [page, ...page.frames()]) {
                    try {
                        const copied = await f.evaluate(() => window.__lastCopiedText || '');
                        if (copied && copied.trim()) {
                            returnedClipboardText = copied.trim();
                            console.log(`[Click] 📋 Captured clipboard text from click: ${returnedClipboardText.substring(0, 80)}...`);
                            break;
                        }
                    } catch (_) {}
                }
                if (selector.toLowerCase().includes('send') || selector.toLowerCase().includes('submit') || selector.toLowerCase().includes('login')) {
                    await page.keyboard.press('Enter').catch(() => {});
                    await page.waitForTimeout(2000);
                    await handleCaptchaIfPresent(page);
                }
                await page.waitForTimeout(500);
            }

            // ── Send Text ──────────────────────────────────────────────────────
            else if (step.type === 'Send Text') {
                if (!step.searchValue) throw new Error('Send Text: missing searchValue');
                let selector = normalizeSelector(step.searchValue, step.searchKind);
                const locator = await findLocatorWithRetry(page, selector);
                await locator.focus().catch(() => {});
                await locator.click({ force: true }).catch(() => {});
                await page.waitForTimeout(100);

                // Clear existing content
                try {
                    await locator.evaluate(el => {
                        if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') el.value = '';
                        else el.textContent = '';
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    });
                } catch (_) {}

                // Type text
                if (step.simulateTyping) {
                    await locator.pressSequentially(step.textValue || '', { delay: 50 });
                } else {
                    try { await page.keyboard.insertText(step.textValue || ''); }
                    catch (_) { await locator.fill(step.textValue || ''); }
                }

                // Fire events so React/Angular pick up the change
                try {
                    await locator.evaluate(el => {
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    });
                } catch (_) {}
            }

            // ── Upload Image ───────────────────────────────────────────────────
            else if (step.type === 'Upload Image') {
                if (!step.searchValue || !step.imageUrl) throw new Error('Upload Image: missing searchValue or imageUrl');
                let selector = normalizeSelector(step.searchValue, step.searchKind);

                const rawUrls = Array.isArray(step.imageUrl)
                    ? step.imageUrl
                    : (step.imageUrl || '').split(/[\n,]+/).map(s => s.trim()).filter(Boolean);

                for (let imgIdx = 0; imgIdx < rawUrls.length; imgIdx++) {
                    const currentImageUrl = rawUrls[imgIdx];
                    console.log(`  [Upload Image] Processing image ${imgIdx + 1}/${rawUrls.length}: ${currentImageUrl}`);

                    // 1. Direct file input upload if an input[type="file"] or #file-input is available
                    let fileInputFound = false;
                    try {
                        const imgRes = await fetch(currentImageUrl);
                        if (imgRes.ok) {
                            const imgBuf = Buffer.from(await imgRes.arrayBuffer());
                            const imgMime = imgRes.headers.get('content-type') || 'image/jpeg';
                            const imgExt = (imgMime.split('/')[1] || 'jpg').replace('jpeg', 'jpg');
                            const tmpPath = path.join(os.tmpdir(), `upload_${Date.now()}_${imgIdx}.${imgExt}`);
                            fs.writeFileSync(tmpPath, imgBuf);

                            const frames = [page, ...page.frames()];
                            for (const f of frames) {
                                const fi = f.locator('input[type="file"], #file-input, input[accept*="image"]').first();
                                if (await fi.count() > 0) {
                                    await fi.setInputFiles(tmpPath);
                                    console.log(`  [Upload Image] Attached via input[type="file"] ✅ (${imgIdx + 1}/${rawUrls.length})`);
                                    await page.waitForTimeout(2000);
                                    fileInputFound = true;
                                    break;
                                }
                            }
                            try { fs.unlinkSync(tmpPath); } catch (_) {}
                        }
                    } catch (e) {
                        console.warn(`  [Upload Image] File input attempt error: ${e.message}`);
                    }

                    if (fileInputFound) continue;

                    // 2. Clipboard strategy fallback
                    const releaseLock = await acquireClipboardLock();
                    try {
                        await browserContext.grantPermissions(['clipboard-read', 'clipboard-write']);

                        const imgPage = await browserContext.newPage();
                        await imgPage.bringToFront();
                        await imgPage.goto(currentImageUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });

                        await imgPage.evaluate(async () => {
                            const img = document.querySelector('img');
                            if (!img) throw new Error('No img element found on page');
                            const canvas = document.createElement('canvas');
                            canvas.width  = img.naturalWidth  || img.width  || 500;
                            canvas.height = img.naturalHeight || img.height || 500;
                            canvas.getContext('2d').drawImage(img, 0, 0);
                            const blob = await new Promise(r => canvas.toBlob(r, 'image/png'));
                            await navigator.clipboard.write([new ClipboardItem({ 'image/png': blob })]);
                        });

                        await imgPage.close();
                        await page.bringToFront();

                        const locator = await findLocatorWithRetry(page, selector);
                        await locator.click({ force: true });
                        await page.waitForTimeout(100);
                        await page.keyboard.press('Control+V');
                        await page.waitForTimeout(2000);
                    } finally {
                        releaseLock();
                    }
                }
            }

            // ── Download Image ─────────────────────────────────────────────────
            else if (step.type === 'Download Image') {
                if (!step.searchValue) throw new Error('Download Image: missing searchValue');
                let selector = normalizeSelector(step.searchValue, step.searchKind);

                console.log(`  [Download] Waiting for Gemini image generation to complete...`);
                await page.waitForTimeout(2000);

                const stopSels = [
                    '#chat-container .loader',
                    '.loader',
                    'div.loader',
                    '.loading-dots',
                    '[class*="loader"]',
                    'button#send-btn:disabled',
                    'button#send-btn[disabled]',
                    '#send-btn:disabled',
                    'button[aria-label*="Stop" i]',
                    'button[aria-label*="Arrêter" i]',
                    'button[aria-label*="stop" i]',
                    'button.stop-button',
                    'button[data-test-id*="stop"]',
                    'mat-progress-bar',
                    '.sparkle-container',
                    '.loading-indicator',
                    'bard-loading',
                    'gemini-loading',
                    'div[aria-label*="Generating" i]',
                    'div[aria-label*="Génération" i]',
                    'div[aria-label*="Processing" i]',
                    '*:has-text("Processing your request")',
                    '*:has-text("Processing...")',
                    '*:has-text("Generating...")',
                    '*:has-text("Generating image")',
                    '*:has-text("Creating image")',
                    '*:has-text("Thinking...")',
                    '[class*="spinner" i]',
                    '[class*="loading-spinner" i]',
                    '[class*="animate-spin" i]',
                    '[aria-busy="true"]'
                ];

                const isCurrentlyGenerating = async () => {
                    const frames = [...page.frames().filter(f => f !== page), page];
                    for (const f of frames) {
                        try {
                            for (const s of stopSels) {
                                try {
                                    if (await f.locator(s).first().isVisible({ timeout: 150 }).catch(() => false)) {
                                        return true;
                                    }
                                } catch (_) {}
                            }
                            const isTextGen = await f.evaluate(() => {
                                const text = document.body ? (document.body.innerText || '') : '';
                                return /processing your request/i.test(text) ||
                                       /generating image/i.test(text) ||
                                       /creating image/i.test(text) ||
                                       /thinking\.\.\./i.test(text) ||
                                       (text.includes('Processing') && text.includes('Gemini'));
                            }).catch(() => false);
                            if (isTextGen) return true;
                        } catch (_) {}
                    }
                    return false;
                };

                const genWaitStart = Date.now();
                let hasSeenGenerating = false;
                while (Date.now() - genWaitStart < 180000) {
                    const isGen = await isCurrentlyGenerating();
                    if (isGen) {
                        hasSeenGenerating = true;
                        const elapsed = Math.round((Date.now() - genWaitStart) / 1000);
                        if (elapsed % 4 === 0) {
                            console.log(`  [Download] Gemini is still generating image... (${elapsed}s elapsed)`);
                        }
                        await page.waitForTimeout(1000);
                    } else {
                        if (!hasSeenGenerating && (Date.now() - genWaitStart < 6000)) {
                            await page.waitForTimeout(1000);
                            continue;
                        }

                        let hasGeneratedAiImage = false;
                        const checkFrames = [...page.frames().filter(f => f !== page), page];
                        for (const f of checkFrames) {
                            try {
                                const fi = f.locator('#chat-container .message.ai:last-of-type img, #chat-container .message.ai:last-of-type a[download], #chat-container a[download]').last();
                                if (await fi.count() > 0) {
                                    const src = await fi.getAttribute('src').catch(() => null) || await fi.getAttribute('href').catch(() => null);
                                    if (src && !src.startsWith('data:image/svg') && !src.includes('avatar') && !src.includes('profile')) {
                                        hasGeneratedAiImage = true;
                                        break;
                                    }
                                }
                            } catch (_) {}
                        }

                        if (hasGeneratedAiImage) {
                            console.log(`  [Download] Generated AI image verified in chat-container!`);
                            break;
                        }

                        await page.waitForTimeout(2000);
                        const stillGen = await isCurrentlyGenerating();
                        if (!stillGen) {
                            console.log(`  [Download] Generation indicator cleared. Proceeding to locate generated image.`);
                            break;
                        }
                    }
                }

                console.log(`  [Download] Generation complete. Locating target image element...`);

                let locator = null;
                const allFrames = [...page.frames().filter(f => f !== page), page];

                if (selector) {
                    for (const f of allFrames) {
                        try {
                            const found = f.locator(selector).last();
                            if (await found.count() > 0) {
                                locator = found;
                                console.log(`  [Download] Found user selector "${selector}" in frame: ${f.url().slice(0, 60)}...`);
                                break;
                            }
                        } catch (_) {}
                    }
                }

                if (!locator) {
                    const candidateImageSelectors = [
                        '#chat-container .message.ai:last-of-type img',
                        '#chat-container .message.ai:last-of-type a[download]',
                        '#chat-container .message.ai:last-of-type a[href*="blob:"]',
                        '#chat-container .message.ai:last-of-type a',
                        '.message.ai:last-of-type img',
                        '.message.ai:last-of-type a[download]',
                        '#chat-container a[download]',
                        'a:has-text("Download Image")',
                        'a[download]',
                        '#chat-container a[href*="blob:"]',
                        '#chat-container img[src*="blob:"]',
                        '#chat-container img[src*="googleusercontent"]',
                        'img[src*="blob:"]',
                        'img[src*="googleusercontent"]',
                        '.generated-image img',
                        'canvas-view img',
                        'mat-card img'
                    ];
                    for (const f of allFrames) {
                        for (const imgSel of candidateImageSelectors) {
                            try {
                                const found = f.locator(imgSel).last();
                                if (await found.count() > 0) {
                                    locator = found;
                                    console.log(`  [Download] Found generated image with selector "${imgSel}" in frame: ${f.url().slice(0, 60)}...`);
                                    break;
                                }
                            } catch (_) {}
                        }
                        if (locator) break;
                    }
                }

                if (!locator && selector) {
                    locator = await findLocatorWithRetry(page, selector, 15000).catch(() => null);
                }

                if (!locator) {
                    throw new Error('Download Image: Could not locate generated image element');
                }

                await page.waitForTimeout(1000);

                let buffer = null;
                let ext = 'png';

                // Tier 1: Try in-page extraction (safe from tainted canvas errors)
                let extractInfo = null;
                try {
                    extractInfo = await locator.evaluate(async (el) => {
                        const isSvgUrl = (u) => !u || u.startsWith('data:image/svg') || /\.svg($|\?)/i.test(u) || u.includes('/svg');

                        let imgEl = el.tagName === 'IMG' ? el : el.querySelector('img');
                        if (!imgEl && el.closest) {
                            const msg = el.closest('.message, .message.ai, mat-card, .chat-message') || el.parentElement;
                            if (msg) imgEl = msg.querySelector('img');
                        }

                        if (!imgEl && el.tagName === 'CANVAS') {
                            try { return { base64: el.toDataURL('image/png'), ext: 'png' }; } catch (_) {}
                        }
                        const canvasEl = el.querySelector ? el.querySelector('canvas') : null;
                        if (canvasEl) {
                            try { return { base64: canvasEl.toDataURL('image/png'), ext: 'png' }; } catch (_) {}
                        }

                        let url = '';
                        if (el.tagName === 'A') {
                            const h = el.href || el.getAttribute('href') || '';
                            if (h && (h.startsWith('blob:') || !isSvgUrl(h))) {
                                url = h;
                            }
                        }
                        if (!url && imgEl) {
                            const s = imgEl.currentSrc || imgEl.src || imgEl.getAttribute('src') || '';
                            if (!isSvgUrl(s)) url = s;
                        }
                        if (!url) {
                            url = el.href || el.getAttribute('href') || el.src || el.getAttribute('src') || '';
                        }
                        if (!url) {
                            const bg = window.getComputedStyle(el).backgroundImage;
                            const m = bg && bg.match(/url\(["']?(.*?)["']?\)/);
                            if (m && !isSvgUrl(m[1])) url = m[1];
                        }

                        if (url && url.startsWith('data:image') && !isSvgUrl(url)) {
                            return { base64: url };
                        }

                        // In-page fetch (handles blob: URLs and same-origin URLs)
                        if (url && !isSvgUrl(url)) {
                            try {
                                const res = await fetch(url);
                                if (res.ok) {
                                    const blob = await res.blob();
                                    if (!blob.type.includes('svg')) {
                                        const b64 = await new Promise((resolve, reject) => {
                                            const r = new FileReader();
                                            r.onloadend = () => resolve(r.result);
                                            r.onerror = reject;
                                            r.readAsDataURL(blob);
                                        });
                                        return { base64: b64, mimeType: blob.type, size: blob.size };
                                    }
                                }
                            } catch (_) {}
                        }

                        // Try canvas drawing inside try-catch to avoid tainted canvas SecurityError crash
                        if (imgEl && (imgEl.naturalWidth > 64 || imgEl.width > 64) && !isSvgUrl(imgEl.src)) {
                            try {
                                const canvas = document.createElement('canvas');
                                canvas.width = imgEl.naturalWidth || imgEl.width || 800;
                                canvas.height = imgEl.naturalHeight || imgEl.height || 600;
                                const ctx = canvas.getContext('2d');
                                ctx.drawImage(imgEl, 0, 0);
                                return { base64: canvas.toDataURL('image/png'), ext: 'png' };
                            } catch (_) {}
                        }

                        return { url: url || null };
                    });
                } catch (e) {
                    console.warn(`  [Download] In-page evaluation warning: ${e.message}`);
                }

                // Process base64 if returned
                if (extractInfo?.base64) {
                    const m = extractInfo.base64.match(/^data:([^;]+);base64,(.+)$/);
                    if (m) {
                        const mime = m[1].toLowerCase();
                        if (mime.includes('jpeg') || mime.includes('jpg')) ext = 'jpg';
                        else if (mime.includes('webp')) ext = 'webp';
                        else if (mime.includes('png')) ext = 'png';
                        else if (mime.includes('gif')) ext = 'gif';
                        else if (mime.includes('svg')) ext = 'svg';
                        else ext = 'png';
                        buffer = Buffer.from(m[2], 'base64');
                    }
                }

                // Tier 2: Playwright API request (bypasses CORS and carries context cookies)
                if (!buffer && extractInfo?.url && !extractInfo.url.startsWith('blob:') && !extractInfo.url.includes('.svg')) {
                    try {
                        console.log(`  [Download] Fetching image via Playwright context: ${extractInfo.url.slice(0, 100)}...`);
                        const resp = await page.context().request.get(extractInfo.url, { timeout: 30000 });
                        if (resp.ok()) {
                            buffer = await resp.body();
                            const ct = resp.headers()['content-type'] || '';
                            if (ct.includes('jpeg') || ct.includes('jpg')) ext = 'jpg';
                            else if (ct.includes('webp')) ext = 'webp';
                            else if (ct.includes('png')) ext = 'png';
                            else if (ct.includes('gif')) ext = 'gif';
                        }
                    } catch (e) {
                        console.warn(`  [Download] Playwright context fetch failed: ${e.message}`);
                    }
                }

                // Tier 3: Node native fetch
                if (!buffer && extractInfo?.url && !extractInfo.url.startsWith('blob:') && !extractInfo.url.includes('.svg')) {
                    try {
                        console.log(`  [Download] Fetching image via Node fetch: ${extractInfo.url.slice(0, 100)}...`);
                        const resp = await fetch(extractInfo.url);
                        if (resp.ok) {
                            const ab = await resp.arrayBuffer();
                            buffer = Buffer.from(ab);
                            const ct = resp.headers.get('content-type') || '';
                            if (ct.includes('jpeg') || ct.includes('jpg')) ext = 'jpg';
                            else if (ct.includes('webp')) ext = 'webp';
                            else if (ct.includes('png')) ext = 'png';
                        }
                    } catch (e) {
                        console.warn(`  [Download] Node fetch failed: ${e.message}`);
                    }
                }

                // Tier 4: Element screenshot fallback (foolproof for any visible image or canvas)
                if (!buffer) {
                    console.log('  [Download] Using Playwright element screenshot fallback...');
                    try {
                        const imgCount = await locator.locator('img').count();
                        const targetLoc = imgCount > 0 ? locator.locator('img').first() : locator;
                        buffer = await targetLoc.screenshot({ type: 'png', timeout: 15000 });
                        ext = 'png';
                    } catch (e) {
                        console.warn(`  [Download] Element screenshot fallback: ${e.message}`);
                        buffer = await locator.screenshot({ type: 'png', timeout: 15000 });
                        ext = 'png';
                    }
                }

                if (!buffer || buffer.length === 0) {
                    throw new Error('Download Image: Failed to extract image data through all fallback methods');
                }

                // Auto-detect extension from magic bytes if possible
                if (buffer.length >= 4) {
                    if (buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4E && buffer[3] === 0x47) ext = 'png';
                    else if (buffer[0] === 0xFF && buffer[1] === 0xD8 && buffer[2] === 0xFF) ext = 'jpg';
                    else if (buffer.toString('utf8', 0, 4) === 'RIFF' && buffer.toString('utf8', 8, 12) === 'WEBP') ext = 'webp';
                    else if (buffer.toString('utf8', 0, 3) === 'GIF') ext = 'gif';
                }

                // Sanity check: Ensure SVG / tiny UI icon (< 5000 bytes) is NEVER accepted as AI image
                if (buffer && (ext === 'svg' || ext === 'svgxml' || buffer.length < 5000)) {
                    console.warn(`  [Download] Extracted file appears to be a UI icon (${ext}, ${buffer.length} bytes). Retrying extraction with screenshot fallback...`);
                    try {
                        const imgLoc = locator.locator('img').first();
                        if (await imgLoc.count() > 0) {
                            buffer = await imgLoc.screenshot({ type: 'png', timeout: 10000 });
                        } else {
                            buffer = await locator.screenshot({ type: 'png', timeout: 10000 });
                        }
                        ext = 'png';
                    } catch (_) {}
                }

                const saveDir = path.isAbsolute(step.saveDir || '')
                    ? step.saveDir
                    : path.join(__dirname, step.saveDir || 'downloads');
                if (!fs.existsSync(saveDir)) fs.mkdirSync(saveDir, { recursive: true });

                const fileName = `download_${Date.now()}.${ext}`;
                fs.writeFileSync(path.join(saveDir, fileName), buffer);
                console.log(`  [Download] Saved → ${fileName} (${buffer.length} bytes)`);

                localDownloadedImageUrl = `${hostUrl.replace(/\/+$/, '')}/downloads/${fileName}`;
                finalDownloadedImageUrl = localDownloadedImageUrl;
                console.log(`  [Download] Saved locally → ${localDownloadedImageUrl}`);

                // ── R2 upload ────────────────────────────────────────────────────
                if (r2Endpoint && r2Bucket && r2KeyId && r2Secret) {
                    try {
                        const cleanEndpoint = r2Endpoint.trim().replace(/\/+$/, '');
                        const r2 = new S3Client({
                            region: 'auto',
                            endpoint: cleanEndpoint,
                            credentials: { accessKeyId: r2KeyId.trim(), secretAccessKey: r2Secret.trim() },
                            forcePathStyle: false
                        });
                        const contentType = ext === 'jpg' ? 'image/jpeg' : (ext === 'svg' ? 'image/svg+xml' : `image/${ext}`);
                        await r2.send(new PutObjectCommand({
                            Bucket: r2Bucket.trim(), Key: fileName,
                            Body: buffer, ContentType: contentType
                        }));
                        let pd = r2PublicDomain
                            ? r2PublicDomain.trim().replace(/^https?:\/\//i, '').replace(/\/+$/, '')
                            : null;
                        if (!pd && cfApiToken) {
                            const accountId = (r2Endpoint || '').match(/https:\/\/([a-z0-9]{32})\.r2\.cloudflarestorage\.com/i)?.[1];
                            if (accountId) {
                                try {
                                    const devRes = await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets/${r2Bucket.trim()}/domains/managed`, {
                                        headers: { 'Authorization': `Bearer ${cfApiToken.trim()}` }
                                    });
                                    const devData = await devRes.json();
                                    if (devData.result?.domain && devData.result?.enabled) {
                                        pd = devData.result.domain;
                                        console.log(`  [R2] Auto-resolved public domain: ${pd}`);
                                    }
                                } catch (_) {}
                            }
                        }
                        finalDownloadedImageUrl = pd
                            ? `https://${pd}/${fileName}`
                            : `${cleanEndpoint}/${r2Bucket.trim()}/${fileName}`;
                        console.log(`  [R2] ✅ Uploaded → ${finalDownloadedImageUrl}`);
                    } catch (e) {
                        r2UploadError = e.message;
                        console.error('  [R2] ❌ Upload failed:', e.message);
                    }
                }
            }

            // ── Scroll ─────────────────────────────────────────────────────────
            else if (step.type === 'Scroll') {
                const dir = step.direction || 'Bottom';
                if (step.containerValue) {
                    try {
                        const locator = await findLocatorWithRetry(page, normalizeSelector(step.containerValue), 5000);
                        await locator.evaluate((node, d) => {
                            node.scrollTop = d === 'Bottom' ? node.scrollHeight : 0;
                        }, dir);
                    } catch (_) {}
                } else {
                    await page.evaluate(d => {
                        window.scrollTo(0, d === 'Bottom' ? document.body.scrollHeight : 0);
                    }, dir);
                }
                await page.waitForTimeout(200);
            }

            // ── Sleep ──────────────────────────────────────────────────────────
            else if (step.type === 'Sleep') {
                await page.waitForTimeout(parseInt(step.duration, 10) || 1000);
            }

            // ── Error Check ────────────────────────────────────────────────────
            else if (step.type === 'Error Check') {
                if (!step.searchValue) throw new Error('Error Check: selector (searchValue) is required');
                let sel = normalizeSelector(step.searchValue, step.searchKind);
                const timeoutMs = parseInt(step.timeoutMs, 10) || 3000;
                const errMsg    = step.errorMessage || 'Error element detected on page';

                let found = false;
                try {
                    const frames = [page, ...page.frames()];
                    await Promise.any(frames.map(async (f) => {
                        const loc = f.locator(sel).first();
                        await loc.waitFor({ state: 'visible', timeout: timeoutMs });
                        found = true;
                    }));
                } catch (_) {
                    found = false;
                }

                if (found) {
                    console.log(`  [Error Check] ⛔ Element visible: "${sel}" — throwing: ${errMsg}`);
                    throw new Error(errMsg);
                } else {
                    console.log(`  [Error Check] ✅ Element not found (${sel}) — continuing`);
                }
            }

            // ── Wait for Operation ───────────────────────────────────────────
            else if (step.type === 'Wait for' || step.type === 'Wait For' || step.type === 'wait-for' || step.type === 'waitFor') {
                const searchKind = step.searchKind || 'XPath';
                const searchValue = (step.searchValue || '').trim();
                const waitCondition = (step.waitCondition || 'hidden').toLowerCase();
                const timeoutMs = parseInt(step.timeoutMs, 10) || 60000;
                const isDisappear = (waitCondition === 'hidden' || waitCondition === 'disappear' || waitCondition === 'detached');

                console.log(`  [Wait for] ⏳ Waiting for "${searchValue}" (${searchKind}) to ${isDisappear ? 'DISAPPEAR (hidden)' : 'APPEAR (visible)'} (Max timeout: ${timeoutMs}ms)...`);

                if (!searchValue) {
                    console.warn(`  [Wait for] ⚠️ No searchValue provided, skipping wait.`);
                } else {
                    let sel = normalizeSelector(searchValue, searchKind);
                    if (searchKind === 'Text' || searchKind === 'text') {
                        sel = `text=${searchValue}`;
                    }

                    let fallbackText = '';
                    const m = searchValue.match(/contains\s*\(\s*(?:text\(\)|\.)\s*,\s*['"]([^'"]+)['"]\s*\)/i);
                    if (m) {
                        fallbackText = m[1];
                    } else if (!searchValue.startsWith('/') && !searchValue.startsWith('.') && !searchValue.startsWith('#')) {
                        fallbackText = searchValue;
                    }

                    const startTime = Date.now();
                    const checkInterval = 500;

                    if (isDisappear) {
                        await page.waitForTimeout(500);

                        let isStillVisible = true;
                        while (Date.now() - startTime < timeoutMs) {
                            let anyVisible = false;
                            const frames = [page, ...page.frames()];

                            for (const f of frames) {
                                try {
                                    const loc = f.locator(sel);
                                    const count = await loc.count().catch(() => 0);
                                    for (let idx = 0; idx < count; idx++) {
                                        if (await loc.nth(idx).isVisible().catch(() => false)) {
                                            anyVisible = true;
                                            break;
                                        }
                                    }

                                    if (!anyVisible && fallbackText) {
                                        const tLoc = f.getByText(fallbackText);
                                        const tCount = await tLoc.count().catch(() => 0);
                                        for (let idx = 0; idx < tCount; idx++) {
                                            if (await tLoc.nth(idx).isVisible().catch(() => false)) {
                                                anyVisible = true;
                                                break;
                                            }
                                        }
                                    }
                                } catch (_) {}
                                if (anyVisible) break;
                            }

                            if (!anyVisible) {
                                isStillVisible = false;
                                break;
                            }
                            await page.waitForTimeout(checkInterval);
                        }

                        if (isStillVisible) {
                            const elapsed = Date.now() - startTime;
                            const errMsg = `Wait for timed out after ${elapsed}ms: element/text "${searchValue}" is still present on the page without hiding.`;
                            console.error(`  [Wait for] ⛔ ${errMsg}`);
                            throw new Error(errMsg);
                        } else {
                            console.log(`  [Wait for] ✅ "${searchValue}" has disappeared (took ${Date.now() - startTime}ms).`);
                        }
                    } else {
                        let appeared = false;
                        while (Date.now() - startTime < timeoutMs) {
                            const frames = [page, ...page.frames()];
                            for (const f of frames) {
                                try {
                                    const loc = f.locator(sel);
                                    const count = await loc.count().catch(() => 0);
                                    for (let idx = 0; idx < count; idx++) {
                                        if (await loc.nth(idx).isVisible().catch(() => false)) {
                                            appeared = true;
                                            break;
                                        }
                                    }
                                    if (!appeared && fallbackText) {
                                        const tLoc = f.getByText(fallbackText);
                                        const tCount = await tLoc.count().catch(() => 0);
                                        for (let idx = 0; idx < tCount; idx++) {
                                            if (await tLoc.nth(idx).isVisible().catch(() => false)) {
                                                appeared = true;
                                                break;
                                            }
                                        }
                                    }
                                } catch (_) {}
                                if (appeared) break;
                            }

                            if (appeared) break;
                            await page.waitForTimeout(checkInterval);
                        }

                        if (!appeared) {
                            const elapsed = Date.now() - startTime;
                            const errMsg = `Wait for timed out after ${elapsed}ms: element/text "${searchValue}" did not appear.`;
                            console.error(`  [Wait for] ⛔ ${errMsg}`);
                            throw new Error(errMsg);
                        } else {
                            console.log(`  [Wait for] ✅ "${searchValue}" is now visible (took ${Date.now() - startTime}ms).`);
                        }
                    }
                }
            }

            // ── Clipboard Operation ───────────────────────────────────────────
            else if (step.type === 'Clipboard' || step.type === 'clipboard') {
                console.log(`  [Clipboard] 📋 Executing clipboard operation...`);
                let copiedText = returnedClipboardText || '';

                // Ensure hooks are active in all frames
                for (const f of [page, ...page.frames()]) {
                    await injectClipboardHook(f);
                }

                let targetFrame = null;
                let loc = null;
                const hasSearchVal = step.searchValue && step.searchValue.trim().length > 0;

                // 1. If we already captured clipboard text from a previous Click step AND step.searchValue is empty
                // OR matches the button already clicked, KEEP the captured text!
                if (copiedText && (!hasSearchVal || (step.searchValue && /copy/i.test(step.searchValue)))) {
                    console.log(`  [Clipboard] ✅ Using clipboard text already captured from previous step (${copiedText.length} chars)`);
                } else if (hasSearchVal) {
                    // 2. An explicit search selector was specified on this Clipboard step
                    const rawVal = step.searchValue.trim();
                    const candidateSelectors = getCandidateSelectors(normalizeSelector(rawVal, step.searchKind));

                    const searchDeadline = Date.now() + 15000;
                    while (Date.now() < searchDeadline && !loc) {
                        for (const f of [page, ...page.frames()]) {
                            for (const s of candidateSelectors) {
                                try {
                                    const l = f.locator(s).first();
                                    if ((await l.count().catch(() => 0)) > 0) {
                                        loc = l;
                                        targetFrame = f;
                                        break;
                                    }
                                } catch (_) {}
                            }
                            if (loc) break;
                        }
                        if (!loc) await page.waitForTimeout(400);
                    }

                    if (loc) {
                        await loc.scrollIntoViewIfNeeded().catch(() => {});

                        const btnLoc = loc.locator('xpath=./ancestor-or-self::button').first();
                        const hasButtonAncestor = (await btnLoc.count().catch(() => 0)) > 0;
                        const clickTarget = hasButtonAncestor ? btnLoc : loc;

                        const isButtonOrAction = await clickTarget.evaluate(el => {
                            const tag = (el.tagName || '').toUpperCase();
                            const role = (el.getAttribute('role') || '').toLowerCase();
                            const isBtn = tag === 'BUTTON' || role === 'button' || el.classList.contains('btn');
                            const text = (el.innerText || el.textContent || el.getAttribute('aria-label') || el.title || '').toLowerCase();
                            const hasCopyWord = text.includes('copy') || text.includes('copier') || text.includes('preview') || text.includes('html');
                            return isBtn || hasCopyWord || typeof el.onclick === 'function';
                        }).catch(() => false);

                        if (isButtonOrAction) {
                            console.log(`  [Clipboard] Target is a copy button/action. Waiting for it to become enabled...`);
                            const enableDeadline = Date.now() + 20000;
                            while (Date.now() < enableDeadline) {
                                const isDisabled = await clickTarget.evaluate(el => {
                                    return Boolean(el.disabled || el.hasAttribute('disabled') || el.getAttribute('aria-disabled') === 'true');
                                }).catch(() => false);
                                if (!isDisabled) {
                                    console.log(`  [Clipboard] Button is enabled and ready to click.`);
                                    break;
                                }
                                await page.waitForTimeout(400);
                            }

                            if (targetFrame) await injectClipboardHook(targetFrame);

                            console.log(`  [Clipboard] Clicking copy target button...`);
                            await clickTarget.click({ force: true }).catch(async () => {
                                const box = await clickTarget.boundingBox().catch(() => null);
                                if (box) await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
                            });
                            await page.waitForTimeout(600);

                            // Read copied text from target frame
                            if (targetFrame) {
                                const t = await targetFrame.evaluate(() => window.__lastCopiedText || '').catch(() => '');
                                if (t && t.trim()) copiedText = t.trim();
                            }
                        } else {
                            // Extract text content or input value directly
                            const elText = await loc.evaluate(el => {
                                if (el.value !== undefined && el.value !== null && el.value !== '') return el.value;
                                return el.innerText || el.textContent || '';
                            }).catch(() => '');
                            if (elText && elText.trim()) copiedText = elText.trim();
                        }
                    } else {
                        console.warn(`  [Clipboard] Target selector "${rawVal}" not found.`);
                    }
                }

                // 3. Check window.__lastCopiedText across all frames if not yet found
                if (!copiedText) {
                    for (const f of [page, ...page.frames()]) {
                        try {
                            const t = await f.evaluate(() => window.__lastCopiedText || '');
                            if (t && t.trim()) {
                                copiedText = t.trim();
                                break;
                            }
                        } catch (_) {}
                    }
                }

                // 4. Check navigator.clipboard.readText across frames
                if (!copiedText) {
                    for (const f of [page, ...page.frames()]) {
                        try {
                            const t = await f.evaluate(async () => {
                                if (navigator.clipboard && navigator.clipboard.readText) {
                                    return await navigator.clipboard.readText();
                                }
                                return '';
                            });
                            if (t && t.trim()) {
                                copiedText = t.trim();
                                break;
                            }
                        } catch (_) {}
                    }
                }

                // 5. Intelligent container fallback across all frames (check HTML output containers first, then preview)
                if (!copiedText) {
                    const candidateFrames = targetFrame ? [targetFrame, ...page.frames()] : [page, ...page.frames()];
                    for (const f of candidateFrames) {
                        try {
                            const found = await f.evaluate(() => {
                                const htmlEl = document.getElementById('outputHtmlCode') || document.getElementById('rawHtmlCode');
                                if (htmlEl) {
                                    const val = (htmlEl.textContent || htmlEl.innerText || '').trim();
                                    if (val && val.length > 5) return val;
                                }
                                const prevEl = document.getElementById('outputPreview') || document.getElementById('previewContainer') || document.querySelector('.preview-rendered');
                                if (prevEl) {
                                    const val = (prevEl.innerText || prevEl.textContent || '').trim();
                                    if (val && val.length > 5) return val;
                                }
                                return '';
                            });
                            if (found && found.trim()) {
                                copiedText = found.trim();
                                break;
                            }
                        } catch (_) {}
                    }
                }

                copiedText = (copiedText || '').trim();
                returnedClipboardText = copiedText;
                console.log(`  [Clipboard] ✅ Captured text (${copiedText.length} characters): "${copiedText.substring(0, 100)}${copiedText.length > 100 ? '...' : ''}"`);
                await page.waitForTimeout(400);
            }
            // ── Captcha / Bypass ───────────────────────────────────────────────
            // Detects, clicks reCAPTCHA checkbox across all frames/iframes,
            // triggers form submission and waits for redirect back to target.
            else if (step.type === 'Captcha') {
                const timeoutMs = parseInt(step.timeoutMs, 10) || 60000;
                const redirectUrl = step.redirectUrl || '';

                console.log(`  [Captcha] Running CAPTCHA bypass / click step (timeout: ${timeoutMs}ms, redirect: "${redirectUrl}")...`);
                const solved = await solveOrBypassGoogleCaptcha(page, timeoutMs, redirectUrl);
                if (solved) {
                    console.log(`  [Captcha] ✅ CAPTCHA verification step completed successfully.`);
                } else if (!page.url().includes('google.com/sorry') && !page.url().includes('recaptcha')) {
                    console.log(`  [Captcha] ✅ Page is on target URL: ${page.url()}`);
                } else {
                    console.warn(`  [Captcha] ⚠️ CAPTCHA was not resolved automatically.`);
                }
            }
        }

        await page.waitForTimeout(800);

        return {
            success: true,
            link: returnedClipboardText || '',
            url: returnedClipboardText || '',
            text: returnedClipboardText || '',
            clipboard: returnedClipboardText || '',
            imageUrl: finalDownloadedImageUrl || '',
            localImageUrl: localDownloadedImageUrl || '',
            r2UploadError
        };

    } catch (error) {
        console.error('[Worker] Automation error:', error.message);
        throw error;
    } finally {
        // ── 1. Safely close all pages and browser context to free RAM ──
        if (browserContext) {
            try {
                const pages = typeof browserContext.pages === 'function' ? browserContext.pages() : [];
                for (const p of pages) {
                    try { await p.close(); } catch (_) {}
                }
                if (browserContext._browser) {
                    await browserContext._browser.close().catch(() => {});
                } else {
                    await browserContext.close().catch(() => {});
                }
            } catch (_) {}
            browserContext = null;
        }

        // ── 2. Remove temporary profile directory with retries ──
        if (tempUserDataDir) {
            await removeDirWithRetry(tempUserDataDir, 5, 600);
        }

        // ── 3. Clean up any leftover profile directories from crashes ──
        cleanTempProfiles(tempUserDataDir);

        // ── 4. Clean old downloaded files ──
        cleanOldDownloads();

        // ── 5. Trigger V8 Garbage Collection to free RAM if available ──
        if (global.gc) {
            try { global.gc(); } catch (_) {}
        }
    }
}

// Initial cleanup on worker startup
cleanTempProfiles();
cleanOldDownloads();

// ═════════════════════════════════════════════════════════════════════════════
//  BULLMQ WORKER
// ═════════════════════════════════════════════════════════════════════════════

const worker = new Worker('automationQueue', async (job) => {
    console.log(`\n[Worker] ▶ Job ${job.id}  name=${job.name}`);
    await job.updateProgress(5);

    // Job payload differs slightly between /api/execute and /api/run-task
    //   execute  → { scenario, steps, hostUrl }
    //   run-task → { data,     steps, hostUrl }
    const scenarioConfig = job.data.scenario || job.data.data || {};
    const { steps, hostUrl } = job.data;

    if (!steps || steps.length === 0) throw new Error('No steps in job payload');

    await job.updateProgress(10);
    const result = await runAutomation(scenarioConfig, steps, hostUrl);
    await job.updateProgress(100);

    console.log(`[Worker] ✅ Job ${job.id} done  url=${result.imageUrl || '(none)'}`);
    return result;

}, {
    connection:  redisConnection,
    concurrency: CONCURRENCY
});

worker.on('completed', (job, result) => {
    console.log(`[Worker] Job ${job.id} completed`);
});

worker.on('failed', (job, err) => {
    console.error(`[Worker] Job ${job?.id} failed:`, err.message);
});

worker.on('error', err => {
    console.error('[Worker] Worker error:', err.message);
});

console.log(`\n⚙️  Playwright worker started  concurrency=${CONCURRENCY}`);
console.log(`    Waiting for jobs from Redis at ${process.env.REDIS_HOST || '127.0.0.1'}:${process.env.REDIS_PORT || 6379}\n`);
