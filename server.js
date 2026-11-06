'use strict';
// ─────────────────────────────────────────────────────────────────────────────
// server.js  —  Self-contained API + automation engine
// No Redis. No Docker. Pure Node.js in-memory queue.
// ─────────────────────────────────────────────────────────────────────────────
const express      = require('express');
const path         = require('path');
const fs           = require('fs');
const os           = require('os');
const { execSync } = require('child_process');
let chromium;
try {
    chromium = require('patchright').chromium;
} catch (_) {
    chromium = require('playwright').chromium;
}
const { S3Client, ListBucketsCommand, DeleteBucketCommand, CreateBucketCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const lockfile     = require('proper-lockfile');
const InMemoryQueue = require('./queue');

// ─── Queue (concurrency controlled, no external service) ─────────────────────
const CONCURRENCY = parseInt(process.env.CONCURRENCY, 10) || 5;
const jobQueue    = new InMemoryQueue({ concurrency: CONCURRENCY });
jobQueue.on('completed', (job) => console.log(`[Queue] ✅ Job ${job.id} completed`));
jobQueue.on('failed',    (job, err) => console.error(`[Queue] ❌ Job ${job.id} failed:`, err.message));

// ─── Express ──────────────────────────────────────────────────────────────────
const app = express();
app.set('trust proxy', true);
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use('/downloads', express.static(path.join(__dirname, 'downloads')));

const SCENARIOS_FILE = path.join(__dirname, 'scenarios.json');
function readScenarios() {
    if (!fs.existsSync(SCENARIOS_FILE)) return [];
    return JSON.parse(fs.readFileSync(SCENARIOS_FILE, 'utf8'));
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────
async function removeDirWithRetry(dirPath, retries = 5, delay = 600) {
    if (!dirPath || !fs.existsSync(dirPath)) return;
    for (let i = 0; i < retries; i++) {
        try {
            await new Promise(r => setTimeout(r, delay));
            fs.rmSync(dirPath, { recursive: true, force: true });
            console.log(`[Clean] Removed temp dir: ${path.basename(dirPath)}`);
            return;
        } catch (err) {
            if (i === retries - 1) {
                console.warn(`[Clean] Cleanup warning for ${path.basename(dirPath)}: ${err.message}`);
            }
        }
    }
}

const activeTempProfiles = new Set();

function cleanTempProfiles() {
    const profilesDir = path.join(__dirname, 'local_profiles');
    if (!fs.existsSync(profilesDir)) return;
    try {
        const entries = fs.readdirSync(profilesDir, { withFileTypes: true });
        const now = Date.now();
        let cleanedCount = 0;
        for (const entry of entries) {
            if (entry.isDirectory() && entry.name.includes('_run_')) {
                const fullPath = path.resolve(path.join(profilesDir, entry.name));
                // Never delete a profile currently used by an active worker!
                if (activeTempProfiles.has(fullPath)) continue;

                // Only clean orphaned directories older than 15 minutes (avoids race condition during creation)
                try {
                    const stat = fs.statSync(fullPath);
                    if (now - stat.mtimeMs > 15 * 60 * 1000) {
                        fs.rmSync(fullPath, { recursive: true, force: true });
                        cleanedCount++;
                    }
                } catch (_) {}
            }
        }
        if (cleanedCount > 0) {
            console.log(`[Clean] Freed disk space: removed ${cleanedCount} orphaned leftover profile directories`);
        }
        removeAllLocks(profilesDir);
    } catch (e) {
        console.warn('[Clean] Profile cleanup error:', e.message);
    }
}

function cleanOldDownloads(maxAgeMs = 3600000) {
    const downloadsDir = path.join(__dirname, 'downloads');
    if (!fs.existsSync(downloadsDir)) return;
    try {
        const now = Date.now();
        for (const file of fs.readdirSync(downloadsDir)) {
            const filePath = path.join(downloadsDir, file);
            try {
                const stat = fs.statSync(filePath);
                const isErrorFile = file.startsWith('error_');
                const cutoff = isErrorFile ? 600000 : maxAgeMs;
                if (now - stat.mtimeMs > cutoff) {
                    fs.unlinkSync(filePath);
                }
            } catch (_) {}
        }
    } catch (_) {}
}

const IGNORED_PROFILE_ENTRIES = new Set([
    'Cache', 'Code Cache', 'GPUCache', 'Service Worker', 
    'CacheStorage', 'Crashpad', 'Blob Storage', 'File System',
    'SingletonLock', 'SingletonCookie', 'SingletonSocket', 'LOCK',
    'GrShaderCache', 'ShaderCache', 'DawnWebGPUCache', 'DawnGraphiteCache',
    'Shared Dictionary', 'shared_proto_db', 'optimization_guide_hint_cache_store'
]);

function removeAllLocks(dir) {
    if (!fs.existsSync(dir)) return;
    try {
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
            const fullPath = path.join(dir, entry.name);
            if (entry.name.startsWith('Singleton') || entry.name === 'LOCK') {
                try { fs.rmSync(fullPath, { force: true, recursive: true }); } catch (_) {}
            } else if (entry.isDirectory()) {
                removeAllLocks(fullPath);
            }
        }
    } catch (_) {}
}

async function safeCopyProfile(src, dest) {
    if (!fs.existsSync(src)) {
        await fs.promises.mkdir(dest, { recursive: true });
        return;
    }
    // Remove stale lock files from source directory before cloning
    removeAllLocks(src);
    await fs.promises.mkdir(dest, { recursive: true });
    await fs.promises.cp(src, dest, {
        recursive: true,
        filter: (entryPath) => !IGNORED_PROFILE_ENTRIES.has(path.basename(entryPath))
    });
    // Ensure cloned destination is completely free of any Chromium lock files
    removeAllLocks(dest);
}

let _clipboardChain = Promise.resolve();
function acquireClipboardLock() {
    let releaseFn;
    const next = new Promise(r => { releaseFn = r; });
    const wait = _clipboardChain.then(() => releaseFn);
    _clipboardChain = _clipboardChain.then(() => next);
    return wait;
}

async function dismissCommonOverlays(page, timeoutPerBtn = 500) {
    const sels = [
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
        'immersive-share-disclaimer-dialog button',
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
    for (const sel of sels) {
        try {
            for (const frame of [page, ...page.frames()]) {
                const btn = frame.locator(sel).first();
                if (await btn.isVisible({ timeout: timeoutPerBtn }).catch(() => false)) {
                    const isExitOrRedirect = await btn.evaluate(el => {
                        const cls = (el.className || '').toLowerCase();
                        const txt = (el.innerText || '').toLowerCase();
                        return cls.includes('exit') || txt.includes('try gemini') || txt.includes('essayer gemini');
                    }).catch(() => false);
                    if (isExitOrRedirect) continue;

                    await btn.click({ force: true }).catch(() => {});
                    await page.waitForTimeout(500); break;
                }
            }
        } catch (_) {}
    }
}

async function handleSharePageIfPresent(page, targetShareUrl = '') {
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

    let confirmed = await tryClickConfirmButton();

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

        if (!confirmed) {
            confirmed = await tryClickConfirmButton();
        }

        await dismissErrorDialogs();
        await page.waitForTimeout(500);
    }
    console.warn('[SharePage] ⚠️ Canvas app iframe not attached within 30s. Current URL:', page.url());
    return false;
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
            if (url.includes('google.com/sorry') || url.includes('/sorry/index')) return true;
            // Only trigger if we are actually on a Google domain that redirected to a captcha block
            if (url.includes('google.') && (url.includes('sorry') || url.includes('recaptcha'))) return true;
            return false;
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
        // 1. Check if we have already redirected away from the CAPTCHA page
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
    const isGoogleCaptcha = url.includes('google.com/sorry') || url.includes('/sorry/index') || (url.includes('google.') && url.includes('sorry'));
    if (!isGoogleCaptcha) {
        return false;
    }
    const solved = await solveOrBypassGoogleCaptcha(page, 60000);
    if (!solved && (page.url().includes('google.com/sorry') || (page.url().includes('google.') && page.url().includes('sorry')))) {
        console.warn(`\n⚠️  [CAPTCHA] Automatic click pending / challenge active. Waiting up to 60s for verification...`);
        const deadline = Date.now() + 60000;
        while (Date.now() < deadline) {
            await page.waitForTimeout(2000);
            const curUrl = (page.url() || '').toLowerCase();
            if (!curUrl.includes('google.com/sorry') && !(curUrl.includes('google.') && curUrl.includes('sorry'))) {
                console.log(`✅ [CAPTCHA Solved] Verification completed! Resuming automation...`);
                await page.waitForTimeout(2000);
                return true;
            }
        }
        throw new Error(`CAPTCHA was not solved within timeout. Please reduce concurrency or check your browser.`);
    }
    return true;
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
    if (selector.includes('prompt-input') || selector.includes('prompt') || selector.includes('input') || selector.includes('rich-textarea')) {
        list.push('#prompt-input');
        list.push('rich-textarea [contenteditable="true"]');
        list.push('rich-textarea p');
        list.push('div[contenteditable="true"]');
        list.push('.ql-editor');
        list.push('textarea');
        list.push('[aria-label*="prompt" i]');
        list.push('[aria-label*="Enter a prompt" i]');
        list.push('[aria-label*="demander" i]');
        list.push('[aria-label*="ask" i]');
        list.push('[data-placeholder]');
    }

    // Common Gemini Send Button Fallbacks
    if (selector.includes('send-btn') || selector.includes('send') || selector.includes('envoyer')) {
        list.push('#send-btn');
        list.push('button[aria-label*="Send" i]');
        list.push('button[aria-label*="Envoyer" i]');
        list.push('button.send-button');
        list.push('mat-icon:has-text("send")');
        list.push('button:has(mat-icon:has-text("send"))');
        list.push('button[mattooltip*="Send" i]');
        list.push('.send-button-container button');
        list.push('div[aria-label*="Send" i]');
        list.push('button:has-text("Envoyer")');
        list.push('button:has-text("Send")');
        list.push('button[aria-label*="message" i]');
    }

    // Common Gemini Share / Canvas / Upload targets
    if (selector.includes('share-landing-page') || selector.includes('web-preview') || selector.includes('bard-sidenav') || selector.includes('upload')) {
        list.push('rich-textarea [contenteditable="true"]');
        list.push('rich-textarea p');
        list.push('div[contenteditable="true"]');
        list.push('.ql-editor');
        list.push('#prompt-input');
        list.push('rich-textarea');
        list.push('immersive-share-landing-page');
    }

    // Common Gemini Image Download Fallbacks
    if (selector.includes('chat-container') || selector.includes('/a') || selector.includes('download') || selector.includes('img') || selector.includes('image')) {
        list.push('#chat-container a');
        list.push('#chat-container img');
        list.push('div.message-content img');
        list.push('div[data-message-id] img');
        list.push('.message.ai img');
        list.push('canvas-view img');
        list.push('[data-test-id*="image"] img');
        list.push('mat-card img');
        list.push('img[src*="googleusercontent"]');
        list.push('img[src*="blob:"]');
        list.push('img.generated-image');
        list.push('a[download]');
        list.push('a:has-text("Download")');
        list.push('a:has-text("Télécharger")');
    }

    return [...new Set(list)];
}

async function findLocatorWithRetry(page, selector, timeoutMs = 30000) {
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

// ─────────────────────────────────────────────────────────────────────────────
// CORE AUTOMATION ENGINE
// ─────────────────────────────────────────────────────────────────────────────

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
            console.warn(`[Browser] Profile '${profileName || userDataDir}' is locked by another instance. Auto-recovering...`);
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

async function runAutomation(cfg, steps, hostUrl = 'http://localhost:3000') {
    let browserContext, tempUserDataDir = null,
        finalDownloadedImageUrl = null, localDownloadedImageUrl = null, r2UploadError = null, returnedLink = null, returnedClipboardText = null;
    try {
        if (cfg.forceCloseChrome && cfg.browserType !== 'Isolated') {
            try {
                if (process.platform === 'win32') {
                    execSync('taskkill /F /IM chrome.exe /T', { stdio: 'ignore' });
                } else {
                    execSync('pkill -f chrome || pkill -f chromium || true', { stdio: 'ignore' });
                }
                await new Promise(r => setTimeout(r, 1000));
            } catch (_) {}
        }
        const ctxOpts = cfg.userAgent ? { userAgent: cfg.userAgent } : {};

        const isLinux = process.platform === 'linux';
        const hasDisplay = Boolean(process.env.DISPLAY);
        const shouldBeHeadless = cfg.headless === true || (isLinux && !hasDisplay);
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

        if (cfg.chromeProfile && cfg.chromeProfile.trim()) {
            let userDataDir, launchOpts;
            const isIsolated = (cfg.browserType || 'Isolated') === 'Isolated';
            if (isIsolated) {
                const baseDir = path.join(__dirname, 'local_profiles', cfg.chromeProfile);
                const rid = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                tempUserDataDir = path.join(__dirname, 'local_profiles', `${cfg.chromeProfile}_run_${rid}`);
                activeTempProfiles.add(path.resolve(tempUserDataDir));
                console.log(`[Profile] Cloning ${cfg.chromeProfile} → ${path.basename(tempUserDataDir)}`);
                await safeCopyProfile(baseDir, tempUserDataDir);
                userDataDir = tempUserDataDir;
                const headlessArgs = shouldBeHeadless ? [
                    '--window-size=1280,720',
                    '--disable-features=VizDisplayCompositor',
                ] : [];
                launchOpts = {
                    headless: shouldBeHeadless,
                    args: [
                        '--disable-blink-features=AutomationControlled',
                        '--no-first-run',
                        '--no-default-browser-check',
                        '--disable-background-networking',
                        '--disable-sync',
                        '--password-store=basic',
                        ...linuxFlags,
                        ...headlessArgs
                    ],
                    ignoreDefaultArgs: ['--enable-automation'],
                    ignoreHTTPSErrors: true,
                    viewport: shouldBeHeadless ? { width: 1280, height: 720 } : null,
                    ...ctxOpts
                };
            } else {
                userDataDir = isLinux
                    ? path.join(os.homedir(), '.config', 'google-chrome')
                    : path.join(os.homedir(), 'AppData', 'Local', 'Google', 'Chrome', 'User Data');
                const headlessArgsSystem = shouldBeHeadless ? ['--window-size=1280,720'] : [];
                launchOpts = {
                    channel: isLinux ? undefined : 'chrome',
                    headless: shouldBeHeadless,
                    args: [`--profile-directory=${cfg.chromeProfile}`, '--disable-blink-features=AutomationControlled', '--password-store=basic', ...linuxFlags, ...headlessArgsSystem],
                    ignoreDefaultArgs: ['--enable-automation'],
                    ignoreHTTPSErrors: true,
                    viewport: shouldBeHeadless ? { width: 1280, height: 720 } : null,
                    ...ctxOpts
                };
            }
            browserContext = await safeLaunchPersistentContext(userDataDir, launchOpts, cfg.chromeProfile, cfg.forceCloseChrome);
            if (typeof browserContext.grantPermissions === 'function') {
                try { await browserContext.grantPermissions(['clipboard-read', 'clipboard-write']).catch(() => {}); } catch (_) {}
            }
        } else {
            const headlessArgsBare = shouldBeHeadless ? ['--window-size=1280,720'] : [];
            const browser = await chromium.launch({
                headless: shouldBeHeadless,
                args: ['--disable-blink-features=AutomationControlled', '--password-store=basic', ...linuxFlags, ...headlessArgsBare]
            });
            browserContext = await browser.newContext({
                ignoreHTTPSErrors: true,
                viewport: shouldBeHeadless ? { width: 1280, height: 720 } : null,
                ...ctxOpts
            });
            if (typeof browserContext.grantPermissions === 'function') {
                try { await browserContext.grantPermissions(['clipboard-read', 'clipboard-write']).catch(() => {}); } catch (_) {}
            }
            browserContext._browser = browser;
        }

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

        // In headless mode set explicit viewport so Gemini renders full layout
        if (cfg.headless) await page.setViewportSize({ width: 1280, height: 720 }).catch(() => {});
        await page.bringToFront();

        for (let i = 0; i < steps.length; i++) {
            const step = steps[i];
            console.log(`[Step ${i+1}/${steps.length}] ${step.type}`);

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
                if (navUrl.includes('/share/')) {
                    await handleSharePageIfPresent(page, navUrl);
                } else if (cfg.headless) {
                    await page.waitForTimeout(2500);
                    await dismissCommonOverlays(page, 2000);
                } else {
                    await page.waitForTimeout(1000);
                    await dismissCommonOverlays(page);
                }
            }
            else if (step.type === 'Click') {
                if (!step.searchValue) throw new Error('Click: missing searchValue');
                let sel = normalizeSelector(step.searchValue, step.searchKind);
                let loc;
                try {
                    loc = await findLocatorWithRetry(page, sel);
                } catch (findErr) {
                    if (sel.toLowerCase().includes('send') || sel.toLowerCase().includes('envoyer')) {
                        console.warn(`[Click] Send button "${sel}" not found, trying candidate send buttons...`);
                        const sendFallback = page.locator('button[aria-label*="Envoyer" i], button[aria-label*="Send" i], button.send-button, #send-btn, button:has(mat-icon:has-text("send")), button:has-text("Envoyer")').first();
                        await sendFallback.waitFor({ state: 'attached', timeout: 10000 }).catch(() => {});
                        if (await sendFallback.isVisible({ timeout: 5000 }).catch(() => false)) {
                            loc = sendFallback;
                        } else {
                            throw findErr;
                        }
                    } else {
                        throw findErr;
                    }
                }
                await loc.scrollIntoViewIfNeeded().catch(() => {});

                // Extract link from <a> tag if returnLink is enabled
                let extractedLink = null;
                if (step.returnLink === true) {
                    try {
                        extractedLink = await loc.evaluate(el => {
                            if (el.tagName === 'A' && el.href) return el.href;
                            const childA = el.querySelector('a[href]');
                            if (childA && childA.href) return childA.href;
                            const parentA = el.closest('a[href]');
                            if (parentA && parentA.href) return parentA.href;
                            return el.getAttribute('href') || null;
                        });
                        if (extractedLink) {
                            console.log(`[Click] 🔗 Extracted link from <a> tag: ${extractedLink}`);
                            returnedLink = extractedLink;
                        }
                    } catch (linkErr) {
                        console.warn(`[Click] Could not extract link from element: ${linkErr.message}`);
                    }
                }

                // If target button is currently disabled, wait for it to become enabled (up to 20s)
                const isBtnDisabled = await loc.evaluate(el => Boolean(el.disabled || el.hasAttribute('disabled') || el.getAttribute('aria-disabled') === 'true')).catch(() => false);
                if (isBtnDisabled) {
                    console.log(`[Click] Target "${sel}" is currently disabled. Waiting up to 20s for it to become enabled...`);
                    const enableDeadline = Date.now() + 20000;
                    while (Date.now() < enableDeadline) {
                        const stillDisabled = await loc.evaluate(el => Boolean(el.disabled || el.hasAttribute('disabled') || el.getAttribute('aria-disabled') === 'true')).catch(() => false);
                        if (!stillDisabled) {
                            console.log(`[Click] Target "${sel}" is now enabled.`);
                            break;
                        }
                        await page.waitForTimeout(300);
                    }
                }

                for (const f of [page, ...page.frames()]) {
                    await injectClipboardHook(f).catch(() => {});
                }

                if (step.clickType === 'Real Mouse Click') {
                    const box = await loc.boundingBox();
                    if (box) await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
                    else await loc.click({ force: true });
                } else {
                    await loc.click({ force: true });
                }

                // Check if this click triggered a copy action in any frame
                await page.waitForTimeout(500);
                for (const f of [page, ...page.frames()]) {
                    try {
                        const copied = await f.evaluate(() => window.__lastCopiedText || '');
                        if (copied && copied.trim()) {
                            returnedClipboardText = copied.trim();
                            console.log(`[Click] 📋 Captured clipboard text from click (${returnedClipboardText.length} chars): "${returnedClipboardText.substring(0, 100)}${returnedClipboardText.length > 100 ? '...' : ''}"`);
                            break;
                        }
                    } catch (_) {}
                }

                // If target selector or button text suggests HTML copy and hook didn't catch it, read directly from output container
                if (!returnedClipboardText && (/html/i.test(sel) || /copy/i.test(sel))) {
                    for (const f of [page, ...page.frames()]) {
                        try {
                            const htmlVal = await f.evaluate(() => {
                                const el = document.getElementById('outputHtmlCode') || document.getElementById('rawHtmlCode');
                                return el ? (el.textContent || el.innerText || '').trim() : '';
                            });
                            if (htmlVal && htmlVal.length > 5) {
                                returnedClipboardText = htmlVal;
                                console.log(`[Click] 📋 Captured HTML text from output container (${returnedClipboardText.length} chars)`);
                                break;
                            }
                        } catch (_) {}
                    }
                }
                if (sel.toLowerCase().includes('send') || sel.toLowerCase().includes('submit') || sel.toLowerCase().includes('envoyer') || sel.toLowerCase().includes('login')) {
                    await page.keyboard.press('Enter').catch(() => {});
                    await page.waitForTimeout(2000);
                    await handleCaptchaIfPresent(page);
                }

                // If returnLink is enabled and not yet extracted from clicked element, check resulting post link or redirect
                if (step.returnLink === true && !returnedLink) {
                    await page.waitForTimeout(1500);
                    try {
                        const postLink = await page.evaluate(() => {
                            const candidate = document.querySelector('a.view-post, a[href*="/blog/"], a[href*="/post/"], a[href*="/item/"], .success-message a, .alert a');
                            return candidate ? candidate.href : null;
                        });
                        if (postLink) {
                            console.log(`[Click] 🔗 Captured resulting post link: ${postLink}`);
                            returnedLink = postLink;
                        } else {
                            const copied = await page.evaluate(() => window.__lastCopiedText || '').catch(() => '');
                            if (copied) {
                                console.log(`[Click] 📋 Captured clipboard text from click: ${copied.substring(0, 80)}...`);
                                returnedClipboardText = copied;
                            }
                        }
                    } catch (_) {}
                }

                await page.waitForTimeout(500);
            }
            else if (step.type === 'Send Text') {
                if (!step.searchValue) throw new Error('Send Text: missing searchValue');
                let sel = normalizeSelector(step.searchValue, step.searchKind);
                let loc;
                try {
                    loc = await findLocatorWithRetry(page, sel);
                } catch (findErr) {
                    console.warn(`[Send Text] Target "${sel}" not found, trying Canvas/Gemini rich-textarea fallback...`);
                    const fallback = page.locator('rich-textarea p, rich-textarea [contenteditable="true"], div[contenteditable="true"], .ql-editor, textarea, #prompt-input').first();
                    await fallback.waitFor({ state: 'attached', timeout: 10000 }).catch(() => {});
                    if (await fallback.isVisible({ timeout: 5000 }).catch(() => false)) {
                        loc = fallback;
                    } else {
                        throw findErr;
                    }
                }
                await loc.focus().catch(() => {});
                await loc.click({ force: true }).catch(() => {});
                await page.waitForTimeout(100);

                try {
                    await loc.evaluate(el => {
                        if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {
                            el.value = '';
                        } else {
                            el.textContent = '';
                            el.innerHTML = '';
                        }
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    });
                } catch (_) {}

                if (step.simulateTyping) {
                    await loc.pressSequentially(step.textValue || '', { delay: 50 });
                } else {
                    try {
                        await page.keyboard.insertText(step.textValue || '');
                    } catch (_) {
                        await loc.fill(step.textValue || '').catch(async () => {
                            await loc.evaluate((el, val) => {
                                el.textContent = val;
                                el.dispatchEvent(new Event('input', { bubbles: true }));
                            }, step.textValue || '');
                        });
                    }
                }

                try {
                    await loc.evaluate(el => {
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    });
                } catch (_) {}
            }
            else if (step.type === 'HTML-Text' || step.type === 'html-text' || step.type === 'HTML Text') {
                if (!step.searchValue) throw new Error('HTML-Text: missing searchValue');
                let sel = normalizeSelector(step.searchValue, step.searchKind);
                let loc;
                try {
                    loc = await findLocatorWithRetry(page, sel);
                } catch (findErr) {
                    console.warn(`[HTML-Text] Target "${sel}" not found, trying rich-text / contenteditable fallback...`);
                    const fallback = page.locator('.note-editable, [contenteditable="true"], .ql-editor, textarea[name*="content" i], textarea[name*="description" i], textarea').first();
                    await fallback.waitFor({ state: 'attached', timeout: 8000 }).catch(() => {});
                    if (await fallback.isVisible({ timeout: 4000 }).catch(() => false)) {
                        loc = fallback;
                    } else {
                        throw findErr;
                    }
                }

                const rawHtml = step.htmlValue || step.textValue || '';

                await loc.focus().catch(() => {});
                await loc.click({ force: true }).catch(() => {});
                await page.waitForTimeout(150);

                // Multi-layer HTML injection into rich text / WYSIWYG editor
                await loc.evaluate((el, html) => {
                    el.focus();

                    // 1. Browser-native document.execCommand('insertHTML')
                    let inserted = false;
                    try {
                        const sel = window.getSelection();
                        const range = document.createRange();
                        range.selectNodeContents(el);
                        sel.removeAllRanges();
                        sel.addRange(range);
                        inserted = document.execCommand('insertHTML', false, html);
                    } catch (_) {}

                    // 2. Synthetic Clipboard paste event with text/html MIME type
                    if (!inserted) {
                        try {
                            const dt = new DataTransfer();
                            dt.setData('text/html', html);
                            dt.setData('text/plain', el.innerText || '');
                            const pasteEvent = new ClipboardEvent('paste', {
                                bubbles: true,
                                cancelable: true,
                                clipboardData: dt
                            });
                            el.dispatchEvent(pasteEvent);
                        } catch (_) {}
                    }

                    // 3. Direct DOM .innerHTML update & Summernote / Trumbowyg / TinyMCE API sync
                    if (el.isContentEditable || el.getAttribute('contenteditable') === 'true') {
                        el.innerHTML = html;
                    } else if (el.tagName === 'TEXTAREA' || el.tagName === 'INPUT') {
                        el.value = html;
                    } else {
                        const childEditable = el.querySelector('[contenteditable="true"], .note-editable, .ql-editor');
                        if (childEditable) {
                            childEditable.innerHTML = html;
                        } else {
                            el.innerHTML = html;
                        }
                    }

                    // 4. Editor framework API hooks (Summernote, Trumbowyg, TinyMCE, Quill)
                    try {
                        if (window.$) {
                            if (window.$(el).summernote) window.$(el).summernote('code', html);
                            if (window.$('.note-editable').length && window.$('.note-editable').summernote) {
                                window.$('.note-editable').summernote('code', html);
                            }
                            if (window.$(el).trumbowyg) window.$(el).trumbowyg('html', html);
                        }
                        if (window.tinymce && window.tinymce.activeEditor) {
                            window.tinymce.activeEditor.setContent(html);
                        }
                    } catch (_) {}

                    // 5. Synchronize underlying form textareas (crucial for form submit / publish!)
                    const form = el.closest('form') || document.querySelector('form');
                    if (form) {
                        form.querySelectorAll('textarea').forEach(ta => {
                            ta.value = html;
                            ta.dispatchEvent(new Event('input', { bubbles: true }));
                            ta.dispatchEvent(new Event('change', { bubbles: true }));
                        });
                    }

                    // 6. Dispatch standard DOM change events
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    el.dispatchEvent(new Event('blur', { bubbles: true }));
                }, rawHtml);

                await page.waitForTimeout(300);
            }
            else if (step.type === 'Upload Image') {
                let sel = normalizeSelector(step.searchValue, step.searchKind);
                const rawUrls = Array.isArray(step.imageUrl)
                    ? step.imageUrl
                    : (step.imageUrl || '').split(/[\n,]+/).map(s => s.trim()).filter(Boolean);

                if (rawUrls.length === 0) {
                    console.warn('[Upload Image] No image URLs provided to upload, skipping.');
                }

                for (let imgIdx = 0; imgIdx < rawUrls.length; imgIdx++) {
                    const currentImageUrl = rawUrls[imgIdx];
                    console.log(`[Upload Image] Processing image ${imgIdx + 1}/${rawUrls.length}: ${currentImageUrl}`);

                    // 1. Direct file input upload if an input[type="file"] or #file-input is available
                    let fileInputFound = false;
                    try {
                        const imgRes = await fetch(currentImageUrl);
                        if (imgRes.ok) {
                            const imgBuf = Buffer.from(await imgRes.arrayBuffer());
                            const imgMime = imgRes.headers.get('content-type') || 'image/jpeg';
                            const imgExt = (imgMime.split('/')[1] || 'jpg').replace('jpeg', 'jpg');
                            const tmpPath = path.join(os.tmpdir(), `upload_${Date.now()}_${Math.random().toString(36).slice(2, 9)}_${imgIdx}.${imgExt}`);
                            fs.writeFileSync(tmpPath, imgBuf);

                            const frames = [page, ...page.frames()];
                            for (const f of frames) {
                                const fi = f.locator('input[type="file"], #file-input, input[accept*="image"]').first();
                                if (await fi.count() > 0) {
                                    await fi.setInputFiles(tmpPath);
                                    console.log(`[Upload Image] Attached via input[type="file"] ✅ (${imgIdx + 1}/${rawUrls.length})`);
                                    await page.waitForTimeout(2000);
                                    fileInputFound = true;
                                    break;
                                }
                            }
                            try { fs.unlinkSync(tmpPath); } catch (_) {}
                        }
                    } catch (e) {
                        console.warn(`[Upload Image] File input attempt error: ${e.message}`);
                    }

                    if (fileInputFound) continue;

                    // 2. Universal CDP-based Clipboard injection with concurrency lock
                    let clipOk = false;
                    let cdpSession = null;
                    const releaseClipboard = await acquireClipboardLock();
                    try {
                        cdpSession = await page.context().newCDPSession(page);
                        const imgRes = await fetch(currentImageUrl);
                        if (!imgRes.ok) throw new Error(`Cannot fetch image: HTTP ${imgRes.status}`);
                        const imgBuf  = Buffer.from(await imgRes.arrayBuffer());
                        const imgMime = imgRes.headers.get('content-type') || 'image/jpeg';
                        const b64Data = `data:${imgMime};base64,${imgBuf.toString('base64')}`;

                        let targetFocused = false;
                        if (sel) {
                            try {
                                const targetLoc = await findLocatorWithRetry(page, sel, 2500);
                                await targetLoc.focus().catch(() => {});
                                await targetLoc.click({ force: true }).catch(() => {});
                                await page.waitForTimeout(200);
                                targetFocused = true;
                            } catch (_) {}
                        }
                        if (!targetFocused) {
                            try {
                                const promptLoc = page.locator('rich-textarea p, rich-textarea [contenteditable="true"], div[contenteditable="true"], .ql-editor, textarea, #prompt-input').first();
                                if (await promptLoc.isVisible({ timeout: 1500 }).catch(() => false)) {
                                    await promptLoc.focus().catch(() => {});
                                    await promptLoc.click({ force: true }).catch(() => {});
                                    await page.waitForTimeout(200);
                                }
                            } catch (_) {}
                        }

                        const cdpResult = await cdpSession.send('Runtime.evaluate', {
                            expression: `(async () => {
                                try {
                                    const res  = await fetch('${b64Data}');
                                    const blob = await res.blob();
                                    const img  = new Image();
                                    img.src    = URL.createObjectURL(blob);
                                    await new Promise((r, j) => { img.onload = r; img.onerror = j; });
                                    const canvas = document.createElement('canvas');
                                    canvas.width  = img.naturalWidth  || 500;
                                    canvas.height = img.naturalHeight || 500;
                                    canvas.getContext('2d').drawImage(img, 0, 0);
                                    const pngBlob = await new Promise(r => canvas.toBlob(r, 'image/png'));
                                    await navigator.clipboard.write([new ClipboardItem({ 'image/png': pngBlob })]);
                                    return { ok: true };
                                } catch(e) { return { ok: false, err: e.message }; }
                            })()`,
                            awaitPromise: true,
                            userGesture: true,
                            returnByValue: true
                        });
                        const val = cdpResult?.result?.value;
                        if (val?.ok) {
                            clipOk = true;
                            console.log(`[Upload Image] Clipboard written via CDP ✅ (${imgIdx + 1}/${rawUrls.length})`);
                        }

                        if (clipOk) {
                            await page.keyboard.press('Control+V');
                            await page.waitForTimeout(2000);
                            console.log(`[Upload Image] Ctrl+V sent (${imgIdx + 1}/${rawUrls.length})`);
                        }
                    } catch (cdpErr) {
                        console.warn('[Upload Image] CDP error:', cdpErr.message);
                    } finally {
                        if (cdpSession) {
                            await cdpSession.detach().catch(() => {});
                        }
                        releaseClipboard();
                    }
                }
            }

            else if (step.type === 'Download Image') {
                let sel = normalizeSelector(step.searchValue, step.searchKind);

                console.log(`[Download] Waiting for Gemini image generation to complete...`);
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
                            console.log(`[Download] Gemini is still generating image... (${elapsed}s elapsed)`);
                        }
                        await page.waitForTimeout(1000);
                    } else {
                        // If we haven't seen it generating yet, give it up to 6 seconds for UI to show the processing indicator
                        if (!hasSeenGenerating && (Date.now() - genWaitStart < 6000)) {
                            await page.waitForTimeout(1000);
                            continue;
                        }

                        // Check if the AI message with image is present and ready
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
                            console.log(`[Download] Generated AI image verified in chat-container!`);
                            break;
                        }

                        // Double check after 2 seconds to make sure it's really finished and not a UI transition flicker
                        await page.waitForTimeout(2000);
                        const stillGen = await isCurrentlyGenerating();
                        if (!stillGen) {
                            console.log(`[Download] Generation indicator cleared. Proceeding to locate generated image.`);
                            break;
                        }
                    }
                }

                console.log(`[Download] Locating target image element...`);

                // Check for generated image in all frames (content iframes first, then outer page)
                let loc = null;
                const allFrames = [...page.frames().filter(f => f !== page), page];

                // 1. Try user selector first across all frames (iframes first!)
                if (sel) {
                    for (const f of allFrames) {
                        try {
                            const found = f.locator(sel).last();
                            if (await found.count() > 0) {
                                loc = found;
                                console.log(`[Download] Found user selector "${sel}" in frame: ${f.url().slice(0, 60)}...`);
                                break;
                            }
                        } catch (_) {}
                    }
                }

                // 2. Candidate selectors if user selector didn't match or wasn't provided
                if (!loc) {
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
                                    loc = found;
                                    console.log(`[Download] Found generated image with selector "${imgSel}" in frame: ${f.url().slice(0, 60)}...`);
                                    break;
                                }
                            } catch (_) {}
                        }
                        if (loc) break;
                    }
                }

                if (!loc && sel) {
                    loc = await findLocatorWithRetry(page, sel, 15000).catch(() => null);
                }

                if (!loc) {
                    throw new Error('Download Image: Could not locate generated image element');
                }

                await page.waitForTimeout(1000);

                let buffer = null;
                let ext = 'png';

                // Tier 1: Try in-page extraction (safe from tainted canvas errors & handles blob URLs)
                let extractInfo = null;
                try {
                    extractInfo = await loc.evaluate(async (el) => {
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
                    console.warn(`[Download] In-page evaluation warning: ${e.message}`);
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
                        console.log(`[Download] Fetching image via Playwright context: ${extractInfo.url.slice(0, 100)}...`);
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
                        console.warn(`[Download] Playwright context fetch failed: ${e.message}`);
                    }
                }

                // Tier 3: Node native fetch
                if (!buffer && extractInfo?.url && !extractInfo.url.startsWith('blob:') && !extractInfo.url.includes('.svg')) {
                    try {
                        console.log(`[Download] Fetching image via Node fetch: ${extractInfo.url.slice(0, 100)}...`);
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
                        console.warn(`[Download] Node fetch failed: ${e.message}`);
                    }
                }

                // Tier 4: Element screenshot fallback (foolproof for any visible image or canvas)
                if (!buffer) {
                    console.log('[Download] Using Playwright element screenshot fallback...');
                    try {
                        const imgCount = await loc.locator('img').count();
                        const targetLoc = imgCount > 0 ? loc.locator('img').first() : loc;
                        buffer = await targetLoc.screenshot({ type: 'png', timeout: 15000 });
                        ext = 'png';
                    } catch (e) {
                        console.warn(`[Download] Element screenshot fallback: ${e.message}`);
                        buffer = await loc.screenshot({ type: 'png', timeout: 15000 });
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
                    console.warn(`[Download] Extracted file appears to be a UI icon (${ext}, ${buffer.length} bytes). Retrying extraction with screenshot fallback...`);
                    try {
                        const imgLoc = loc.locator('img').first();
                        if (await imgLoc.count() > 0) {
                            buffer = await imgLoc.screenshot({ type: 'png', timeout: 10000 });
                        } else {
                            buffer = await loc.screenshot({ type: 'png', timeout: 10000 });
                        }
                        ext = 'png';
                    } catch (_) {}
                }

                const saveDir = path.isAbsolute(step.saveDir || '') ? step.saveDir : path.join(__dirname, step.saveDir || 'downloads');
                if (!fs.existsSync(saveDir)) fs.mkdirSync(saveDir, { recursive: true });

                const fileName = `download_${Date.now()}_${Math.random().toString(36).slice(2, 7)}.${ext}`;
                const localFilePath = path.join(saveDir, fileName);
                fs.writeFileSync(localFilePath, buffer);
                localDownloadedImageUrl = `${hostUrl.replace(/\/+$/, '')}/downloads/${fileName}`;
                finalDownloadedImageUrl = localDownloadedImageUrl;
                console.log(`[Download] Saved locally → ${localDownloadedImageUrl} (${buffer.length} bytes)`);

                if (cfg.r2Endpoint && cfg.r2Bucket && cfg.r2KeyId && cfg.r2Secret) {
                    try {
                        const ep = cfg.r2Endpoint.trim().replace(/\/+$/, '');
                        const r2 = new S3Client({ region: 'auto', endpoint: ep, credentials: { accessKeyId: cfg.r2KeyId.trim(), secretAccessKey: cfg.r2Secret.trim() }, forcePathStyle: false });
                        const contentType = ext === 'jpg' ? 'image/jpeg' : (ext === 'svg' ? 'image/svg+xml' : `image/${ext}`);
                        await r2.send(new PutObjectCommand({ Bucket: cfg.r2Bucket.trim(), Key: fileName, Body: buffer, ContentType: contentType }));
                        let pd = cfg.r2PublicDomain ? cfg.r2PublicDomain.trim().replace(/^https?:\/\//i, '').replace(/\s+/g, '-').replace(/\/+$/, '') : null;
                        if (!pd && cfg.cfApiToken) {
                            const accountId = extractAccountId(cfg.r2Endpoint);
                            if (accountId) {
                                try {
                                    const devRes = await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets/${cfg.r2Bucket.trim()}/domains/managed`, {
                                        headers: { 'Authorization': `Bearer ${cfg.cfApiToken.trim()}` }
                                    });
                                    const devData = await devRes.json();
                                    if (devData.result?.domain && devData.result?.enabled) {
                                        pd = devData.result.domain;
                                        console.log(`[R2] Auto-resolved public domain: ${pd}`);
                                    } else if (devData.result?.domain && !devData.result?.enabled) {
                                        // Auto-enable public domain if disabled
                                        try {
                                            const pubRes = await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets/${cfg.r2Bucket.trim()}/domains/managed`, {
                                                method: 'PUT',
                                                headers: { 'Authorization': `Bearer ${cfg.cfApiToken.trim()}`, 'Content-Type': 'application/json' },
                                                body: JSON.stringify({ enabled: true })
                                            });
                                            const pubData = await pubRes.json();
                                            if (pubData.success && pubData.result?.domain) {
                                                pd = pubData.result.domain;
                                                console.log(`[R2] Auto-enabled and resolved public domain: ${pd}`);
                                            }
                                        } catch (_) {}
                                    }
                                } catch (_) {}
                            }
                        }
                        finalDownloadedImageUrl = pd ? `https://${pd}/${fileName}` : `${ep}/${cfg.r2Bucket.trim()}/${fileName}`;
                        console.log(`[R2] ✅ ${finalDownloadedImageUrl}`);
                    } catch (e) { r2UploadError = e.message; console.error('[R2] ❌', e.message); }
                }
            }
            else if (step.type === 'Scroll') {
                const dir = step.direction || 'Bottom';
                if (step.containerValue) {
                    try { const l = await findLocatorWithRetry(page, normalizeSelector(step.containerValue), 5000); await l.evaluate((n, d) => { n.scrollTop = d === 'Bottom' ? n.scrollHeight : 0; }, dir); } catch (_) {}
                } else { await page.evaluate(d => window.scrollTo(0, d === 'Bottom' ? document.body.scrollHeight : 0), dir); }
                await page.waitForTimeout(200);
            }
            else if (step.type === 'Sleep') { await page.waitForTimeout(parseInt(step.duration, 10) || 1000); }

            // ── Error Check ───────────────────────────────────────────────────
            // Looks for an element on the page. If it IS visible → abort with the
            // configured error message so the row is marked FAILED. If absent → continue.
            else if (step.type === 'Error Check') {
                if (!step.searchValue) throw new Error('Error Check: selector (searchValue) is required');
                let sel = normalizeSelector(step.searchValue, step.searchKind);
                const timeoutMs = parseInt(step.timeoutMs, 10) || 3000;
                const errMsg    = step.errorMessage || 'Error element detected on page';

                let found = false;
                try {
                    // Try all frames; resolve as soon as ANY frame sees the element
                    const frames = [page, ...page.frames()];
                    await Promise.any(frames.map(async (f) => {
                        const loc = f.locator(sel).first();
                        await loc.waitFor({ state: 'visible', timeout: timeoutMs });
                        found = true;
                    }));
                } catch (_) {
                    // AggregateError / timeout → element not found → all good
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
            link: returnedLink || '',
            url: returnedLink || (returnedClipboardText ? returnedClipboardText : ''),
            text: returnedClipboardText || '',
            clipboard: returnedClipboardText || '',
            imageUrl: finalDownloadedImageUrl || '',
            localImageUrl: localDownloadedImageUrl || '',
            r2UploadError
        };

    } catch (error) {
        console.error('[Automation] Error:', error.message);
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
            activeTempProfiles.delete(path.resolve(tempUserDataDir));
            await removeDirWithRetry(tempUserDataDir, 5, 600);
        }

        // ── 4. Clean old downloaded files ──
        cleanOldDownloads();

        // ── 5. Trigger V8 Garbage Collection to free RAM if available ──
        if (global.gc) {
            try { global.gc(); } catch (_) {}
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// QUEUE ENDPOINTS
// ─────────────────────────────────────────────────────────────────────────────

function waitForJobCompletion(jobId, timeoutMs = 180000) {
    return new Promise((resolve, reject) => {
        const check = jobQueue.getJob(jobId);
        if (check && check.status === 'completed') return resolve(check.result);
        if (check && check.status === 'failed') return reject(new Error(check.error));

        let timer;
        const onCompleted = (job, result) => {
            if (job.id === jobId) {
                cleanup();
                resolve(result);
            }
        };
        const onFailed = (job, err) => {
            if (job.id === jobId) {
                cleanup();
                reject(err instanceof Error ? err : new Error(err || 'Job failed'));
            }
        };
        const cleanup = () => {
            clearTimeout(timer);
            jobQueue.removeListener('completed', onCompleted);
            jobQueue.removeListener('failed', onFailed);
        };

        jobQueue.on('completed', onCompleted);
        jobQueue.on('failed', onFailed);

        timer = setTimeout(() => {
            cleanup();
            reject(new Error(`Timeout waiting for job ${jobId} after ${timeoutMs}ms`));
        }, timeoutMs);
    });
}

function extractImagesAndText(req) {
    const query  = req.query || {};
    const body   = req.body  || {};
    const merged = { ...query, ...body };

    const texte = (merged.texte || merged.prompt || merged.text || '').toString().trim();
    const images = [];

    // 1. Check 'images' array or comma/newline separated string
    if (merged.images) {
        if (Array.isArray(merged.images)) {
            merged.images.forEach(img => {
                if (typeof img === 'string' && img.trim() && !images.includes(img.trim())) {
                    images.push(img.trim());
                }
            });
        } else if (typeof merged.images === 'string') {
            merged.images.split(/[\n,]+/).forEach(img => {
                const trimmed = img.trim();
                if (trimmed && !images.includes(trimmed)) images.push(trimmed);
            });
        }
    }

    // 2. Check 'image' field (single URL or comma-separated URLs)
    if (merged.image && typeof merged.image === 'string') {
        merged.image.split(/[\n,]+/).forEach(img => {
            const trimmed = img.trim();
            if (trimmed && !images.includes(trimmed)) images.push(trimmed);
        });
    }

    // 3. Check numbered fields: image1, image2, image3, img1, img2, image_1, image_2, etc.
    const numberedKeys = Object.keys(merged)
        .filter(k => /^(image|img)[_-]?\d+$/i.test(k))
        .sort((a, b) => {
            const numA = parseInt(a.replace(/\D/g, ''), 10) || 0;
            const numB = parseInt(b.replace(/\D/g, ''), 10) || 0;
            return numA - numB;
        });

    for (const key of numberedKeys) {
        const val = merged[key];
        if (typeof val === 'string' && val.trim()) {
            const trimmed = val.trim();
            if (!images.includes(trimmed)) images.push(trimmed);
        }
    }

    // 4. Check multiple / numbered text fields: texte1, texte2, text1, text2, etc.
    const textValues = [];
    if (texte) textValues.push(texte);

    const numberedTextKeys = Object.keys(merged)
        .filter(k => /^(texte|text|prompt)[_-]?\d+$/i.test(k))
        .sort((a, b) => {
            const numA = parseInt(a.replace(/\D/g, ''), 10) || 0;
            const numB = parseInt(b.replace(/\D/g, ''), 10) || 0;
            return numA - numB;
        });

    for (const key of numberedTextKeys) {
        const val = merged[key];
        if (typeof val === 'string' && val.trim()) {
            const trimmed = val.trim();
            if (!textValues.includes(trimmed)) textValues.push(trimmed);
        }
    }

    // 5. Check HTML fields: html, html_content, content, raw_html, html1, html2, etc.
    const rawHtml = (merged.html || merged.html_content || merged.content || merged.raw_html || '').toString().trim();
    const htmlValues = [];
    if (rawHtml) htmlValues.push(rawHtml);

    const numberedHtmlKeys = Object.keys(merged)
        .filter(k => /^(html|html_content|content)[_-]?\d+$/i.test(k))
        .sort((a, b) => {
            const numA = parseInt(a.replace(/\D/g, ''), 10) || 0;
            const numB = parseInt(b.replace(/\D/g, ''), 10) || 0;
            return numA - numB;
        });

    for (const key of numberedHtmlKeys) {
        const val = merged[key];
        if (typeof val === 'string' && val.trim() && !htmlValues.includes(val.trim())) {
            htmlValues.push(val.trim());
        }
    }

    return { texte, images, textValues, rawHtml, htmlValues, merged };
}

// Execute a saved scenario by name
app.all('/api/execute/:scenarioName', async (req, res) => {
    const name = req.params.scenarioName;
    const { texte, images, textValues, rawHtml, htmlValues, merged } = extractImagesAndText(req);

    const scenario = readScenarios().find(s => s.scenarioName === name);
    if (!scenario) return res.status(404).json({ success: false, error: `Scenario "${name}" not found` });

    const steps = JSON.parse(JSON.stringify(scenario.steps || []));
    const sendTextSteps = steps.filter(s => s.type === 'Send Text');
    const htmlTextSteps = steps.filter(s => s.type === 'HTML-Text' || s.type === 'html-text' || s.type === 'HTML Text');
    const uploadImageSteps = steps.filter(s => s.type === 'Upload Image');

    // Determine what this scenario actually requires from dynamic parameters
    const hasExplicitSystemText = sendTextSteps.some(s => s.isUsedBySystem === true);
    const hasTemplateText = steps.some(s => {
        return (typeof s.textValue === 'string' && /\{\{texte\d*\}\}/i.test(s.textValue)) ||
               (typeof s.url === 'string' && /\{\{texte\d*\}\}/i.test(s.url));
    });
    // Legacy fallback: if exactly 1 Send Text step exists and isUsedBySystem is undefined, it's considered system
    const isLegacySingleSendText = sendTextSteps.length === 1 && sendTextSteps[0].isUsedBySystem === undefined;
    const scenarioNeedsText = hasExplicitSystemText || hasTemplateText || isLegacySingleSendText;

    // Check if scenario requires dynamic HTML
    const hasExplicitSystemHtml = htmlTextSteps.some(s => s.isUsedBySystem !== false);
    const hasTemplateHtml = steps.some(s => {
        return (typeof s.htmlValue === 'string' && /\{\{(html|texte)\d*\}\}/i.test(s.htmlValue));
    });
    const scenarioNeedsHtml = (htmlTextSteps.length > 0 && hasExplicitSystemHtml) || hasTemplateHtml;

    // Check if scenario requires dynamic image
    const hasExplicitSystemImage = uploadImageSteps.some(s => s.isUsedBySystem !== false);
    const hasTemplateImage = steps.some(s => {
        return (typeof s.imageUrl === 'string' && /\{\{image\d*\}\}/i.test(s.imageUrl)) ||
               (typeof s.url === 'string' && /\{\{image\d*\}\}/i.test(s.url)) ||
               (typeof s.textValue === 'string' && /\{\{image\d*\}\}/i.test(s.textValue));
    });
    const scenarioNeedsImage = (uploadImageSteps.length > 0 && hasExplicitSystemImage) || hasTemplateImage;

    const missing = [];
    if (scenarioNeedsText && !texte && textValues.length === 0) {
        missing.push('texte');
    }
    if (scenarioNeedsHtml && !rawHtml && htmlValues.length === 0 && !texte && textValues.length === 0) {
        missing.push('html (or content, texte)');
    }
    if (scenarioNeedsImage && images.length === 0) {
        missing.push('image (or image1, image2, images)');
    }

    if (missing.length > 0) {
        return res.status(400).json({
            success: false,
            error: 'error data',
            message: `Missing required parameter(s): ${missing.join(', ')}`
        });
    }

    const primaryImage = images[0] || '';
    let uploadStepIndex = 0;
    let systemTextIndex = 0;
    let systemHtmlIndex = 0;

    for (const step of steps) {
        // --- SEND TEXT STEP HANDLING ---
        if (step.type === 'Send Text') {
            let isSystem = false;
            if (step.isUsedBySystem === true) {
                isSystem = true;
            } else if (step.isUsedBySystem === false) {
                isSystem = false; // preserve scenario inserted data
            } else {
                // legacy fallback: only treat as system if single Send Text step
                isSystem = (sendTextSteps.length === 1);
            }

            if (isSystem) {
                let dynamicVal = null;
                // Check custom paramKey first (e.g. step.paramKey = "title")
                if (step.paramKey && merged[step.paramKey] !== undefined) {
                    dynamicVal = (merged[step.paramKey] || '').toString();
                } else if (textValues[systemTextIndex] !== undefined) {
                    dynamicVal = textValues[systemTextIndex];
                } else if (texte) {
                    dynamicVal = texte;
                }

                if (dynamicVal !== null) {
                    step.textValue = dynamicVal;
                }
                systemTextIndex++;
            }
        }

        // --- HTML-TEXT STEP HANDLING ---
        if (step.type === 'HTML-Text' || step.type === 'html-text' || step.type === 'HTML Text') {
            const isSystem = step.isUsedBySystem !== false;
            if (isSystem) {
                let dynamicVal = null;
                // Check custom paramKey first (e.g. step.paramKey = "content" or "body")
                if (step.paramKey && merged[step.paramKey] !== undefined) {
                    dynamicVal = (merged[step.paramKey] || '').toString();
                } else if (htmlValues[systemHtmlIndex] !== undefined) {
                    dynamicVal = htmlValues[systemHtmlIndex];
                } else if (rawHtml) {
                    dynamicVal = rawHtml;
                } else if (texte) {
                    dynamicVal = texte;
                }

                if (dynamicVal !== null) {
                    step.htmlValue = dynamicVal;
                    step.textValue = dynamicVal;
                }
                systemHtmlIndex++;
            }
        }

        // --- UPLOAD IMAGE STEP HANDLING ---
        if (step.type === 'Upload Image') {
            const isSystem = step.isUsedBySystem !== false;
            let replaced = false;

            // Support explicit template variable {{imageX}}
            if (step.imageUrl && typeof step.imageUrl === 'string') {
                if (/\{\{image\d*\}\}/i.test(step.imageUrl)) {
                    step.imageUrl = step.imageUrl.replace(/\{\{image\}\}/gi, primaryImage);
                    images.forEach((img, idx) => {
                        step.imageUrl = step.imageUrl.replace(new RegExp(`\\{\\{image${idx + 1}\\}\\}`, 'gi'), img);
                    });
                    replaced = true;
                }
            }

            if (!replaced && isSystem && images.length > 0) {
                if (uploadImageSteps.length > 1) {
                    // Scenario has multiple Upload Image steps -> map sequentially: step 1 -> image1, step 2 -> image2, etc.
                    // Only override if a new image URL was supplied, otherwise preserve default reference image
                    step.imageUrl = (images[uploadStepIndex] !== undefined && images[uploadStepIndex] !== '')
                        ? images[uploadStepIndex]
                        : step.imageUrl;
                } else {
                    // Scenario has single Upload Image step -> if multiple images provided, assign all joined by comma
                    step.imageUrl = images.length > 1 ? images.join(',') : (primaryImage || step.imageUrl);
                }
            }
            if (isSystem) {
                uploadStepIndex++;
            }
        }

        // --- GENERIC TEMPLATE PLACEHOLDER REPLACEMENTS ---
        if (step.url && typeof step.url === 'string') {
            step.url = step.url.replace(/\{\{texte\}\}/gi, texte).replace(/\{\{image\}\}/gi, primaryImage);
            images.forEach((img, idx) => {
                step.url = step.url.replace(new RegExp(`\\{\\{image${idx + 1}\\}\\}`, 'gi'), img);
            });
            textValues.forEach((txt, idx) => {
                step.url = step.url.replace(new RegExp(`\\{\\{texte${idx + 1}\\}\\}`, 'gi'), txt);
            });
            Object.keys(merged).forEach(k => {
                if (typeof merged[k] === 'string' || typeof merged[k] === 'number') {
                    step.url = step.url.replace(new RegExp(`\\{\\{${k}\\}\\}`, 'gi'), merged[k].toString());
                }
            });
        }
        if (step.textValue && typeof step.textValue === 'string') {
            step.textValue = step.textValue.replace(/\{\{image\}\}/gi, primaryImage);
            images.forEach((img, idx) => {
                step.textValue = step.textValue.replace(new RegExp(`\\{\\{image${idx + 1}\\}\\}`, 'gi'), img);
            });
            textValues.forEach((txt, idx) => {
                step.textValue = step.textValue.replace(new RegExp(`\\{\\{texte${idx + 1}\\}\\}`, 'gi'), txt);
            });
            Object.keys(merged).forEach(k => {
                if (typeof merged[k] === 'string' || typeof merged[k] === 'number') {
                    step.textValue = step.textValue.replace(new RegExp(`\\{\\{${k}\\}\\}`, 'gi'), merged[k].toString());
                }
            });
        }
        if (step.htmlValue && typeof step.htmlValue === 'string') {
            step.htmlValue = step.htmlValue.replace(/\{\{html\}\}/gi, rawHtml || texte).replace(/\{\{texte\}\}/gi, texte);
            htmlValues.forEach((h, idx) => {
                step.htmlValue = step.htmlValue.replace(new RegExp(`\\{\\{html${idx + 1}\\}\\}`, 'gi'), h);
            });
            Object.keys(merged).forEach(k => {
                if (typeof merged[k] === 'string' || typeof merged[k] === 'number') {
                    step.htmlValue = step.htmlValue.replace(new RegExp(`\\{\\{${k}\\}\\}`, 'gi'), merged[k].toString());
                }
            });
        }
    }

    // Update concurrency dynamically if requested or configured in scenario
    const requestedConcurrency = req.query.concurrency || (req.body && req.body.concurrency) || scenario.concurrency;
    if (requestedConcurrency) {
        jobQueue.setConcurrency(requestedConcurrency);
    }

    const hostUrl = `${req.protocol}://${req.get('host')}`;
    const jobId = jobQueue.add('execute', { scenario, steps, hostUrl }, async (data, updateProgress) => {
        updateProgress(10);
        const result = await runAutomation(data.scenario, data.steps, data.hostUrl);
        updateProgress(100);
        return result;
    });

    console.log(`[Queue] Enqueued job ${jobId}  scenario="${name}" (workers=${jobQueue.concurrency})`);

    // Check if client wants synchronous execution or direct image URL / redirect
    const isSync     = req.query.sync === 'true' || req.query.wait === 'true' || (req.body && (req.body.sync || req.body.wait));
    const isRedirect = req.query.redirect === 'true' || req.query.format === 'redirect';
    const isRawUrl   = req.query.raw === 'true' || req.query.format === 'raw' || req.query.format === 'url';

    if (isSync || isRedirect || isRawUrl) {
        try {
            const result = await waitForJobCompletion(jobId);
            const url = result?.imageUrl || '';
            const link = result?.link || '';
            const text = result?.text || result?.clipboard || '';
            const localUrl = result?.localImageUrl || '';
            const targetUrl = link || url || text;
            if (isRedirect && targetUrl && (targetUrl.startsWith('http://') || targetUrl.startsWith('https://'))) return res.redirect(targetUrl);
            if (isRawUrl) return res.type('text/plain').send(text || targetUrl);
            return res.json({ success: true, jobId, link, url: targetUrl, text, clipboard: text, imageUrl: url, localImageUrl: localUrl, result });
        } catch (err) {
            return res.status(500).json({ success: false, jobId, error: err.message });
        }
    }

    res.status(202).json({ success: true, jobId, message: 'Poll GET /api/job/' + jobId + ' for result' });
});

// Manual task from the dashboard
app.post('/api/run-task', async (req, res) => {
    const data  = req.body;
    const steps = data.steps;
    if (!steps || !steps.length) return res.status(400).json({ success: false, error: 'No steps provided.' });

    const requestedConcurrency = data.concurrency;
    if (requestedConcurrency) {
        jobQueue.setConcurrency(requestedConcurrency);
    }

    const hostUrl = `${req.protocol}://${req.get('host')}`;
    const jobId = jobQueue.add('run-task', { data, steps, hostUrl }, async (payload, updateProgress) => {
        updateProgress(10);
        const result = await runAutomation(payload.data, payload.steps, payload.hostUrl);
        updateProgress(100);
        return result;
    });

    console.log(`[Queue] Enqueued manual job ${jobId} (workers=${jobQueue.concurrency})`);
    res.status(202).json({ success: true, jobId, message: 'Poll GET /api/job/' + jobId + ' for result' });
});

// Job status polling
app.get('/api/job/:id', (req, res) => {
    const job = jobQueue.getJob(req.params.id);
    if (!job) return res.status(404).json({ success: false, error: 'Job not found' });
    res.json({ id: job.id, status: job.status, progress: job.progress, result: job.result, error: job.error });
});

// Queue dashboard (lightweight, no external UI dependency)
app.get('/admin/queues', (req, res) => {
    const stats = jobQueue.getStats();
    const jobs  = jobQueue.getJobs(30);
    res.json({ stats, jobs });
});

// ─────────────────────────────────────────────────────────────────────────────
// SYSTEM / PROFILE / SCENARIO / R2 ENDPOINTS (unchanged)
// ─────────────────────────────────────────────────────────────────────────────

app.post('/api/shutdown', (req, res) => { res.json({ success: true }); setTimeout(() => process.exit(0), 200); });

app.get('/api/scenarios',  (req, res) => res.json(readScenarios()));
app.post('/api/scenarios', (req, res) => {
    const s = req.body, all = readScenarios(), i = all.findIndex(x => x.scenarioName === s.scenarioName);
    if (i >= 0) all[i] = s; else all.push(s);
    fs.writeFileSync(SCENARIOS_FILE, JSON.stringify(all, null, 2));
    res.json({ success: true });
});
app.delete('/api/scenarios/:name', (req, res) => {
    const all = readScenarios().filter(s => s.scenarioName !== req.params.name);
    fs.writeFileSync(SCENARIOS_FILE, JSON.stringify(all, null, 2));
    res.json({ success: true });
});

app.get('/vnc', (req, res) => {
    const rawHost = req.headers['x-forwarded-host'] || req.headers.host || req.hostname || '';
    const host = rawHost.split(':')[0];
    if (host.includes('.instabox.tech') && host.startsWith('3000-')) {
        const vncHost = host.replace(/^3000-/, '6080-');
        return res.redirect(`https://${vncHost}/vnc.html?autoconnect=true&resize=remote`);
    }
    res.redirect(`http://${host}:6080/vnc.html?autoconnect=true&resize=remote`);
});

app.post('/api/open-profile', async (req, res) => {
    const { browserType, chromeProfile, forceCloseChrome } = req.body;
    if (!chromeProfile) return res.status(400).json({ success: false, error: 'Profile name required.' });
    try {
        if (forceCloseChrome && browserType !== 'Isolated') {
            try {
                if (process.platform === 'win32') {
                    execSync('taskkill /F /IM chrome.exe /T', { stdio: 'ignore' });
                } else {
                    execSync('pkill -f chrome || pkill -f chromium || true', { stdio: 'ignore' });
                }
                await new Promise(r => setTimeout(r, 1000));
            } catch (_) {}
        }
        const isLinux = process.platform === 'linux';
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

        let userDataDir, launchOptions;
        if (browserType === 'Isolated') {
            userDataDir = path.join(__dirname, 'local_profiles', chromeProfile);
            if (!fs.existsSync(path.join(__dirname, 'local_profiles'))) fs.mkdirSync(path.join(__dirname, 'local_profiles'));
            launchOptions = {
                headless: false,
                args: ['--disable-blink-features=AutomationControlled', '--password-store=basic', ...linuxFlags],
                ignoreDefaultArgs: ['--enable-automation'],
                ignoreHTTPSErrors: true,
                viewport: null
            };
        } else {
            userDataDir   = isLinux ? path.join(os.homedir(), '.config', 'google-chrome') : path.join(os.homedir(), 'AppData', 'Local', 'Google', 'Chrome', 'User Data');
            launchOptions = {
                channel: isLinux ? undefined : 'chrome',
                headless: false,
                args: [`--profile-directory=${chromeProfile}`, '--disable-blink-features=AutomationControlled', '--password-store=basic', ...linuxFlags],
                ignoreDefaultArgs: ['--enable-automation'],
                ignoreHTTPSErrors: true,
                viewport: null
            };
        }
        const ctx = await safeLaunchPersistentContext(userDataDir, launchOptions, chromeProfile, forceCloseChrome);
        let p = ctx.pages()[0];
        if (!p) p = await ctx.newPage();
        await p.goto('https://www.google.com').catch(() => {});
        const rawHost = req.headers['x-forwarded-host'] || req.headers.host || req.hostname || '';
        const host = rawHost.split(':')[0];
        let vncUrl = `http://${host}:6080/vnc.html?autoconnect=true&resize=remote`;
        if (host.includes('.instabox.tech') && host.startsWith('3000-')) {
            const vncHost = host.replace(/^3000-/, '6080-');
            vncUrl = `https://${vncHost}/vnc.html?autoconnect=true&resize=remote`;
        }
        res.json({ success: true, message: 'Browser opened successfully', vncUrl });
    } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

// R2 helpers
function extractAccountId(url) { const m = (url||'').match(/https:\/\/([a-z0-9]{32})\.r2\.cloudflarestorage\.com/i); return m ? m[1] : null; }
function cfHeaders(token) { return { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }; }

app.post('/api/r2/buckets', async (req, res) => {
    const { endpoint, cfApiToken, accessKey, secretKey } = req.body;
    const accountId = extractAccountId(endpoint);
    if (cfApiToken && accountId) {
        try { const r = await (await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets`, { headers: cfHeaders(cfApiToken) })).json(); if (r.success) return res.json({ success: true, buckets: r.result.buckets.map(b => b.name) }); } catch (_) {}
    }
    if (accessKey && secretKey && endpoint) {
        try { const s3 = new S3Client({ region: 'auto', endpoint, credentials: { accessKeyId: accessKey, secretAccessKey: secretKey } }); const d = await s3.send(new ListBucketsCommand({})); return res.json({ success: true, buckets: (d.Buckets||[]).map(b=>b.Name).filter(Boolean) }); } catch (e) { return res.json({ success: false, error: e.message }); }
    }
    res.json({ success: false, error: 'Missing credentials.' });
});

app.post('/api/r2/bucket-domain', async (req, res) => {
    const { endpoint, cfApiToken, bucket } = req.body;
    const accountId = extractAccountId(endpoint);
    if (!accountId || !cfApiToken || !bucket) {
        return res.json({ success: false, error: 'Missing account, token, or bucket.' });
    }
    const cleanBucket = bucket.trim().toLowerCase();
    try {
        const r = await (await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets/${cleanBucket}/domains/managed`, {
            headers: cfHeaders(cfApiToken)
        })).json();

        if (r.success && r.result?.domain) {
            let domain = r.result.domain;
            // If managed domain exists but is not yet enabled, automatically enable it!
            if (!r.result.enabled) {
                try {
                    const pubRes = await (await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets/${cleanBucket}/domains/managed`, {
                        method: 'PUT',
                        headers: { ...cfHeaders(cfApiToken), 'Content-Type': 'application/json' },
                        body: JSON.stringify({ enabled: true })
                    })).json();
                    if (pubRes.success && pubRes.result?.domain) {
                        domain = pubRes.result.domain;
                    }
                } catch (_) {}
            }
            return res.json({ success: true, domain });
        }

        const c = await (await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets/${cleanBucket}/domains/custom`, {
            headers: cfHeaders(cfApiToken)
        })).json();
        if (c.success && c.result?.domains?.[0]?.domain) {
            return res.json({ success: true, domain: c.result.domains[0].domain });
        }
        res.json({ success: false, error: 'No public domain configured for this bucket.' });
    } catch (e) {
        res.json({ success: false, error: e.message });
    }
});

app.post('/api/r2/create-bucket', async (req, res) => {
    const { endpoint, cfApiToken, bucketName, accessKey, secretKey } = req.body;
    if (!bucketName || !bucketName.trim()) {
        return res.json({ success: false, error: 'Bucket name is required.' });
    }
    const cleanName = bucketName.trim().toLowerCase();
    const accountId = extractAccountId(endpoint);

    // 1. If Cloudflare API Token & Account ID are provided, use Cloudflare API
    if (accountId && cfApiToken) {
        try {
            const createRes = await (await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets`, {
                method: 'POST',
                headers: cfHeaders(cfApiToken),
                body: JSON.stringify({ name: cleanName })
            })).json();

            if (!createRes.success) {
                return res.json({
                    success: false,
                    error: createRes.errors?.[0]?.message || 'Failed to create Cloudflare R2 bucket'
                });
            }

            // 2. Automatically enable managed public domain (pub-xxx.r2.dev)
            let domain = null;
            try {
                const pubRes = await (await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets/${cleanName}/domains/managed`, {
                    method: 'PUT',
                    headers: { ...cfHeaders(cfApiToken), 'Content-Type': 'application/json' },
                    body: JSON.stringify({ enabled: true })
                })).json();
                if (pubRes.success && pubRes.result?.domain) {
                    domain = pubRes.result.domain;
                }
            } catch (_) {}

            return res.json({
                success: true,
                message: `Bucket "${cleanName}" created successfully!${domain ? `\nPublic access enabled: ${domain}` : ''}`,
                bucket: cleanName,
                domain: domain
            });
        } catch (e) {
            return res.json({ success: false, error: e.message });
        }
    }

    // 2. Fallback: S3 CreateBucketCommand
    if (accessKey && secretKey && endpoint) {
        try {
            const ep = endpoint.trim().replace(/\/+$/, '');
            const s3 = new S3Client({ region: 'auto', endpoint: ep, credentials: { accessKeyId: accessKey, secretAccessKey: secretKey } });
            await s3.send(new CreateBucketCommand({ Bucket: cleanName }));
            return res.json({
                success: true,
                message: `Bucket "${cleanName}" created successfully via S3 API!`,
                bucket: cleanName
            });
        } catch (e) {
            return res.json({ success: false, error: e.message });
        }
    }

    res.json({ success: false, error: 'Cloudflare API Token or S3 credentials required.' });
});

app.post('/api/r2/delete-bucket', async (req, res) => {
    const { endpoint, cfApiToken, bucketName, accessKey, secretKey } = req.body;
    const accountId = extractAccountId(endpoint);
    if (cfApiToken && accountId) {
        try { const r = await (await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets/${bucketName}`, { method: 'DELETE', headers: cfHeaders(cfApiToken) })).json(); if (r.success) return res.json({ success: true }); } catch (_) {}
    }
    if (accessKey && secretKey && endpoint) {
        try { const s3 = new S3Client({ region: 'auto', endpoint, credentials: { accessKeyId: accessKey, secretAccessKey: secretKey } }); await s3.send(new DeleteBucketCommand({ Bucket: bucketName })); return res.json({ success: true }); } catch (e) { return res.json({ success: false, error: e.message }); }
    }
    res.json({ success: false, error: 'Missing credentials.' });
});

app.post('/api/r2/test', async (req, res) => {
    const { endpoint, bucket, accessKey, secretKey } = req.body;
    if (!endpoint || !bucket) return res.json({ success: false, error: 'Endpoint and bucket required.' });
    try { const s3 = new S3Client({ region: 'auto', endpoint, credentials: { accessKeyId: accessKey||'x', secretAccessKey: secretKey||'x' } }); const { ListObjectsV2Command } = require('@aws-sdk/client-s3'); await s3.send(new ListObjectsV2Command({ Bucket: bucket, MaxKeys: 1 })); res.json({ success: true, message: '⚡ Connection successful!' }); } catch (e) { res.json({ success: false, error: e.message }); }
});

app.post('/api/r2/upload-test', async (req, res) => {
    const { endpoint, bucket, accessKey, secretKey, r2PublicDomain, cfApiToken } = req.body;
    if (!endpoint || !bucket || !accessKey || !secretKey) return res.json({ success: false, error: 'All fields required.' });
    const cleanBucket = bucket.trim().toLowerCase();
    try {
        const s3 = new S3Client({ region: 'auto', endpoint, credentials: { accessKeyId: accessKey, secretAccessKey: secretKey } });
        const key = `test_${Date.now()}.txt`;
        await s3.send(new PutObjectCommand({ Bucket: cleanBucket, Key: key, Body: `Test upload verified at ${new Date().toISOString()}`, ContentType: 'text/plain' }));

        // Determine public URL
        let pd = r2PublicDomain ? r2PublicDomain.trim().replace(/^https?:\/\//i, '').replace(/\s+/g, '-').replace(/\/+$/, '') : null;
        if (!pd && cfApiToken) {
            const accountId = extractAccountId(endpoint);
            if (accountId) {
                try {
                    const devRes = await (await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets/${cleanBucket}/domains/managed`, {
                        headers: cfHeaders(cfApiToken)
                    })).json();
                    if (devRes.result?.domain) pd = devRes.result.domain;
                } catch (_) {}
            }
        }

        const publicUrl = pd ? `https://${pd}/${key}` : null;
        return res.json({
            success: true,
            message: `📤 "${key}" uploaded successfully!${publicUrl ? `\n🌐 Public URL: ${publicUrl}` : ''}`,
            key,
            publicUrl,
            domain: pd
        });
    } catch (e) {
        res.json({ success: false, error: e.message });
    }
});

app.get('/api/concurrency', (req, res) => {
    res.json({ success: true, concurrency: jobQueue.concurrency, running: jobQueue.running });
});

app.post('/api/concurrency', (req, res) => {
    const { concurrency } = req.body;
    if (concurrency) {
        jobQueue.setConcurrency(concurrency);
        console.log(`[Queue] Concurrency updated to ${jobQueue.concurrency}`);
        return res.json({ success: true, concurrency: jobQueue.concurrency });
    }
    res.status(400).json({ success: false, error: 'Concurrency value missing' });
});

// ─────────────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    cleanTempProfiles();
    cleanOldDownloads();
    setInterval(cleanTempProfiles, 15 * 60 * 1000).unref();
    setInterval(cleanOldDownloads, 15 * 60 * 1000).unref();
    console.log(`\n🚀  Server running   → http://localhost:${PORT}`);
    console.log(`📋  Queue dashboard  → http://localhost:${PORT}/admin/queues`);
    console.log(`⚙️   Concurrency      = ${jobQueue.concurrency} parallel browsers\n`);
});
