const { chromium } = require('playwright');
const { execSync } = require('child_process');

(async () => {
    const sourceDir = '/app/local_profiles/Default';
    const tempDir = `/app/local_profiles/test_rewrite_${Date.now()}`;
    execSync(`cp -r "${sourceDir}" "${tempDir}"`);
    console.log('Testing canvas rewrite...');

    try {
        const context = await chromium.launchPersistentContext(tempDir, {
            headless: true,
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
        });
        const page = context.pages()[0] || await context.newPage();
        await page.goto('https://gemini.google.com/share/1e783705b71d', { waitUntil: 'domcontentloaded', timeout: 30000 });
        await page.waitForTimeout(3000);

        // Click Continue if disclaimer is present
        const continueBtn = page.locator('button:has-text("Continue"), button:has-text("Continuer"), button.confirm-button').first();
        if (await continueBtn.isVisible({ timeout: 2000 })) {
            await continueBtn.click({ force: true });
            await page.waitForTimeout(3000);
        }

        // Find canvas frame
        const canvasFrame = page.frames().find(f => (f.url() || '').includes('scf.usercontent.goog') || (f.url() || '').startsWith('blob:'));
        if (!canvasFrame) {
            console.log('Canvas frame not found! Frames:', page.frames().map(f => f.url()));
            await context.close();
            return;
        }

        console.log('Found canvas frame:', canvasFrame.url());

        // Fill inputText
        await canvasFrame.locator('#inputText').first().fill('<h2>Test Article</h2><p>This is a test recipe for verification.</p>');
        console.log('Filled #inputText');

        // Check if promptInput exists and fill
        const promptEl = canvasFrame.locator('#promptInput').first();
        if (await promptEl.isVisible({ timeout: 1000 }).catch(() => false)) {
            await promptEl.fill('Rewrite concisely with clean HTML tags');
            console.log('Filled #promptInput');
        }

        // Click runRewriteBtn
        const rewriteBtn = canvasFrame.locator('#runRewriteBtn').first();
        await rewriteBtn.click({ force: true });
        console.log('Clicked #runRewriteBtn! Waiting 10s...');
        await page.waitForTimeout(10000);

        // Check canvas text / results
        const result = await canvasFrame.evaluate(() => {
            const out = document.querySelector('#outputContainer, #output, #outputText, [class*="output"]');
            const btns = Array.from(document.querySelectorAll('button')).map(b => ({
                id: b.id,
                text: b.innerText.trim(),
                disabled: b.disabled || b.hasAttribute('disabled')
            }));
            return {
                outputSnippet: out ? out.innerText.slice(0, 200) : null,
                buttons: btns
            };
        });

        console.log('Result:', JSON.stringify(result, null, 2));

        await context.close();
    } finally {
        execSync(`rm -rf "${tempDir}"`);
    }
})();
