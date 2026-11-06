const { chromium } = require('playwright');

(async () => {
    const browser = await chromium.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
    });
    const page = await browser.newPage();
    console.log('Navigating to https://gemini.google.com/share/1e783705b71d ...');
    await page.goto('https://gemini.google.com/share/1e783705b71d', { waitUntil: 'domcontentloaded', timeout: 30000 });
    
    // Wait for Continue button
    console.log('Waiting for Continue button...');
    const btn = page.locator('button.confirm-button, button:has-text("Continue"), button:has-text("Continuer")').first();
    await btn.waitFor({ state: 'visible', timeout: 15000 });
    console.log('Found Continue button:', await btn.innerText());
    await btn.click({ force: true });
    console.log('Clicked Continue!');

    // Wait for canvas iframe
    console.log('Waiting for canvas iframe...');
    let canvasFrame = null;
    for (let i = 0; i < 20; i++) {
        canvasFrame = page.frames().find(f => (f.url() || '').includes('scf.usercontent.goog') || (f.url() || '').startsWith('blob:'));
        if (canvasFrame) break;
        await page.waitForTimeout(500);
    }

    if (canvasFrame) {
        console.log('✅ Canvas frame attached:', canvasFrame.url());
        const hasInput = await canvasFrame.locator('#inputText').count();
        const hasBtn = await canvasFrame.locator('#runRewriteBtn').count();
        console.log('Elements inside canvas:', { hasInput, hasBtn });
    } else {
        console.log('❌ Canvas frame not found');
    }

    await browser.close();
})();
