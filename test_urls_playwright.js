const { chromium } = require('playwright');

const urls = [
    'https://gemini.google.com/share/1e783705b71d',
    'https://gemini.google.com/share/1e703705b71d',
    'https://gemini.google.com/share/98c2813aa5c7',
    'https://gemini.google.com/share/a245ca357b45'
];

(async () => {
    // Launch headless chromium with linux flags
    const browser = await chromium.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
    });

    for (const url of urls) {
        console.log(`\n========================================`);
        console.log(`Testing URL: ${url}`);
        const context = await browser.newContext({
            userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        });
        const page = await context.newPage();
        try {
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
            await page.waitForTimeout(5000);
            console.log('Final URL:', page.url());
            
            const info = await page.evaluate(() => {
                const modal = document.querySelector('mat-dialog-container, [role="dialog"], .cdk-overlay-container');
                const h1 = document.querySelector('h1');
                const buttons = Array.from(document.querySelectorAll('button')).map(b => b.innerText.trim()).filter(Boolean);
                return {
                    title: document.title,
                    h1Text: h1 ? h1.innerText : '',
                    modalText: modal ? modal.innerText.replace(/\s+/g, ' ') : null,
                    buttons: buttons.slice(0, 10),
                    frames: window.frames.length
                };
            });
            console.log('Title:', info.title);
            console.log('H1:', info.h1Text);
            console.log('Modal text:', info.modalText);
            console.log('Buttons:', info.buttons);
            console.log('Frames count:', info.frames);
            console.log('Page frames:', page.frames().map(f => f.url()));
        } catch (err) {
            console.error('Error loading:', err.message);
        } finally {
            await context.close();
        }
    }

    await browser.close();
})();
