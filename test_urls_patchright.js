const { chromium } = require('patchright');

const urls = [
    'https://gemini.google.com/share/1e783705b71d',
    'https://gemini.google.com/share/1e703705b71d',
    'https://gemini.google.com/share/98c2813aa5c7',
    'https://gemini.google.com/share/a245ca357b45'
];

(async () => {
    // Test with fresh context (incognito / clean)
    const browser = await chromium.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
    });

    for (const url of urls) {
        console.log(`\n=== Testing (clean profile): ${url} ===`);
        const page = await browser.newPage();
        try {
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
            await page.waitForTimeout(4000);
            console.log('Final URL:', page.url());
            
            // Check for dialog or error text
            const text = await page.evaluate(() => {
                const modal = document.querySelector('mat-dialog-container, [role="dialog"], .cdk-overlay-container');
                return {
                    title: document.title,
                    modalText: modal ? modal.innerText.replace(/\n+/g, ' ') : null,
                    bodySnippet: document.body.innerText.slice(0, 300).replace(/\n+/g, ' ')
                };
            });
            console.log('Title:', text.title);
            console.log('Modal:', text.modalText);
            console.log('Body:', text.bodySnippet);
        } catch (err) {
            console.error('Error loading:', err.message);
        } finally {
            await page.close();
        }
    }

    await browser.close();
})();
