const http = require('http');

function post(path, body) {
    return new Promise((resolve, reject) => {
        const data = JSON.stringify(body);
        const req = http.request({
            hostname: '127.0.0.1',
            port: 3000,
            path: path,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(data)
            }
        }, res => {
            let buf = '';
            res.on('data', chunk => buf += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(buf)); } catch (e) { resolve(buf); }
            });
        });
        req.on('error', reject);
        req.write(data);
        req.end();
    });
}

function get(path) {
    return new Promise((resolve, reject) => {
        http.get('http://127.0.0.1:3000' + path, res => {
            let buf = '';
            res.on('data', chunk => buf += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(buf)); } catch (e) { resolve(buf); }
            });
        }).on('error', reject);
    });
}

(async () => {
    const scenarios = await get('/api/scenarios');
    const chatScenario = JSON.parse(JSON.stringify(scenarios.find(s => s.scenarioName === 'chat')));
    
    // Test with fresh isolated profile ChatClean
    chatScenario.chromeProfile = "ChatClean";
    console.log('Enqueuing scenario "chat" with clean profile ChatClean...');
    const runRes = await post('/api/run-task', chatScenario);
    console.log('Enqueue response:', runRes);

    const jobId = runRes.jobId;
    for (let i = 0; i < 40; i++) {
        await new Promise(r => setTimeout(r, 3000));
        const job = await get(`/api/job/${jobId}`);
        console.log(`[${i * 3}s] Job status:`, job.status);
        if (job.status === 'completed') {
            console.log('✅ Succeeded!');
            console.log('Result length:', (job.result?.text || job.result?.clipboard || '').length);
            break;
        } else if (job.status === 'failed') {
            console.error('❌ Failed:', job.error);
            break;
        }
    }
})();
