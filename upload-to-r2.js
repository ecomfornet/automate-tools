/**
 * upload-to-r2.js
 * Standalone script: finds the most recently downloaded image and uploads it to R2.
 * Usage: node upload-to-r2.js
 */
'use strict';

const fs = require('fs');
const path = require('path');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

// ─── Load credentials from scenarios.json ────────────────────────────────────
const scenariosPath = path.join(__dirname, 'scenarios.json');
let scenarios = [];
try {
    scenarios = JSON.parse(fs.readFileSync(scenariosPath, 'utf-8'));
} catch (e) {
    console.error('❌ Could not read scenarios.json:', e.message);
    process.exit(1);
}

// Use the scenario named "Nano" (or the first one with R2 credentials)
const scenario = scenarios.find(s => s.scenarioName === 'Nano') || scenarios.find(s => s.r2Endpoint && s.r2Bucket);
if (!scenario) {
    console.error('❌ No scenario with R2 credentials found in scenarios.json');
    process.exit(1);
}

const { r2Endpoint, r2Bucket, r2KeyId, r2Secret } = scenario;
console.log(`✅ Using scenario: "${scenario.scenarioName}"`);
console.log(`   Bucket: ${r2Bucket}`);
console.log(`   Endpoint: ${r2Endpoint}`);

if (!r2Endpoint || !r2Bucket || !r2KeyId || !r2Secret) {
    console.error('❌ Missing R2 credentials in scenario');
    process.exit(1);
}

// ─── Find most recently modified file in download* directories ───────────────
function findLatestFile() {
    const dirs = ['download', 'downloads', 'download '].map(d => path.join(__dirname, d));
    let latest = null;
    let latestTime = 0;

    for (const dir of dirs) {
        if (!fs.existsSync(dir)) continue;
        const files = fs.readdirSync(dir);
        for (const f of files) {
            const full = path.join(dir, f);
            const stat = fs.statSync(full);
            if (stat.isFile() && stat.mtimeMs > latestTime) {
                latestTime = stat.mtimeMs;
                latest = full;
            }
        }
    }
    return latest;
}

const filePath = findLatestFile();
if (!filePath) {
    console.error('❌ No downloaded files found in download/downloads directories');
    process.exit(1);
}

const fileName = path.basename(filePath);
const buffer = fs.readFileSync(filePath);
const ext = path.extname(fileName).slice(1);
const contentType = ext === 'jpg' || ext === 'jpeg' ? 'image/jpeg'
    : ext === 'png' ? 'image/png'
    : ext === 'gif' ? 'image/gif'
    : ext === 'webp' ? 'image/webp'
    : 'application/octet-stream';

console.log(`\n📁 File to upload: ${filePath}`);
console.log(`   Size: ${(buffer.length / 1024).toFixed(1)} KB`);
console.log(`   Content-Type: ${contentType}`);

// ─── Upload ───────────────────────────────────────────────────────────────────
async function upload() {
    const cleanEndpoint = r2Endpoint.trim().replace(/\/+$/, '');
    const r2Client = new S3Client({
        region: 'auto',
        endpoint: cleanEndpoint,
        credentials: {
            accessKeyId: r2KeyId.trim(),
            secretAccessKey: r2Secret.trim()
        },
        forcePathStyle: false
    });

    console.log(`\n⬆️  Uploading to Cloudflare R2...`);
    try {
        await r2Client.send(new PutObjectCommand({
            Bucket: r2Bucket.trim(),
            Key: fileName,
            Body: buffer,
            ContentType: contentType
        }));
        const publicDomain = scenario.r2PublicDomain ? scenario.r2PublicDomain.trim() : '';
        let publicUrl;
        if (publicDomain) {
            const cleanDomain = publicDomain.replace(/^https?:\/\//i, '').replace(/\/+$/, '');
            publicUrl = `https://${cleanDomain}/${fileName}`;
        } else {
            publicUrl = `${cleanEndpoint}/${r2Bucket.trim()}/${fileName}`;
        }
        console.log(`\n✅ Upload successful!`);
        console.log(`🔗 URL: ${publicUrl}`);
    } catch (err) {
        console.error(`\n❌ Upload failed: ${err.message}`);
        console.error(err);
        process.exit(1);
    }
}

upload();
