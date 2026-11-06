const fs = require('fs');

const content = fs.readFileSync('server.js', 'utf8');

// Find the first occurrence of "const PORT"
const first = content.indexOf('const PORT');
if (first === -1) { console.error('Could not find PORT declaration'); process.exit(1); }

// Find the second occurrence
const second = content.indexOf('const PORT', first + 1);
if (second === -1) { console.log('No duplicate PORT found — file is already clean.'); process.exit(0); }

console.log(`Found duplicate PORT at index ${second}`);

// We want to keep everything up to (but not including) the duplicate block.
// The duplicate block starts with a separator comment or the PORT line itself.
// Walk backwards from 'second' to find the start of the separator line.
let cutPoint = second;
// Try to include the separator comment line before it
const separatorPattern = '// ─';
const prevSep = content.lastIndexOf(separatorPattern, second - 1);
if (prevSep !== -1 && second - prevSep < 200) {
    // Check there's only whitespace/newlines between prevSep and second
    const between = content.slice(prevSep, second);
    if (/^[\/\/ ─\r\n]+$/.test(between)) {
        cutPoint = prevSep;
    }
}

const fixed = content.slice(0, cutPoint).trimEnd() + '\n';

// Now append the single clean PORT + app.listen block
const portBlock = `
// ─────────────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    cleanTempProfiles();
    cleanOldDownloads();
    console.log(\`\\n🚀  Server running   → http://localhost:\${PORT}\`);
    console.log(\`📋  Queue dashboard  → http://localhost:\${PORT}/admin/queues\`);
    console.log(\`⚙️   Concurrency      = \${CONCURRENCY} parallel browsers\\n\`);
});
`;

fs.writeFileSync('server.js', fixed + portBlock, 'utf8');
console.log('✅ Duplicate removed. server.js fixed.');
