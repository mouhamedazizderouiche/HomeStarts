const path = require('path');
const { spawn } = require('child_process');

const backendDir = path.resolve(__dirname, '..', '..', 'backend');
const SCRIPTS = [
  { cmd: '../scripts/etl/enrichPlayerImagesTransfermarkt.js', name: 'tm-transfermarkt' },
  { cmd: '../scripts/etl/enrichPlayerImages.js', name: 'thesportsdb' },
  { cmd: '../scripts/etl/enrichByNameTmId.js', name: 'tm-by-name' }
];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function runScript(entry) {
  return new Promise((resolve) => {
    console.log(`[supervisor] Starting ${entry.name}`);
    const p = spawn('node', [entry.cmd], { cwd: backendDir, stdio: ['ignore', 'pipe', 'pipe'] });
    p.stdout.on('data', (d) => process.stdout.write(`[${entry.name}] ${d}`));
    p.stderr.on('data', (d) => process.stderr.write(`[${entry.name}] ${d}`));
    p.on('close', (code) => {
      console.log(`[supervisor] ${entry.name} exited with code ${code}`);
      resolve(code);
    });
  });
}

(async function main(){
  console.log('[supervisor] Continuous enricher started');
  while (true) {
    for (const s of SCRIPTS) {
      try {
        const code = await runScript(s);
        if (code !== 0) {
          console.log(`[supervisor] ${s.name} failed (code=${code}), retrying after 5s`);
          await sleep(5000);
          // retry once
          const r2 = await runScript(s);
          if (r2 !== 0) {
            console.log(`[supervisor] ${s.name} failed again (code=${r2}), continuing to next job`);
          }
        }
      } catch (err) {
        console.error(`[supervisor] Unexpected error running ${s.name}:`, err.message);
      }
      // short pause between jobs
      await sleep(2000);
    }
    console.log('[supervisor] Cycle complete — sleeping 30s before next cycle');
    await sleep(30000);
  }
})();
