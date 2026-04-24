const { runImport } = require("./importPlayers");
const { runSyncMatches } = require("./syncMatches");

const run = async () => {
  console.log("[updateWeekly] Starting weekly ETL update...");
  await runImport();
  console.log("[updateWeekly] Players refresh completed.");
  await runSyncMatches();
  console.log("[updateWeekly] Matches refresh completed.");
};

if (require.main === module) {
  run()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error("[updateWeekly] Failed:", error.message);
      process.exit(1);
    });
}

module.exports = {
  run
};
