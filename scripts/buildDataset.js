const { runImport } = require("./etl/importPlayers");

runImport()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error("[buildDataset] Failed:", error.message);
    process.exit(1);
  });
