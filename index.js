const db = require('./modules/DbHelper');
const AccessParser = require('./modules/AccessParser');
const config = require('./config');

(async () => {
   try {
      // # Check and create tables

      const drzavniEvidenciCleanStart = config.drzavniEvidenciCleanStart;
      const strukturiNaPrihodiCleanStart = config.strukturiNaPrihodiCleanStart;

      await AccessParser.checkAndCreateTablesDrzavniEvidenci(drzavniEvidenciCleanStart);
      await AccessParser.checkAndCreateTablesStrukturaNaPrihodi(strukturiNaPrihodiCleanStart);

      // # Get all files
      const files = AccessParser.getAccessFiles();

      // # Process all files
      for await (const file of files) {
         console.log(`📂 Working on ${file}`);
         await AccessParser.processFileDrzavniEvidenci(file);
         await AccessParser.processFileStrukturaNaPrihodi(file);
      }

      await AccessParser.runMergeProcedures();

      console.log('✔️ Done processing all files.');
   } catch (err) {
      console.error('❌ Error:', err);
   } finally {
      await db.close();
   }
})();
