const db = require('./modules/DbHelper');
const AccessParser = require('./modules/AccessParser');
const fs = require('fs');

const checkAndCreateTables = async () => {
   try {
      const tableNames = [
         'DrzavniEvidenci',
         'DrzavniEvidenci510',
         'DrzavniEvidenci540',
         'DrzavniEvidenci570',
         'DrzavniEvidenci600',
         'DrzavniEvidenciTemp',
         'DrzavniEvidenci510Temp',
         'DrzavniEvidenci540Temp',
         'DrzavniEvidenci570Temp',
         'DrzavniEvidenci600Temp'
      ];

      for await (const table of tableNames) {
         const existsQuery = `
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = @table
      `;
         const exists = await db.query(existsQuery, { table });

         if (exists.length === 0) {
            console.log(`⚠️ Table ${table} does not exist. Creating...`);

            let columns = `
          ID INT PRIMARY KEY,
          EMBS NVARCHAR(250),
          Godina INT,
          Tip NVARCHAR(10),
        `;

            // AOP601 to AOP724
            for (let i = 601; i <= 724; i++) {
               columns += `AOP${i} FLOAT NULL,\n`;
            }

            if (table === 'DrzavniEvidenci510' || table === 'DrzavniEvidenci510Temp') {
               columns += `Smetka NVARCHAR(10),\n`;
            }

            columns += `Created_At DATETIME DEFAULT GETDATE()`;

            const createQuery = `
              CREATE TABLE ${table} (
              ${columns}
              )
          `;

            await db.query(createQuery);
            console.log(`✅ Table ${table} created.`);
         } else {
            console.log(`✅ Table ${table} already exists.`);
         }
      }
      console.log('✔️ Done checking/creating all tables.');
   } catch (err) {
      throw new Error(err);
   }
};

(async () => {
   try {
      await checkAndCreateTables();
      const files = AccessParser.getAccessFiles();

      for await (const file of files) {
         console.log(`📂 Parsing file: ${file}`);
         const result = await AccessParser.processFile(file);

         fs.writeFileSync('test.json', JSON.stringify(result, null, 2));
      }
   } catch (err) {
      console.error('❌ Error:', err);
   } finally {
      await db.close();
   }
})();
