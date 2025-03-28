const db = require('./modules/DbHelper');
const AccessParser = require('./modules/AccessParser');
const config = require('./config');

// # Method for checking and creating tables
// @param {boolean} shouldDelete
// @default false
// @returns {Promise<void>}
const checkAndCreateTables = async (shouldDelete = false) => {
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

         if (exists.length > 0 && shouldDelete) {
            console.log(`🗑️ Dropping existing table: ${table}...`);
            await db.query(`DROP TABLE ${table}`);
         }

         if (exists.length === 0 || shouldDelete) {
            console.log(`⚠️ Table ${table} does not exist or was dropped. Creating...`);

            let columns = `
               ID INT IDENTITY(1,1) PRIMARY KEY,
               EMBS NVARCHAR(250),
               Godina INT,
               Tip NVARCHAR(10),
               ${table.includes('Temp') ? 'DocTypeID NVARCHAR(10),' : ''}
            `;

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

            // 📌 Create indexes after table creation
            let indexCols = ['EMBS', 'Tip', 'Godina'];
            if (table.includes('510')) {
               indexCols.push('Smetka');
            }

            const indexName = `idx_${table}_mergekeys`;
            const indexQuery = `
               CREATE NONCLUSTERED INDEX ${indexName}
               ON ${table} (${indexCols.join(', ')})
            `;

            await db.query(indexQuery);
            console.log(`📈 Index ${indexName} created on ${table} (${indexCols.join(', ')})`);
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
      // # Check and create tables

      const cleanStart = config.cleanStart;

      await checkAndCreateTables(cleanStart);

      // # Get all files
      const files = AccessParser.getAccessFiles();

      // # Process all files
      for await (const file of files) {
         console.log(`📂 Parsing file: ${file}`);
         await AccessParser.processFile(file);
      }

      await AccessParser.runMergeProcedures();

      console.log('✔️ Done processing all files.');
   } catch (err) {
      console.error('❌ Error:', err);
   } finally {
      await db.close();
   }
})();
