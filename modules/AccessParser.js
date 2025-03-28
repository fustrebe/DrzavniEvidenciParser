const fs = require('fs');
const path = require('path');
const odbc = require('odbc');
const db = require('./DbHelper');
const config = require('../config');

class AccessParser {
   // @param {string} folderPath
   constructor(folderPath = './access') {
      this.folderPath = path.resolve(folderPath);
      this.passwords = { ...config.files };
      this.ensureFolderExists();
   }

   // # Ensuring that folder exists
   ensureFolderExists() {
      if (!fs.existsSync(this.folderPath)) {
         fs.mkdirSync(this.folderPath);
      }
   }

   //# Method for getting the files
   getAccessFiles() {
      return fs
         .readdirSync(this.folderPath)
         .filter((file) => file.endsWith('.mdb') || file.endsWith('.accdb'))
         .map((file) => path.join(this.folderPath, file));
   }

   // # Helper to split arrays into chunks
   // @param {Array} arr
   // @param {number} size
   chunkArray(arr, size) {
      const chunks = [];
      for (let i = 0; i < arr.length; i += size) {
         chunks.push(arr.slice(i, i + size));
      }
      return chunks;
   }

   // # Method for processing file
   // @param {string} filePath
   async processFile(filePath) {
      const fileName = path.basename(filePath);
      const password = this.passwords[fileName];
      const connectionString = `
         Driver={Microsoft Access Driver (*.mdb, *.accdb)};
         Dbq=${filePath};
         ${password ? `PWD=${password};` : ''}
         ReadOnly=True;
      `;

      const aopRange = Array.from({ length: 124 }, (_, i) => 601 + i);

      try {
         const connection = await odbc.connect(connectionString);

         let tables = await connection.tables(null, null, null, 'TABLE');
         const tableNames = tables.map((t) => t.TABLE_NAME.toLowerCase());
         tables = null;

         const required = ['dbo_adtarget', 'dbo_aatarget'];
         const missing = required.filter((t) => !tableNames.includes(t));
         if (missing.length > 0) {
            throw new Error(`File "${fileName}" is missing required table(s): ${missing.join(', ')}`);
         }

         const getDataQuery = (year, column) => `
            SELECT 
               b.leid AS EMBS,
               IIF(b.DocTypeID IN (110,120,133), 1, IIF(b.DocTypeID IN (140,150), 2, 0)) AS Tip,
               b.Operationid AS TipSubjekt,
               a.FormID,
               ${year} AS Godina,
               a.AccountNo,
               a.${column} AS AOP_Value,
               b.AATypeID AS Smetka,
               b.DocTypeID AS DocTypeID
            FROM dbo_adtarget AS a
            INNER JOIN dbo_aatarget AS b ON a.DocumentID = b.documentid
            WHERE a.FormID IN (9, 12, 19, 22, 38)
              AND a.AccountNo BETWEEN 601 AND 724
              AND b.DocTypeID <> 115
         `;

         const rowsPrevious = await connection.query(getDataQuery(config.years.previous, 'Previous'));
         const rowsCurrent = await connection.query(getDataQuery(config.years.current, 'CurrentYear'));
         const allRows = [...rowsPrevious, ...rowsCurrent];

         await connection.close();

         rowsPrevious.length = 0;
         rowsCurrent.length = 0;

         let pivoted = {};
         for (const row of allRows) {
            const key = `${row.EMBS}_${row.Tip}_${row.TipSubjekt}_${row.FormID}_${row.Godina}_${row.Smetka}_${row.DocTypeID}`;
            if (!pivoted[key]) {
               pivoted[key] = {
                  EMBS: row.EMBS ? row.EMBS.substring(1) : null,
                  Tip: row.Tip,
                  TipSubjekt: row.TipSubjekt,
                  FormID: row.FormID,
                  Godina: row.Godina,
                  DocTypeID: row.DocTypeID,
                  Smetka: row.Smetka
               };
               for (const aop of aopRange) {
                  pivoted[key][`AOP${aop}`] = null;
               }
            }
            pivoted[key][`AOP${row.AccountNo}`] = row.AOP_Value;
         }

         const finalData = Object.values(pivoted).sort((a, b) => a.EMBS.localeCompare(b.EMBS));
         pivoted = null;

         let insertBatches = {
            DrzavniEvidenciTemp: [],
            DrzavniEvidenci510Temp: [],
            DrzavniEvidenci540Temp: [],
            DrzavniEvidenci570Temp: [],
            DrzavniEvidenci600Temp: []
         };

         for (const row of finalData) {
            const tableMapping = {
               450: 'DrzavniEvidenciTemp',
               510: 'DrzavniEvidenci510Temp',
               540: 'DrzavniEvidenci540Temp',
               570: 'DrzavniEvidenci570Temp',
               600: 'DrzavniEvidenci600Temp'
            };
            const table = tableMapping[row.TipSubjekt];
            insertBatches[table].push(row);
         }

         await db.insertToTable('DrzavniEvidenciTemp', insertBatches.DrzavniEvidenciTemp);
         await db.insertToTable('DrzavniEvidenci510Temp', insertBatches.DrzavniEvidenci510Temp, true);
         await db.insertToTable('DrzavniEvidenci540Temp', insertBatches.DrzavniEvidenci540Temp);
         await db.insertToTable('DrzavniEvidenci570Temp', insertBatches.DrzavniEvidenci570Temp);
         await db.insertToTable('DrzavniEvidenci600Temp', insertBatches.DrzavniEvidenci600Temp);

         // Clearing the final data
         insertBatches = null;
         finalData.length = 0;

         console.log(`✅ File ${fileName} processed and inserted with bulk successfully.`);
      } catch (err) {
         console.error(`❌ Failed to process file ${fileName}:`, err.message);
         throw err;
      }
   }

   // # Method for merging all files
   async runMergeProcedures() {
      const tables = [
         'DrzavniEvidenci',
         'DrzavniEvidenci510',
         'DrzavniEvidenci540',
         'DrzavniEvidenci570',
         'DrzavniEvidenci600'
      ];

      for (const table of tables) {
         try {
            console.log(`🚀 Merging data into ${table}...`);
            await db.query(`EXEC sp_MergeDrzavniEvidenciDynamic @TargetTable = @table`, {
               table
            });
            console.log(`✅ Merge completed for ${table}`);
         } catch (err) {
            console.error(`❌ Failed to merge ${table}:`, err.message);
         }
      }

      console.log('🎉 All merges complete.');
   }
}

const instance = new AccessParser();
module.exports = instance;
