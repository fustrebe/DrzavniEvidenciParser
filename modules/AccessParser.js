const fs = require('fs');
const path = require('path');
const odbc = require('odbc');
const ExcelJS = require('exceljs');

class AccessParser {
   constructor(folderPath = './access', passwordFile = './access_passwords.txt') {
      this.folderPath = path.resolve(folderPath);
      this.passwordFile = path.resolve(passwordFile);
      this.passwords = {};
      this.ensureFolderExists();
      this.loadPasswords();
   }

   // # Ensuring that folder exists
   ensureFolderExists() {
      if (!fs.existsSync(this.folderPath)) {
         fs.mkdirSync(this.folderPath);
      }
   }

   //# Method for loading the passwords file
   loadPasswords() {
      if (!fs.existsSync(this.passwordFile)) {
         fs.writeFileSync(this.passwordFile, 'Target1.mdb=123456\nTarget2.accdb=654321\n');
         return;
      }
      const lines = fs.readFileSync(this.passwordFile, 'utf-8').split(/\r?\n/);
      lines.forEach((line) => {
         const [filename, password] = line.split('=');
         if (filename && password) {
            this.passwords[filename.trim()] = password.trim();
         }
      });
   }

   //# Method for getting the files
   getAccessFiles() {
      return fs
         .readdirSync(this.folderPath)
         .filter((file) => file.endsWith('.mdb') || file.endsWith('.accdb'))
         .map((file) => path.join(this.folderPath, file));
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

      const aopRange = Array.from({ length: 124 }, (_, i) => 601 + i); // AOP601–AOP724

      try {
         const connection = await odbc.connect(connectionString);

         // ✅ Step 1: Validate required tables
         const tables = await connection.tables(null, null, null, 'TABLE');
         const tableNames = tables.map((t) => t.TABLE_NAME.toLowerCase());
         const required = ['dbo_adtarget', 'dbo_aatarget'];
         const missing = required.filter((t) => !tableNames.includes(t));

         if (missing.length > 0) {
            throw new Error(`File "${fileName}" is missing required table(s): ${missing.join(', ')}`);
         }

         // ✅ Step 2: Define query builder
         const getDataQuery = (year, column) => `
            SELECT 
               b.leid AS EMBS,
               IIF(b.DocTypeID IN (110,120,133), 1, IIF(b.DocTypeID IN (140,150), 2, 0)) AS Tip,
               b.Operationid AS TipSubjekt,
               a.FormID,
               ${year} AS Godina,
               a.AccountNo,
               a.${column} AS AOP_Value,
               b.AATypeID AS Smetka
            FROM dbo_adtarget AS a
            INNER JOIN dbo_aatarget AS b ON a.DocumentID = b.documentid
            WHERE a.FormID IN (9, 12, 19, 22, 38)
              AND a.AccountNo BETWEEN 601 AND 724
              AND b.DocTypeID <> 115
         `;

         // ✅ Step 3: Query + merge
         const rows2022 = await connection.query(getDataQuery(2022, 'Previous'));
         const rows2023 = await connection.query(getDataQuery(2023, 'CurrentYear'));
         const allRows = [...rows2022, ...rows2023];

         await connection.close();

         // ✅ Step 4: Pivot rows
         const pivoted = {};
         for (const row of allRows) {
            const key = `${row.EMBS}_${row.Tip}_${row.TipSubjekt}_${row.FormID}_${row.Godina}_${row.Smetka}`;
            if (!pivoted[key]) {
               pivoted[key] = {
                  EMBS: row.EMBS,
                  Tip: row.Tip,
                  TipSubjekt: row.TipSubjekt,
                  FormID: row.FormID,
                  Godina: row.Godina,
                  Smetka: row.Smetka
               };
               for (const aop of aopRange) {
                  pivoted[key][`AOP${aop}`] = null;
               }
            }
            pivoted[key][`AOP${row.AccountNo}`] = row.AOP_Value;
         }

         const finalData = Object.values(pivoted);

         // 🔢 Sort by EMBS
         finalData.sort((a, b) => a.EMBS.localeCompare(b.EMBS));

         // ✅ Step 5: Stream Excel export
         const outputPath = path.join(this.folderPath, `Processed_${path.parse(fileName).name}.xlsx`);
         const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
            filename: outputPath,
            useStyles: true
         });

         const worksheet = workbook.addWorksheet('DrzavniEvidenci');

         const headers = ['EMBS', 'Tip', 'TipSubjekt', 'FormID', 'Godina', 'Smetka', ...aopRange.map((n) => `AOP${n}`)];

         worksheet.columns = headers.map((header) => ({
            header,
            key: header,
            width: header.startsWith('AOP') ? 12 : 15,
            style: {
               font: { size: 11 },
               numFmt: header.startsWith('AOP') ? '#,##0.00' : undefined
            }
         }));

         for (const row of finalData) {
            const cleanRow = {};
            for (const key of headers) {
               cleanRow[key] = row[key] ?? null;
            }
            worksheet.addRow(cleanRow).commit();
         }

         await worksheet.commit();
         await workbook.commit();

         console.log(`✅ Excel file saved: ${outputPath}`);
         return outputPath;
      } catch (err) {
         console.error(`❌ Failed to process file ${fileName}:`, err.message);
         throw err;
      }
   }
}

const instance = new AccessParser();
module.exports = instance;
