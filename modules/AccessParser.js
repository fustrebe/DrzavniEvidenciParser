const fs = require('fs');
const path = require('path');
const odbc = require('odbc');
const db = require('./DbHelper');
const config = require('../config');
const { connect } = require('http2');

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

   // # Methods for checking and creating tables, including the stored procedure
   // @param {boolean} shouldDelete
   // @default false
   // @returns {Promise<void>}

   // # Drzavni Evidenci
   checkAndCreateTablesDrzavniEvidenci = async (shouldDelete = false) => {
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

         // 🔍 Check if stored procedure exists
         const spExistsQuery = `
         SELECT * FROM sys.objects 
         WHERE type = 'P' AND name = 'sp_MergeDrzavniEvidenciDynamic'
      `;
         const spExists = await db.query(spExistsQuery);

         if (spExists.length === 0) {
            console.log(`⚠️ Stored procedure sp_MergeDrzavniEvidenciDynamic does not exist. Creating...`);

            const spQuery = `
            EXEC sp_executesql N'
            CREATE PROCEDURE dbo.sp_MergeDrzavniEvidenciDynamic
                @TargetTable NVARCHAR(100)
            AS
            BEGIN
                SET NOCOUNT ON;

                DECLARE @HasSmetka BIT = CASE WHEN @TargetTable = ''DrzavniEvidenci510'' THEN 1 ELSE 0 END;
                DECLARE @TempTable NVARCHAR(100) = @TargetTable + ''Temp'';

                DECLARE @JoinKeys NVARCHAR(MAX) = ''
                    target.EMBS = source.EMBS AND
                    target.Tip = source.Tip AND
                    target.Godina = source.Godina'';

                IF @HasSmetka = 1
                    SET @JoinKeys += '' AND target.Smetka = source.Smetka'';

                DECLARE @AOPCols NVARCHAR(MAX) = '''';
                DECLARE @AOPVals NVARCHAR(MAX) = '''';
                DECLARE @AOPUpdates NVARCHAR(MAX) = '''';
                DECLARE @i INT = 601;

                WHILE @i <= 724
                BEGIN
                    SET @AOPCols += ''AOP'' + CAST(@i AS NVARCHAR) + '', '';
                    SET @AOPVals += ''source.AOP'' + CAST(@i AS NVARCHAR) + '', '';
                    SET @AOPUpdates += ''AOP'' + CAST(@i AS NVARCHAR) + '' = source.AOP'' + CAST(@i AS NVARCHAR) + '', '';
                    SET @i += 1;
                END

                -- Trim trailing comma+space
                SET @AOPCols = LEFT(@AOPCols, LEN(@AOPCols) - 1);
                SET @AOPVals = LEFT(@AOPVals, LEN(@AOPVals) - 1);
                SET @AOPUpdates = LEFT(@AOPUpdates, LEN(@AOPUpdates) - 1);

                DECLARE @InsertCols NVARCHAR(MAX) = ''
                    EMBS, Tip, Godina'' + 
                    CASE WHEN @HasSmetka = 1 THEN '', Smetka'' ELSE '''' END + 
                    '', '' + @AOPCols + '', Created_At'';

                DECLARE @InsertVals NVARCHAR(MAX) = ''
                    source.EMBS, source.Tip, source.Godina'' + 
                    CASE WHEN @HasSmetka = 1 THEN '', source.Smetka'' ELSE '''' END + 
                    '', '' + @AOPVals + '', source.Created_At'';

                DECLARE @UpdateSet NVARCHAR(MAX) = @AOPUpdates + '', Created_At = source.Created_At'';

                DECLARE @DedupPartition NVARCHAR(MAX) = ''
                    PARTITION BY EMBS, Tip, Godina'' + 
                    CASE WHEN @HasSmetka = 1 THEN '', Smetka'' ELSE '''' END;

                DECLARE @SQL NVARCHAR(MAX) = ''
                ;WITH Deduplicated AS (
                    SELECT '' + 
                        CASE WHEN @HasSmetka = 1 
                            THEN ''EMBS, Tip, Godina, Smetka, '' 
                            ELSE ''EMBS, Tip, Godina, '' 
                        END +
                        @AOPCols + '', Created_At
                    FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                '' + @DedupPartition + ''
                                ORDER BY 
                                CASE 
                                    WHEN CAST(DocTypeID AS NVARCHAR) = ''''120'''' THEN 1
                                    WHEN CAST(DocTypeID AS NVARCHAR) = ''''150'''' THEN 1
                                    WHEN CAST(DocTypeID AS NVARCHAR) = ''''110'''' THEN 2
                                    WHEN CAST(DocTypeID AS NVARCHAR) = ''''140'''' THEN 2
                                    ELSE 3
                                END,
                                Created_At DESC
                            ) AS rn
                        FROM '' + QUOTENAME(@TempTable) + ''
                    ) filtered
                    WHERE rn = 1
                )
                MERGE '' + QUOTENAME(@TargetTable) + '' AS target
                USING Deduplicated AS source
                ON '' + @JoinKeys + ''
                WHEN MATCHED THEN
                    UPDATE SET '' + @UpdateSet + ''
                WHEN NOT MATCHED THEN
                    INSERT ('' + @InsertCols + '')
                    VALUES ('' + @InsertVals + '');
                '';

                EXEC sp_executesql @SQL;
            END';
         `;

            await db.query(spQuery);
            console.log(`✅ Stored procedure sp_MergeDrzavniEvidenciDynamic created.`);
         }
      } catch (err) {
         console.error(`❌ Error: ${err.message}`);
      }
   };

   // # Struktura Na Prihodi
   checkAndCreateTablesStrukturaNaPrihodi = async (shouldDelete = false) => {
      try {
         const tableNames = [
            'StrukturaNaPrihodi',
            'StrukturaNaPrihodi510',
            'StrukturaNaPrihodi520',
            'StrukturaNaPrihodi540',
            'StrukturaNaPrihodi550',
            'StrukturaNaPrihodi570',
            'StrukturaNaPrihodi600',
            'StrukturaNaPrihodiTemp',
            'StrukturaNaPrihodi510Temp',
            'StrukturaNaPrihodi520Temp',
            'StrukturaNaPrihodi540Temp',
            'StrukturaNaPrihodi550Temp',
            'StrukturaNaPrihodi570Temp',
            'StrukturaNaPrihodi600Temp'
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
               ${table.includes('Temp') ? 'DocTypeID NVARCHAR(10),\n' : ''}
            `;

               for (let i = 2001; i <= 2619; i++) {
                  columns += `AOP${i} FLOAT NULL,\n`;
               }

               if (table.includes('510') || table.includes('520')) {
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

               // 🔍 Create index
               let indexCols = ['EMBS', 'Tip', 'Godina'];
               if (table.includes('510') || table.includes('520')) {
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

         // ✅ Stored procedure
         const spCheck = `
         SELECT * FROM sys.objects 
         WHERE type = 'P' AND name = 'sp_MergeStrukturaNaPrihodiDynamic'
      `;
         const spExists = await db.query(spCheck);

         if (spExists.length === 0) {
            console.log(`⚠️ Stored procedure sp_MergeStrukturaNaPrihodiDynamic does not exist. Creating...`);

            const spQuery = `
         CREATE PROCEDURE [dbo].[sp_MergeStrukturaNaPrihodiDynamic]
            @TargetTable NVARCHAR(100)
         AS
         BEGIN
            SET NOCOUNT ON;

            DECLARE @HasSmetka BIT = CASE WHEN @TargetTable IN ('StrukturaNaPrihodi510', 'StrukturaNaPrihodi520') THEN 1 ELSE 0 END;
            DECLARE @TempTable NVARCHAR(100) = @TargetTable + 'Temp';

            DECLARE @JoinKeys NVARCHAR(MAX) = '
               target.EMBS = source.EMBS AND
               target.Tip = source.Tip AND
               target.Godina = source.Godina';

            IF @HasSmetka = 1
               SET @JoinKeys += ' AND target.Smetka = source.Smetka';

            DECLARE @AOPCols NVARCHAR(MAX) = '';
            DECLARE @AOPVals NVARCHAR(MAX) = '';
            DECLARE @AOPUpdates NVARCHAR(MAX) = '';
            DECLARE @i INT = 2001;

            WHILE @i <= 2619
            BEGIN
               SET @AOPCols += 'AOP' + CAST(@i AS NVARCHAR) + ', ';
               SET @AOPVals += 'source.AOP' + CAST(@i AS NVARCHAR) + ', ';
               SET @AOPUpdates += 'AOP' + CAST(@i AS NVARCHAR) + ' = source.AOP' + CAST(@i AS NVARCHAR) + ', ';
               SET @i += 1;
            END

            SET @AOPCols = LEFT(@AOPCols, LEN(@AOPCols) - 1);
            SET @AOPVals = LEFT(@AOPVals, LEN(@AOPVals) - 1);
            SET @AOPUpdates = LEFT(@AOPUpdates, LEN(@AOPUpdates) - 1);

            DECLARE @InsertCols NVARCHAR(MAX) = '
               EMBS, Tip, Godina' + 
               CASE WHEN @HasSmetka = 1 THEN ', Smetka' ELSE '' END + 
               ', ' + @AOPCols + ', Created_At';

            DECLARE @InsertVals NVARCHAR(MAX) = '
               source.EMBS, source.Tip, source.Godina' + 
               CASE WHEN @HasSmetka = 1 THEN ', source.Smetka' ELSE '' END + 
               ', ' + @AOPVals + ', source.Created_At';

            DECLARE @UpdateSet NVARCHAR(MAX) = @AOPUpdates + ', Created_At = source.Created_At';

            DECLARE @DedupPartition NVARCHAR(MAX) = '
               PARTITION BY EMBS, Tip, Godina' + 
               CASE WHEN @HasSmetka = 1 THEN ', Smetka' ELSE '' END;

            DECLARE @SQL NVARCHAR(MAX) = '
            ;WITH Deduplicated AS (
               SELECT ' + 
                  CASE WHEN @HasSmetka = 1 
                     THEN 'EMBS, Tip, Godina, Smetka, ' 
                     ELSE 'EMBS, Tip, Godina, ' 
                  END +
                  @AOPCols + ', Created_At
               FROM (
                  SELECT *,
                     ROW_NUMBER() OVER (
                        ' + @DedupPartition + '
                        ORDER BY 
                           CASE 
                              WHEN CAST(DocTypeID AS NVARCHAR) = ''120'' THEN 1
                              WHEN CAST(DocTypeID AS NVARCHAR) = ''150'' THEN 1
                              WHEN CAST(DocTypeID AS NVARCHAR) = ''110'' THEN 2
                              WHEN CAST(DocTypeID AS NVARCHAR) = ''140'' THEN 2
                              ELSE 3
                           END,
                           Created_At DESC
                     ) AS rn
                  FROM ' + QUOTENAME(@TempTable) + '
               ) filtered
               WHERE rn = 1
            )
            MERGE ' + QUOTENAME(@TargetTable) + ' AS target
            USING Deduplicated AS source
            ON ' + @JoinKeys + '
            WHEN MATCHED THEN
               UPDATE SET ' + @UpdateSet + '
            WHEN NOT MATCHED THEN
               INSERT (' + @InsertCols + ')
               VALUES (' + @InsertVals + ');
            ';

            EXEC sp_executesql @SQL;
         END
         `;

            await db.query(spQuery);
            console.log(`✅ Stored procedure sp_MergeStrukturaNaPrihodiDynamic created.`);
         }

         console.log('✔️ Done checking/creating all StrukturaNaPrihodi tables.');
      } catch (err) {
         console.error(`❌ Error: ${err.message}`);
         throw err;
      }
   };

   // # Dzavni Evidenci
   // @param {string} filePath
   async processFileDrzavniEvidenci(filePath) {
      const fileName = path.basename(filePath);
      const password = this.passwords[fileName];
      const connectionString = `
         Driver={Microsoft Access Driver (*.mdb, *.accdb)};
         Dbq=${filePath};
         ${password ? `PWD=${password};` : ''}
         ReadOnly=True;
      `;
      let connection;

      const aopRange = Array.from({ length: 124 }, (_, i) => 601 + i);
      const tipSubjektMap = {
         450: 'DrzavniEvidenciTemp',
         510: 'DrzavniEvidenci510Temp',
         540: 'DrzavniEvidenci540Temp',
         570: 'DrzavniEvidenci570Temp',
         600: 'DrzavniEvidenci600Temp'
      };

      const queryBuilder = (year, column) => `
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
         FROM dbo_adtarget a
         INNER JOIN dbo_aatarget b ON a.DocumentID = b.documentid
         WHERE a.FormID IN (9, 12, 19, 22, 38)
           AND a.AccountNo BETWEEN 601 AND 724
           AND b.DocTypeID <> 115
      `;

      try {
         console.log(`🔗 Connecting to ${fileName}...`);
         connection = await odbc.connect(connectionString);
         console.log(`✅ Connected to ${fileName}`);

         let tables = await connection.tables(null, null, null, 'TABLE');
         const tableNames = tables.map((t) => t.TABLE_NAME.toLowerCase());
         tables = null;

         const required = ['dbo_adtarget', 'dbo_aatarget'];
         const missing = required.filter((t) => !tableNames.includes(t));
         if (missing.length > 0) {
            throw new Error(`File "${fileName}" is missing required table(s): ${missing.join(', ')}`);
         }

         const prevRows = await connection.query(queryBuilder(config.years.previous, 'Previous'));
         const currRows = await connection.query(queryBuilder(config.years.current, 'CurrentYear'));

         await connection.close();
         connection = null; // Clear the connection variable
         console.log(`🔗 Connection to ${fileName} closed.`);

         const allRows = [...prevRows, ...currRows];
         prevRows.length = 0;
         currRows.length = 0;

         for await (const tipSubjekt of Object.keys(tipSubjektMap).map(Number)) {
            const tableName = tipSubjektMap[tipSubjekt];
            const filtered = allRows.filter((r) => r.TipSubjekt === tipSubjekt);
            if (filtered.length === 0) continue;

            const pivoted = {};

            for (const row of filtered) {
               const key = `${row.EMBS}_${row.Tip}_${row.TipSubjekt}_${row.FormID}_${row.Godina}_${row.Smetka}_${row.DocTypeID}`;
               if (!pivoted[key]) {
                  pivoted[key] = {
                     EMBS: row.EMBS?.substring(1) || null,
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

            // # Comment filter if needed
            const final = Object.values(pivoted)
               .filter((entry) => {
                  return Object.keys(entry).some((key) => key.startsWith('AOP') && entry[key] != null);
               })
               .sort((a, b) => a.EMBS.localeCompare(b.EMBS));
            await db.insertToTable(tableName, final, tipSubjekt === 510, 601, 724, true);

            // 🔥 Clear memory for next tipSubjekt
            filtered.length = 0;
            final.length = 0;
         }

         allRows.length = 0;
         console.log(`✅ ${fileName} DrzavniEvidenci processing complete.`);
      } catch (err) {
         console.error(`❌ Failed to process DrzavniEvidenci for ${fileName}:`, err.message);
         throw err;
      } finally {
         if (connection) {
            await connection.close();
            connection = null;
         }
         console.log(`🔗 Connection to ${fileName} closed.`);
      }
   }

   // # Struktura Na Prihodi
   // @param {string} filePath
   async processFileStrukturaNaPrihodi(filePath) {
      const fileName = path.basename(filePath);
      const password = this.passwords[fileName];
      const connectionString = `
         Driver={Microsoft Access Driver (*.mdb, *.accdb)};
         Dbq=${filePath};
         ${password ? `PWD=${password};` : ''}
         ReadOnly=True;
      `;
      let connection;

      const aopStrukturaRange = Array.from({ length: 619 }, (_, i) => 2001 + i);
      const tipSubjektMap = {
         450: 'StrukturaNaPrihodiTemp',
         510: 'StrukturaNaPrihodi510Temp',
         520: 'StrukturaNaPrihodi520Temp',
         540: 'StrukturaNaPrihodi540Temp',
         550: 'StrukturaNaPrihodi550Temp',
         570: 'StrukturaNaPrihodi570Temp',
         600: 'StrukturaNaPrihodi600Temp'
      };

      const buildQuery = (year, column) => `
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
         FROM dbo_adtarget a
         INNER JOIN dbo_aatarget b ON a.DocumentID = b.documentid
         WHERE a.FormID IN (28, 29, 30, 31, 32, 33, 35)
           AND a.AccountNo BETWEEN 2001 AND 2619
           AND b.DocTypeID <> 115
      `;

      try {
         console.log(`🔗 Connecting to ${fileName}...`);
         connection = await odbc.connect(connectionString);
         console.log(`✅ Connected to ${fileName}`);

         let tables = await connection.tables(null, null, null, 'TABLE');
         const tableNames = tables.map((t) => t.TABLE_NAME.toLowerCase());
         tables = null;

         const required = ['dbo_adtarget', 'dbo_aatarget'];
         const missing = required.filter((t) => !tableNames.includes(t));
         if (missing.length > 0) {
            throw new Error(`File "${fileName}" is missing required table(s): ${missing.join(', ')}`);
         }

         const prevRows = await connection.query(buildQuery(config.years.previous, 'Previous'));
         const currRows = await connection.query(buildQuery(config.years.current, 'CurrentYear'));

         await connection.close();
         connection = null; // Clear the connection variable
         console.log(`🔗 Connection to ${fileName} closed.`);

         const allRows = [...prevRows, ...currRows];
         prevRows.length = 0;
         currRows.length = 0;

         for await (const tipSubjekt of Object.keys(tipSubjektMap).map(Number)) {
            const tableName = tipSubjektMap[tipSubjekt];
            const filtered = allRows.filter((r) => r.TipSubjekt === tipSubjekt);
            if (filtered.length === 0) continue;

            const pivoted = {};

            for (const row of filtered) {
               const key = `${row.EMBS}_${row.Tip}_${row.Godina}_${row.Smetka}_${row.DocTypeID}`;
               if (!pivoted[key]) {
                  pivoted[key] = {
                     EMBS: row.EMBS ? row.EMBS.substring(1) : null,
                     Tip: row.Tip,
                     Godina: row.Godina,
                     DocTypeID: row.DocTypeID,
                     Smetka: row.Smetka
                  };
                  for (const aop of aopStrukturaRange) {
                     pivoted[key][`AOP${aop}`] = null;
                  }
               }
               pivoted[key][`AOP${row.AccountNo}`] = row.AOP_Value;
            }

            // # Comment filter if needed
            const final = Object.values(pivoted)
               .filter((entry) => {
                  return Object.keys(entry).some((key) => key.startsWith('AOP') && entry[key] != null);
               })
               .sort((a, b) => a.EMBS.localeCompare(b.EMBS));
            await db.insertToTable(tableName, final, [510, 520].includes(tipSubjekt), 2001, 2619, true);

            // 🧹 Clean up memory for next group
            filtered.length = 0;
            final.length = 0;
         }

         allRows.length = 0;
         console.log(`✅ Finished processing StrukturaNaPrihodi from file ${fileName}`);
      } catch (err) {
         console.error(`❌ Error processing StrukturaNaPrihodi from file ${fileName}:`, err.message);
         throw err;
      } finally {
         if (connection) {
            await connection.close();
            connection = null;
         }
         console.log(`🔗 Connection to ${fileName} closed.`);
      }
   }

   // # Method for merging all files
   async runMergeProcedures() {
      const drzavniTables = ['DrzavniEvidenci', 'DrzavniEvidenci510', 'DrzavniEvidenci540', 'DrzavniEvidenci570', 'DrzavniEvidenci600'];

      const strukturaTables = [
         'StrukturaNaPrihodi',
         'StrukturaNaPrihodi510',
         'StrukturaNaPrihodi520',
         'StrukturaNaPrihodi540',
         'StrukturaNaPrihodi550',
         'StrukturaNaPrihodi570',
         'StrukturaNaPrihodi600'
      ];

      // 🚀 Drzavni Evidenci
      for (const table of drzavniTables) {
         try {
            console.log(`🚀 Merging data into ${table}...`);
            await db.query(`EXEC sp_MergeDrzavniEvidenciDynamic @TargetTable = @table`, { table });
            console.log(`✅ Merge completed for ${table}`);
         } catch (err) {
            console.error(`❌ Failed to merge ${table}:`, err.message);
         }
      }

      // 🚀 Struktura Na Prihodi
      for (const table of strukturaTables) {
         try {
            console.log(`🚀 Merging data into ${table}...`);
            await db.query(`EXEC sp_MergeStrukturaNaPrihodiDynamic @TargetTable = @table`, { table });
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
