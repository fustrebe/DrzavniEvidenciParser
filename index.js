const db = require('./modules/DbHelper');
const AccessParser = require('./modules/AccessParser');
const config = require('./config');

// # Method for checking and creating tables, including the stored procedure
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
