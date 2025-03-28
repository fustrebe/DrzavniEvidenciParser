const sql = require('mssql');
require('dotenv').config();

class DbHelper {
   constructor() {
      this.pool = null;
   }

   async connect() {
      if (!this.pool) {
         const config = {
            user: process.env.DB_USER,
            password: process.env.DB_PASS,
            server: process.env.DB_SERVER,
            port: parseInt(process.env.DB_PORT),
            database: process.env.DB_NAME,
            options: {
               encrypt: false,
               trustServerCertificate: true,
               requestTimeout: 60000
            },
            pool: {
               max: 10,
               min: 0,
               idleTimeoutMillis: 30000
            }
         };

         try {
            this.pool = await sql.connect(config);
            console.log('✅ Connected to MSSQL');
         } catch (err) {
            console.error('❌ MSSQL connection error:', err);
            throw err;
         }
      }
   }

   // @param {string} sqlString
   // @param {Object} params
   async query(sqlString, params = {}) {
      if (!this.pool) {
         await this.connect();
      }

      const request = this.pool.request();
      for (const key in params) {
         request.input(key, params[key]);
      }

      const result = await request.query(sqlString);
      return result.recordset;
   }

   async close() {
      if (this.pool) {
         await this.pool.close();
         this.pool = null;
      }
   }

   // # Helper for inserting data into table
   // @param {string} tableName
   // @param {Array} rows
   // @param {boolean} includeSmetka
   async insertToTable(tableName, rows, includeSmetka = false) {
      if (!rows || rows.length === 0) return;

      if (!this.pool) {
         await this.connect();
      }

      const table = new sql.Table(tableName);
      table.create = false;

      table.columns.add('EMBS', sql.NVarChar(250));
      table.columns.add('Godina', sql.Int);
      table.columns.add('Tip', sql.NVarChar(10));
      table.columns.add('DocTypeID', sql.NVarChar(10));

      for (let i = 601; i <= 724; i++) {
         table.columns.add(`AOP${i}`, sql.Float);
      }
      if (includeSmetka) {
         table.columns.add('Smetka', sql.NVarChar(10));
      }
      table.columns.add('Created_At', sql.DateTime);

      for (const row of rows) {
         const values = [
            String(row.EMBS),
            row.Godina ? parseInt(row.Godina) : null,
            String(row.Tip),
            String(row.DocTypeID),
            ...Array.from({ length: 124 }, (_, i) => row[`AOP${601 + i}`])
         ];
         if (includeSmetka) values.push(String(row.Smetka));
         values.push(new Date());
         table.rows.add(...values);
      }

      await this.pool.request().bulk(table);
   }
}

const instance = new DbHelper();
module.exports = instance;
