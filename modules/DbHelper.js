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
               trustServerCertificate: true
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
}

const instance = new DbHelper();
module.exports = instance;
