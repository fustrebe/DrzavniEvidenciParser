# 🗂️ Državni Evidenci Parser

A Node.js application that automates the extraction, transformation, and loading (ETL) of financial data from Microsoft Access (.mdb) files into MSSQL Server tables. Designed for large-scale, efficient, and repeatable data imports, it includes auto-deduplication, pivoting, and merging logic — all fully customizable via `config.js`.

---

## 🚀 Features

-  ✅ Reads `.mdb` (Access) files using ODBC
-  ✅ Password-protected file support
-  ✅ Custom year-based extraction from `Previous` and `CurrentYear` fields
-  ✅ Auto-creates MSSQL tables (`DrzavniEvidenci*` and Temp versions)
-  ✅ Bulk inserts using MSSQL’s high-performance API
-  ✅ Auto-pivots AOP fields (`AOP601–AOP724`)
-  ✅ Merges into final tables via stored procedure
-  ✅ Supports dynamic config via `config/config.js`

---

## 🛠️ Technologies Used

| Stack       | Version                              |
| ----------- | ------------------------------------ |
| Node.js     | `v18.19.0` ✅ Tested on this version |
| `odbc`      | For Access `.mdb` reading            |
| `mssql`     | For high-performance MSSQL inserts   |
| `exceljs`   | For Excel export (optional)          |
| `dotenv`    | For database credentials             |
| `config.js` | Central configuration management     |

---

## 📦 Project Structure

```
/access/
/config.js         → config.js (application setup)
/modules/          → AccessParser.js, DbHelper.js
/index.js          → Main entry point
.env               → Environment variables for MSSQL
```

---

## ⚙️ Setup Instructions

### 1. ✅ Clone the repo

```bash
git clone https://github.com/yourname/drzavni-evidenci-parser.git
cd drzavni-evidenci-parser
```

### 2. ✅ Install dependencies

```bash
npm install
```

### 3. ✅ Setup `.env` file

Create `.env` in the root of your project with the following:

```env
DB_USER=your_user
DB_PASS=your_password
DB_NAME=your_database
DB_SERVER=your_server
DB_PORT=your_db_port
```

### 4. ✅ Configure your application

Edit `/config/config.js` to define:

```js
module.exports = {
   files: {
      'Target1.mdb': 'Password1',
      'Target2.mdb': 'Password2'
   },
   years: {
      previous: 2022,
      current: 2023
   },
   cleanStart: true // ⚠️ Deletes all tables and recreates them
};
```

---

## ▶️ How to Run the Program

```bash
node index.js
```

This will:

1. Check if all required tables exist (and optionally recreate them)
2. Load and parse all `.mdb` files from the `/access/` folder
3. Extract and pivot the financial data
4. Bulk insert the results into MSSQL Temp tables
5. Merge final values into their respective permanent tables

---

## ⚠️ Important Notes

-  Only `.mdb` files listed in `config.js > files` will be processed
-  `cleanStart: true` will drop and recreate tables at startup — use with caution
-  All pivoted data is based on columns `AOP601` to `AOP724`
-  Duplicate rows with the same `EMBS + Tip + Godina [+ Smetka]` will be auto-handled

---

## 🧪 Testing

Tested with:

-  **Node.js** `v18.19.0`
-  **MSSQL Server 2019+**
-  **Access Driver**: `Microsoft Access Driver (*.mdb, *.accdb)`
-  ODBC driver should be properly installed and configured

---

## 📚 Stored Procedure

Stored procedure used: `sp_MergeDrzavniEvidenciDynamic`

It:

-  Merges data from Temp tables into final tables
-  Deduplicates by DocTypeID (e.g. prefers 120 over 110)
-  Dynamically accepts a table name (`DrzavniEvidenci`, `DrzavniEvidenci510`, etc.)

You can run manually:

```sql
EXEC sp_MergeDrzavniEvidenciDynamic @TargetTable = 'DrzavniEvidenci510';
```
