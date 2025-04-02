# AccessParser

AccessParser is a Node.js utility for parsing `.mdb` Microsoft Access database files, extracting structured financial data, and inserting it into SQL Server via bulk insert operations.

---

## 🏗 Project Overview

This tool is tailored for handling two main types of records:

1. **Drzavni Evidenci** (Government Records)
2. **Struktura Na Prihodi** (Revenue Structure)

Each record type supports multiple entity types (e.g., 450, 510, 540, etc.) and populates its corresponding temp table in SQL Server.

---

## 🔧 Configuration

Update your `config.js` file (located in the root folder) with the following structure:

```js
module.exports = {
   years: {
      previous: 2022,
      current: 2023
   },
   drzavniEvidenciCleanStart: true,
   strukturiNaPrihodiCleanStart: true,
   files: {
      'Target1.mdb': 'password1',
      'Target2.mdb': 'password2'
   }
};
```

---

## 📂 File Processing

### 1. Drzavni Evidenci

Tables:

-  `DrzavniEvidenciTemp`
-  `DrzavniEvidenci510Temp`
-  `DrzavniEvidenci540Temp`
-  `DrzavniEvidenci570Temp`
-  `DrzavniEvidenci600Temp`

Supported AOP Range: `601 - 724`

Method: `processFileDrzavniEvidenci(filePath)`  
Behavior: Processes one `TipSubjekt` at a time (e.g. 450 → 510 → ...), minimizes memory usage.

### 2. Struktura Na Prihodi

Tables:

-  `StrukturaNaPrihodiTemp`
-  `StrukturaNaPrihodi510Temp`
-  `StrukturaNaPrihodi520Temp`
-  `StrukturaNaPrihodi540Temp`
-  `StrukturaNaPrihodi550Temp`
-  `StrukturaNaPrihodi570Temp`
-  `StrukturaNaPrihodi600Temp`

Supported AOP Range: `2001 - 2619`

Method: `processFileStrukturaNaPrihodi(filePath)`  
Behavior: Same memory-optimized logic as DrzavniEvidenci. Streams in one `TipSubjekt` at a time.

---

## 🛠 Database Initialization

Before inserting data, the following methods ensure all necessary tables and indexes are created:

-  `checkAndCreateTablesDrzavniEvidenci(shouldDelete)`
-  `checkAndCreateTablesStrukturaNaPrihodi(shouldDelete)`

`shouldDelete = true` will drop and recreate the tables.

---

## 🔁 Merging Temp Tables into Final Tables

Stored procedures are created and used to deduplicate and merge data using:

```js
await runMergeProcedures();
```

Procedures:

-  `sp_MergeDrzavniEvidenciDynamic`
-  `sp_MergeStrukturaNaPrihodiDynamic`

---

## 🧠 Memory Management

Memory usage is optimized by:

-  Processing data type-by-type (e.g. 450, 510...)
-  Releasing large variables with `.length = 0` or `= null`
-  Avoiding large in-memory batching

---

## 🚀 Running the Tool

```bash
node index.js
```

---

## 🔐 SQL Server Access

Set these environment variables:

-  `DB_SERVER`
-  `DB_PORT`
-  `DB_NAME`
-  `DB_USER`
-  `DB_PASS`

---

## 📌 Notes

-  `.mdb` files are accessed using `node-odbc`.
-  SQL inserts are done via `mssql` `request.bulk()` for performance.
-  Everything is designed to scale with large data inputs (~100k rows+ per file).

---
