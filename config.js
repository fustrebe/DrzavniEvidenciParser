module.exports = {
   /**
    * 🔐 Map of Access (.mdb) filenames to their passwords
    * These passwords will be used when opening the files via ODBC
    */
   files: {
      'Target1.mdb': 'T@rG3t2o2$',
      'Target2.mdb': 'T@rG3t2o2$_@D',
      'Target3.mdb': 'T@rG3t2o2$_#D',
      'Target4.mdb': 'T@rG3t2o2$_4D',
      'Target5.mdb': 'T@rG3t2o2$_PD'
   },

   /**
    * 📅 Define which years you want to pull from the Access file
    * These map to `a.Previous` and `a.CurrentYear` in your Access query
    */
   years: {
      previous: 2022,
      current: 2023
   },

   /**
    * ⚠️⚠️⚠️ DANGER ZONE ⚠️⚠️⚠️
    * If true, deletes ALL `DrzavniEvidenci*` tables (Temp and Original)
    * before recreating them. This gives you a clean start.
    * Use with caution — existing data will be lost!
    */
   drzavniEvidenciCleanStart: true,
   /**
    * ⚠️⚠️⚠️ DANGER ZONE ⚠️⚠️⚠️
    * If true, deletes ALL `StrukturiNaPrihodi*` tables (Temp and Original)
    * before recreating them. This gives you a clean start.
    * Use with caution — existing data will be lost!
    */
   strukturiNaPrihodiCleanStart: true
};
