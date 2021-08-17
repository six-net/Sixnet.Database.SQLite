using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Data.SQLite
{
    public class SQLiteBulkInsertOptions : IBulkInsertOptions
    {
        /// <summary>
        /// Whether use transaction
        /// </summary>
        public bool UseTransaction { get; set; }
    }
}
