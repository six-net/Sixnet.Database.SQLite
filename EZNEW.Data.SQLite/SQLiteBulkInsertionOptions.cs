using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Data.SQLite
{
    /// <summary>
    /// Defines sqlite bulk insertion options
    /// </summary>
    public class SQLiteBulkInsertionOptions : IBulkInsertionOptions
    {
        /// <summary>
        /// Indicates whether use transaction
        /// </summary>
        public bool UseTransaction { get; set; }
    }
}
