using Sixnet.Development.Data.Database;

namespace Sixnet.Database.SQLite
{
    /// <summary>
    /// Defines sqlite bulk insertion options
    /// </summary>
    public class SQLiteBulkInsertionOptions : ISixnetBulkInsertionOptions
    {
        /// <summary>
        /// Indicates whether use transaction
        /// </summary>
        public bool UseTransaction { get; set; }
    }
}
