using Sixnet.Development.Data;
using Sixnet.Development.Data.Database;

namespace Sixnet.Database.SQLite
{
    /// <summary>
    /// Defines sqlite bulk insertion options
    /// </summary>
    public class SixnetSqliteBulkInsertionOptions : ISixnetBulkInsertionOptions
    {
        /// <summary>
        /// Gets or sets the data operation options
        /// </summary>
        public SixnetDataOperationOptions DataOperationOptions { get; set; }
    }
}
