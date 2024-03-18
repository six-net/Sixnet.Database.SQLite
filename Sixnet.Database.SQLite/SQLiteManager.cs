using System.Collections.Concurrent;
using System.Data;
using Microsoft.Data.Sqlite;
using Sixnet.Development.Data;
using Sixnet.Development.Data.Database;

namespace Sixnet.Database.SQLite
{
    /// <summary>
    /// Defines sqlite manager
    /// </summary>
    internal static class SQLiteManager
    {
        #region Fields

        /// <summary>
        /// Gets current database server type
        /// </summary>
        internal const DatabaseType CurrentDatabaseServerType = DatabaseType.SQLite;

        /// <summary>
        /// Key word prefix
        /// </summary>
        internal const string KeywordPrefix = "`";

        /// <summary>
        /// Key word suffix
        /// </summary>
        internal const string KeywordSuffix = "`";

        /// <summary>
        /// Default query translator
        /// </summary>
        static readonly SQLiteDataCommandResolver DefaultResolver = new SQLiteDataCommandResolver();

        ///// <summary>
        ///// Sqlite connections
        ///// </summary>
        //static ConcurrentDictionary<string, IDbConnection> Connections = new ConcurrentDictionary<string, IDbConnection>();

        //internal static readonly ConcurrentDictionary<string, object> ServerLocks = new ConcurrentDictionary<string, object>();

        #endregion

        #region Get database connection

        /// <summary>
        /// Get database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns>Return database connection</returns>
        public static IDbConnection GetConnection(DatabaseServer server)
        {
            //var serverIdentityValue = server.GetServerIdentityValue();
            //if (!Connections.TryGetValue(serverIdentityValue, out var conn))
            //{
            //    lock (Connections)
            //    {
            //        if (Connections.TryGetValue(serverIdentityValue, out conn))
            //        {
            //            return conn;
            //        }
            //        conn = DataManager.GetDatabaseConnection(server) ?? new SqliteConnection(server.ConnectionString);
            //        Connections[serverIdentityValue] = conn;
            //    }
            //}
            //return conn;
           var conn = SixnetDataManager.GetDatabaseConnection(server) ?? new SqliteConnection(server.ConnectionString);
            //if(!ServerLocks.TryGetValue(serverIdentityValue,out var serverLock))
            //{
            //    lock(ServerLocks)
            //    {  

            //    }
            //}
            return conn;
        }

        #endregion

        #region Wrap keyword

        /// <summary>
        /// Wrap keyword by the KeywordPrefix and the KeywordSuffix
        /// </summary>
        /// <param name="originalValue">Original value</param>
        /// <returns></returns>
        internal static string WrapKeyword(string originalValue)
        {
            return $"{KeywordPrefix}{originalValue}{KeywordSuffix}";
        }

        #endregion

        #region Get command resolver

        /// <summary>
        /// Get command resolver
        /// </summary>
        /// <returns>Return a command resolver</returns>
        internal static SQLiteDataCommandResolver GetCommandResolver()
        {
            return DefaultResolver;
        }

        #endregion
    }
}
