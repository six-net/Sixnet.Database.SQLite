using System;
using System.Collections.Concurrent;
using System.Data;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using Sixnet.Development.Data;
using Sixnet.Development.Data.Database;

namespace Sixnet.Database.SQLite
{
    /// <summary>
    /// Defines sqlite manager
    /// </summary>
    internal static class SixnetSqliteManager
    {
        #region Fields

        /// <summary>
        /// Default query translator
        /// </summary>
        internal static readonly SixnetSqliteDataCommandResolver DefaultResolver = new();

        #endregion

        #region Get database connection

        /// <summary>
        /// Get database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns>Return database connection</returns>
        public static IDbConnection GetConnection(SixnetDatabaseServer server)
        {
            var conn = SixnetDataManager.GetDatabaseConnection(server) ?? RegisterCustomFunctions(new SqliteConnection(SixnetDataManager.ResolveConnectionString(server)));
            return conn;
        }

        #endregion

        #region Get command resolver

        /// <summary>
        /// Get command resolver
        /// </summary>
        /// <returns>Return a command resolver</returns>
        internal static SixnetSqliteDataCommandResolver GetCommandResolver()
        {
            return DefaultResolver;
        }

        #endregion

        #region Register functions

        static SqliteConnection RegisterCustomFunctions(SqliteConnection connection)
        {
            #region IndexOf

            connection.CreateFunction<string, string, int>("SNT_INDEX_OF", (input, value) =>
            {
                if (input == null || value == null)
                {
                    return -1;
                }
                return input.IndexOf(value, StringComparison.Ordinal);
            });

            connection.CreateFunction<string, string, int, int, int>("SNT_INDEX_OF", (input, value, startIndex, count) =>
            {
                if (input == null || value == null || startIndex < 0 || startIndex >= input.Length)
                {
                    return -1;
                }
                if (count < 0 || startIndex + count > input.Length)
                {
                    count = input.Length - startIndex;
                }
                return input.IndexOf(value, startIndex, count, StringComparison.Ordinal);
            });

            #endregion

            #region IndexOfAny

            connection.CreateFunction<string, string, int>("SNT_INDEX_OF_ANY", (input, chars) =>
            {
                if (input == null || chars == null)
                {
                    return -1;
                }
                return input.IndexOfAny(chars.ToCharArray());
            });

            connection.CreateFunction<string, string, int, int, int>("SNT_INDEX_OF_ANY", (input, chars, startIndex, count) =>
            {
                if (input == null || chars == null || startIndex < 0 || startIndex >= input.Length)
                {
                    return -1;
                }
                if (count < 0 || startIndex + count > input.Length)
                {
                    count = input.Length - startIndex;
                }
                return input.IndexOfAny(chars.ToCharArray(), startIndex, count);
            });

            #endregion

            #region LastIndexOf

            connection.CreateFunction<string, string, int>("SNT_LAST_INDEX_OF", (input, value) =>
            {
                if (input == null || value == null)
                {
                    return -1;
                }
                return input.LastIndexOf(value, StringComparison.Ordinal);
            });

            connection.CreateFunction<string, string, int, int, int>("SNT_LAST_INDEX_OF", (input, value, startIndex, count) =>
            {
                if (input == null || value == null || startIndex < 0 || startIndex >= input.Length)
                {
                    return -1;
                }
                if (count < 0 || startIndex - count + 1 < 0)
                {
                    count = startIndex + 1;
                }
                return input.LastIndexOf(value, startIndex, count, StringComparison.Ordinal);
            });

            #endregion

            #region LastIndexOfAny

            connection.CreateFunction<string, string, int>("SNT_LAST_INDEX_OF_ANY", (input, chars) =>
            {
                if (input == null || chars == null)
                {
                    return -1;
                }
                return input.LastIndexOfAny(chars.ToCharArray());
            });

            connection.CreateFunction<string, string, int, int, int>("SNT_LAST_INDEX_OF_ANY", (input, chars, startIndex, count) =>
            {
                if (input == null || chars == null || startIndex < 0 || startIndex >= input.Length)
                {
                    return -1;
                }
                if (count < 0 || startIndex - count + 1 < 0)
                {
                    count = startIndex + 1;
                }
                return input.LastIndexOfAny(chars.ToCharArray(), startIndex, count);
            });

            #endregion

            #region Replace

            connection.CreateFunction<string, string, string, string>("SNT_REPLACE_IGNORE_CASE", (input, oldValue, newValue) =>
            {
                if (input == null || oldValue == null)
                {
                    return input;
                }
                return Regex.Replace(input, Regex.Escape(oldValue), newValue ?? "", RegexOptions.IgnoreCase);
            });

            #endregion

            return connection;
        }

        #endregion
    }
}
