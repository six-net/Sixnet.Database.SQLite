using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Dapper;
using Sixnet.Development.Data.Database;
using Sixnet.Exceptions;

namespace Sixnet.Database.SQLite
{
    /// <summary>
    /// Imeplements database provider for the SQLite
    /// </summary>
    public class SQLiteProvider : BaseDatabaseProvider
    {
        #region Constructor

        public SQLiteProvider()
        {
            queryDatabaseTablesScript = "SELECT NAME AS TableName FROM SQLITE_MASTER WHERE TYPE='table' AND NAME NOT LIKE 'sqlite_%';";
        }

        #endregion

        #region Connection

        /// <summary>
        /// Get database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns></returns>
        public override IDbConnection GetDbConnection(DatabaseServer server)
        {
            return SQLiteManager.GetConnection(server);
        }

        #endregion

        #region Command resolver

        /// <summary>
        /// Get data command resolver
        /// </summary>
        /// <returns></returns>
        protected override IDataCommandResolver GetDataCommandResolver()
        {
            return SQLiteManager.GetCommandResolver();
        }

        #endregion

        #region Parameter

        /// <summary>
        /// Convert data command parametes
        /// </summary>
        /// <param name="parameters">Data command parameters</param>
        /// <returns></returns>
        protected override DynamicParameters ConvertDataCommandParameters(DataCommandParameters parameters)
        {
            return parameters?.ConvertToDynamicParameters(SQLiteManager.CurrentDatabaseServerType);
        }

        #endregion

        #region Bulk

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="databaseBulkInsertCommand">Database command</param>
        public override async Task BulkInsertAsync(DatabaseBulkInsertCommand databaseBulkInsertCommand)
        {
            var server = databaseBulkInsertCommand?.Connection?.DatabaseServer;
            ThrowHelper.ThrowArgNullIf(server == null, nameof(DatabaseBulkInsertCommand.Connection.DatabaseServer));

            var dataTable = databaseBulkInsertCommand.DataTable;
            ThrowHelper.ThrowArgNullIf(dataTable == null, nameof(DatabaseBulkInsertCommand.DataTable));

            var sqliteResolver = new SQLiteDataCommandResolver();

            using (var conn = new SqliteConnection(server?.ConnectionString))
            {
                try
                {
                    conn.Open();
                    SqliteTransaction tran = null;
                    var bulkInsertOptions = databaseBulkInsertCommand.BulkInsertionOptions;
                    if (bulkInsertOptions is SQLiteBulkInsertionOptions sqliteBulkInsertOptions && sqliteBulkInsertOptions != null)
                    {
                        if (sqliteBulkInsertOptions.UseTransaction)
                        {
                            tran = conn.BeginTransaction();
                        }
                    }
                    else //default use transaction
                    {
                        tran = conn.BeginTransaction();
                    }
                    var columns = new List<string>(dataTable.Columns.Count);
                    var parameters = new Dictionary<string, SqliteParameter>(dataTable.Columns.Count);
                    var command = conn.CreateCommand();
                    foreach (DataColumn col in dataTable.Columns)
                    {
                        columns.Add(col.ColumnName);
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = $"{sqliteResolver.FormatParameterName(col.ColumnName)}";
                        parameters[col.ColumnName] = parameter;
                        command.Parameters.Add(parameter);
                    }

                    command.CommandText = $@"INSERT INTO {dataTable.TableName} 
({string.Join(",", columns.Select(c => $"{SQLiteManager.KeywordPrefix}{c}{SQLiteManager.KeywordSuffix}"))}) 
VALUES ({string.Join(",", columns.Select(c => $"{sqliteResolver.FormatParameterName(c)}"))})";

                    foreach (DataRow row in dataTable.Rows)
                    {
                        foreach (var parameterItem in parameters)
                        {
                            parameterItem.Value.Value = row[parameterItem.Key];
                        }
                        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                    if (tran != null)
                    {
                        tran.Commit();
                        tran.Dispose();
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    if (conn != null && conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                }
            }
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="databaseBulkInsertCommand">Database command</param>
        public override void BulkInsert(DatabaseBulkInsertCommand databaseBulkInsertCommand)
        {
            var server = databaseBulkInsertCommand?.Connection?.DatabaseServer;
            ThrowHelper.ThrowArgNullIf(server == null, nameof(DatabaseBulkInsertCommand.Connection.DatabaseServer));

            var dataTable = databaseBulkInsertCommand.DataTable;
            ThrowHelper.ThrowArgNullIf(dataTable == null, nameof(DatabaseBulkInsertCommand.DataTable));

            var sqliteResolver = new SQLiteDataCommandResolver();

            using (var conn = new SqliteConnection(server?.ConnectionString))
            {
                try
                {
                    conn.Open();
                    SqliteTransaction tran = null;
                    var bulkInsertOptions = databaseBulkInsertCommand.BulkInsertionOptions;
                    if (bulkInsertOptions is SQLiteBulkInsertionOptions sqliteBulkInsertOptions && sqliteBulkInsertOptions != null)
                    {
                        if (sqliteBulkInsertOptions.UseTransaction)
                        {
                            tran = conn.BeginTransaction();
                        }
                    }
                    else //default use transaction
                    {
                        tran = conn.BeginTransaction();
                    }
                    var columns = new List<string>(dataTable.Columns.Count);
                    var parameters = new Dictionary<string, SqliteParameter>(dataTable.Columns.Count);
                    var command = conn.CreateCommand();
                    foreach (DataColumn col in dataTable.Columns)
                    {
                        columns.Add(col.ColumnName);
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = $"{sqliteResolver.FormatParameterName(col.ColumnName)}";
                        parameters[col.ColumnName] = parameter;
                        command.Parameters.Add(parameter);
                    }

                    command.CommandText = $@"INSERT INTO {dataTable.TableName} 
({string.Join(",", columns.Select(c => $"{SQLiteManager.KeywordPrefix}{c}{SQLiteManager.KeywordSuffix}"))}) 
VALUES ({string.Join(",", columns.Select(c => $"{sqliteResolver.FormatParameterName(c)}"))})";

                    foreach (DataRow row in dataTable.Rows)
                    {
                        foreach (var parameterItem in parameters)
                        {
                            parameterItem.Value.Value = row[parameterItem.Key];
                        }
                        command.ExecuteNonQuery();
                    }
                    if (tran != null)
                    {
                        tran.Commit();
                        tran.Dispose();
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    if (conn != null && conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                }
            }
        }

        #endregion
    }
}
