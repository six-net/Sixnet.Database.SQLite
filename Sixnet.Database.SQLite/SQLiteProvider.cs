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
        protected override ISixnetDataCommandResolver GetDataCommandResolver()
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
        public override async Task BulkInsertAsync(BulkInsertDatabaseCommand databaseBulkInsertCommand)
        {
            try
            {
                var dataTable = databaseBulkInsertCommand.DataTable;
                SixnetDirectThrower.ThrowArgNullIf(dataTable == null, nameof(BulkInsertDatabaseCommand.DataTable));
                var sqliteResolver = new SQLiteDataCommandResolver();
                var conn = databaseBulkInsertCommand.Connection.DbConnection as SqliteConnection;
                var bulkInsertOptions = databaseBulkInsertCommand.BulkInsertionOptions;
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
            }
            catch (Exception ex)
            {
                throw GetSqlException(ex);
            }
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="databaseBulkInsertCommand">Database command</param>
        public override void BulkInsert(BulkInsertDatabaseCommand databaseBulkInsertCommand)
        {
            try
            {
                var dataTable = databaseBulkInsertCommand.DataTable;
                SixnetDirectThrower.ThrowArgNullIf(dataTable == null, nameof(BulkInsertDatabaseCommand.DataTable));
                var sqliteResolver = new SQLiteDataCommandResolver();
                var conn = databaseBulkInsertCommand.Connection.DbConnection as SqliteConnection;
                var bulkInsertOptions = databaseBulkInsertCommand.BulkInsertionOptions;
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
            }
            catch (Exception ex)
            {
                throw GetSqlException(ex);
            }
        }

        #endregion

        #region Get exception

        protected override Exception GetSqlException(Exception ex)
        {
            if (ex is SqliteException sqlException)
            {
                switch (sqlException.SqliteErrorCode)
                {
                    case 19:
                        return new SixnetSqlAlreadExistsException(sqlException.Message, sqlException);
                }
            }
            return ex;
        }

        #endregion
    }
}
