using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Dapper;
using EZNEW.Data.Configuration;
using EZNEW.Development.Command;
using EZNEW.Development.Query;
using EZNEW.Development.Query.Translation;
using EZNEW.Development.Entity;
using EZNEW.Exceptions;
using EZNEW.Data.Modification;

namespace EZNEW.Data.SQLite
{
    /// <summary>
    /// Imeplements database provider for the SQLite
    /// </summary>
    public class SQLiteProvider : IDatabaseProvider
    {
        const DatabaseServerType CurrentDatabaseServerType = SQLiteManager.CurrentDatabaseServerType;

        #region Execute

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<ICommand> commands)
        {
            return ExecuteAsync(server, executionOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executionOptions, params ICommand[] commands)
        {
            return ExecuteAsync(server, executionOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executionOptions, params ICommand[] commands)
        {
            IEnumerable<ICommand> cmdCollection = commands;
            return await ExecuteAsync(server, executionOptions, cmdCollection).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<ICommand> commands)
        {
            #region group execution commands

            IQueryTranslator translator = SQLiteManager.GetQueryTranslator(DataAccessContext.Create(server));
            List<DatabaseExecutionCommand> databaseExecutionCommands = new List<DatabaseExecutionCommand>();
            var batchExecutionConfig = DataManager.GetBatchExecutionConfiguration(server.ServerType) ?? BatchExecutionConfiguration.Default;
            var groupStatementsCount = batchExecutionConfig.GroupStatementsCount;
            groupStatementsCount = groupStatementsCount < 0 ? 1 : groupStatementsCount;
            var groupParameterCount = batchExecutionConfig.GroupParametersCount;
            groupParameterCount = groupParameterCount < 0 ? 1 : groupParameterCount;
            StringBuilder commandTextBuilder = new StringBuilder();
            CommandParameters parameters = null;
            int statementsCount = 0;
            bool forceReturnValue = false;
            int cmdCount = 0;

            DatabaseExecutionCommand GetGroupExecuteCommand()
            {
                var executionCommand = new DatabaseExecutionCommand()
                {
                    CommandText = commandTextBuilder.ToString(),
                    CommandType = CommandType.Text,
                    MustAffectedData = forceReturnValue,
                    Parameters = parameters
                };
                statementsCount = 0;
                translator.ParameterSequence = 0;
                commandTextBuilder.Clear();
                parameters = null;
                forceReturnValue = false;
                return executionCommand;
            }

            foreach (var cmd in commands)
            {
                DatabaseExecutionCommand databaseExecutionCommand = GetDatabaseExecutionCommand(translator, cmd as DefaultCommand);
                if (databaseExecutionCommand == null)
                {
                    continue;
                }

                //Trace log
                SQLiteManager.LogExecutionCommand(databaseExecutionCommand);

                cmdCount++;
                if (databaseExecutionCommand.PerformAlone)
                {
                    if (statementsCount > 0)
                    {
                        databaseExecutionCommands.Add(GetGroupExecuteCommand());
                    }
                    databaseExecutionCommands.Add(databaseExecutionCommand);
                    continue;
                }
                commandTextBuilder.AppendLine(databaseExecutionCommand.CommandText);
                parameters = parameters == null ? databaseExecutionCommand.Parameters : parameters.Union(databaseExecutionCommand.Parameters);
                forceReturnValue |= databaseExecutionCommand.MustAffectedData;
                statementsCount++;
                if (translator.ParameterSequence >= groupParameterCount || statementsCount >= groupStatementsCount)
                {
                    databaseExecutionCommands.Add(GetGroupExecuteCommand());
                }
            }
            if (statementsCount > 0)
            {
                databaseExecutionCommands.Add(GetGroupExecuteCommand());
            }

            #endregion

            return await ExecuteDatabaseCommandAsync(server, executionOptions, databaseExecutionCommands, executionOptions?.ExecutionByTransaction ?? cmdCount > 1).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute database command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="databaseExecutionCommands">Database execution commands</param>
        /// <param name="useTransaction">Whether use transaction</param>
        /// <returns>Return affected data number</returns>
        async Task<int> ExecuteDatabaseCommandAsync(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<DatabaseExecutionCommand> databaseExecutionCommands, bool useTransaction)
        {
            int resultValue = 0;
            bool success = true;
            using (var conn = SQLiteManager.GetConnection(server))
            {
                IDbTransaction transaction = null;
                if (useTransaction)
                {
                    transaction = SQLiteManager.GetExecutionTransaction(conn, executionOptions);
                }
                try
                {
                    foreach (var cmd in databaseExecutionCommands)
                    {
                        var cmdDefinition = new CommandDefinition(cmd.CommandText, SQLiteManager.ConvertCmdParameters(cmd.Parameters), transaction: transaction, commandType: cmd.CommandType, cancellationToken: executionOptions?.CancellationToken ?? default);
                        var executeResultValue = await conn.ExecuteAsync(cmdDefinition).ConfigureAwait(false);
                        success = success && (cmd.MustAffectedData ? executeResultValue > 0 : true);
                        resultValue += executeResultValue;
                        if (useTransaction && !success)
                        {
                            break;
                        }
                    }
                    if (!useTransaction)
                    {
                        return resultValue;
                    }
                    if (success)
                    {
                        transaction.Commit();
                    }
                    else
                    {
                        resultValue = 0;
                        transaction.Rollback();
                    }
                    return resultValue;
                }
                catch (Exception ex)
                {
                    resultValue = 0;
                    transaction?.Rollback();
                    throw ex;
                }
            }
        }

        /// <summary>
        /// Get database execution command
        /// </summary>
        /// <param name="queryTranslator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database execution command</returns>
        DatabaseExecutionCommand GetDatabaseExecutionCommand(IQueryTranslator queryTranslator, DefaultCommand command)
        {
            DatabaseExecutionCommand GetTextCommand()
            {
                return new DatabaseExecutionCommand()
                {
                    CommandText = command.Text,
                    Parameters = SQLiteManager.ConvertParameter(command.Parameters),
                    CommandType = SQLiteManager.GetCommandType(command),
                    MustAffectedData = command.MustAffectedData,
                    HasPreScript = true
                };
            }
            if (command.ExecutionMode == CommandExecutionMode.CommandText)
            {
                return GetTextCommand();
            }
            DatabaseExecutionCommand databaseExecutionCommand;
            switch (command.OperationType)
            {
                case CommandOperationType.Insert:
                    databaseExecutionCommand = GetDatabaseInsertionCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Update:
                    databaseExecutionCommand = GetDatabaseUpdateCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Delete:
                    databaseExecutionCommand = GetDatabaseDeletionCommand(queryTranslator, command);
                    break;
                default:
                    databaseExecutionCommand = GetTextCommand();
                    break;
            }
            return databaseExecutionCommand;
        }

        /// <summary>
        /// Get database insertion execution command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database insertion command</returns>
        DatabaseExecutionCommand GetDatabaseInsertionCommand(IQueryTranslator translator, DefaultCommand command)
        {
            translator.DataAccessContext.SetCommand(command);
            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            var fields = DataManager.GetEditFields(CurrentDatabaseServerType, command.EntityType);
            var fieldCount = fields.GetCount();
            var insertFormatResult = SQLiteManager.FormatInsertionFields(command.EntityType, fieldCount, fields, command.Parameters, translator.ParameterSequence);
            if (insertFormatResult == null)
            {
                return null;
            }
            string cmdText = $"INSERT INTO {SQLiteManager.WrapKeyword(objectName)} ({string.Join(",", insertFormatResult.Item1)}) VALUES ({string.Join(",", insertFormatResult.Item2)});";
            CommandParameters parameters = insertFormatResult.Item3;
            translator.ParameterSequence += fieldCount;
            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = SQLiteManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters
            };
        }

        /// <summary>
        /// Get database update command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database update command</returns>
        DatabaseExecutionCommand GetDatabaseUpdateCommand(IQueryTranslator translator, DefaultCommand command)
        {
            if (command?.Fields.IsNullOrEmpty() ?? true)
            {
                throw new EZNEWException($"No fields are set to update");
            }

            #region query translation

            translator.DataAccessContext.SetCommand(command);
            var tranResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
            {
                conditionString += "WHERE " + tranResult.ConditionString;
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script 

            CommandParameters parameters = SQLiteManager.ConvertParameter(command.Parameters) ?? new CommandParameters();
            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            var fields = SQLiteManager.GetFields(command.EntityType, command.Fields);
            int parameterSequence = translator.ParameterSequence;
            List<string> updateSetArray = new List<string>();
            foreach (var field in fields)
            {
                var parameterValue = parameters.GetParameterValue(field.PropertyName);
                var parameterName = field.PropertyName;
                string newValueExpression = string.Empty;
                if (parameterValue != null)
                {
                    parameterSequence++;
                    parameterName = SQLiteManager.FormatParameterName(parameterName, parameterSequence);
                    parameters.Rename(field.PropertyName, parameterName);
                    if (parameterValue is IModificationValue)
                    {
                        var modifyValue = parameterValue as IModificationValue;
                        parameters.ModifyValue(parameterName, modifyValue.Value);
                        if (parameterValue is CalculationModificationValue)
                        {
                            var calculateModifyValue = parameterValue as CalculationModificationValue;
                            string calChar = SQLiteManager.GetSystemCalculationOperator(calculateModifyValue.Operator);
                            newValueExpression = $"{translator.ObjectPetName}.{SQLiteManager.WrapKeyword(field.FieldName)}{calChar}{SQLiteManager.ParameterPrefix}{parameterName}";
                        }
                    }
                }
                if (string.IsNullOrWhiteSpace(newValueExpression))
                {
                    newValueExpression = $"{SQLiteManager.ParameterPrefix}{parameterName}";
                }
                updateSetArray.Add($"{SQLiteManager.WrapKeyword(field.FieldName)}={newValueExpression}");
            }
            string cmdText = string.Empty;
            string wrapObjectName = SQLiteManager.WrapKeyword(objectName);
            if (string.IsNullOrWhiteSpace(joinScript))
            {
                cmdText = $"{preScript}UPDATE {wrapObjectName} AS {translator.ObjectPetName} SET {string.Join(",", updateSetArray)} {conditionString};";
            }
            else
            {
                var primaryKeyFields = DataManager.GetFields(CurrentDatabaseServerType, command.EntityType, EntityManager.GetPrimaryKeys(command.EntityType)).ToList();
                if (primaryKeyFields.IsNullOrEmpty())
                {
                    throw new EZNEWException($"{command.EntityType?.FullName} not set primary key");
                }
                string updateTableShortName = "UTB";

                cmdText = $"{preScript}UPDATE {wrapObjectName} AS {updateTableShortName} SET {string.Join(",", updateSetArray)} WHERE {string.Join("||", primaryKeyFields.Select(pk => updateTableShortName + "." + SQLiteManager.WrapKeyword(pk.FieldName)))} IN (SELECT {string.Join("||", primaryKeyFields.Select(pk => translator.ObjectPetName + "." + SQLiteManager.WrapKeyword(pk.FieldName)))} FROM {wrapObjectName} AS {translator.ObjectPetName} {joinScript} {conditionString});";
            }
            translator.ParameterSequence = parameterSequence;

            #endregion

            #region parameter

            var queryParameters = SQLiteManager.ConvertParameter(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = SQLiteManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        /// <summary>
        /// Get database deletion command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database deletion command</returns>
        DatabaseExecutionCommand GetDatabaseDeletionCommand(IQueryTranslator translator, DefaultCommand command)
        {
            translator.DataAccessContext.SetCommand(command);

            #region query translation

            var tranResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
            {
                conditionString += "WHERE " + tranResult.ConditionString;
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            string cmdText = string.Empty;
            string wrapObjectName = SQLiteManager.WrapKeyword(objectName);
            if (string.IsNullOrWhiteSpace(joinScript))
            {
                cmdText = $"{preScript}DELETE FROM {wrapObjectName} AS {translator.ObjectPetName} {conditionString};";
            }
            else
            {
                var primaryKeyFields = DataManager.GetFields(CurrentDatabaseServerType, command.EntityType, EntityManager.GetPrimaryKeys(command.EntityType)).ToList();
                if (primaryKeyFields.IsNullOrEmpty())
                {
                    throw new EZNEWException($"{command.EntityType?.FullName} not set primary key");
                }
                string deleteTableShortName = "DTB";
                cmdText = $"{preScript}DELETE FROM {wrapObjectName} AS {deleteTableShortName} WHERE {string.Join("||", primaryKeyFields.Select(pk => deleteTableShortName + "." + SQLiteManager.WrapKeyword(pk.FieldName)))} IN (SELECT {string.Join("||", primaryKeyFields.Select(pk => translator.ObjectPetName + "." + SQLiteManager.WrapKeyword(pk.FieldName)))} FROM {wrapObjectName} AS {translator.ObjectPetName} {joinScript} {conditionString});";
            }

            #endregion

            #region parameter

            CommandParameters parameters = SQLiteManager.ConvertParameter(command.Parameters) ?? new CommandParameters();
            var queryParameters = SQLiteManager.ConvertParameter(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = SQLiteManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        #endregion

        #region Query

        /// <summary>
        /// Query datas
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return datas</returns>
        public IEnumerable<T> Query<T>(DatabaseServer server, ICommand command)
        {
            return QueryAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query datas
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return datas</returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            IQueryTranslator translator = SQLiteManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var tranResult = translator.Translate(command.Query);
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    int size = command.Query.QuerySize;
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    string orderString = string.IsNullOrWhiteSpace(tranResult.SortString) ? string.Empty : $"ORDER BY {tranResult.SortString}";
                    var queryFields = SQLiteManager.GetQueryFields(command.Query, command.EntityType, true);
                    string outputFormatedField = string.Join(",", SQLiteManager.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    if (string.IsNullOrWhiteSpace(tranResult.CombineScript))
                    {
                        cmdText = $"{tranResult.PreScript}SELECT {outputFormatedField} FROM {SQLiteManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {orderString} {(size > 0 ? $"LIMIT 0,{size}" : string.Empty)}";
                    }
                    else
                    {
                        string innerFormatedField = string.Join(",", SQLiteManager.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                        cmdText = $"{tranResult.PreScript}SELECT {outputFormatedField} FROM (SELECT {innerFormatedField} FROM {SQLiteManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}) AS {translator.ObjectPetName} {orderString} {(size > 0 ? $"LIMIT 0,{size}" : string.Empty)}";
                    }
                    break;
            }

            #endregion

            #region parameter

            var parameters = SQLiteManager.ConvertCmdParameters(SQLiteManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            SQLiteManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = SQLiteManager.GetConnection(server))
            {
                var tran = SQLiteManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: SQLiteManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query paging data
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return paging data</returns>
        public IEnumerable<T> QueryPaging<T>(DatabaseServer server, ICommand command)
        {
            return QueryPagingAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query paging data
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return paging data</returns>
        public async Task<IEnumerable<T>> QueryPagingAsync<T>(DatabaseServer server, ICommand command)
        {
            int beginIndex = 0;
            int pageSize = 1;
            if (command?.Query?.PagingInfo != null)
            {
                beginIndex = command.Query.PagingInfo.Page;
                pageSize = command.Query.PagingInfo.PageSize;
                beginIndex = (beginIndex - 1) * pageSize;
            }
            return await QueryOffsetAsync<T>(server, command, beginIndex, pageSize).ConfigureAwait(false);
        }

        /// <summary>
        /// Query datas offset the specified numbers
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="size">Query size</param>
        /// <returns>Return datas</returns>
        public IEnumerable<T> QueryOffset<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            return QueryOffsetAsync<T>(server, command, offsetNum, size).Result;
        }

        /// <summary>
        /// Query datas offset the specified numbers
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="size">Query size</param>
        /// <returns>Return datas</returns>
        public async Task<IEnumerable<T>> QueryOffsetAsync<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            IQueryTranslator translator = SQLiteManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var tranResult = translator.Translate(command.Query);

            #endregion

            #region script

            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;
            string cmdText;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    string limitString = $"LIMIT {offsetNum},{size}";
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    string defaultFieldName = SQLiteManager.GetDefaultFieldName(command.EntityType);
                    var queryFields = SQLiteManager.GetQueryFields(command.Query, command.EntityType, true);
                    string innerFormatedField = string.Join(",", SQLiteManager.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                    string outputFormatedField = string.Join(",", SQLiteManager.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    string queryScript = $"SELECT {innerFormatedField} FROM {SQLiteManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}";
                    cmdText = $"{(string.IsNullOrWhiteSpace(tranResult.PreScript) ? $"WITH {SQLiteManager.PagingTableName} AS ({queryScript})" : $"{tranResult.PreScript},{SQLiteManager.PagingTableName} AS ({queryScript})")}SELECT (SELECT COUNT({SQLiteManager.WrapKeyword(defaultFieldName)}) FROM {SQLiteManager.PagingTableName}) AS {DataManager.PagingTotalCountFieldName},{outputFormatedField} FROM {SQLiteManager.PagingTableName} AS {translator.ObjectPetName} ORDER BY {(string.IsNullOrWhiteSpace(tranResult.SortString) ? $"{translator.ObjectPetName}.{SQLiteManager.WrapKeyword(defaultFieldName)} DESC" : $"{tranResult.SortString}")} {limitString}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = SQLiteManager.ConvertCmdParameters(SQLiteManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            SQLiteManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = SQLiteManager.GetConnection(server))
            {
                var tran = SQLiteManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: SQLiteManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Indecats whether exists data
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether exists data</returns>
        public bool Exists(DatabaseServer server, ICommand command)
        {
            return ExistsAsync(server, command).Result;
        }

        /// <summary>
        /// Indecats whether exists data
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether exists data</returns>
        public async Task<bool> ExistsAsync(DatabaseServer server, ICommand command)
        {
            #region query translation

            var translator = SQLiteManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            command.Query.ClearQueryFields();
            var queryFields = EntityManager.GetPrimaryKeys(command.EntityType).ToArray();
            if (queryFields.IsNullOrEmpty())
            {
                queryFields = EntityManager.GetQueryFields(command.EntityType).ToArray();
            }
            command.Query.AddQueryFields(queryFields);
            var tranResult = translator.Translate(command.Query);
            string conditionString = string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}";
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            string cmdText = $"{preScript}SELECT EXISTS(SELECT {string.Join(",", SQLiteManager.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false))} FROM {SQLiteManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {conditionString} {tranResult.CombineScript})";

            #endregion

            #region parameter

            var parameters = SQLiteManager.ConvertCmdParameters(SQLiteManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            SQLiteManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = SQLiteManager.GetConnection(server))
            {
                var tran = SQLiteManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, cancellationToken: command.Query?.GetCancellationToken() ?? default);
                int value = await conn.ExecuteScalarAsync<int>(cmdDefinition).ConfigureAwait(false);
                return value > 0;
            }
        }

        /// <summary>
        /// Query aggregation value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return aggregation value</returns>
        public T AggregateValue<T>(DatabaseServer server, ICommand command)
        {
            return AggregateValueAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query aggregation value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return aggregation value</returns>
        public async Task<T> AggregateValueAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            bool queryObject = command.Query.ExecutionMode == QueryExecutionMode.QueryObject;
            string funcName = SQLiteManager.GetAggregateFunctionName(command.OperationType);
            EntityField defaultField = null;
            if (queryObject)
            {
                if (string.IsNullOrWhiteSpace(funcName))
                {
                    throw new NotSupportedException($"Not support {command.OperationType}");
                }
                if (SQLiteManager.CheckAggregationOperationMustNeedField(command.OperationType))
                {
                    if (command.Query.QueryFields.IsNullOrEmpty())
                    {
                        throw new EZNEWException($"You must specify the field to perform for the {funcName} operation");
                    }
                    defaultField = DataManager.GetField(CurrentDatabaseServerType, command.EntityType, command.Query.QueryFields.First());
                }
                else
                {
                    defaultField = DataManager.GetDefaultField(CurrentDatabaseServerType, command.EntityType);
                }
                //combine fields
                if (!command.Query.Combines.IsNullOrEmpty())
                {
                    var combineKeys = EntityManager.GetPrimaryKeys(command.EntityType).Union(new string[1] { defaultField.PropertyName }).ToArray();
                    command.Query.ClearQueryFields();
                    foreach (var combineItem in command.Query.Combines)
                    {
                        combineItem.Query.ClearQueryFields();
                        if (combineKeys.IsNullOrEmpty())
                        {
                            combineItem.Query.ClearNotQueryFields();
                            command.Query.ClearNotQueryFields();
                        }
                        else
                        {
                            command.Query.AddQueryFields(combineKeys);
                            if (combineItem.Type == CombineType.Union || combineItem.Type == CombineType.UnionAll)
                            {
                                combineItem.Query.AddQueryFields(combineKeys);
                            }
                        }
                    }
                }
            }
            IQueryTranslator translator = SQLiteManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var tranResult = translator.Translate(command.Query);

            #endregion

            #region script

            string cmdText;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    cmdText = string.IsNullOrWhiteSpace(tranResult.CombineScript)
                        ? $"{tranResult.PreScript}SELECT {funcName}({SQLiteManager.FormatField(translator.ObjectPetName, defaultField, false)}) FROM {SQLiteManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")}"
                        : $"{tranResult.PreScript}SELECT {funcName}({SQLiteManager.FormatField(translator.ObjectPetName, defaultField, false)}) FROM (SELECT {string.Join(",", SQLiteManager.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false))} FROM {SQLiteManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}) AS {translator.ObjectPetName}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = SQLiteManager.ConvertCmdParameters(SQLiteManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            SQLiteManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = SQLiteManager.GetConnection(server))
            {
                var tran = SQLiteManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: SQLiteManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.ExecuteScalarAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query data set
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return data set</returns>
        public async Task<DataSet> QueryMultipleAsync(DatabaseServer server, ICommand command)
        {
            //Trace log
            SQLiteManager.LogScript(command.Text, command.Parameters);
            using (var conn = SQLiteManager.GetConnection(server))
            {
                var tran = SQLiteManager.GetQueryTransaction(conn, command.Query);
                DynamicParameters parameters = SQLiteManager.ConvertCmdParameters(SQLiteManager.ConvertParameter(command.Parameters));
                var cmdDefinition = new CommandDefinition(command.Text, parameters, transaction: tran, commandType: SQLiteManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                using (var reader = await conn.ExecuteReaderAsync(cmdDefinition).ConfigureAwait(false))
                {
                    DataSet dataSet = new DataSet();
                    while (!reader.IsClosed && reader.Read())
                    {
                        DataTable dataTable = new DataTable();
                        dataTable.Load(reader);
                        dataSet.Tables.Add(dataTable);
                    }
                    return dataSet;
                }
            }
        }

        #endregion

        #region Bulk

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public void BulkInsert(DatabaseServer server, DataTable dataTable, IBulkInsertionOptions bulkInsertOptions = null)
        {
            BulkInsertAsync(server, dataTable).Wait();
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public async Task BulkInsertAsync(DatabaseServer server, DataTable dataTable, IBulkInsertionOptions bulkInsertOptions = null)
        {
            if (server == null)
            {
                throw new ArgumentNullException(nameof(server));
            }
            if (dataTable == null)
            {
                throw new ArgumentNullException(nameof(dataTable));
            }
            using (var conn = new SqliteConnection(server?.ConnectionString))
            {
                try
                {
                    conn.Open();
                    SqliteTransaction tran = null;
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
                    List<string> columns = new List<string>(dataTable.Columns.Count);
                    Dictionary<string, SqliteParameter> parameters = new Dictionary<string, SqliteParameter>(dataTable.Columns.Count);
                    var command = conn.CreateCommand();
                    foreach (DataColumn col in dataTable.Columns)
                    {
                        columns.Add(col.ColumnName);
                        SqliteParameter parameter = command.CreateParameter();
                        parameter.ParameterName = $"{SQLiteManager.ParameterPrefix}{col.ColumnName}";
                        parameters[col.ColumnName] = parameter;
                        command.Parameters.Add(parameter);
                    }

                    command.CommandText = $@"INSERT INTO {dataTable.TableName} 
({string.Join(",", columns.Select(c => $"{SQLiteManager.KeywordPrefix}{c}{SQLiteManager.KeywordSuffix}"))}) 
VALUES ({string.Join(",", columns.Select(c => $"{SQLiteManager.ParameterPrefix}{c}"))})";

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

        #endregion
    }
}
