using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

using Sixnet.Development.Data;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Dapper;
using Sixnet.Development.Data.Database;
using Sixnet.Development.Data.Field;
using Sixnet.Development.Entity;
using Sixnet.Development.Queryable;
using Sixnet.Exceptions;

namespace Sixnet.Database.SQLite
{
    /// <summary>
    /// Defines command resolver for sqlite
    /// </summary>
    internal partial class SQLiteDataCommandResolver : BaseDataCommandResolver
    {
        #region Constructor

        public SQLiteDataCommandResolver()
        {
            DatabaseType = DatabaseType.SQLite;
            DefaultFieldFormatter = new SQLiteDefaultFieldFormatter();
            ParameterPrefix = "@";
            FormatKeywordFunc = SQLiteManager.FormatKeyword;
            WrapKeywordFunc = SQLiteManager.WrapKeyword;
            RecursiveKeyword = "WITH RECURSIVE";
            DbTypeDefaultValues = new Dictionary<DbType, string>()
            {
                { DbType.Byte, "0" },
                { DbType.SByte, "0" },
                { DbType.Int16, "0" },
                { DbType.UInt16, "0" },
                { DbType.Int32, "0" },
                { DbType.UInt32, "0" },
                { DbType.Int64, "0" },
                { DbType.UInt64, "0" },
                { DbType.Single, "0" },
                { DbType.Double, "0" },
                { DbType.Decimal, "0" },
                { DbType.Boolean, "0" },
                { DbType.String, "''" },
                { DbType.StringFixedLength, "''" },
                { DbType.Guid, Guid.Empty.ToString() },
                { DbType.DateTime, "CURRENT_TIMESTAMP" },
                { DbType.DateTime2, "CURRENT_TIMESTAMP" },
                { DbType.DateTimeOffset, "CURRENT_TIMESTAMP" },
                { DbType.Time, "TIME()" }
            };
        }

        #endregion

        #region Get query statement

        /// <summary>
        /// Get query statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <param name="translationResult">Queryable translation result</param>
        /// <param name="location">Queryable location</param>
        /// <returns></returns>
        protected override QueryDatabaseStatement GenerateQueryStatementCore(DataCommandResolveContext context, QueryableTranslationResult translationResult, QueryableLocation location)
        {
            var queryable = translationResult.GetOriginalQueryable();
            string sqlStatement;
            IEnumerable<ISixnetField> outputFields = null;
            switch (queryable.ExecutionMode)
            {
                case QueryableExecutionMode.Script:
                    sqlStatement = translationResult.GetCondition();
                    break;
                case QueryableExecutionMode.Regular:
                default:
                    // table pet name
                    var tablePetName = context.GetTablePetName(queryable, queryable.GetModelType());
                    //sort
                    var sort = translationResult.GetSort();
                    var hasSort = !string.IsNullOrWhiteSpace(sort);
                    //limit
                    var limit = GetLimitString(queryable.SkipCount, queryable.TakeCount, hasSort);
                    //combine
                    var combine = translationResult.GetCombine();
                    var hasCombine = !string.IsNullOrWhiteSpace(combine);
                    //group
                    var group = translationResult.GetGroup();
                    //having
                    var having = translationResult.GetHavingCondition();
                    //pre script output
                    var targetScript = translationResult.GetPreOutputStatement();

                    if (string.IsNullOrWhiteSpace(targetScript))
                    {
                        //target
                        var targetStatement = GetFromTargetStatement(context, queryable, location, tablePetName);
                        outputFields = targetStatement.OutputFields;
                        //condition
                        var condition = translationResult.GetCondition(ConditionStartKeyword);
                        //join
                        var join = translationResult.GetJoin();
                        //target statement
                        targetScript = $"{targetStatement.Script}{join}{condition}{group}{having}";
                    }
                    else
                    {
                        targetScript = $"{targetScript}{group}{having}";
                        outputFields = translationResult.GetPreOutputFields();
                    }

                    // output fields
                    if (outputFields.IsNullOrEmpty() || !queryable.SelectedFields.IsNullOrEmpty())
                    {
                        outputFields = SixnetDataManager.GetQueryableFields(DatabaseType, queryable.GetModelType(), queryable, context.IsRootQueryable(queryable));
                    }
                    var outputFieldString = FormatFieldsString(context, queryable, location, FieldLocation.Output, outputFields);
                    //pre script
                    var preScript = GetPreScript(context, location);
                    //statement
                    sqlStatement = $"SELECT{GetDistinctString(queryable)} {outputFieldString} FROM {targetScript}{sort}{limit}";
                    switch (queryable.OutputType)
                    {
                        case QueryableOutputType.Count:
                            sqlStatement = hasCombine
                                ? hasSort
                                    ? $"{preScript}SELECT COUNT(1) FROM ((SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine}){TablePetNameKeyword}{tablePetName}"
                                    : $"{preScript}SELECT COUNT(1) FROM (({sqlStatement}){combine}){TablePetNameKeyword}{tablePetName}"
                                : $"{preScript}SELECT COUNT(1) FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}";
                            break;
                        case QueryableOutputType.Predicate:
                            sqlStatement = hasCombine
                                ? hasSort
                                    ? $"{preScript}SELECT 1 WHERE EXISTS((SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine})"
                                    : $"{preScript}SELECT 1 WHERE EXISTS(({sqlStatement}){combine})"
                                : $"{preScript}SELECT 1 WHERE EXISTS({sqlStatement})";
                            break;
                        default:
                            sqlStatement = hasCombine
                            ? hasSort
                                ? $"{preScript}(SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine}"
                                : $"{preScript}({sqlStatement}){combine}"
                            : $"{preScript}{sqlStatement}";
                            break;
                    }
                    break;
            }

            //parameters
            var parameters = context.GetParameters();

            //log script
            if (location == QueryableLocation.Top)
            {
                LogScript(sqlStatement, parameters);
            }

            return QueryDatabaseStatement.Create(sqlStatement, parameters, outputFields);
        }

        #endregion

        #region Get insert statement

        /// <summary>
        /// Get insert statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override List<ExecutionDatabaseStatement> GenerateInsertStatements(DataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var entityType = dataCommandExecutionContext.Command.GetEntityType();
            var fields = SixnetDataManager.GetInsertableFields(DatabaseType, entityType);
            var fieldCount = fields.GetCount();
            var insertFields = new List<string>(fieldCount);
            var insertValues = new List<string>(fieldCount);
            DataField autoIncrementField = null;
            DataField splitField = null;
            dynamic splitValue = default;

            foreach (var field in fields)
            {
                if (field.InRole(FieldRole.Increment))
                {
                    autoIncrementField ??= field;
                    if (!autoIncrementField.InRole(FieldRole.PrimaryKey) && field.InRole(FieldRole.PrimaryKey)) // get first primary key field
                    {
                        autoIncrementField = field;
                    }
                    if (!SixnetDataManager.AllowInsertIncrementField(context.DataCommandExecutionContext))
                    {
                        continue;
                    }
                }
                // fields
                insertFields.Add(FormatAndWrapKeywordFunc(field.GetFieldName(DatabaseType), DatabaseObjectNameType.ColumnName));
                // values
                var insertValue = command.FieldsAssignment.GetNewValue(field.PropertyName);
                insertValues.Add(FormatInsertValueField(context, command.Queryable, insertValue));

                // split value
                if (field.InRole(FieldRole.SplitValue))
                {
                    splitValue = insertValue;
                    splitField = field;
                }
            }

            SixnetDirectThrower.ThrowNotSupportIf(autoIncrementField != null && splitField != null, $"Not support auto increment field for split table:{entityType.Name}");

            if (splitField != null)
            {
                dataCommandExecutionContext.SetSplitValues(new List<dynamic>(1) { splitValue });
            }
            var tableNames = dataCommandExecutionContext.GetTableNames();

            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.IsNullOrEmpty(), $"Get table name failed for {entityType.Name}");

            var statementBuilder = new StringBuilder();
            var scriptTemplate = $"INSERT INTO {{0}} ({string.Join(",", insertFields)}) VALUES ({string.Join(",", insertValues)});";
            foreach (var tableName in tableNames)
            {
                statementBuilder.AppendLine(string.Format(scriptTemplate, FormatAndWrapKeywordFunc(tableName, DatabaseObjectNameType.TableName)));
            }
            if (autoIncrementField != null)
            {
                var incrField = $"{command.Id}";
                statementBuilder.AppendLine($"SELECT LAST_INSERT_ROWID() {ColumnPetNameKeyword} {incrField};");
            }
            return new List<ExecutionDatabaseStatement>(1)
            {
                new ExecutionDatabaseStatement()
                {
                    Script = statementBuilder.ToString(),
                    ScriptType = GetCommandType(command),
                    Parameters = context.GetParameters(),
                    MustAffectData = autoIncrementField != null || (command.Options?.MustAffectData ?? false),
                }
            };
        }

        #endregion

        #region Get update statement

        /// <summary>
        /// Get update statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override List<ExecutionDatabaseStatement> GenerateUpdateStatements(DataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            SixnetException.ThrowIf(command?.FieldsAssignment?.NewValues.IsNullOrEmpty() ?? true, "No set update field");

            #region translate

            var translationResult = Translate(context);
            var condition = translationResult?.GetCondition(ConditionStartKeyword);
            var join = translationResult?.GetJoin();
            var preScripts = context.GetPreScripts();

            #endregion

            #region script 

            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var tablePetName = command.Queryable == null ? context.GetNewTablePetName() : context.GetDefaultTablePetName(command.Queryable);
            var newValues = command.FieldsAssignment.NewValues;
            var updateSetArray = new List<string>();
            foreach (var newValueItem in newValues)
            {
                var newValue = newValueItem.Value;
                var propertyName = newValueItem.Key;
                var updateField = SixnetDataManager.GetField(dataCommandExecutionContext.Server.DatabaseType, command.GetEntityType(), DataField.Create(propertyName)) as DataField;
                SixnetDirectThrower.ThrowSixnetExceptionIf(updateField == null, $"Not found field:{propertyName}");
                var fieldFormattedName = FormatAndWrapKeywordFunc(updateField.GetFieldName(DatabaseType), DatabaseObjectNameType.ColumnName);
                var newValueExpression = FormatUpdateValueField(context, command, newValue);
                updateSetArray.Add($"{fieldFormattedName}={newValueExpression}");
            }
            var tableNames = dataCommandExecutionContext.GetTableNames(command);
            var entityType = dataCommandExecutionContext.Command.GetEntityType();
            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.IsNullOrEmpty(), $"Get table name failed for {entityType.Name}");

            string scriptTemplate;
            if (string.IsNullOrWhiteSpace(join) && preScripts.IsNullOrEmpty())
            {
                scriptTemplate = $"UPDATE {{0}}{TablePetNameKeyword}{tablePetName} SET {string.Join(",", updateSetArray)}{condition};";
            }
            else
            {
                var primaryKeyFields = SixnetDataManager.GetFields(DatabaseType, entityType, SixnetEntityManager.GetPrimaryKeyFields(entityType));
                SixnetException.ThrowIf(primaryKeyFields.IsNullOrEmpty(), $"{entityType?.FullName} not set primary key fields");

                var primaryKeyString = string.Join("||", primaryKeyFields.Select(pk => FormatField(context, command.Queryable, pk, QueryableLocation.Top, FieldLocation.Criterion, tablePetName: tablePetName)));
                var queryStatement = GenerateQueryStatementCore(context, translationResult, QueryableLocation.UsingSource);
                scriptTemplate = $"UPDATE {{0}}{TablePetNameKeyword}{tablePetName} SET {string.Join(",", updateSetArray)} WHERE {primaryKeyString} IN (SELECT {primaryKeyString} FROM ({queryStatement.Script}){TablePetNameKeyword}{tablePetName});";
            }

            // parameters
            var parameters = ConvertParameter(command.ScriptParameters) ?? new DataCommandParameters();
            parameters.Union(context.GetParameters());

            // statements
            var statements = new List<ExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(new ExecutionDatabaseStatement()
                {
                    Script = string.Format(scriptTemplate, FormatAndWrapKeywordFunc(tableName, DatabaseObjectNameType.TableName)),
                    ScriptType = GetCommandType(command),
                    Parameters = parameters,
                    MustAffectData = true,
                    HasPreScript = !preScripts.IsNullOrEmpty()
                });
            }

            #endregion

            return statements;
        }

        #endregion

        #region Get delete statement

        /// <summary>
        /// Get delete statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override List<ExecutionDatabaseStatement> GenerateDeleteStatements(DataCommandResolveContext context)
        {
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var command = dataCommandExecutionContext.Command;

            #region translate

            var translationResult = Translate(context);
            var condition = translationResult?.GetCondition(ConditionStartKeyword);
            var join = translationResult?.GetJoin();
            var preScripts = context.GetPreScripts();

            #endregion

            #region script

            var tableNames = dataCommandExecutionContext.GetTableNames(command);
            var entityType = dataCommandExecutionContext.Command.GetEntityType();
            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.IsNullOrEmpty(), $"Get table name failed for {entityType.Name}");
            var tablePetName = command.Queryable == null ? context.GetNewTablePetName() : context.GetDefaultTablePetName(command.Queryable);

            string scriptTemplate;
            if (string.IsNullOrWhiteSpace(join) && preScripts.IsNullOrEmpty())
            {
                scriptTemplate = $"DELETE FROM {{0}}{TablePetNameKeyword}{tablePetName}{condition};";
            }
            else
            {
                var primaryKeyFields = SixnetDataManager.GetFields(DatabaseType, entityType, SixnetEntityManager.GetPrimaryKeyFields(entityType));
                SixnetException.ThrowIf(primaryKeyFields.IsNullOrEmpty(), $"{entityType?.FullName} not set primary key fields");

                var primaryKeyString = string.Join("||", primaryKeyFields.Select(pk => FormatField(context, command.Queryable, pk, QueryableLocation.Top, FieldLocation.Criterion, tablePetName: tablePetName)));
                var queryStatement = GenerateQueryStatementCore(context, translationResult, QueryableLocation.UsingSource);
                scriptTemplate = $"DELETE FROM {{0}}{TablePetNameKeyword}{tablePetName} WHERE {primaryKeyString} IN (SELECT {primaryKeyString} FROM ({queryStatement.Script}){TablePetNameKeyword}{tablePetName});";
            }

            // parameters
            var parameters = ConvertParameter(command.ScriptParameters) ?? new DataCommandParameters();
            parameters.Union(context.GetParameters());

            // statement
            var statements = new List<ExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(new ExecutionDatabaseStatement()
                {
                    Script = string.Format(scriptTemplate, FormatAndWrapKeywordFunc(tableName, DatabaseObjectNameType.TableName)),
                    ScriptType = GetCommandType(command),
                    MustAffectData = command.Options?.MustAffectData ?? false,
                    Parameters = parameters,
                    HasPreScript = !preScripts.IsNullOrEmpty()
                });
            }

            #endregion

            return statements;
        }

        #endregion

        #region Get create table statements

        /// <summary>
        /// Get create table statements
        /// </summary>
        /// <param name="migrationCommand">Migration command</param>
        /// <returns></returns>
        protected override List<ExecutionDatabaseStatement> GetCreateTableStatements(MigrationDatabaseCommand migrationCommand)
        {
            var migrationInfo = migrationCommand.MigrationInfo;
            if (migrationInfo?.NewTables.IsNullOrEmpty() ?? true)
            {
                return new List<ExecutionDatabaseStatement>(0);
            }
            var newTables = migrationInfo.NewTables;
            var statements = new List<ExecutionDatabaseStatement>();
            var options = migrationCommand.MigrationInfo;
            foreach (var newTableInfo in newTables)
            {
                if (newTableInfo?.EntityType == null || (newTableInfo?.TableNames.IsNullOrEmpty() ?? true))
                {
                    continue;
                }
                var entityType = newTableInfo.EntityType;
                var entityConfig = SixnetEntityManager.GetEntityConfig(entityType);
                SixnetDirectThrower.ThrowSixnetExceptionIf(entityConfig == null, $"Get entity config failed for {entityType.Name}");

                var newFieldScripts = new List<string>();
                var primaryKeyNames = new List<string>();
                var hasIncrementField = false;
                foreach (var field in entityConfig.AllFields)
                {
                    var dataField = SixnetDataManager.GetField(SQLiteManager.CurrentDatabaseServerType, entityType, field.Value);
                    if (dataField is DataField dataEntityField)
                    {
                        var dataFieldName = SQLiteManager.WrapKeyword(dataEntityField.GetFieldName(DatabaseType), DatabaseObjectNameType.ColumnName);
                        newFieldScripts.Add($"{dataFieldName}{GetFieldDefinition(dataEntityField, migrationInfo)}");
                        hasIncrementField |= dataEntityField.InRole(FieldRole.Increment);
                        if (dataEntityField.InRole(FieldRole.PrimaryKey))
                        {
                            primaryKeyNames.Add($"{dataFieldName} ASC");
                        }
                    }
                }
                if (hasIncrementField)
                {
                    primaryKeyNames.Clear();
                }
                foreach (var tableName in newTableInfo.TableNames)
                {
                    var createTableStatement = new ExecutionDatabaseStatement()
                    {
                        Script = $"CREATE TABLE IF NOT EXISTS {tableName} ({string.Join(",", newFieldScripts)}{(primaryKeyNames.IsNullOrEmpty() ? "" : ", PRIMARY KEY (" + string.Join(",", primaryKeyNames) + ")")});"
                    };
                    statements.Add(createTableStatement);

                    // Log script
                    LogExecutionStatement(createTableStatement);
                }
            }
            return statements;
        }

        #endregion

        #region Get add filed statements

        /// <summary>
        /// Get create field statement
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override List<ExecutionDatabaseStatement> GetAddFieldStatements(MigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.NewFields.IsNullOrEmpty() ?? true)
            {
                return new List<ExecutionDatabaseStatement>(0);
            }

            var statements = new List<ExecutionDatabaseStatement>();
            foreach (var tableItem in migrationCommand.MigrationInfo.NewFields)
            {
                if (!tableItem.Value.IsNullOrEmpty())
                {
                    foreach (var field in tableItem.Value)
                    {
                        var dataFieldName = field.GetFieldName(DatabaseType);
                        var existScript = $"SELECT COUNT(*) FROM PRAGMA_TABLE_INFO('{tableItem.Key}') WHERE NAME='{dataFieldName}' COLLATE NOCASE;";
                        var hasColumn = migrationCommand.Connection.DbConnection.ExecuteScalar<int>(existScript) > 0;
                        if (!hasColumn)
                        {
                            var newFieldStatement = new ExecutionDatabaseStatement()
                            {
                                Script = $"ALTER TABLE {tableItem.Key} ADD COLUMN {WrapKeywordFunc(dataFieldName, DatabaseObjectNameType.ColumnName)}{GetFieldDefinition(field, migrationCommand.MigrationInfo)};"
                            };
                            statements.Add(newFieldStatement);
                            // Log script
                            LogExecutionStatement(newFieldStatement);
                        }
                    }
                }
            }
            return statements;
        }

        #endregion

        #region Get delete filed statements

        protected override List<ExecutionDatabaseStatement> GetDeleteFieldStatements(MigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.DeletableFields.IsNullOrEmpty() ?? true)
            {
                return new List<ExecutionDatabaseStatement>(0);
            }

            var statements = new List<ExecutionDatabaseStatement>();
            foreach (var tableItem in migrationCommand.MigrationInfo.DeletableFields)
            {
                if (!tableItem.Value.IsNullOrEmpty())
                {
                    foreach (var field in tableItem.Value)
                    {
                        var dataFieldName = field.GetFieldName(DatabaseType);
                        var existScript = $"SELECT COUNT(*) FROM PRAGMA_TABLE_INFO('{tableItem.Key}') WHERE NAME='{dataFieldName}' COLLATE NOCASE;";
                        var hasColumn = migrationCommand.Connection.DbConnection.ExecuteScalar<int>(existScript) > 0;
                        if (hasColumn)
                        {
                            var deleteStatement = new ExecutionDatabaseStatement()
                            {
                                Script = $"ALTER TABLE {tableItem.Key} DROP COLUMN {WrapKeywordFunc(dataFieldName, DatabaseObjectNameType.ColumnName)};"
                            };
                            statements.Add(deleteStatement);
                            // Log script
                            LogExecutionStatement(deleteStatement);
                        }
                    }
                }
            }
            return statements;
        }

        #endregion

        #region Get update field statements 

        protected override List<ExecutionDatabaseStatement> GetUpdateFieldStatements(MigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.UpdatableFields.IsNullOrEmpty() ?? true)
            {
                return new List<ExecutionDatabaseStatement>(0);
            }

            var statements = new List<ExecutionDatabaseStatement>();
            foreach (var tableItem in migrationCommand.MigrationInfo.UpdatableFields)
            {
                if (tableItem.Value.IsNullOrEmpty())
                {
                    continue;
                }
                foreach (var fieldItem in tableItem.Value)
                {
                    var field = fieldItem.Value;
                    var nowFieldName = fieldItem.Key;
                    var newFieldName = field.GetFieldName(DatabaseType);
                    var existScript = $"SELECT COUNT(*) FROM PRAGMA_TABLE_INFO('{tableItem.Key}') WHERE NAME='{nowFieldName}' COLLATE NOCASE;";
                    var hasColumn = migrationCommand.Connection.DbConnection.ExecuteScalar<int>(existScript) > 0;
                    if (hasColumn)
                    {
                        var updateStatement = new ExecutionDatabaseStatement()
                        {
                            Script = $"ALTER TABLE {tableItem.Key} ALTER COLUMN {WrapKeywordFunc(nowFieldName, DatabaseObjectNameType.ColumnName)}{GetFieldDefinition(field, migrationCommand.MigrationInfo)};"
                        };
                        statements.Add(updateStatement);
                        LogExecutionStatement(updateStatement);
                        if (!string.Equals(nowFieldName, newFieldName, StringComparison.OrdinalIgnoreCase))
                        {
                            var renameStatement = new ExecutionDatabaseStatement()
                            {
                                Script = $"ALTER TABLE {tableItem.Key} RENAME COLUMN {WrapKeywordFunc(nowFieldName, DatabaseObjectNameType.ColumnName)} TO {WrapKeywordFunc(newFieldName, DatabaseObjectNameType.ColumnName)};"
                            };
                            statements.Add(renameStatement);
                            LogExecutionStatement(renameStatement);
                        }
                    }
                }
            }
            return statements;
        }

        #endregion

        #region Get rename table statements

        protected override List<ExecutionDatabaseStatement> GetRenameTableStatements(MigrationDatabaseCommand migrationCommand)
        {
            var migrationInfo = migrationCommand?.MigrationInfo;
            if (migrationInfo?.RenameTables.IsNullOrEmpty() ?? true)
            {
                return new List<ExecutionDatabaseStatement>(0);
            }
            var renameTables = migrationInfo.RenameTables;
            var statements = new List<ExecutionDatabaseStatement>();
            var options = migrationCommand.MigrationInfo;
            foreach (var tableItem in renameTables)
            {
                var hasTableScript = $"SELECT COUNT(*) FROM SQLITE_MASTER WHERE TYPE = 'TABLE' COLLATE NOCASE AND NAME = '{tableItem.Key}' COLLATE NOCASE;";
                var hasTable = migrationCommand.Connection.DbConnection.ExecuteScalar<int>(hasTableScript) > 0;
                if (hasTable)
                {
                    var renameTableStatement = new ExecutionDatabaseStatement()
                    {
                        Script = $"ALTER TABLE {tableItem.Key} RENAME TO {tableItem.Value};"
                    };
                    statements.Add(renameTableStatement);

                    // Log script
                    LogExecutionStatement(renameTableStatement);
                }
            }
            return statements;
        }

        #endregion

        #region Get limit string

        /// <summary>
        /// Get limit string
        /// </summary>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="takeNum">Take num</param>
        /// <returns></returns>
        protected override string GetLimitString(int offsetNum, int takeNum, bool hasSort)
        {
            if (takeNum < 1)
            {
                return string.Empty;
            }
            if (offsetNum < 0)
            {
                offsetNum = 0;
            }
            return $" LIMIT {offsetNum},{takeNum}";

        }

        #endregion

        #region Get field sql data type

        /// <summary>
        /// Get sql data type
        /// </summary>
        /// <param name="field">Field</param>
        /// <returns></returns>
        protected override string GetSqlDataType(DataField field, MigrationInfo options)
        {
            SixnetDirectThrower.ThrowArgNullIf(field == null, nameof(field));
            if (!string.IsNullOrWhiteSpace(field.DbType))
            {
                return field.DbType;
            }
            var dbType = field.GetDataType().GetDbType();
            var dbTypeName = "";
            switch (dbType)
            {
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                case DbType.Guid:
                case DbType.DateTime:
                case DbType.Date:
                case DbType.DateTime2:
                case DbType.DateTimeOffset:
                case DbType.UInt64:
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.Time:
                case DbType.Xml:
                    dbTypeName = "TEXT";
                    break;
                case DbType.Boolean:
                case DbType.Byte:
                case DbType.Int16:
                case DbType.SByte:
                case DbType.Int32:
                case DbType.UInt16:
                case DbType.UInt32:
                case DbType.Int64:
                    dbTypeName = "INTEGER";
                    break;
                case DbType.Currency:
                case DbType.Decimal:
                case DbType.Double:
                case DbType.Single:
                    dbTypeName = "REAL";
                    break;
                case DbType.Object:
                case DbType.Binary:
                    dbTypeName = "BLOB";
                    break;
                default:
                    throw new NotSupportedException(dbType.ToString());
            }
            return dbTypeName;
        }

        #endregion

        #region Get field identity

        /// <summary>
        /// Get field identity
        /// </summary>
        /// <param name="field">Field</param>
        /// <param name="options">Options</param>
        /// <returns></returns>
        protected override string GetFieldIdentity(DataField field, MigrationInfo options)
        {
            SixnetDirectThrower.ThrowArgNullIf(field == null, nameof(field));
            if (!field.InRole(FieldRole.Increment))
            {
                return string.Empty;
            }
            return " PRIMARY KEY AUTOINCREMENT";
        }

        #endregion
    }
}
