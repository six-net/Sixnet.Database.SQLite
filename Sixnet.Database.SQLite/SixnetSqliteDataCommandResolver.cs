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
    internal partial class SixnetSqliteDataCommandResolver : SixnetBaseDataCommandResolver
    {
        #region Constructor

        public SixnetSqliteDataCommandResolver()
        {
            KeywordPrefix = "`";
            KeywordSuffix = "`";
            DatabaseType = SixnetDatabaseType.SQLite;
            DefaultFieldFormatter = new SixnetSqliteDefaultFieldFormatter();
            ParameterPrefix = "@";
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
        protected override SixnetQueryDatabaseStatement GenerateQueryStatementCore(SixnetDataCommandResolveContext context, SixnetQueryableTranslationResult translationResult, SixnetQueryableLocation location)
        {
            var queryable = translationResult.GetOriginalQueryable();
            string sqlStatement;
            IEnumerable<ISixnetField> outputFields = null;
            switch (queryable.ExecutionMode)
            {
                case SixnetQueryableExecutionMode.Script:
                    sqlStatement = translationResult.GetCondition();
                    break;
                case SixnetQueryableExecutionMode.Regular:
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
                    var outputFieldString = FormatFieldsString(context, queryable, location, SixnetFieldLocation.Output, outputFields);
                    //pre script
                    var preScript = GetPreScript(context, location);
                    //statement
                    sqlStatement = $"SELECT{GetDistinctString(queryable)} {outputFieldString} FROM {targetScript}{sort}{limit}";
                    switch (queryable.OutputType)
                    {
                        case SixnetQueryableOutputType.Count:
                            sqlStatement = hasCombine
                                ? hasSort
                                    ? $"{preScript}SELECT COUNT(1) FROM ((SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine}){TablePetNameKeyword}{tablePetName}"
                                    : $"{preScript}SELECT COUNT(1) FROM (({sqlStatement}){combine}){TablePetNameKeyword}{tablePetName}"
                                : $"{preScript}SELECT COUNT(1) FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}";
                            break;
                        case SixnetQueryableOutputType.Predicate:
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
            if (location == SixnetQueryableLocation.Top)
            {
                LogScript(sqlStatement, parameters);
            }

            return SixnetQueryDatabaseStatement.Create(sqlStatement, parameters, outputFields);
        }

        #endregion

        #region Get insert statement

        /// <summary>
        /// Get insert statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GenerateInsertStatements(SixnetDataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var entityType = dataCommandExecutionContext.Command.GetEntityType();
            var fields = SixnetDataManager.GetInsertableFields(DatabaseType, entityType);
            var fieldCount = fields.GetCount();
            var insertFields = new List<string>(fieldCount);
            var insertValues = new List<string>(fieldCount);
            SixnetDataField autoIncrementField = null;
            SixnetDataField splitField = null;
            dynamic splitValue = default;

            foreach (var field in fields)
            {
                if (field.InRole(SixnetFieldRole.Increment))
                {
                    autoIncrementField ??= field;
                    if (!autoIncrementField.InRole(SixnetFieldRole.PrimaryKey) && field.InRole(SixnetFieldRole.PrimaryKey)) // get first primary key field
                    {
                        autoIncrementField = field;
                    }
                    if (!SixnetDataManager.AllowInsertIncrementField(context.DataCommandExecutionContext))
                    {
                        continue;
                    }
                }
                // fields
                insertFields.Add(FormatAndWrapObjectName(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                // values
                var insertValue = command.FieldsAssignment.GetNewValue(field.PropertyName);
                insertValues.Add(FormatInsertValueField(context, command.Queryable, insertValue));

                // split value
                if (field.InRole(SixnetFieldRole.SplitValue))
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
                statementBuilder.AppendLine(string.Format(scriptTemplate, FormatAndWrapObjectName(tableName)));
            }
            if (autoIncrementField != null)
            {
                var incrField = $"{command.Id}";
                statementBuilder.AppendLine($"SELECT LAST_INSERT_ROWID() {ColumnPetNameKeyword} {incrField};");
            }
            return new List<SixnetExecutionDatabaseStatement>(1)
            {
                new SixnetExecutionDatabaseStatement()
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
        protected override List<SixnetExecutionDatabaseStatement> GenerateUpdateStatements(SixnetDataCommandResolveContext context)
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
                var updateField = SixnetDataManager.GetField(dataCommandExecutionContext.Server.DatabaseType, command.GetEntityType(), SixnetDataField.Create(propertyName)) as SixnetDataField;
                SixnetDirectThrower.ThrowSixnetExceptionIf(updateField == null, $"Not found field:{propertyName}");
                var fieldFormattedName = FormatAndWrapObjectName(updateField.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column);
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

                var primaryKeyString = string.Join("||", primaryKeyFields.Select(pk => FormatField(context, command.Queryable, pk, SixnetQueryableLocation.Top, SixnetFieldLocation.Criterion, tablePetName: tablePetName)));
                var queryStatement = GenerateQueryStatementCore(context, translationResult, SixnetQueryableLocation.UsingSource);
                scriptTemplate = $"UPDATE {{0}}{TablePetNameKeyword}{tablePetName} SET {string.Join(",", updateSetArray)} WHERE {primaryKeyString} IN (SELECT {primaryKeyString} FROM ({queryStatement.Script}){TablePetNameKeyword}{tablePetName});";
            }

            // parameters
            var parameters = ConvertParameter(command.ScriptParameters) ?? new SixnetDataCommandParameters();
            parameters.Union(context.GetParameters());

            // statements
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(new SixnetExecutionDatabaseStatement()
                {
                    Script = string.Format(scriptTemplate, FormatAndWrapObjectName(tableName)),
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
        protected override List<SixnetExecutionDatabaseStatement> GenerateDeleteStatements(SixnetDataCommandResolveContext context)
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

                var primaryKeyString = string.Join("||", primaryKeyFields.Select(pk => FormatField(context, command.Queryable, pk, SixnetQueryableLocation.Top, SixnetFieldLocation.Criterion, tablePetName: tablePetName)));
                var queryStatement = GenerateQueryStatementCore(context, translationResult, SixnetQueryableLocation.UsingSource);
                scriptTemplate = $"DELETE FROM {{0}}{TablePetNameKeyword}{tablePetName} WHERE {primaryKeyString} IN (SELECT {primaryKeyString} FROM ({queryStatement.Script}){TablePetNameKeyword}{tablePetName});";
            }

            // parameters
            var parameters = ConvertParameter(command.ScriptParameters) ?? new SixnetDataCommandParameters();
            parameters.Union(context.GetParameters());

            // statement
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(new SixnetExecutionDatabaseStatement()
                {
                    Script = string.Format(scriptTemplate, FormatAndWrapObjectName(tableName)),
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
        protected override List<SixnetExecutionDatabaseStatement> GetCreateTableStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            var migrationInfo = migrationCommand.MigrationInfo;
            if (migrationInfo?.NewTables.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }
            var newTables = migrationInfo.NewTables;
            var statements = new List<SixnetExecutionDatabaseStatement>();
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
                    var dataField = SixnetDataManager.GetField(DatabaseType, entityType, field.Value);
                    if (dataField is SixnetDataField dataEntityField)
                    {
                        var dataFieldName = FormatAndWrapObjectName(SixnetDatabaseObjectName.Create(dataEntityField.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                        newFieldScripts.Add($"{dataFieldName}{GetFieldDefinition(dataEntityField, migrationInfo)}");
                        hasIncrementField |= dataEntityField.InRole(SixnetFieldRole.Increment);
                        if (dataEntityField.InRole(SixnetFieldRole.PrimaryKey))
                        {
                            primaryKeyNames.Add($"{dataFieldName} ASC");
                        }
                    }
                }
                if (hasIncrementField)
                {
                    primaryKeyNames.Clear();
                }
                foreach (var table in newTableInfo.TableNames)
                {
                    var formattedTableName = FormatAndWrapObjectName(table);
                    var createTableStatement = new SixnetExecutionDatabaseStatement()
                    {
                        Script = $"CREATE TABLE IF NOT EXISTS {formattedTableName} ({string.Join(",", newFieldScripts)}{(primaryKeyNames.IsNullOrEmpty() ? "" : ", PRIMARY KEY (" + string.Join(",", primaryKeyNames) + ")")});"
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
        protected override List<SixnetExecutionDatabaseStatement> GetAddFieldStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.NewFields.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }

            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var table in migrationCommand.MigrationInfo.NewFields)
            {
                if (!table.Value.IsNullOrEmpty())
                {
                    var formattedTableName = GetObjectFullName(FormatObjectName(table.Key));
                    foreach (var field in table.Value)
                    {
                        var dataFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                        var existScript = $"SELECT COUNT(*) FROM PRAGMA_TABLE_INFO('{formattedTableName}') WHERE NAME='{dataFieldName.Name}' COLLATE NOCASE;";
                        var hasColumn = migrationCommand.Connection.DbConnection.ExecuteScalar<int>(existScript) > 0;
                        if (!hasColumn)
                        {
                            var newFieldStatement = new SixnetExecutionDatabaseStatement()
                            {
                                Script = $"ALTER TABLE {formattedTableName} ADD COLUMN {WrapObjectName(dataFieldName).Name}{GetFieldDefinition(field, migrationCommand.MigrationInfo)};"
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

        protected override List<SixnetExecutionDatabaseStatement> GetDeleteFieldStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.DeletedFields.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }

            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var table in migrationCommand.MigrationInfo.DeletedFields)
            {
                if (!table.Value.IsNullOrEmpty())
                {
                    var formattedTableName = GetObjectFullName(FormatObjectName(table.Key));
                    foreach (var field in table.Value)
                    {
                        var dataFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                        var existScript = $"SELECT COUNT(*) FROM PRAGMA_TABLE_INFO('{formattedTableName}') WHERE NAME='{dataFieldName.Name}' COLLATE NOCASE;";
                        var hasColumn = migrationCommand.Connection.DbConnection.ExecuteScalar<int>(existScript) > 0;
                        if (hasColumn)
                        {
                            var deleteStatement = new SixnetExecutionDatabaseStatement()
                            {
                                Script = $"ALTER TABLE {formattedTableName} DROP COLUMN {WrapObjectName(dataFieldName).Name};"
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

        protected override List<SixnetExecutionDatabaseStatement> GetUpdateFieldStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.UpdatedFields.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }

            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var table in migrationCommand.MigrationInfo.UpdatedFields)
            {
                if (table.Value.IsNullOrEmpty())
                {
                    continue;
                }
                var formattedTableName = GetObjectFullName(FormatObjectName(table.Key));
                foreach (var fieldItem in table.Value)
                {
                    var field = fieldItem.Value;
                    var nowFieldName = fieldItem.Key;
                    var newFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                    var existScript = $"SELECT COUNT(*) FROM PRAGMA_TABLE_INFO('{formattedTableName}') WHERE NAME='{nowFieldName}' COLLATE NOCASE;";
                    var hasColumn = migrationCommand.Connection.DbConnection.ExecuteScalar<int>(existScript) > 0;
                    if (hasColumn)
                    {
                        if (!string.Equals(nowFieldName, newFieldName.Name, StringComparison.OrdinalIgnoreCase))
                        {
                            var renameStatement = new SixnetExecutionDatabaseStatement()
                            {
                                Script = $"ALTER TABLE {formattedTableName} RENAME COLUMN {WrapObjectName(SixnetDatabaseObjectName.Create(nowFieldName, SixnetDatabaseObjectType.Column)).Name} TO {WrapObjectName(newFieldName).Name};"
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

        protected override List<SixnetExecutionDatabaseStatement> GetRenameTableStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            var migrationInfo = migrationCommand?.MigrationInfo;
            if (migrationInfo?.RenamedTables.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }
            var renameTables = migrationInfo.RenamedTables;
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var table in renameTables)
            {
                var oldFormattedTableName = GetObjectFullName(FormatObjectName(table.Key));
                var newFormattedTableName = GetObjectFullName(FormatObjectName(table.Value));

                var hasTableScript = $"SELECT COUNT(*) FROM SQLITE_MASTER WHERE TYPE = 'TABLE' COLLATE NOCASE AND NAME = '{oldFormattedTableName}' COLLATE NOCASE;";
                var hasTable = migrationCommand.Connection.DbConnection.ExecuteScalar<int>(hasTableScript) > 0;
                if (hasTable)
                {
                    var renameTableStatement = new SixnetExecutionDatabaseStatement()
                    {
                        Script = $"ALTER TABLE {oldFormattedTableName} RENAME TO {newFormattedTableName};"
                    };
                    statements.Add(renameTableStatement);

                    // Log script
                    LogExecutionStatement(renameTableStatement);
                }
            }
            return statements;
        }

        #endregion

        #region Get delete all table statements

        /// <summary>
        /// Get delete all table statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GetDeleteAllTableStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            var statements = new List<SixnetExecutionDatabaseStatement>();
            var sql = @"
SELECT 'DROP TABLE IF EXISTS ""' || name || '"";'
FROM sqlite_master
WHERE type = 'table' AND name NOT LIKE 'sqlite_%';
";
            var deleteScripts = migrationCommand.Connection.DbConnection.Query<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction);
            foreach (var script in deleteScripts)
            {
                statements.Add(new SixnetExecutionDatabaseStatement()
                {
                    Script = script
                });
            }

            return statements;
        }

        #endregion

        #region Get delete all view statements

        /// <summary>
        /// Get delete all view statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GetDeleteAllViewStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            var statements = new List<SixnetExecutionDatabaseStatement>();
            var sql = @"
SELECT 'DROP VIEW IF EXISTS ""' || name || '"";'
FROM sqlite_master
WHERE type = 'view';
";
            var deleteScripts = migrationCommand.Connection.DbConnection.Query<string>(sql, transaction: migrationCommand.Connection.Transaction.DbTransaction);
            foreach (var script in deleteScripts)
            {
                statements.Add(new SixnetExecutionDatabaseStatement()
                {
                    Script = script
                });
            }

            return statements;
        }

        #endregion

        #region Get delete all function statements

        /// <summary>
        /// Get delete all function statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GetDeleteAllFunctionStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return new List<SixnetExecutionDatabaseStatement>(0);
        }

        #endregion

        #region Get delete all custom type statements

        /// <summary>
        /// Get delete all custom type statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GetDeleteAllCustomTypeStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return new List<SixnetExecutionDatabaseStatement>(0);
        }

        #endregion

        #region Get delete all procedure statements

        /// <summary>
        /// Get delete all procedure statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GetDeleteAllProcedureStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return new List<SixnetExecutionDatabaseStatement>(0);
        }

        #endregion

        #region Add foreign key

        List<SixnetExecutionDatabaseStatement> GetAddForeignKeyStatementsCore(List<SixnetEntityForeignKeyInfo> foreignKeyInfos)
        {
            return new List<SixnetExecutionDatabaseStatement>(0);
        }

        protected override List<SixnetExecutionDatabaseStatement> GetAddForeignKeyStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return GetAddForeignKeyStatementsCore(migrationCommand?.MigrationInfo?.NewForeignKeys);
        }


        #endregion

        #region Delete foreign key

        protected override List<SixnetExecutionDatabaseStatement> GetDeleteForeignKeyStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.DeletedForeignKeys.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var foreignKeyInfo in migrationCommand.MigrationInfo.DeletedForeignKeys)
            {
                var sourceFieldName = FormatObjectName(foreignKeyInfo.SourceField);
                var formattedSourceTableName = FormatObjectName(foreignKeyInfo.SourceTable);
                var wrapedSourceTableName = FormatAndWrapObjectName(foreignKeyInfo.SourceTable);
                var constraintName = WrapObjectName(SixnetDatabaseObjectName.Create($"FK_{formattedSourceTableName}_{sourceFieldName.Name}", SixnetDatabaseObjectType.Constraint));
                var foreignKeyStatement = new SixnetExecutionDatabaseStatement()
                {
                    Script = $"ALTER TABLE {wrapedSourceTableName} DROP CONSTRAINT IF EXISTS {constraintName};"
                };
                statements.Add(foreignKeyStatement);

                // Log script
                LogExecutionStatement(foreignKeyStatement);
            }
            return statements;
        }

        protected override List<SixnetExecutionDatabaseStatement> GetDeleteAllForeignKeyStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return new List<SixnetExecutionDatabaseStatement>(1)
            {
                new SixnetExecutionDatabaseStatement()
                {
                    Script = "PRAGMA foreign_keys = OFF;"
                }
            };
        }

        #endregion

        #region Add index

        List<SixnetExecutionDatabaseStatement> GetAddIndexStatementsCore(List<SixnetEntityIndexInfo> indexInfos)
        {
            if (indexInfos.IsNullOrEmpty())
            {
                return new List<SixnetExecutionDatabaseStatement>(0);
            }
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var indexInfo in indexInfos)
            {
                var formattedTableName = FormatObjectName(indexInfo.Table);
                var formattedAndWrapedTableName = FormatAndWrapObjectName(indexInfo.Table);
                var indexName = $"INX_{formattedTableName.Name}";
                var indexFieldStrings = new List<string>();
                var indexFields = indexInfo.Fields.OrderBy(c => c.Sequence).ThenBy(c => c.Name);
                foreach (var indexItemField in indexFields)
                {
                    var entityFieldName = FormatObjectName(indexItemField.Name);
                    indexName = $"{indexName}_{entityFieldName.Name}";
                    indexFieldStrings.Add($"{WrapObjectName(entityFieldName).Name} {(indexItemField.Desc ? "DESC" : "ASC")}");
                }
                var indexStatement = new SixnetExecutionDatabaseStatement()
                {
                    Script = $"CREATE {(indexInfo.Unique ? "UNIQUE " : "")}INDEX IF NOT EXISTS {indexName} ON {formattedAndWrapedTableName} ({string.Join(",", indexFieldStrings)});"
                };
                statements.Add(indexStatement);

                // Log script
                LogExecutionStatement(indexStatement);
            }
            return statements;
        }

        protected override List<SixnetExecutionDatabaseStatement> GetAddIndexStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            return GetAddIndexStatementsCore(migrationCommand?.MigrationInfo?.NewIndexes);
        }

        #endregion

        #region Delete index

        /// <summary>
        /// Get delete index statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GetDeleteIndexStatements(SixnetMigrationDatabaseCommand migrationCommand)
        {
            if (migrationCommand?.MigrationInfo?.DeletedIndexes.IsNullOrEmpty() ?? true)
            {
                return new List<SixnetExecutionDatabaseStatement>();
            }
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var indexInfo in migrationCommand.MigrationInfo.DeletedIndexes)
            {
                var formattedTableName = FormatObjectName(indexInfo.Table);
                var formattedAndWrapedTableName = FormatAndWrapObjectName(indexInfo.Table);
                var indexName = $"INX_{formattedTableName.Name}";
                var indexFields = indexInfo.Fields.OrderBy(c => c.Sequence).ThenBy(c => c.Name);
                foreach (var indexItemField in indexFields)
                {
                    var entityFieldName = FormatObjectName(indexItemField.Name);
                    indexName = $"{indexName}_{entityFieldName.Name}";
                }
                var indexStatement = new SixnetExecutionDatabaseStatement()
                {
                    Script = $"DROP INDEX IF EXISTS {indexName};"
                };
                statements.Add(indexStatement);

                // Log script
                LogExecutionStatement(indexStatement);
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
        protected override string GetSqlDataType(SixnetDataField field, SixnetMigrationInfo options)
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
        protected override string GetFieldIdentity(SixnetDataField field, SixnetMigrationInfo options)
        {
            SixnetDirectThrower.ThrowArgNullIf(field == null, nameof(field));
            if (!field.InRole(SixnetFieldRole.Increment))
            {
                return string.Empty;
            }
            return " PRIMARY KEY AUTOINCREMENT";
        }

        #endregion
    }
}
