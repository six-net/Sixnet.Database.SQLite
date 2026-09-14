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
            OnlyIncrementPrimaryKey = true;
            MaxIdentifierLength = 255;
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

        #region Data access

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
            switch (queryable.Info.ExecutionMode)
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
                    var limit = GetLimitString(queryable.Info.SkipCount, queryable.Info.TakeCount, hasSort);
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
                    if (outputFields.IsNullOrEmpty() || !queryable.Info.SelectedFields.IsNullOrEmpty())
                    {
                        outputFields = SixnetDataManager.GetQueryableFields(DatabaseType, queryable.GetModelType(), queryable, context.IsRootQueryable(queryable));
                    }
                    var outputFieldString = FormatFieldsString(context, queryable, location, SixnetFieldLocation.Output, outputFields);
                    //pre script
                    var preScript = GetPreScript(context, location);
                    //statement
                    sqlStatement = $"SELECT{GetDistinctString(queryable)} {outputFieldString} FROM {targetScript}{sort}{limit}";
                    switch (queryable.Info.OutputType)
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
                        case SixnetQueryableOutputType.TempTable:
                            sqlStatement = hasCombine
                            ? hasSort
                                ? $"(SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine}"
                                : $"({sqlStatement}){combine}"
                            : $"{sqlStatement}";
                            sqlStatement = $"{preScript}(CREATE TEMP TABLE {queryable.Info.TempTableName} AS SELECT * FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName})";
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

            return SixnetQueryDatabaseStatement.Create(DatabaseType, location, sqlStatement, parameters, outputFields);
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
                SixnetExecutionDatabaseStatement.Create(DatabaseType, data =>
                {
                    data.Script = statementBuilder.ToString();
                    data.ScriptType = GetCommandType(command);
                    data.Parameters = context.GetParameters();
                    data.MustAffectData = autoIncrementField != null || (command.Options?.MustAffectData ?? false);
                })
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
                statements.Add(SixnetExecutionDatabaseStatement.Create(DatabaseType, data =>
                {
                    data.Script = string.Format(scriptTemplate, FormatAndWrapObjectName(tableName));
                    data.ScriptType = GetCommandType(command);
                    data.Parameters = parameters;
                    data.MustAffectData = true;
                    data.HasPreScript = !preScripts.IsNullOrEmpty();
                }));
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
                statements.Add(SixnetExecutionDatabaseStatement.Create(DatabaseType, data =>
                {
                    data.Script = string.Format(scriptTemplate, FormatAndWrapObjectName(tableName));
                    data.ScriptType = GetCommandType(command);
                    data.MustAffectData = command.Options?.MustAffectData ?? false;
                    data.Parameters = parameters;
                    data.HasPreScript = !preScripts.IsNullOrEmpty();
                }));
            }

            #endregion

            return statements;
        }

        #endregion

        #endregion

        #region Migration

        #region Get create table statements

        protected override SixnetDatabaseScriptInfo GetCreateTableScripts(SixnetGetCreateTableDefineScriptParameter parameter)
        {
            var tableName = FormatAndWrapObjectName(parameter.Table);
            var columnInfo = parameter.ColumnDefineInfo;
            var fields = columnInfo.ColumnScripts;
            var parimaryKeys = columnInfo.PrimaryKeys;
            var migrationInfo = parameter.MigrationInfo;
            var script = $"CREATE TABLE IF NOT EXISTS {tableName} ({string.Join(",", fields)}{(parimaryKeys.IsNullOrEmpty() ? "" : ", PRIMARY KEY (" + string.Join(",", parimaryKeys) + ")")});";
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>() { script }
            };
        }

        #endregion

        #region Get delete all table statements

        protected override SixnetDatabaseScriptInfo GetDeleteAllTableScripts(SixnetDeleteAllTableParameter parameter)
        {
            var command = parameter.Command;
            var sql = @"
SELECT 'DROP TABLE IF EXISTS ""' || name || '"";'
FROM sqlite_master
WHERE type = 'table' AND name NOT LIKE 'sqlite_%';
";
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = command.Connection.DbConnection.Query<string>(sql, transaction: command.Connection.Transaction.DbTransaction)?.ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region Get rename table statements

        /// <summary>
        /// Get rename table statements
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetRenameTableScripts(SixnetRenameTableParameter parameter)
        {
            var command = parameter.Command;
            var oldFormattedTableName = GetObjectFullName(FormatObjectName(parameter.CurrentTableName));
            var newFormattedTableName = GetObjectFullName(FormatObjectName(parameter.NewTableName));

            var hasTableScript = $"SELECT COUNT(*) FROM SQLITE_MASTER WHERE TYPE = 'TABLE' COLLATE NOCASE AND NAME = '{oldFormattedTableName}' COLLATE NOCASE;";
            var hasTable = command.Connection.DbConnection.ExecuteScalar<int>(hasTableScript, transaction: command.Connection.Transaction.DbTransaction) > 0;
            var scripts = new List<string>();
            if (hasTable)
            {
                scripts.Add($"ALTER TABLE {oldFormattedTableName} RENAME TO {newFormattedTableName};");
            }
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = scripts
            };
        }

        #endregion


        #region Get add filed statements

        /// <summary>
        /// Get add field scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetAddFieldScripts(SixnetAddFieldParameter parameter)
        {
            var table = parameter.Table;
            var fields = parameter.Fields;
            var command = parameter.Command;
            var formattedTableName = FormatAndWrapObjectName(table);
            var scripts = new List<string>();
            foreach (var field in fields)
            {
                var dataFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                var existScript = $"SELECT COUNT(*) FROM PRAGMA_TABLE_INFO('{formattedTableName}') WHERE NAME='{dataFieldName.Name}' COLLATE NOCASE;";
                var hasColumn = command.Connection.DbConnection.ExecuteScalar<int>(existScript, transaction: command.Connection.Transaction.DbTransaction) > 0;
                if (!hasColumn)
                {
                    scripts.Add($"ALTER TABLE {formattedTableName} ADD COLUMN {WrapObjectName(dataFieldName).Name}{GetFieldDefinition(field, command.MigrationInfo)};");
                }
            }
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = scripts
            };
        }

        #endregion

        #region Get delete filed statements

        protected override SixnetDatabaseScriptInfo GetDeleteFieldScripts(SixnetDeleteFieldParameter parameter)
        {
            var scripts = new List<string>();
            var table = parameter.Table;
            var fields = parameter.Fields;
            var command = parameter.Command;
            var formattedTableName = FormatAndWrapObjectName(table);
            foreach (var field in fields)
            {
                var dataFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                var existScript = $"SELECT COUNT(*) FROM PRAGMA_TABLE_INFO('{formattedTableName}') WHERE NAME='{dataFieldName.Name}' COLLATE NOCASE;";
                var hasColumn = command.Connection.DbConnection.ExecuteScalar<int>(existScript, transaction: command.Connection.Transaction.DbTransaction) > 0;
                if (hasColumn)
                {
                    scripts.Add($"ALTER TABLE {formattedTableName} DROP COLUMN {WrapObjectName(dataFieldName).Name};");
                }
            }
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = scripts
            };
        }

        #endregion

        #region Get update field statements 

        /// <summary>
        /// Get update field scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetUpdateFieldScripts(SixnetUpdateFieldParameter parameter)
        {
            var scripts = new List<string>();
            var table = parameter.Table;
            var fields = parameter.Fields;
            var command = parameter.Command;
            var formattedTableName = FormatAndWrapObjectName(table);
            foreach (var fieldItem in fields)
            {
                var field = fieldItem.Value;
                var nowFieldName = fieldItem.Key;
                var nowFormatedFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(nowFieldName, SixnetDatabaseObjectType.Column));
                var newFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));

                var existScript = $"SELECT COUNT(*) FROM PRAGMA_TABLE_INFO('{formattedTableName}') WHERE NAME='{nowFieldName}' COLLATE NOCASE;";
                var hasColumn = command.Connection.DbConnection.ExecuteScalar<int>(existScript, transaction: command.Connection.Transaction.DbTransaction) > 0;
                if (hasColumn)
                {
                    if (!string.Equals(nowFieldName, newFieldName.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        scripts.Add($"ALTER TABLE {formattedTableName} RENAME COLUMN {WrapObjectName(SixnetDatabaseObjectName.Create(nowFieldName, SixnetDatabaseObjectType.Column)).Name} TO {WrapObjectName(newFieldName).Name};");
                    }
                }
            }
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = scripts
            };
        }

        #endregion


        #region Add foreign key

        /// <summary>
        /// Get add foreign key scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetAddForeignKeyScripts(SixnetAddForeignKeyParameter parameter)
        {
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>(0)
            };
        }


        #endregion

        #region Delete foreign key

        /// <summary>
        /// Get delete foreign key scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public override SixnetDatabaseScriptInfo GetDeleteForeignKeyScripts(SixnetDeleteForeignKeyParameter parameter)
        {
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>(0)
            };
        }

        /// <summary>
        /// Get delete all foreign key scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetDeleteAllForeignKeyScripts(SixnetDeleteAllForeignKeyParameter parameter)
        {
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    "PRAGMA foreign_keys = OFF;"
                }
            };
        }

        #endregion


        #region Add index

        /// <summary>
        /// Get add index scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetAddIndexScripts(SixnetAddIndexParameter parameter)
        {
            var indexInfo = parameter.IndexInfo;
            var formattedAndWrapedTableName = FormatAndWrapObjectName(indexInfo.Table);
            var indexDefine = GetIndexDefine(indexInfo);
            var indexName = indexDefine.Item1;
            var indexFieldStrings = indexDefine.Item2;

            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    $"CREATE {(indexInfo.Unique ? "UNIQUE " : "")}INDEX IF NOT EXISTS {indexName} ON {formattedAndWrapedTableName} ({string.Join(",", indexFieldStrings)});"
                }
            };
        }

        #endregion

        #region Delete index

        /// <summary>
        /// Get delete index scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetDeleteIndexScripts(SixnetDeleteIndexParameter parameter)
        {
            var indexInfo = parameter.IndexInfo;
            var indexName = GetIndexDefine(indexInfo).Item1;
            var formattedAndWrapedTableName = FormatAndWrapObjectName(indexInfo.Table);

            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    $"DROP INDEX IF EXISTS {indexName};"
                }
            };
        }

        #endregion


        #region Get delete all view statements

        protected override SixnetDatabaseScriptInfo GetDeleteAllViewScripts(SixnetDeleteAllViewParameter parameter)
        {
            var sql = @"
SELECT 'DROP VIEW IF EXISTS ""' || name || '"";'
FROM sqlite_master
WHERE type = 'view';
";
            var deleteScripts = parameter.Command.Connection.DbConnection.Query<string>(sql, transaction: parameter.Command.Connection.Transaction.DbTransaction);
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = deleteScripts?.ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region Get delete all function statements

        /// <summary>
        /// Get delete all function statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetDeleteAllFunctionScripts(SixnetDeleteAllFunctionParameter parameter)
        {
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>(0)
            };
        }

        #endregion

        #region Get delete all custom type statements

        protected override SixnetDatabaseScriptInfo GetDeleteAllCustomTypeScripts(SixnetDeleteAllCustomerTypeParameter parameter)
        {
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>(0)
            };
        }

        #endregion

        #region Get delete all procedure statements

        protected override SixnetDatabaseScriptInfo GetDeleteAllProcedureScripts(SixnetDeleteAllProcedureParameter parameter)
        {
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>(0)
            };
        }

        #endregion

        #endregion

        #region Util

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

        #endregion
    }
}
