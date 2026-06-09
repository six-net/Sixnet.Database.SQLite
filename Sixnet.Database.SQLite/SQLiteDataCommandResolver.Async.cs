using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Sixnet.Development.Data;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Database;
using Sixnet.Development.Data.Field;
using Sixnet.Development.Entity;
using Sixnet.Development.Queryable;
using Sixnet.Exceptions;

namespace Sixnet.Database.SQLite
{
    internal partial class SQLiteDataCommandResolver
    {
        #region Get query statement

        /// <summary>
        /// Get query statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <param name="translationResult">Queryable translation result</param>
        /// <param name="location">Queryable location</param>
        /// <returns></returns>
        protected override async Task<SixnetQueryDatabaseStatement> GenerateQueryStatementCoreAsync(SixnetDataCommandResolveContext context, SixnetQueryableTranslationResult translationResult, SixnetQueryableLocation location)
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
                        var targetStatement = await GetFromTargetStatementAsync(context, queryable, location, tablePetName).ConfigureAwait(false);
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
                    var outputFieldString = await FormatFieldsStringAsync(context, queryable, location, SixnetFieldLocation.Output, outputFields).ConfigureAwait(false);
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
        protected override async Task<List<SixnetExecutionDatabaseStatement>> GenerateInsertStatementsAsync(SixnetDataCommandResolveContext context)
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
                insertValues.Add(await FormatInsertValueFieldAsync(context, command.Queryable, insertValue).ConfigureAwait(false));

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
            var tableNames = await dataCommandExecutionContext.GetTableNamesAsync().ConfigureAwait(false);

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
        protected override async Task<List<SixnetExecutionDatabaseStatement>> GenerateUpdateStatementsAsync(SixnetDataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            SixnetException.ThrowIf(command?.FieldsAssignment?.NewValues.IsNullOrEmpty() ?? true, "No set update field");

            #region translate

            var translationResult = await TranslateAsync(context).ConfigureAwait(false);
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
                var newValueExpression = await FormatUpdateValueFieldAsync(context, command, newValue).ConfigureAwait(false);
                updateSetArray.Add($"{fieldFormattedName}={newValueExpression}");
            }
            var tableNames = await dataCommandExecutionContext.GetTableNamesAsync(command).ConfigureAwait(false);
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

                var formatedPrimaryKeyFields = new List<string>();
                foreach (var primaryKeyField in primaryKeyFields)
                {
                    formatedPrimaryKeyFields.Add(await FormatFieldAsync(context, command.Queryable, primaryKeyField, SixnetQueryableLocation.Top, SixnetFieldLocation.Criterion, tablePetName: tablePetName).ConfigureAwait(false));
                }
                var primaryKeyString = string.Join("||", formatedPrimaryKeyFields);
                var queryStatement = await GenerateQueryStatementCoreAsync(context, translationResult, SixnetQueryableLocation.UsingSource).ConfigureAwait(false);
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
        protected override async Task<List<SixnetExecutionDatabaseStatement>> GenerateDeleteStatementsAsync(SixnetDataCommandResolveContext context)
        {
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var command = dataCommandExecutionContext.Command;

            #region translate

            var translationResult = await TranslateAsync(context).ConfigureAwait(false);
            var condition = translationResult?.GetCondition(ConditionStartKeyword);
            var join = translationResult?.GetJoin();
            var preScripts = context.GetPreScripts();

            #endregion

            #region script

            var tableNames = await dataCommandExecutionContext.GetTableNamesAsync(command).ConfigureAwait(false);
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

                var formatedPrimaryKeyFields = new List<string>();
                foreach (var primaryKeyField in primaryKeyFields)
                {
                    formatedPrimaryKeyFields.Add(await FormatFieldAsync(context, command.Queryable, primaryKeyField, SixnetQueryableLocation.Top, SixnetFieldLocation.Criterion, tablePetName: tablePetName).ConfigureAwait(false));
                }
                var primaryKeyString = string.Join("||", formatedPrimaryKeyFields);
                var queryStatement = await GenerateQueryStatementCoreAsync(context, translationResult, SixnetQueryableLocation.UsingSource).ConfigureAwait(false);
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
    }
}
