﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using EZNEW.Development.Command;
using EZNEW.Development.Query;
using EZNEW.Development.Query.Translation;
using EZNEW.Development.Entity;
using EZNEW.Exceptions;
using EZNEW.Data.Conversion;

namespace EZNEW.Data.SQLite
{
    /// <summary>
    /// Query translator implementation for the SQLite
    /// </summary>
    public class SQLiteQueryTranslator : IQueryTranslator
    {
        #region Fields

        const string EqualOperator = "=";
        const string GreaterThanOperator = ">";
        const string GreaterThanOrEqualOperator = ">=";
        const string NotEqualOperator = "<>";
        const string LessThanOperator = "<";
        const string LessThanOrEqualOperator = "<=";
        const string InOperator = "IN";
        const string NotInOperator = "NOT IN";
        const string LikeOperator = "LIKE";
        const string NotLikeOperator = "NOT LIKE";
        const string IsNullOperator = "IS NULL";
        const string NotNullOperator = "IS NOT NULL";
        const string DescKeyWord = "DESC";
        const string AscKeyWord = "ASC";
        public const string DefaultObjectPetName = "TB";
        const string TreeTableName = "RecurveTable";
        const string TreeTablePetName = "RTT";
        static readonly Dictionary<JoinType, string> joinOperatorDict = new Dictionary<JoinType, string>()
        {
            { JoinType.InnerJoin,"INNER JOIN" },
            { JoinType.CrossJoin,"CROSS JOIN" },
            { JoinType.LeftJoin,"LEFT JOIN" },
            { JoinType.RightJoin,"RIGHT JOIN" },
            { JoinType.FullJoin,"FULL JOIN" }
        };
        int subObjectSequence = 0;
        int recurveObjectSequence = 0;

        #endregion

        #region Properties

        /// <summary>
        /// Gets or sets the query object pet name
        /// </summary>
        public string ObjectPetName => DefaultObjectPetName;

        /// <summary>
        /// Gets or sets the parameter sequence
        /// </summary>
        public int ParameterSequence { get; set; } = 0;

        /// <summary>
        /// Gets or sets the data access context
        /// </summary>
        public DataAccessContext DataAccessContext { get; set; }

        /// <summary>
        /// Gets the database server
        /// </summary>
        DatabaseServer DatabaseServer => DataAccessContext.Server;

        /// <summary>
        /// Gets the database server type
        /// </summary>
        DatabaseServerType DatabaseServerType => SQLiteManager.CurrentDatabaseServerType;

        #endregion

        #region Functions

        /// <summary>
        /// Translate query object
        /// </summary>
        /// <param name="query">Query object</param>
        /// <returns>Return a translation result</returns>
        public QueryTranslationResult Translate(IQuery query)
        {
            Init();
            var result = ExecuteTranslation(query, QueryLocation.Top);
            if (!result.WithScripts.IsNullOrEmpty())
            {
                result.PreScript = FormatWithScript(result.WithScripts);
            }
            if (!string.IsNullOrWhiteSpace(result.JoinExtraConditionString))
            {
                result.ConditionString = string.IsNullOrWhiteSpace(result.ConditionString) ? result.JoinExtraConditionString : $"{result.ConditionString} AND {result.JoinExtraConditionString}";
            }
            return result;
        }

        /// <summary>
        /// Execute translation
        /// </summary>
        /// <param name="query">Query object</param>
        /// <param name="location">Query location</param>
        /// <param name="parameters">Parameters</param>
        /// <param name="objectName">Entity object name</param>
        /// <param name="useSort">Indicates whether use sort</param>
        /// <returns>Return a translation result</returns>
        public QueryTranslationResult ExecuteTranslation(IQuery query, QueryLocation location, CommandParameters parameters = null, string objectName = "", bool useSort = true)
        {
            if (query == null)
            {
                return QueryTranslationResult.Empty;
            }
            StringBuilder conditionBuilder = new StringBuilder();
            if (query.ExecutionMode == QueryExecutionMode.QueryObject)
            {
                StringBuilder orderBuilder = new StringBuilder();
                parameters = parameters ?? new CommandParameters();
                objectName = string.IsNullOrWhiteSpace(objectName) ? DefaultObjectPetName : objectName;
                List<string> withScripts = new List<string>();
                string recurveTableName = string.Empty;
                string recurveTablePetName = string.Empty;

                #region condition

                if (!query.Conditions.IsNullOrEmpty())
                {
                    int index = 0;
                    foreach (var condition in query.Conditions)
                    {
                        var conditionResult = TranslateCondition(query, condition, parameters, objectName);
                        if (!conditionResult.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(conditionResult.WithScripts);
                            recurveTableName = conditionResult.RecurveObjectName;
                            recurveTablePetName = conditionResult.RecurvePetName;
                        }
                        conditionBuilder.Append($" {(index > 0 ? condition.Connector.ToString().ToUpper() : string.Empty)} {conditionResult.ConditionString}");
                        index++;
                    }
                }

                #endregion

                #region sort

                if (useSort && !query.Sorts.IsNullOrEmpty())
                {
                    foreach (var sortEntry in query.Sorts)
                    {
                        orderBuilder.Append($"{ConvertSortFieldName(query, objectName, sortEntry)} {(sortEntry.Desc ? DescKeyWord : AscKeyWord)},");
                    }
                }

                #endregion

                #region combine

                StringBuilder combineBuilder = new StringBuilder();
                if (!query.Combines.IsNullOrEmpty())
                {
                    foreach (var combineEntry in query.Combines)
                    {
                        if (combineEntry?.Query == null)
                        {
                            continue;
                        }
                        var combineObjectPetName = GetNewSubObjectPetName();
                        string combineObjectName = DataAccessContext.GetCombineEntityObjectName(combineEntry.Query);
                        var combineQueryResult = ExecuteTranslation(combineEntry.Query, QueryLocation.Combine, parameters, combineObjectPetName, true);
                        string combineConditionString = string.IsNullOrWhiteSpace(combineQueryResult.ConditionString) ? string.Empty : $"WHERE {combineQueryResult.ConditionString}";
                        combineBuilder.Append($" {GetCombineOperator(combineEntry.Type)} SELECT {string.Join(",", SQLiteManager.FormatQueryFields(combineObjectPetName, query, query.GetEntityType(), true, false))} FROM {SQLiteManager.WrapKeyword(combineObjectName)} AS {combineObjectPetName} {(combineQueryResult.AllowJoin ? combineQueryResult.JoinScript : string.Empty)} {combineConditionString}");
                        if (!combineQueryResult.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(combineQueryResult.WithScripts);
                            recurveTableName = combineQueryResult.RecurveObjectName;
                            recurveTablePetName = combineQueryResult.RecurvePetName;
                        }
                    }
                }

                #endregion

                #region join

                bool allowJoin = true;
                StringBuilder joinBuilder = new StringBuilder();
                StringBuilder joinExtraCondition = new StringBuilder();
                if (!query.Joins.IsNullOrEmpty())
                {
                    foreach (var joinEntry in query.Joins)
                    {
                        if (joinEntry == null || joinEntry.JoinObjectFilter == null)
                        {
                            continue;
                        }
                        if (joinEntry.JoinObjectFilter.GetEntityType() == null)
                        {
                            throw new EZNEWException("IQuery object must set entity type if use in join operation");
                        }
                        string joinObjectPetName = GetNewSubObjectPetName();
                        var joinQueryResult = ExecuteTranslation(joinEntry.JoinObjectFilter, QueryLocation.Join, parameters, joinObjectPetName, true);
                        if (string.IsNullOrWhiteSpace(joinQueryResult.CombineScript))
                        {
                            var joinResult = GetJoinCondition(query, joinEntry, parameters, objectName, joinObjectPetName);
                            if (!joinResult.WithScripts.IsNullOrEmpty())
                            {
                                withScripts.AddRange(joinResult.WithScripts);
                                recurveTableName = joinResult.RecurveObjectName;
                                recurveTablePetName = joinResult.RecurvePetName;
                            }
                            var joinConnection = joinResult.ConditionString;
                            if (!string.IsNullOrWhiteSpace(joinQueryResult.ConditionString))
                            {
                                if (joinQueryResult.AllowJoin && PositionJoinConditionToConnection(joinEntry.Type))
                                {
                                    joinConnection += $"{(string.IsNullOrWhiteSpace(joinConnection) ? " ON" : " AND ")}{joinQueryResult.ConditionString}";
                                }
                                else
                                {
                                    conditionBuilder.Append($"{(conditionBuilder.Length == 0 ? string.Empty : " AND ")}{joinQueryResult.ConditionString}");
                                }
                            }
                            if (!string.IsNullOrWhiteSpace(joinQueryResult.JoinExtraConditionString))
                            {
                                conditionBuilder.Append($"{(conditionBuilder.Length == 0 ? string.Empty : " AND ")}{joinQueryResult.JoinExtraConditionString}");
                            }
                            joinBuilder.Append($" {GetJoinOperator(joinEntry.Type)} {SQLiteManager.WrapKeyword(DataAccessContext.GetJoinEntityObjectName(joinEntry.JoinObjectFilter))} AS {joinObjectPetName}{joinConnection}");
                            if (joinEntry.JoinObjectExtraFilter != null)
                            {
                                var extraQueryResult = ExecuteTranslation(joinEntry.JoinObjectExtraFilter, QueryLocation.Join, parameters, joinObjectPetName, true);
                                if (!string.IsNullOrWhiteSpace(extraQueryResult.ConditionString))
                                {
                                    joinExtraCondition.Append(joinExtraCondition.Length > 0 ? $" AND {extraQueryResult.ConditionString}" : extraQueryResult.ConditionString);
                                }
                            }
                            if (joinQueryResult.AllowJoin && !string.IsNullOrWhiteSpace(joinQueryResult.JoinScript))
                            {
                                joinBuilder.Append($" {joinQueryResult.JoinScript}");
                            }
                        }
                        else
                        {
                            var combineJoinObjName = GetNewSubObjectPetName();
                            var joinResult = GetJoinCondition(query, joinEntry, parameters, objectName, combineJoinObjName);
                            if (!joinResult.WithScripts.IsNullOrEmpty())
                            {
                                withScripts.AddRange(joinResult.WithScripts);
                                recurveTableName = joinResult.RecurveObjectName;
                                recurveTablePetName = joinResult.RecurvePetName;
                            }
                            var joinConnection = joinResult.ConditionString;
                            joinBuilder.Append($" {GetJoinOperator(joinEntry.Type)} (SELECT {string.Join(",", SQLiteManager.FormatQueryFields(joinObjectPetName, joinEntry.JoinObjectFilter, joinEntry.JoinObjectFilter.GetEntityType(), false, false))} FROM {SQLiteManager.WrapKeyword(DataAccessContext.GetJoinEntityObjectName(joinEntry.JoinObjectFilter))} AS {joinObjectPetName} {(joinQueryResult.AllowJoin ? joinQueryResult.JoinScript : string.Empty)} {(string.IsNullOrWhiteSpace(joinQueryResult.ConditionString) ? string.Empty : "WHERE " + joinQueryResult.ConditionString)} {joinQueryResult.CombineScript}) AS {combineJoinObjName}{joinConnection}");
                        }
                        if (!joinQueryResult.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(joinQueryResult.WithScripts);
                            recurveTableName = joinQueryResult.RecurveObjectName;
                            recurveTablePetName = joinQueryResult.RecurvePetName;
                        }
                    }
                }
                string joinScript = joinBuilder.ToString();

                #endregion

                #region recurve script

                string conditionString = conditionBuilder.ToString();
                string joinExtraConditionString = joinExtraCondition.ToString();
                if (query.Recurve != null)
                {
                    allowJoin = false;
                    string nowConditionString = conditionString;
                    if (!string.IsNullOrWhiteSpace(joinExtraConditionString))
                    {
                        nowConditionString = string.IsNullOrWhiteSpace(nowConditionString) ? joinExtraConditionString : $"{nowConditionString} AND {joinExtraConditionString}";
                        joinExtraConditionString = string.Empty;
                    }
                    EntityField recurveField = DataManager.GetField(DatabaseServerType, query, query.Recurve.DataField);
                    EntityField recurveRelationField = DataManager.GetField(DatabaseServerType, query, query.Recurve.RelationField);
                    var recurveTable = GetNewRecurveTableName();
                    recurveTablePetName = recurveTable.Item1;
                    recurveTableName = recurveTable.Item2;
                    conditionString = $"{objectName}.{SQLiteManager.WrapKeyword(recurveField.FieldName)} IN (SELECT {recurveTablePetName}.{SQLiteManager.WrapKeyword(recurveField.FieldName)} FROM {SQLiteManager.WrapKeyword(recurveTableName)} AS {recurveTablePetName})";
                    DataAccessContext.SetActivityQuery(query, location);
                    string queryObjectName = DataManager.GetEntityObjectName(DataAccessContext);
                    string withScript =
                    $"{recurveTableName} AS (SELECT {objectName}.{SQLiteManager.WrapKeyword(recurveField.FieldName)},{objectName}.{SQLiteManager.WrapKeyword(recurveRelationField.FieldName)} FROM {SQLiteManager.WrapKeyword(queryObjectName)} AS {objectName} {joinScript} {(string.IsNullOrWhiteSpace(nowConditionString) ? string.Empty : $"WHERE {nowConditionString}")} " +
                    $"UNION ALL SELECT {objectName}.{SQLiteManager.WrapKeyword(recurveField.FieldName)},{objectName}.{SQLiteManager.WrapKeyword(recurveRelationField.FieldName)} FROM {SQLiteManager.WrapKeyword(queryObjectName)} AS {objectName} JOIN {recurveTableName} AS {recurveTablePetName} " +
                    $"ON {(query.Recurve.Direction == RecurveDirection.Up ? $"{objectName}.{SQLiteManager.WrapKeyword(recurveField.FieldName)}={recurveTablePetName}.{SQLiteManager.WrapKeyword(recurveRelationField.FieldName)}" : $"{objectName}.{SQLiteManager.WrapKeyword(recurveRelationField.FieldName)}={recurveTablePetName}.{SQLiteManager.WrapKeyword(recurveField.FieldName)}")})";
                    withScripts.Add(withScript);
                }
                var result = QueryTranslationResult.Create(conditionString, orderBuilder.ToString().Trim(','), parameters);
                result.JoinScript = joinScript;
                result.AllowJoin = allowJoin;
                result.WithScripts = withScripts;
                result.RecurveObjectName = recurveTableName;
                result.RecurvePetName = recurveTablePetName;
                result.CombineScript = combineBuilder.ToString();
                result.JoinExtraConditionString = joinExtraConditionString;

                #endregion

                return result;
            }
            else
            {
                conditionBuilder.Append(query.Text);
                return QueryTranslationResult.Create(conditionBuilder.ToString(), string.Empty, query.TextParameters);
            }
        }

        /// <summary>
        /// Translate condition
        /// </summary>
        /// <param name="sourceQuery">Source query</param>
        /// <param name="condition">Condition</param>
        /// <param name="parameters">Parameters</param>
        /// <param name="objectName">Object name</param>
        /// <returns></returns>
        QueryTranslationResult TranslateCondition(IQuery sourceQuery, ICondition condition, CommandParameters parameters, string objectName)
        {
            if (condition == null)
            {
                return QueryTranslationResult.Empty;
            }
            if (condition is Criterion criterion)
            {
                return TranslateCriterion(sourceQuery, criterion, parameters, objectName);
            }
            if (condition is IQuery groupQuery && !groupQuery.Conditions.IsNullOrEmpty())
            {
                groupQuery.SetEntityType(sourceQuery.GetEntityType());
                var conditionCount = groupQuery.Conditions.Count();
                if (conditionCount == 1)
                {
                    var firstCondition = groupQuery.Conditions.First();
                    if (firstCondition is Criterion firstCriterion)
                    {
                        return TranslateCriterion(groupQuery, firstCriterion, parameters, objectName);
                    }
                    return TranslateCondition(groupQuery, firstCondition, parameters, objectName);
                }
                StringBuilder subCondition = new StringBuilder("(");
                List<string> groupWithScripts = new List<string>();
                string recurveTableName = string.Empty;
                string recurveTablePetName = string.Empty;
                int index = 0;
                foreach (var subQueryItem in groupQuery.Conditions)
                {
                    var subGroupResult = TranslateCondition(groupQuery, subQueryItem, parameters, objectName);
                    if (!subGroupResult.WithScripts.IsNullOrEmpty())
                    {
                        recurveTableName = subGroupResult.RecurveObjectName;
                        recurveTablePetName = subGroupResult.RecurvePetName;
                        groupWithScripts.AddRange(subGroupResult.WithScripts);
                    }
                    subCondition.Append($" {(index > 0 ? subQueryItem.Connector.ToString().ToUpper() : string.Empty)} {subGroupResult.ConditionString}");
                    index++;
                }
                var groupResult = QueryTranslationResult.Create(subCondition.Append(")").ToString());
                groupResult.RecurveObjectName = recurveTableName;
                groupResult.RecurvePetName = recurveTablePetName;
                groupResult.WithScripts = groupWithScripts;
                return groupResult;
            }
            return QueryTranslationResult.Empty;
        }

        /// <summary>
        /// Translate criterion
        /// </summary>
        /// <param name="sourceQuery">Source query</param>
        /// <param name="criterion">Criterion</param>
        /// <param name="parameters">Parameters</param>
        /// <param name="objectName">Object name</param>
        /// <returns>Return query translation result</returns>
        QueryTranslationResult TranslateCriterion(IQuery sourceQuery, Criterion criterion, CommandParameters parameters, string objectName)
        {
            if (criterion == null)
            {
                return QueryTranslationResult.Empty;
            }
            string sqlOperator = GetOperator(criterion.Operator);
            bool needParameter = OperatorNeedParameter(criterion.Operator);
            string criterionFieldName = ConvertCriterionFieldName(sourceQuery, objectName, criterion);
            if (!needParameter)
            {
                return QueryTranslationResult.Create($"{criterionFieldName} {sqlOperator}");
            }
            if (criterion.Value is IQuery subquery)
            {
                return TranslateSubquery(subquery, parameters, criterionFieldName, sqlOperator);
            }
            string parameterName = GetNewParameterName(criterion.Field.Name);
            var needWrapParameter = NeedWrapParameter(criterion.Operator);
            if (needWrapParameter)
            {
                var parameterValues = criterion.Value;
                List<string> parameterNames = new List<string>();
                int valParameterIndex = 0;
                foreach (var val in parameterValues)
                {
                    var valParameterName = $"{parameterName}{valParameterIndex++}";
                    parameterNames.Add($"{SQLiteManager.ParameterPrefix}{valParameterName}");
                    parameters.Add(valParameterName, FormatCriterionValue(criterion.Operator, val));
                }
                var criterionCondition = $"{criterionFieldName} {sqlOperator} ({string.Join(",", parameterNames)})";
                return QueryTranslationResult.Create(criterionCondition);
            }
            else
            {
                parameters.Add(parameterName, FormatCriterionValue(criterion.Operator, criterion.Value));
                var criterionCondition = $"{criterionFieldName} {sqlOperator} {SQLiteManager.ParameterPrefix}{parameterName}";
                return QueryTranslationResult.Create(criterionCondition);
            }
        }

        /// <summary>
        /// Translate subquery
        /// </summary>
        /// <param name="subquery">Subquery</param>
        /// <param name="parameters">parameters</param>
        /// <param name="criterionFieldName">Field name</param>
        /// <param name="sqlOperator">Sql operator</param>
        /// <param name="objectName">Object name</param>
        /// <returns></returns>
        /// <exception cref="EZNEWException"></exception>
        QueryTranslationResult TranslateSubquery(IQuery subquery, CommandParameters parameters, string criterionFieldName, string sqlOperator)
        {
            var subqueryObjectName = DataAccessContext.GetSubqueryEntityObjectName(subquery);
            if (subquery.QueryFields.IsNullOrEmpty())
            {
                throw new EZNEWException($"The {subqueryObjectName} query object that is a subquery must have at least one query field set");
            }
            var subqueryField = DataManager.GetField(DatabaseServerType, subquery, subquery.QueryFields.First());
            string subqueryObjectPetName = GetNewSubObjectPetName();
            var subqueryLimitResult = GetSubqueryLimitCondition(sqlOperator, subquery.QuerySize);
            string topString = subqueryLimitResult.Item2;
            var userSort = !string.IsNullOrWhiteSpace(topString);
            var subqueryTranslationResult = ExecuteTranslation(subquery, QueryLocation.Subuery, parameters, subqueryObjectPetName, userSort);
            string subqueryConditionString = string.IsNullOrWhiteSpace(subqueryTranslationResult.ConditionString) ? string.Empty : $"WHERE {subqueryTranslationResult.ConditionString}";
            string subquerySortString = string.IsNullOrWhiteSpace(subqueryTranslationResult.SortString) ? string.Empty : $"ORDER BY {subqueryTranslationResult.SortString}";
            string subqueryCondition;
            if (subqueryLimitResult.Item1)
            {
                subqueryCondition = string.IsNullOrWhiteSpace(subqueryTranslationResult.CombineScript)
                    ? $"{criterionFieldName} {sqlOperator} (SELECT {SQLiteManager.WrapKeyword(subqueryField.FieldName)} FROM (SELECT {subqueryObjectPetName}.{SQLiteManager.WrapKeyword(subqueryField.FieldName)} FROM {SQLiteManager.WrapKeyword(subqueryObjectName)} AS {subqueryObjectPetName} {(subqueryTranslationResult.AllowJoin ? subqueryTranslationResult.JoinScript : string.Empty)} {subqueryConditionString} {subquerySortString} {topString}) AS S{subqueryObjectPetName})"
                    : $"{criterionFieldName} {sqlOperator} (SELECT {SQLiteManager.WrapKeyword(subqueryField.FieldName)} FROM (SELECT {subqueryObjectPetName}.{SQLiteManager.WrapKeyword(subqueryField.FieldName)} FROM (SELECT {string.Join(",", SQLiteManager.FormatQueryFields(subqueryObjectPetName, subquery, subquery.GetEntityType(), true, false))} FROM {SQLiteManager.WrapKeyword(subqueryObjectName)} AS {subqueryObjectPetName} {(subqueryTranslationResult.AllowJoin ? subqueryTranslationResult.JoinScript : string.Empty)} {subqueryConditionString} {subqueryTranslationResult.CombineScript}) AS {subqueryObjectPetName} {subquerySortString} {topString}) AS S{subqueryObjectPetName})";
            }
            else
            {
                subqueryCondition = string.IsNullOrWhiteSpace(subqueryTranslationResult.CombineScript)
                    ? $"{criterionFieldName} {sqlOperator} (SELECT {subqueryObjectPetName}.{SQLiteManager.WrapKeyword(subqueryField.FieldName)} FROM {SQLiteManager.WrapKeyword(subqueryObjectName)} AS {subqueryObjectPetName} {(subqueryTranslationResult.AllowJoin ? subqueryTranslationResult.JoinScript : string.Empty)} {subqueryConditionString} {subquerySortString} {topString})"
                    : $"{criterionFieldName} {sqlOperator} (SELECT {subqueryObjectPetName}.{SQLiteManager.WrapKeyword(subqueryField.FieldName)} FROM (SELECT {string.Join(",", SQLiteManager.FormatQueryFields(subqueryObjectPetName, subquery, subquery.GetEntityType(), true, false))} FROM {SQLiteManager.WrapKeyword(subqueryObjectName)} AS {subqueryObjectPetName} {(subqueryTranslationResult.AllowJoin ? subqueryTranslationResult.JoinScript : string.Empty)} {subqueryConditionString} {subqueryTranslationResult.CombineScript}) AS {subqueryObjectPetName} {subquerySortString} {topString})";
            }
            var subqueryResult = QueryTranslationResult.Create(subqueryCondition);
            if (!subqueryTranslationResult.WithScripts.IsNullOrEmpty())
            {
                subqueryResult.WithScripts = new List<string>(subqueryTranslationResult.WithScripts);
                subqueryResult.RecurveObjectName = subqueryTranslationResult.RecurveObjectName;
                subqueryResult.RecurvePetName = subqueryTranslationResult.RecurvePetName;
            }
            return subqueryResult;
        }

        /// <summary>
        /// Get join condition
        /// </summary>
        /// <param name="sourceQuery">Source query</param>
        /// <param name="joinEntry">Join entry</param>
        /// <param name="sourceObjectPetName">Source object pet name</param>
        /// <param name="targetObjectPetName">Target object pet name</param>
        /// <returns>Return join condition</returns>
        QueryTranslationResult GetJoinCondition(IQuery sourceQuery, JoinEntry joinEntry, CommandParameters parameters, string sourceObjectPetName, string targetObjectPetName)
        {
            string GetJoinCriteriaConnector(int criteriaIndex, CriterionConnector connector)
            {
                if (criteriaIndex < 1)
                {
                    return string.Empty;
                }
                return $" {connector.ToString().ToUpper()} ";
            }

            if (joinEntry.Type == JoinType.CrossJoin)
            {
                return QueryTranslationResult.Empty;
            }
            var joinCriteria = joinEntry.JoinCriteria;
            var sourceEntityType = sourceQuery.GetEntityType();
            var targetEntityType = joinEntry.JoinObjectFilter.GetEntityType();
            if (joinCriteria.IsNullOrEmpty())
            {
                if (sourceEntityType == targetEntityType)
                {
                    var primaryKeys = EntityManager.GetPrimaryKeys(sourceEntityType);
                    if (primaryKeys.IsNullOrEmpty())
                    {
                        return QueryTranslationResult.Empty;
                    }
                    joinCriteria = primaryKeys.Select(pk =>
                    {
                        var pkJoinField = new JoinField()
                        {
                            Name = pk,
                            Type = JoinFieldType.Field
                        };
                        return RegularJoinCriterion.Create(FieldInfo.Create(pk), CriterionOperator.Equal, FieldInfo.Create(pk)) as IJoinCriterion;
                    }).ToList();
                }
                else
                {
                    var joinFields = EntityManager.GetRelationFields(sourceEntityType, targetEntityType);
                    joinCriteria = joinFields?.Select(jf =>
                    {
                        return RegularJoinCriterion.Create(FieldInfo.Create(jf.Key), CriterionOperator.Equal, FieldInfo.Create(jf.Value)) as IJoinCriterion;
                    }).ToList();
                }
                if (joinCriteria.IsNullOrEmpty())
                {
                    throw new EZNEWException($"Not set relation key between {sourceEntityType.FullName} and {targetEntityType.FullName}");
                }
            }
            List<string> joinList = new List<string>();
            List<string> withScripts = new List<string>();
            string recurveTableName = string.Empty;
            string recurveTablePetName = string.Empty;
            int joinCriterionIndex = 0;
            foreach (var criterion in joinCriteria)
            {
                if (criterion is RegularJoinCriterion regularJoinCriterion)
                {
                    string criterionConnector = GetJoinCriteriaConnector(joinCriterionIndex++, criterion.Connector);
                    string sqlOperator = GetOperator(regularJoinCriterion.Operator);
                    if (regularJoinCriterion.Value is FieldInfo joinField)
                    {
                        string leftFieldName = string.Empty;
                        string rightFieldName = string.Empty;
                        if (regularJoinCriterion.IsRightCriterion)
                        {
                            leftFieldName = ConvertFieldName(joinEntry.JoinObjectFilter, targetObjectPetName, joinField);
                            rightFieldName = ConvertFieldName(sourceQuery, sourceObjectPetName, regularJoinCriterion.Field);
                        }
                        else
                        {
                            rightFieldName = ConvertFieldName(joinEntry.JoinObjectFilter, targetObjectPetName, joinField);
                            leftFieldName = ConvertFieldName(sourceQuery, sourceObjectPetName, regularJoinCriterion.Field);
                        }
                        joinList.Add($"{criterionConnector}{leftFieldName} {sqlOperator} {rightFieldName}");
                    }
                    else if (regularJoinCriterion.Value is IQuery valueQuery)
                    {
                        string joinCritrionObjectPetName = criterion.IsRightCriterion ? targetObjectPetName : sourceObjectPetName;
                        string sourceFieldName = ConvertFieldName(sourceQuery, joinCritrionObjectPetName, regularJoinCriterion.Field);
                        var subqueryTranResult = TranslateSubquery(valueQuery, parameters, sourceFieldName, sqlOperator);
                        joinList.Add($"{criterionConnector}{subqueryTranResult.ConditionString}");
                        if (!subqueryTranResult.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(subqueryTranResult.WithScripts);
                            recurveTableName = subqueryTranResult.RecurveObjectName;
                            recurveTablePetName = subqueryTranResult.RecurvePetName;
                        }
                    }
                    else
                    {
                        string joinFieldName = ConvertFieldName(regularJoinCriterion.IsRightCriterion ? joinEntry.JoinObjectFilter : sourceQuery, regularJoinCriterion.IsRightCriterion ? targetObjectPetName : sourceObjectPetName, regularJoinCriterion.Field);
                        string parameterName = GetNewParameterName(regularJoinCriterion.Field?.Name);
                        parameters.Add(parameterName, FormatCriterionValue(regularJoinCriterion.Operator, regularJoinCriterion.Value));
                        joinList.Add($"{criterionConnector}{joinFieldName} {sqlOperator} {SQLiteManager.ParameterPrefix}{parameterName}");
                    }

                }
                else if (criterion is QueryJoinCriterion queryJoinCriterion)
                {
                    var queryTranResult = ExecuteTranslation(queryJoinCriterion.Criteria, QueryLocation.JoinConnection, parameters, queryJoinCriterion.IsRightCriterion ? targetObjectPetName : sourceObjectPetName, false);
                    if (!string.IsNullOrWhiteSpace(queryTranResult.ConditionString))
                    {
                        string criterionConnector = GetJoinCriteriaConnector(joinCriterionIndex++, criterion.Connector);
                        joinList.Add($"{criterionConnector}({queryTranResult.ConditionString})");
                        if (!queryTranResult.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(queryTranResult.WithScripts);
                            recurveTableName = queryTranResult.RecurveObjectName;
                            recurveTablePetName = queryTranResult.RecurvePetName;
                        }
                    }
                }
            }
            var joinResult = QueryTranslationResult.Create(joinList.IsNullOrEmpty() ? string.Empty : " ON " + string.Join("", joinList));
            joinResult.WithScripts = withScripts;
            joinResult.RecurveObjectName = recurveTableName;
            joinResult.RecurvePetName = recurveTablePetName;
            return joinResult;
        }

        /// <summary>
        /// Get sql operator by criterion operator
        /// </summary>
        /// <param name="criterionOperator">Criterion operator</param>
        /// <returns></returns>
        string GetOperator(CriterionOperator criterionOperator)
        {
            string sqlOperator = string.Empty;
            switch (criterionOperator)
            {
                case CriterionOperator.Equal:
                    sqlOperator = EqualOperator;
                    break;
                case CriterionOperator.GreaterThan:
                    sqlOperator = GreaterThanOperator;
                    break;
                case CriterionOperator.GreaterThanOrEqual:
                    sqlOperator = GreaterThanOrEqualOperator;
                    break;
                case CriterionOperator.NotEqual:
                    sqlOperator = NotEqualOperator;
                    break;
                case CriterionOperator.LessThan:
                    sqlOperator = LessThanOperator;
                    break;
                case CriterionOperator.LessThanOrEqual:
                    sqlOperator = LessThanOrEqualOperator;
                    break;
                case CriterionOperator.In:
                    sqlOperator = InOperator;
                    break;
                case CriterionOperator.NotIn:
                    sqlOperator = NotInOperator;
                    break;
                case CriterionOperator.Like:
                case CriterionOperator.BeginLike:
                case CriterionOperator.EndLike:
                    sqlOperator = LikeOperator;
                    break;
                case CriterionOperator.NotLike:
                case CriterionOperator.NotBeginLike:
                case CriterionOperator.NotEndLike:
                    sqlOperator = NotLikeOperator;
                    break;
                case CriterionOperator.IsNull:
                    sqlOperator = IsNullOperator;
                    break;
                case CriterionOperator.NotNull:
                    sqlOperator = NotNullOperator;
                    break;
            }
            return sqlOperator;
        }

        /// <summary>
        /// Indicates operator whether need parameter
        /// </summary>
        /// <param name="criterionOperator">Criterion operator</param>
        /// <returns></returns>
        bool OperatorNeedParameter(CriterionOperator criterionOperator)
        {
            bool needParameter = true;
            switch (criterionOperator)
            {
                case CriterionOperator.NotNull:
                case CriterionOperator.IsNull:
                    needParameter = false;
                    break;
            }
            return needParameter;
        }

        /// <summary>
        /// Format criterion value
        /// </summary>
        /// <param name="criterionOperator">Criterion operator</param>
        /// <param name="value">Value</param>
        /// <returns>Return formated criterion value</returns>
        dynamic FormatCriterionValue(CriterionOperator criterionOperator, dynamic value)
        {
            dynamic realValue = value;
            switch (criterionOperator)
            {
                case CriterionOperator.Like:
                case CriterionOperator.NotLike:
                    realValue = $"%{value}%";
                    break;
                case CriterionOperator.BeginLike:
                case CriterionOperator.NotBeginLike:
                    realValue = $"{value}%";
                    break;
                case CriterionOperator.EndLike:
                case CriterionOperator.NotEndLike:
                    realValue = $"%{value}";
                    break;
            }
            return realValue;
        }

        /// <summary>
        /// Convert criterion field name
        /// </summary>
        /// <param name="query">Query object</param>
        /// <param name="objectName">Object name</param>
        /// <param name="criterion">Criterion</param>
        /// <returns></returns>
        string ConvertCriterionFieldName(IQuery query, string objectName, Criterion criterion)
        {
            return ConvertFieldName(query, objectName, criterion.Field);
        }

        /// <summary>
        /// Convert sort field name
        /// </summary>
        /// <param name="query">Query object</param>
        /// <param name="objectName">Object name</param>
        /// <param name="sortEntry">Sort entry</param>
        /// <returns></returns>
        string ConvertSortFieldName(IQuery query, string objectName, SortEntry sortEntry)
        {
            return ConvertFieldName(query, objectName, sortEntry.Field);
        }

        /// <summary>
        /// Convert field name
        /// </summary>
        /// <param name="objectName">Object name</param>
        /// <param name="fieldInfo">Field info</param>
        /// <param name="fieldConversionOptions">Field conversion options</param>
        /// <returns>Return new field name</returns>
        string ConvertFieldName(IQuery query, string objectName, FieldInfo fieldInfo)
        {
            var field = DataManager.GetField(DatabaseServerType, query, fieldInfo?.Name);
            string fieldName = field.FieldName;
            if (!fieldInfo.HasConversion)
            {
                return $"{objectName}.{SQLiteManager.WrapKeyword(fieldName)}";
            }
            var fieldConversionResult = SQLiteManager.ConvertField(DatabaseServer, fieldInfo.ConversionOptions, objectName, fieldName);
            if (string.IsNullOrWhiteSpace(fieldConversionResult?.NewFieldName))
            {
                throw new EZNEWException($"{DatabaseServerType}-{fieldInfo.ConversionOptions.ConversionName}:new field name is null or empty.");
            }
            return fieldConversionResult.NewFieldName;
        }

        /// <summary>
        /// Get join operator
        /// </summary>
        /// <param name="joinType">Join type</param>
        /// <returns></returns>
        string GetJoinOperator(JoinType joinType)
        {
            return joinOperatorDict[joinType];
        }

        /// <summary>
        /// Determines whether position join condition to connection
        /// </summary>
        /// <param name="joinType">Join type</param>
        /// <returns></returns>
        bool PositionJoinConditionToConnection(JoinType joinType)
        {
            switch (joinType)
            {
                case JoinType.CrossJoin:
                    return false;
                case JoinType.InnerJoin:
                case JoinType.LeftJoin:
                case JoinType.RightJoin:
                case JoinType.FullJoin:
                default:
                    return true;
            }
        }

        /// <summary>
        /// Format with script
        /// </summary>
        /// <returns>Return formated with script</returns>
        string FormatWithScript(List<string> withScripts)
        {
            if (withScripts.IsNullOrEmpty())
            {
                return string.Empty;
            }
            return $"WITH RECURSIVE {string.Join(",", withScripts)}";
        }

        /// <summary>
        /// Get new recurve table name
        /// Item1:petname,Item2:fullname
        /// </summary>
        /// <returns></returns>
        Tuple<string, string> GetNewRecurveTableName()
        {
            var recurveIndex = (recurveObjectSequence++).ToString();
            return new Tuple<string, string>
                (
                    $"{TreeTablePetName}{recurveIndex}",
                    $"{TreeTableName}{recurveIndex}"
                );
        }

        /// <summary>
        /// Get a new sub object pet name
        /// </summary>
        /// <returns></returns>
        string GetNewSubObjectPetName()
        {
            return $"TSB{subObjectSequence++}";
        }

        /// <summary>
        /// Gets a new parameter name
        /// </summary>
        /// <returns></returns>
        string GetNewParameterName(string originParameterName)
        {
            return $"{originParameterName}{ParameterSequence++}";
        }

        /// <summary>
        /// Init translator
        /// </summary>
        void Init()
        {
            recurveObjectSequence = subObjectSequence = 0;
        }

        /// <summary>
        /// Get subquery limit condition
        /// Item1:use wapper subquery condition
        /// Item2:limit string
        /// </summary>
        /// <param name="sqlOperator">Sql operator</param>
        /// <param name="querySize">Query size</param>
        Tuple<bool, string> GetSubqueryLimitCondition(string sqlOperator, int querySize)
        {
            var limitString = string.Empty;
            bool useWapper = false;
            switch (sqlOperator)
            {
                case InOperator:
                case NotInOperator:
                    if (querySize > 0)
                    {
                        limitString = $"LIMIT 0,{querySize}";
                        useWapper = true;
                    }
                    break;
                default:
                    limitString = $"LIMIT 0,1";
                    break;
            }
            return new Tuple<bool, string>(useWapper, limitString);
        }

        /// <summary>
        /// Get combine operator
        /// </summary>
        /// <param name="combineType">Combine type</param>
        /// <returns>Return combine operator</returns>
        string GetCombineOperator(CombineType combineType)
        {
            switch (combineType)
            {
                case CombineType.UnionAll:
                default:
                    return "UNION ALL";
                case CombineType.Union:
                    return "UNION";
                case CombineType.Except:
                    return "EXCEPT";
                case CombineType.Intersect:
                    return "INTERSECT";
            }
        }

        /// <summary>
        /// Get combine fields
        /// </summary>
        /// <param name="originalQuery">Original query</param>
        /// <param name="combineQuery">Combine query</param>
        /// <returns></returns>
        IEnumerable<string> GetCombineFields(IQuery originalQuery, IQuery combineQuery)
        {
            if (!combineQuery.QueryFields.IsNullOrEmpty())
            {
                return combineQuery.QueryFields;
            }
            var entityType = combineQuery.GetEntityType();
            var primaryKeys = EntityManager.GetPrimaryKeys(entityType);
            if (primaryKeys.IsNullOrEmpty())
            {
                return EntityManager.GetQueryFields(entityType);
            }
            return primaryKeys;
        }

        /// <summary>
        /// Check parameter whether need wrap
        /// </summary>
        /// <param name="criterionOperator">Criterion operator</param>
        /// <returns></returns>
        bool NeedWrapParameter(CriterionOperator criterionOperator)
        {
            switch (criterionOperator)
            {
                case CriterionOperator.In:
                case CriterionOperator.NotIn:
                    return true;
                default:
                    return false;
            }
        }

        #endregion
    }
}
