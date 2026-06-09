using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Sixnet.Development.Data.Field.Formatting;
using Sixnet.Exceptions;

namespace Sixnet.Database.SQLite
{
    /// <summary>
    /// Default field converter for sqlite
    /// </summary>
    public class SQLiteDefaultFieldFormatter : ISixnetFieldFormatter
    {
        static List<StringComparison> StringIgnoreCaseValues = new List<StringComparison>()
        {
            StringComparison.OrdinalIgnoreCase,
            StringComparison.InvariantCultureIgnoreCase,
            StringComparison.CurrentCultureIgnoreCase
        };

        static readonly Dictionary<string, string> DateTimeFormatMap = new Dictionary<string, string>()
        {
            {"yyyy", "%Y"},
            {"yy", "%y"},
            {"MM", "%m"},
            {"dd", "%d"},
            {"HH", "%H"},
            {"hh", "%I"},
            {"mm", "%M"},
            {"ss", "%S"},
            {"fff", "%f"},
            {"tt", "%p"}
        };
        static Regex DateTimeFormatRegex = null;
        static SQLiteDefaultFieldFormatter()
        {
            var sortedKeys = DateTimeFormatMap.Keys.OrderByDescending(k => k.Length);
            string pattern = string.Join("|", sortedKeys.Select(Regex.Escape));
            DateTimeFormatRegex = new Regex(pattern);
        }

        public string Format(SixnetFormatFieldContext context)
        {
            var formatOption = context.FormatSetting;
            var formatedFieldName = context.FieldName;
            var parameterString = formatOption.Parameter?.ToString();
            formatedFieldName = formatOption.Name switch
            {
                SixnetFieldFormatterNames.TO_STRING => $"CAST({formatedFieldName} AS TEXT)",
                SixnetFieldFormatterNames.DISTINCT => $"DISTINCT {formatedFieldName}",
                SixnetFieldFormatterNames.IS_NULL => $"{formatedFieldName} IS NULL",
                SixnetFieldFormatterNames.NOT_NULL => $"{formatedFieldName} IS NOT NULL",
                SixnetFieldFormatterNames.CHARLENGTH => $"LENGTH({formatedFieldName})",
                SixnetFieldFormatterNames.COUNT => $"COUNT({formatedFieldName})",
                SixnetFieldFormatterNames.SUM => $"SUM({formatedFieldName})",
                SixnetFieldFormatterNames.MAX => $"MAX({formatedFieldName})",
                SixnetFieldFormatterNames.MIN => $"MIN({formatedFieldName})",
                SixnetFieldFormatterNames.AVG => $"AVG({formatedFieldName})",
                SixnetFieldFormatterNames.JSON_VALUE => $"JSON_EXTRACT({formatedFieldName},{parameterString})",
                SixnetFieldFormatterNames.AND => $"({formatedFieldName}&{parameterString})",
                SixnetFieldFormatterNames.OR => $"({formatedFieldName}|{parameterString})",
                SixnetFieldFormatterNames.XOR => $"({formatedFieldName}^{parameterString})",
                SixnetFieldFormatterNames.NOT => $"(~{formatedFieldName})",
                SixnetFieldFormatterNames.ADD => $"({formatedFieldName}+{parameterString})",
                SixnetFieldFormatterNames.SUBTRACT => $"({formatedFieldName}-{parameterString})",
                SixnetFieldFormatterNames.MULTIPLY => $"({formatedFieldName}*{parameterString})",
                SixnetFieldFormatterNames.DIVIDE => $"({formatedFieldName}/{parameterString})",
                SixnetFieldFormatterNames.MODULO => $"({formatedFieldName}%{parameterString})",
                SixnetFieldFormatterNames.LEFT_SHIFT => $"({formatedFieldName}<<{parameterString})",
                SixnetFieldFormatterNames.RIGHT_SHIFT => $"({formatedFieldName}>>{parameterString})",
                SixnetFieldFormatterNames.TRIM => StringTrim(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.TRIM_START => StringTrimStart(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.TRIM_END => StringTrimEnd(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_CONCAT => $"({formatedFieldName}||{parameterString})",
                SixnetFieldFormatterNames.DATE_TIME_DATE => $"DATETIME(DATE({formatedFieldName}))",
                SixnetFieldFormatterNames.DATE_TIME_YEAR => $"CAST(STRFTIME('%Y', {formatedFieldName}) AS INTEGER)",
                SixnetFieldFormatterNames.DATE_TIME_MONTH => $"CAST(STRFTIME('%m', {formatedFieldName}) AS INTEGER)",
                SixnetFieldFormatterNames.DATE_TIME_DAY => $"CAST(STRFTIME('%d', {formatedFieldName}) AS INTEGER)",
                SixnetFieldFormatterNames.DATE_TIME_DAY_OF_YEAR => $"CAST(STRFTIME('%j', {formatedFieldName}) AS INTEGER)",
                SixnetFieldFormatterNames.DATE_TIME_DAY_OF_WEEK => $"CAST(STRFTIME('%w', {formatedFieldName}) AS INTEGER)",
                SixnetFieldFormatterNames.DATE_TIME_HOUR => $"CAST(STRFTIME('%H', {formatedFieldName}) AS INTEGER)",
                SixnetFieldFormatterNames.DATE_TIME_MINUTE => $"CAST(STRFTIME('%M', {formatedFieldName}) AS INTEGER)",
                SixnetFieldFormatterNames.DATE_TIME_SECOND => $"CAST(STRFTIME('%S', {formatedFieldName}) AS INTEGER)",
                SixnetFieldFormatterNames.DATE_TIME_MILLISECOND => $"CAST(STRFTIME('%f', column) * 1000 AS INTEGER)",
                SixnetFieldFormatterNames.DATE_TIME_TIME_OF_DAY => $"TIME({formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_UTC => string.Empty,
                SixnetFieldFormatterNames.DATE_TIME_FORMAT_STRING => FormatDateTimeString(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.DATE_TIME_STRING => FormatDateTimeString(formatedFieldName, "yyyy-MM-dd HH:mm:ss"),
                SixnetFieldFormatterNames.DATE_TIME_WITH_MILLISECOND_STRING => FormatDateTimeString(formatedFieldName, "yyyy-MM-dd HH:mm:fff"),
                SixnetFieldFormatterNames.DATE_STRING => FormatDateTimeString(formatedFieldName, "yyyy-MM-dd"),
                SixnetFieldFormatterNames.US_DATE_STRING => FormatDateTimeString(formatedFieldName, "MM/dd/yyyy"),
                SixnetFieldFormatterNames.JAPAN_DATE_STRING => FormatDateTimeString(formatedFieldName, "yyyy/MM/dd"),
                SixnetFieldFormatterNames.TIME_SPAN_DAYS => $"CAST(ABS({FormatDateTimeDiffString(formatedFieldName, formatOption.Parameter)}) AS INTEGER)",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_DAYS => $"ABS({FormatDateTimeDiffString(formatedFieldName, formatOption.Parameter)})",
                SixnetFieldFormatterNames.TIME_SPAN_HOURS => $"CAST(ABS({FormatDateTimeDiffString(formatedFieldName, formatOption.Parameter)}) * 24 AS INTEGER)",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_HOURS => $"(ABS({FormatDateTimeDiffString(formatedFieldName, formatOption.Parameter)}) * 24)",
                SixnetFieldFormatterNames.TIME_SPAN_MINUTES => $"CAST(ABS({FormatDateTimeDiffString(formatedFieldName, formatOption.Parameter)}) * 1440 AS INTEGER)",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_MINUTES => $"(ABS({FormatDateTimeDiffString(formatedFieldName, formatOption.Parameter)}) * 1440)",
                SixnetFieldFormatterNames.TIME_SPAN_SECONDS => $"CAST(ABS({FormatDateTimeDiffString(formatedFieldName, formatOption.Parameter)}) * 86400 AS INTEGER)",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_SECONDS => $"(ABS({FormatDateTimeDiffString(formatedFieldName, formatOption.Parameter)}) * 86400)",
                SixnetFieldFormatterNames.TIME_SPAN_MILLISECONDS => $"CAST(ABS({FormatDateTimeDiffString(formatedFieldName, formatOption.Parameter)}) * 86400000 AS INTEGER)",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_MILLISECONDS => $"(ABS({FormatDateTimeDiffString(formatedFieldName, formatOption.Parameter)}) * 86400000)",
                SixnetFieldFormatterNames.TO_LOWER => $"LOWER({formatedFieldName})",
                SixnetFieldFormatterNames.TO_UPPER => $"UPPER({formatedFieldName})",
                SixnetFieldFormatterNames.SUB_STRING => Substring(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_REPLACE => ReplaceString(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.DATE_TIME_ADD_YEAR => $"DATETIME({formatedFieldName}, '{parameterString} YEARS')",
                SixnetFieldFormatterNames.DATE_TIME_ADD_MONTH => $"DATETIME({formatedFieldName}, '{parameterString} MONTHS')",
                SixnetFieldFormatterNames.DATE_TIME_ADD_DAY => $"DATETIME({formatedFieldName}, '{parameterString} DAYS')",
                SixnetFieldFormatterNames.DATE_TIME_ADD_HOUR => $"DATETIME({formatedFieldName}, '{parameterString} HOURS')",
                SixnetFieldFormatterNames.DATE_TIME_ADD_MINUTE => $"DATETIME({formatedFieldName}, '{parameterString} MINUTES')",
                SixnetFieldFormatterNames.DATE_TIME_ADD_SECOND => $"DATETIME({formatedFieldName}, '{parameterString} SECONDS')",
                SixnetFieldFormatterNames.DATE_TIME_ADD_MILLISECOND => $"DATETIME(JULIANDAY({formatedFieldName}) + {parameterString}/86400000.0)",
                SixnetFieldFormatterNames.CONVERT_TO_INT => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.CONVERT_TO_BOOLEAN => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.CONVERT_TO_BYTE => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.CONVERT_TO_CHAR => $"CAST({formatedFieldName} AS TEXT)",
                SixnetFieldFormatterNames.CONVERT_TO_DATE_TIME => $"CAST({formatedFieldName} AS TEXT)",
                SixnetFieldFormatterNames.CONVERT_TO_DECIMAL => $"CAST({formatedFieldName} AS REAL)",
                SixnetFieldFormatterNames.CONVERT_TO_DOUBLE => $"CAST({formatedFieldName} AS REAL)",
                SixnetFieldFormatterNames.CONVERT_TO_INT_16 => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.CONVERT_TO_INT_64 => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.CONVERT_TO_SBYTE => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.CONVERT_TO_SINGLE => $"CAST({formatedFieldName} AS REAL)",
                SixnetFieldFormatterNames.CONVERT_TO_UINT_16 => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.CONVERT_TO_UINT_32 => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.CONVERT_TO_UINT_64 => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.MATH_ROUND => $"ROUND({formatedFieldName}, parameterString)",
                SixnetFieldFormatterNames.MATH_ABS => $"ABS({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_CEILING => $"CEILING({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_FLOOR => $"FLOOR({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_TRUNCATE => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.MATH_SIGN => $"(({formatedFieldName}>0)-({formatedFieldName}<0))",
                SixnetFieldFormatterNames.MATH_POW => $"POWER({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.MATH_SQRT => $"SQRT({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_EXP => $"EXP({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_LOG => $"(LOG({formatedFieldName})/LOG({parameterString}))",
                SixnetFieldFormatterNames.MATH_COS => $"COS({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_SIN => $"SIN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_TAN => $"TAN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ACOS => $"ACOS({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ASIN => $"ASIN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ATAN => $"ATAN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ATAN2 => $"ATN2({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.STRING_PAD_LEFT => StringPadLeft(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_PAD_RIGHT => StringPadRight(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_INDEX_OF => StringIndexOf(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_INDEX_OF_ANY => StringIndexOfAny(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_LAST_INDEX_OF => StringLastIndexOf(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_LAST_INDEX_OF_ANY => StringLastIndexOfAny(formatedFieldName, formatOption.Parameter),
                _ => throw new SixnetException($"{context.Server.DatabaseType} does not support field formatter: {formatOption.Name}"),
            };
            return formatedFieldName;
        }

        #region String

        string Substring(string formatedFieldName, dynamic parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeParameter)
            {
                return $"SUBSTR({formatedFieldName},{tupeParameter.Item1 + 1}, {tupeParameter.Item2})";
            }
            else
            {
                return $"SUBSTR({formatedFieldName},{parameter + 1})";
            }
        }

        string ReplaceString(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                if (StringIgnoreCaseValues.Contains(tupeThreeParameter.Item3))
                {
                    return $"SNT_REPLACE_IGNORE_CASE({formatedFieldName},'{tupeThreeParameter.Item1}', '{tupeThreeParameter.Item2}')";
                }
                else
                {
                    return $"REPLACE({formatedFieldName},'{tupeThreeParameter.Item1}', '{tupeThreeParameter.Item2}')";
                }
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                return $"REPLACE({formatedFieldName},'{tupeTwoParameter.Item1}', '{tupeTwoParameter.Item2}')";
            }
            SixnetDirectThrower.ThrowAppException(true, $"Error field formatter: {formatedFieldName}");
            return string.Empty;
        }

        string StringPadLeft(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeParameter)
            {
                var count = tupeParameter.Item1;
                var padStr = new string(tupeParameter.Item2, count);
                return $"SUBSTR('{padStr}' || {formatedFieldName}, -{count})";
            }
            SixnetDirectThrower.ThrowAppException(true, $"Error field formatter: {formatedFieldName}");
            return string.Empty;
        }

        string StringPadRight(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeParameter)
            {
                var count = tupeParameter.Item1;
                var padStr = new string(tupeParameter.Item2, count);
                return $"SUBSTR({formatedFieldName} || '{padStr}', 1 , {count})";
            }
            SixnetDirectThrower.ThrowAppException(true, $"Error field formatter: {formatedFieldName}");
            return string.Empty;
        }

        string StringIndexOf(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                var charValue = tupeThreeParameter.Item1;
                var startIndex = tupeThreeParameter.Item2;
                var count = tupeThreeParameter.Item3;
                return $"SNT_INDEX_OF({formatedFieldName}, '{charValue}', {startIndex}, {count})";
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = tupeTwoParameter.Item1;
                var startIndex = tupeTwoParameter.Item2;
                var count = $"LENGTH({formatedFieldName})";
                return $"SNT_INDEX_OF({formatedFieldName}, '{charValue}', {startIndex}, {count})";
            }
            else
            {
                return $"SNT_INDEX_OF({formatedFieldName}, '{parameter}')";
            }
        }

        string StringIndexOfAny(string formatedFieldName, dynamic parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                var charValue = new string(tupeThreeParameter.Item1);
                var startIndex = tupeThreeParameter.Item2;
                var count = tupeThreeParameter.Item3;
                return $"SNT_INDEX_OF_ANY({formatedFieldName}, '{charValue}', {startIndex}, {count})";
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = new string(tupeTwoParameter.Item1);
                var startIndex = tupeTwoParameter.Item2;
                var count = $"LENGTH({formatedFieldName})";
                return $"SNT_INDEX_OF_ANY({formatedFieldName}, '{charValue}', {startIndex}, {count})";
            }
            else
            {
                var charValue = new string(parameter);
                return $"SNT_INDEX_OF_ANY({formatedFieldName}, '{charValue}')";
            }
        }

        string StringLastIndexOf(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                var charValue = tupeThreeParameter.Item1;
                var startIndex = tupeThreeParameter.Item2;
                var count = tupeThreeParameter.Item3;
                return $"SNT_LAST_INDEX_OF({formatedFieldName}, '{charValue}', {startIndex}, {count})";
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = tupeTwoParameter.Item1;
                var startIndex = tupeTwoParameter.Item2;
                var count = $"LENGTH({formatedFieldName})";
                return $"SNT_LAST_INDEX_OF({formatedFieldName}, '{charValue}', {startIndex}, {count})";
            }
            else
            {
                return $"SNT_LAST_INDEX_OF({formatedFieldName}, '{parameter}')";
            }
        }

        string StringLastIndexOfAny(string formatedFieldName, dynamic parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                var charValue = new string(tupeThreeParameter.Item1);
                var startIndex = tupeThreeParameter.Item2;
                var count = tupeThreeParameter.Item3;
                return $"SNT_LAST_INDEX_OF_ANY({formatedFieldName}, '{charValue}', {startIndex}, {count})";
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = new string(tupeTwoParameter.Item1);
                var startIndex = tupeTwoParameter.Item2;
                var count = $"LENGTH({formatedFieldName})";
                return $"SNT_LAST_INDEX_OF_ANY({formatedFieldName}, '{charValue}', {startIndex}, {count})";
            }
            else
            {
                var charValue = new string(parameter);
                return $"SNT_LAST_INDEX_OF_ANY({formatedFieldName}, '{charValue}')";
            }
        }

        string StringTrim(string formatedFieldName, dynamic parameter)
        {
            if (parameter == null)
            {
                return $"TRIM({formatedFieldName})";
            }
            else
            {
                var charValue = new string(parameter);
                return $"TRIM({formatedFieldName}, '{charValue}')";
            }
        }

        string StringTrimStart(string formatedFieldName, dynamic parameter)
        {
            if (parameter == null)
            {
                return $"LTRIM({formatedFieldName})";
            }
            else
            {
                var charValue = new string(parameter);
                return $"LTRIM({formatedFieldName}, '{charValue}')";
            }
        }

        string StringTrimEnd(string formatedFieldName, dynamic parameter)
        {
            if (parameter == null)
            {
                return $"RTRIM({formatedFieldName})";
            }
            else
            {
                var charValue = new string(parameter);
                return $"RTRIM({formatedFieldName}, '{charValue}')";
            }
        }

        #endregion

        #region DateTime

        static string FormatDateTimeString(string formatedFieldName, dynamic parameter)
        {
            var formatString = parameter?.ToString();
            MatchEvaluator matchEvaluator = match =>
            {
                return DateTimeFormatMap[match.Value];
            };
            var result = DateTimeFormatRegex.Replace(formatString, matchEvaluator);
            return $"STRFTIME('{result}', {formatedFieldName})";
        }

        static string FormatDateTimeDiffString(string formatedFieldName, dynamic parameter)
        {
            return $"JULIANDAY({formatedFieldName?.Replace("(", "").Replace(")", "").Replace("-", ")-JULIANDAY(")})";
        }

        #endregion
    }
}
