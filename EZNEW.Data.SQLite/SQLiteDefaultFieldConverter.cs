using System;
using System.Collections.Generic;
using System.Text;
using EZNEW.Data.Conversion;
using EZNEW.Exceptions;

namespace EZNEW.Data.SQLite
{
    /// <summary>
    /// Default field converter for sqlite
    /// </summary>
    public class SQLiteDefaultFieldConverter : IFieldConverter
    {
        public FieldConversionResult Convert(FieldConversionContext fieldConversionContext)
        {
            if (string.IsNullOrWhiteSpace(fieldConversionContext?.ConversionName))
            {
                return null;
            }
            string formatedFieldName = null;
            switch (fieldConversionContext.ConversionName)
            {
                case FieldConversionNames.StringLength:
                    formatedFieldName = string.IsNullOrWhiteSpace(fieldConversionContext.ObjectName)
                        ? $"LENGTH({fieldConversionContext.ObjectName}.{SQLiteManager.WrapKeyword(fieldConversionContext.FieldName)})"
                        : $"LENGTH({SQLiteManager.WrapKeyword(fieldConversionContext.FieldName)})";
                    break;
                default:
                    throw new EZNEWException($"{SQLiteManager.CurrentDatabaseServerType} does not support field conversion: {fieldConversionContext.ConversionName}");
            }

            return new FieldConversionResult()
            {
                NewFieldName = formatedFieldName
            };
        }
    }
}
