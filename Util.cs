using System;
using System.Data;
using System.Globalization;

namespace MigrationProject
{
    public static class Utils
    {
        public static string GetVal(DataRow row, string colName)
        {
            try { return row[colName]?.ToString().Trim(); } catch { return null; }
        }

        public static string GetVal(DataRow row, int index)
        {
            try { return row[index]?.ToString().Trim(); } catch { return null; }
        }

        public static decimal GetDecimal(DataRow row, string colName)
        {
            string val = GetVal(row, colName);
            if (string.IsNullOrWhiteSpace(val)) return 0;
            if (decimal.TryParse(val.Replace(".", ","), NumberStyles.Any, new CultureInfo("pt-BR"), out decimal d)) return d;
            return 0;
        }

        public static decimal GetDecimal(DataRow row, int index)
        {
            string val = GetVal(row, index);
            if (string.IsNullOrWhiteSpace(val)) return 0;
            if (decimal.TryParse(val.Replace(".", ","), NumberStyles.Any, new CultureInfo("pt-BR"), out decimal d)) return d;
            return 0;
        }

        public static DateTime? GetDate(DataRow row, string colName)
        {
            string val = GetVal(row, colName);
            if (string.IsNullOrWhiteSpace(val) || val.ToUpper() == "NULL") return null;
            if (DateTime.TryParse(val, out DateTime d)) return d;
            return null;
        }

        public static DateTime? GetDate(DataRow row, int index)
        {
            string val = GetVal(row, index);
            if (string.IsNullOrWhiteSpace(val) || val.ToUpper() == "NULL") return null;
            if (DateTime.TryParse(val, out DateTime d)) return d;
            return null;
        }
    }
}