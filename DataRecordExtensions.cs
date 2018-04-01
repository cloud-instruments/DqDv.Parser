/*
Copyright(c) <2018> <University of Washington>
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;

namespace Dqdv.Parser
{
    static class DataRecordExtensions
    {
        public static int IndexOf(this IDataRecord @this, string value, StringComparison comparisonType = StringComparison.InvariantCultureIgnoreCase)
        {
            for (var i = 0; i < @this.FieldCount; ++i)
            {
                if (@this.GetFieldType(i) == typeof(string) && @this.GetString(i).Trim().Equals(value, comparisonType))
                    return i;
            }

            return -1;
        }

        public static int IndexOfAny(this IDataRecord @this, string[] values, StringComparison comparisonType = StringComparison.InvariantCultureIgnoreCase)
        {
            for (var i = 0; i < @this.FieldCount; ++i)
            {
                if (@this.GetFieldType(i) != typeof(string))
                    continue;

                var value = @this.GetString(i).Trim();
                if (values.Any(v => value.Equals(v, comparisonType)))
                    return i;
            }

            return -1;
        }

        public static bool IsEmpty(this IDataRecord @this)
        {
            for (var i = 0; i < @this.FieldCount; ++i)
            {
                if (!@this.IsEmpty(i))
                    return false;
            }

            return true;
        }

        public static bool IsEmpty(this IDataRecord @this, int index)
        {
            if (index < 0 || index >= @this.FieldCount)
                return true;

            if (@this.GetFieldType(index) == null)
                return true;

            return false;
        }

        public static int? TryGetInt32(this IDataRecord @this, int index)
        {
            if (index < 0 || index >= @this.FieldCount)
                return null;

            var fieldType = @this.GetFieldType(index);

            if (fieldType == typeof(int))
                return @this.GetInt32(index);

            if (fieldType == typeof(double))
                return Convert.ToInt32(@this.GetDouble(index));

            if (fieldType == typeof(string))
                return int.TryParse(@this.GetString(index), NumberStyles.Integer, CultureInfo.InvariantCulture, out var result) ? (int?)result : null;

            return null;
        }

        public static double? TryGetDouble(this IDataRecord @this, int index)
        {
            if (index < 0 || index >= @this.FieldCount)
                return null;

            var fieldType = @this.GetFieldType(index);

            if (fieldType == typeof(double))
                return @this.GetDouble(index);

            if (fieldType == typeof(int))
                return @this.GetInt32(index);

            if (fieldType == typeof(string))
                return double.TryParse(@this.GetString(index), NumberStyles.Float, CultureInfo.InvariantCulture, out var result) ? (double?)result : null;

            return null;
        }

        public static TimeSpan? TryGetTimeSpan(this IDataRecord @this, int index)
        {
            TimeSpan ts = @this.GetValue(index) is TimeSpan ? (TimeSpan)@this.GetValue(index) : new TimeSpan();
            return ts;
        }

        public static DateTime? TryGetDateTime(this IDataRecord @this, int index)
        {
            if (index < 0 || index >= @this.FieldCount)
                return null;

            var fieldType = @this.GetFieldType(index);

            if (fieldType == typeof(DateTime))
                return @this.GetDateTime(index);

            if (fieldType == typeof(string))
                return DateTime.TryParse(@this.GetString(index), CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var result) ? (DateTime?)result : null;

            return null;
        }

        public static string TryGetString(this IDataRecord @this, int index)
        {
            if (index < 0 || index >= @this.FieldCount)
                return null;

            var fieldType = @this.GetFieldType(index);

            return fieldType == typeof(string) 
                ? @this.GetString(index) 
                : null;
        }

        public static IEnumerable<object> GetRowObjects(this IDataRecord @this)
        {
            var items = new List<object>();
            for (var i = 0; i < @this.FieldCount; i++)
            {
                items.Add(@this[i]);
            }

            return items;
        }
    }
}
