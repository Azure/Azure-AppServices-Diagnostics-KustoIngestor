using System;
using System.Collections.Generic;
using System.Text;

namespace Kusto.Data.Ingestor.Common
{
    public static class ExtensionMethods
    {
        /// <summary>
        /// Standard date time format to be used when logging messages
        /// </summary>
        /// <param name="dt"></param>
        /// <returns>String representation of the DateTime object in yyyy-MM-dd HH:mm:ss.fff format</returns>
        public static string ToStringForLogging(this DateTime dt) => dt.ToString("yyyy-MM-dd HH:mm:ss.fff");

        /// <summary>
        /// This method assumes that the DateTime being worked upon is already in UTC.
        /// </summary>
        /// <param name="dt"></param>
        /// <returns>String representation of the DateTime object in yyyy-MM-ddTHH:mm:ssZ format</returns>
        public static string ToStringKustoUtcFormat(this DateTime dt) => dt.ToString("yyyy-MM-ddTHH:mm:ssZ");
    }
}
