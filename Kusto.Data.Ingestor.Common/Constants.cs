using System;
using System.Collections.Generic;
using System.Text;

namespace Kusto.Data.Ingestor.Common
{
    public static class Constants
    {
        public static string AppSettingPrefix
        {
            get => "LogAggregator_";
        }
        public static string RegionPrefixKeyName
        {
            get => $"{Constants.AppSettingPrefix}RegionPrefix";
        }
        public static string RegionPrefixParamName
        { 
            get => $"regionPrefix"; 
        }
        public static string AppSettingFileName
        {
            get => "appsettings";
        }
        public static string ApplicationName 
        { 
            get => "AppLensLogAggregator"; 
        }
        public static bool IsRunningOnCloud 
        { 
            get => !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("WEBSITE_INSTANCE_ID"));
        }
        public static string RegionPrefix
        {
            get => Environment.GetEnvironmentVariable($"{Constants.RegionPrefixKeyName}") ?? "CentralUS_";
        }
    }
}
