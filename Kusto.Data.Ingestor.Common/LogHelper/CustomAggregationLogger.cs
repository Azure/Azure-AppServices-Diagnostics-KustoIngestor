using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization;

namespace Kusto.Data.Ingestor.Common.LogHelper
{
    public class LogMessage
    {
        /// <summary>
        /// Timestamp in UTC for when this log object was created.
        /// </summary>
        public string Timestamp { get; private set; } = System.DateTime.UtcNow.ToStringForLogging();

        /// <summary>
        /// Returns the current UTC timestamp. Will reflect the time when an entry was streamed into logs.
        /// </summary>
        public string LogTimestamp { get => System.DateTime.UtcNow.ToStringForLogging(); }

        /// <summary>
        /// Antares region where the aggregator is running.
        /// </summary>
        public string HostingRegion  { get; } = Constants.IsRunningOnCloud ? (Constants.RegionPrefix.EndsWith('_') ? Constants.RegionPrefix[0..^1] : Constants.RegionPrefix) : "LocalMachine";

        /// <summary>
        /// Unique identifier to represent current aggregation operation.
        /// </summary>
        public Guid AggregationRunId { get; set; } = Guid.Empty;

        /// <summary>
        /// Any string that needs to be logged.
        /// </summary>
        public string Message { get; set; } = string.Empty;

        /// <summary>
        /// Exception message while logging error.
        /// </summary>
        public Exception Exception { get; set; } = null;

        /// <summary>
        /// Key Value pair of any additional information that is to be logged.
        /// </summary>
        public Dictionary<string, string> AdditionalDetails { get; set; }

        public LogMessage(Guid aggregationRunId, string message, Dictionary<string, string> additionalDetails = null)
        {
            Timestamp = System.DateTime.UtcNow.ToStringForLogging();
            AggregationRunId = aggregationRunId;
            Message = message;
            AdditionalDetails = additionalDetails;
        }

        public LogMessage(Guid aggregationRunId, string message, Exception ex, Dictionary<string, string> additionalDetails = null) : this(aggregationRunId, message, additionalDetails)
        {
            Exception = ex;
        }
    }
    public interface ICustomAggregationLogger
    {
        /// <summary>
        /// <para>Log information messages. These can be quite chatty and are disabled by default.Can be enabled via LogAggregator:CustomAggregatorDebugLogsEnabled appsetting.</para>
        /// <para>It's still a good practice to add log messages to assist debugging later.</para>
        /// </summary>
        /// <param name="log">Details of what needs to be logged.</param>
        public void LogInformation(LogMessage log);
        
        /// <summary>
        /// Log exceptions and errors. This is enabled by default.
        /// </summary>
        /// <param name="log"></param>
        public void LogError(LogMessage log);
    }
    public class CustomAggregationLogger: ICustomAggregationLogger
    {
        private readonly ILogger<CustomAggregationLogger> _logger;
        private readonly bool debugLogsEnabled = false;
        public CustomAggregationLogger(IConfiguration config, ILogger<CustomAggregationLogger> logger)
        {
            _logger = logger;
            string rootConfigElementName = Constants.AppSettingPrefix.EndsWith('_') ? Constants.AppSettingPrefix[0..^1] : Constants.AppSettingPrefix;
            debugLogsEnabled = Convert.ToBoolean(config[$"{rootConfigElementName}:CustomAggregatorDebugLogsEnabled"] ?? "false") == true;
            _logger.LogInformation($"{System.DateTime.UtcNow.ToStringForLogging()}:Logging config:{rootConfigElementName}:CustomAggregatorDebugLogsEnabled  = {debugLogsEnabled}");
        }

        public void LogError(LogMessage log)
        {
            try
            {
                _logger.LogError(log.Exception, JsonConvert.SerializeObject(log));
            }
            catch (Exception) { }
        }

        public void LogInformation(LogMessage log)
        {
            if(debugLogsEnabled || !Constants.IsRunningOnCloud)
            {
                try
                {
                    if(!Constants.IsRunningOnCloud)
                    {
                        _logger.LogInformation(JsonConvert.SerializeObject(log, Formatting.Indented));
                    }
                    else
                    {
                        _logger.LogInformation(JsonConvert.SerializeObject(log));
                    }
                    
                }
                catch(Exception) { }
            }            
        }
    }
}
