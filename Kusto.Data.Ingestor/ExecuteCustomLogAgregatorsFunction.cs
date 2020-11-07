using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.WebJobs.Description;
using Kusto.Data.Ingestor.DataProviders;
using Kusto.Data.Ingestor.Common;
using Kusto.Data.Ingestor.LogAggregators;
using Kusto.Data.Ingestor.LogAggregators.CustomLogAggregators;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using Kusto.Data.Ingestor.Common.LogHelper;
using Newtonsoft.Json;

namespace Kusto.Data.Ingestor
{
    public class ExecuteCustomLogAgregatorsFunction
    {
        private readonly IConfiguration _config;
        private readonly DefaultDataProviders _dp;
        private readonly ICustomAggregationLogger _customAggregationLogger;
        public ExecuteCustomLogAgregatorsFunction(IConfiguration config, DefaultDataProviders dp, ICustomAggregationLogger customAggregationLogger)
        {
            _config = config;
            _dp = dp;
            _customAggregationLogger = customAggregationLogger;
        }

        private ICustomLogAggregatorConfig GetRegionSpecificConfigSettings(string regionPrefix)
        {
            string rootConfigElementName = Constants.AppSettingPrefix.EndsWith('_') ? Constants.AppSettingPrefix[0..^1] : Constants.AppSettingPrefix;
            string currentHostingRegion = !string.IsNullOrWhiteSpace(regionPrefix) ? regionPrefix : Constants.RegionPrefix;
            currentHostingRegion = currentHostingRegion.EndsWith('_') ? currentHostingRegion[0..^1] : currentHostingRegion;

            string leaderKustoCluster = string.Empty;
            string followerKustoCluster = string.Empty;
            string defaultKustoDatabase = string.Empty;

            IEnumerable<IConfigurationSection> regionConfig = _config.GetSection(rootConfigElementName)?.GetSection(currentHostingRegion)?.GetChildren();
            if (regionConfig?.Any() == true)
            {
                foreach (IConfigurationSection config in regionConfig)
                {
                    if (config.Key.Equals("KustoCluster", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(config.Value))
                    {
                        if (config.Value.EndsWith("follower", StringComparison.OrdinalIgnoreCase))
                        {
                            followerKustoCluster = config.Value.Trim();
                            leaderKustoCluster = followerKustoCluster.Replace("follower", string.Empty, StringComparison.OrdinalIgnoreCase);
                        }
                        else
                        {
                            leaderKustoCluster = config.Value.Trim();
                            followerKustoCluster = $"{leaderKustoCluster}follower";
                        }
                    }
                    else if (config.Key.Equals("DefaultKustoDatabase", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(config.Value))
                    {
                        defaultKustoDatabase = config.Value.Trim();
                    }
                }
            }

            return new CustomLogAggregatorConfig(currentHostingRegion, leaderKustoCluster, followerKustoCluster, defaultKustoDatabase);
        }

        private List<string> GetDisabledAggregatorNames()
        {
            List<string> disabledAggregators = new List<string>();
            string rootConfigElementName = Constants.AppSettingPrefix.EndsWith('_') ? Constants.AppSettingPrefix[0..^1] : Constants.AppSettingPrefix;
            var configSection = _config.GetSection(rootConfigElementName)?.GetSection("DisabledAggregators")?.GetChildren();
            if (configSection?.Any() == true)
            {
                foreach(var item in configSection)
                {
                    if( !string.IsNullOrWhiteSpace(item.Value))
                    {
                        disabledAggregators.Add(item.Value.Trim());
                    }
                }
            }
            return disabledAggregators;
        }

        [FunctionName("ExecuteCustomLogAgregators")]
        [NoAutomaticTrigger]
        public async Task ExecuteCustomLogAgregators(string regionPrefix, ILogger logger, CancellationToken jobHostCtk)
        {
            LogInformation(logger, "Startup message : ExecuteCustomLogAgregators trigerred.", new Dictionary<string, string>() {
                ["is config Null"] = $"{_config == null}"
            });
            
            ICustomLogAggregatorConfig configToPass = GetRegionSpecificConfigSettings(regionPrefix);
            List<string> disabledAggregators = GetDisabledAggregatorNames();

            ConcurrentDictionary<string, ICustomLogAggregatorDispatcher> dispatchers = new ConcurrentDictionary<string, ICustomLogAggregatorDispatcher>();
            
            Assembly customLogAggregatorsAssembly = null;

            List<Assembly> assemblyList = AppDomain.CurrentDomain.GetAssemblies().Where(a => a.GetName().Name.Equals("Kusto.Data.Ingestor.LogAggregators", StringComparison.OrdinalIgnoreCase)).ToList<Assembly>();
            if(assemblyList.Any())
            {
                customLogAggregatorsAssembly = assemblyList[0];
            }
            else
            {
                customLogAggregatorsAssembly = Assembly.Load("Kusto.Data.Ingestor.LogAggregators");
            }

            List<Type> customLogAggregatorTypesList = customLogAggregatorsAssembly.GetTypes().Where(x => typeof(ICustomLogAggregator).IsAssignableFrom(x) && !x.IsInterface && !x.IsAbstract).ToList<Type>();
            if (customLogAggregatorTypesList.Any())
            {
                foreach (Type t in customLogAggregatorTypesList)
                {
                    try
                    {
                        CustomLogAggregatorOptions opts = new CustomLogAggregatorOptions();
                        CustomLogAggregatorOptionsAttribute optsAttr = (CustomLogAggregatorOptionsAttribute)t.GetCustomAttribute(typeof(CustomLogAggregatorOptionsAttribute), false);

                        opts.ReEntrantSafe = optsAttr?.ReEntrantSafe != false;

                        ICustomLogAggregator currCustomLogAggregator = (ICustomLogAggregator)Activator.CreateInstance(t.Assembly.GetName().FullName, t.FullName).Unwrap();

                        bool isAggregatorDisabled = disabledAggregators.Where(a => a.Equals(currCustomLogAggregator.Name.Trim(), StringComparison.OrdinalIgnoreCase)).Any();
                        if(!isAggregatorDisabled)
                        {
                            ICustomLogAggregatorDispatcher dispatcher = new CustomLogAggregatorDispatcher(currCustomLogAggregator, configToPass, opts, logger, _customAggregationLogger, _dp);
                            if(dispatchers.TryAdd(currCustomLogAggregator.Name, dispatcher))
                            {
                                LogInformation(logger, $"Dispatcher configured.", new Dictionary<string, string>()
                                {
                                    ["DispatcherId"] = $"{dispatcher.DispatcherId}",
                                    ["AggregatorName"] = $"{currCustomLogAggregator.Name}",
                                    ["AggregatorAuthor"] = $"{currCustomLogAggregator.Author}",
                                    ["AggregatorDescription"] = $"{currCustomLogAggregator.Description}",
                                });
                            }
                            else
                            {
                                LogInformation(logger, $"Dispatcher not configured for aggregator due to possible duplicate.", new Dictionary<string, string>()
                                {
                                    ["DispatcherId"] = $"{dispatcher.DispatcherId}",
                                    ["AggregatorName"] = $"{currCustomLogAggregator.Name}",
                                    ["AggregatorAuthor"] = $"{currCustomLogAggregator.Author}",
                                    ["AggregatorDescription"] = $"{currCustomLogAggregator.Description}",
                                });
                            }                            
                            
                        }
                        else
                        {
                            LogInformation(logger, $"Dispatcher not configured as aggregator is disabled via DisabledAggregators app setting.", new Dictionary<string, string>()
                            {
                                ["DispatcherId"] = $"{Guid.Empty}",
                                ["AggregatorName"] = $"{currCustomLogAggregator.Name}",
                                ["AggregatorAuthor"] = $"{currCustomLogAggregator.Author}",
                                ["AggregatorDescription"] = $"{currCustomLogAggregator.Description}",
                            });
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError(logger, ex, $"Failed to schedule dispatcher for aggregation of type {t} due to exception:{ex.Message}. This aggregation will not run.");
                    }
                }

                foreach (var dispatcherKvp in dispatchers)
                {
                    dispatcherKvp.Value.ConfigureAndDispatch();
                }
                LogInformation(logger, $"Done calling all dispatchers. Count {dispatchers?.Count}");
            }

            if (jobHostCtk.CanBeCanceled)
            {
                jobHostCtk.Register(() => 
                {
                    LogInformation(logger, $"", new Dictionary<string, string>() { });
                    logger.LogInformation($"{System.DateTime.Now.ToStringForLogging()} : Application is shutting down, performing cleanup routine..");
                    //When the job host is shutting down, cleanup.
                    foreach(var kvp in dispatchers)
                    {                        
                        kvp.Value.Dispose();
                    }
                    dispatchers.Clear();
                    LogInformation(logger, $"", new Dictionary<string, string>() { });
                    logger.LogInformation($"{System.DateTime.Now.ToStringForLogging()} : Application is shutting down, cleanup routine complete.");
                    throw new HostingStopException();
                });
            }

            //Todo, look into a queue and execute aggregation queries to support no code aggregation policies.           

        }

        private void LogError(ILogger logger, Exception ex, string message, Dictionary<string, string> args = null)
        {
            try
            {
                LogMessage msg = new LogMessage(Guid.Empty, $"ExecuteCustomLogAgregators=>{message}", ex, args);
                logger.LogError(ex, JsonConvert.SerializeObject(msg));
            }
            catch (Exception) { }
        }

        private void LogInformation(ILogger logger, string message, Dictionary<string, string> args = null)
        {
            try
            {
                LogMessage msg = new LogMessage(Guid.Empty, $"ExecuteCustomLogAgregators=>{message}", args);

                logger.LogInformation(JsonConvert.SerializeObject(msg));

            }
            catch (Exception) { }
        }
    }
}
