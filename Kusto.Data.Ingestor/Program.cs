using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kusto.Data.Ingestor.Common;
using Kusto.Data.Ingestor.Common.LogHelper;
using Kusto.Data.Ingestor.DataProviders;
using Kusto.Data.Ingestor.DataProviders.Kusto;
using Microsoft.ApplicationInsights.AspNetCore.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.ApplicationInsights;
using Microsoft.Extensions.Primitives;

namespace Kusto.Data.Ingestor
{
    class Program
    {
        public static async Task Main()
        {
            string currentDirectory = Environment.CurrentDirectory;

            IHost host = new HostBuilder()
                .ConfigureHostConfiguration(hostConfig =>
                {
                    hostConfig.SetBasePath(currentDirectory);
                    hostConfig.AddEnvironmentVariables(prefix: "ASPNETCORE_");
                })
                .ConfigureAppConfiguration((hostContext, appConfig) =>
                {
                    appConfig.SetBasePath(currentDirectory);
                    appConfig.AddEnvironmentVariables(prefix: "ASPNETCORE_");
                    appConfig.AddEnvironmentVariables(prefix: Constants.AppSettingPrefix);
                    appConfig.AddEnvironmentVariables(prefix: $"{Constants.AppSettingPrefix}{Constants.RegionPrefix}");
                    appConfig.AddJsonFile($"{Constants.AppSettingFileName}.json", optional: false, reloadOnChange: false);
                    appConfig.AddJsonFile($"{Constants.AppSettingFileName}.{hostContext.HostingEnvironment.EnvironmentName}.json", optional: true, reloadOnChange: false);
                })
                .ConfigureWebJobs(b => {
                })
                .ConfigureLogging((hostContext, loggingConfig) =>
                {
                    loggingConfig.AddConsole();
                    if(Constants.IsRunningOnCloud)
                    {
                        //Give preference to the value set in appsettings.json
                        loggingConfig.AddApplicationInsights(hostContext.Configuration["ApplicationInsights:InstrumentationKey"] ?? Environment.GetEnvironmentVariable("APPINSIGHTS_INSTRUMENTATIONKEY") ?? "f4e62ff6 -53cd-4e11-a106-3ae297e74ea9");
                        if(Enum.TryParse<LogLevel>(hostContext.Configuration["ApplicationInsights:LogLevel:Default"] ?? "Information", true, out LogLevel defaultLogLevel))
                        {
                            loggingConfig.AddFilter<ApplicationInsightsLoggerProvider>("", defaultLogLevel);
                        }
                        else
                        {
                            loggingConfig.AddFilter<ApplicationInsightsLoggerProvider>("", LogLevel.Information);
                        }

                        
                    }
                })
                .ConfigureServices((hostContext, services) =>
                {                   
                    services.AddSingleton<IDataSourcesConfigurationService, DataSourcesConfigurationService>();
                    services.AddSingleton<ICustomAggregationLogger, CustomAggregationLogger>();
                    var servicesProvider = services.BuildServiceProvider();
                    var dataSourcesConfigService = servicesProvider.GetService<IDataSourcesConfigurationService>();
                    var kustoConfiguration = dataSourcesConfigService.Config.KustoConfiguration;

                    services.AddSingleton(kustoConfiguration);
                    services.AddSingleton<IKustoClient, KustoClient>();
                    

                    services.AddSingleton<DefaultDataProviders>();
                })
                .Build();           

            try
            {
                using (host)
                {
                    if (Constants.IsRunningOnCloud)
                    {
                        Microsoft.Azure.WebJobs.WebJobsShutdownWatcher watcher = new Microsoft.Azure.WebJobs.WebJobsShutdownWatcher();
                        if (watcher.Token.CanBeCanceled)
                        {
                            watcher.Token.Register(async () => 
                            {
                                await host.StopAsync();
                            });
                        }
                    }
                    
                    await host.StartAsync();
                    var applicationLifetime = host.Services.GetService<IHostApplicationLifetime>();
                    var jobHost = host.Services.GetService(typeof(Microsoft.Azure.WebJobs.IJobHost)) as Microsoft.Azure.WebJobs.JobHost;
                    Dictionary<string, object> paramsToPass = new Dictionary<string, object>()
                    {
                        [Constants.RegionPrefixParamName] = Constants.RegionPrefix //Not really needed, but keeping it here while I test
                    };                    
                    var continuousJobTask =  jobHost.CallAsync("ExecuteCustomLogAgregators", paramsToPass, cancellationToken: applicationLifetime.ApplicationStopping);
                    await host.WaitForShutdownAsync();
                    await continuousJobTask;

                }
            }
            catch (HostingStopException)
            {
                await host.StopAsync();
                //Swallow the exception, this is expected. Let all other types of exception go unhandled and signal a failure.
            }
        }
    }
}
