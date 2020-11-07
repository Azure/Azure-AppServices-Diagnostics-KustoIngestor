using Kusto.Cloud.Platform.Utils;
using Kusto.Data.Ingestor.Common;
using Kusto.Data.Ingestor.Common.LogHelper;
using Kusto.Data.Ingestor.DataProviders;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kusto.Data.Ingestor
{
    public interface ICustomLogAggregatorDispatcher : IDisposable
    {
        public Guid DispatcherId { get; }
        public ulong InvocationCount { get; }
        public void ConfigureAndDispatch();
    }

    public class CustomLogAggregatorDispatcher : ICustomLogAggregatorDispatcher
    {
        public ulong InvocationCount { get; private set; } = 0;

        private Guid _dispatcherId;
        public Guid DispatcherId
        {
            get => _dispatcherId;
        }

        private System.Timers.Timer _timer = null;
        private readonly ICustomLogAggregator _customLogAggregator;
        private readonly ILogger _logger;
        private readonly ICustomAggregationLogger _customAggregationLogger;
        private readonly ICustomLogAggregatorConfig _customLogAggregatorConfig;
        private readonly ICustomLogAggregatorOptions _customLogAggregatorOptions;
        private readonly DefaultDataProviders _dp;

        //If the aggregation is taking more than X % of time to complete before the next one time is to be invoked, a cancelation request is sent to the aggregator.
        //The aggregator must handle cancellation token to ensure data consistency.
        private const int _AggregationExecutionDurationPercentage = 80; //80%

        //Maximum execution time a custom aggregation is allowed to execute before sending a cancellation request.
        private const int _AggregationMAXExecutionTimeoutInMS = 3600000; //1 hour

        //Wait a minimum time startig the current invocation before re-invoking the aggregation again even if the aggregator requests for a more aggressive re-invocation.
        //For non re-entrant aggregators, the time to wait starts counting from when it was last invoked.
        private const int _MinTimeToWaitBetweenAggregationinvocationInMs = 600000; //10 minutes


        public CustomLogAggregatorDispatcher(ICustomLogAggregator customLogAggregator, ICustomLogAggregatorConfig config, ICustomLogAggregatorOptions aggregatorOpts, ILogger logger, ICustomAggregationLogger customAggregationLogger, DefaultDataProviders dp)
        {
            _customLogAggregator = customLogAggregator;
            _logger = logger;
            _customAggregationLogger = customAggregationLogger;
            _customLogAggregatorConfig = config;
            _customLogAggregatorOptions = aggregatorOpts;
            _dp = dp;
            
            _dispatcherId = Guid.NewGuid();
        }

        public void ConfigureAndDispatch()
        {
            //Create the timer now since this is an attempt to launch an aggregation job.
            _timer = new System.Timers.Timer()
            {
                AutoReset = false,
                Enabled = false
            };

            _ = ConfigureAndStartTimer(Guid.Empty, System.DateTime.UtcNow, true);
        }

        private double ConfigureAndStartTimer(Guid invocationId, DateTime previousAggregationInvocationDateTime, bool configureEventHandler)
        {
            try
            {
                TimeSpan nextInvocationInterval = _customLogAggregator.GetNextInvocationInterval(invocationId, _customAggregationLogger, _customLogAggregatorConfig, previousInvocationDateTimeUtc: previousAggregationInvocationDateTime);
                if(nextInvocationInterval.IsValidAsTimerPeriod() && nextInvocationInterval != TimeSpan.Zero)
                {
                    double nextInvocationTimeInMs = nextInvocationInterval.TotalMilliseconds;
                    if (_timer == null)
                    {
                        _timer = new System.Timers.Timer();
                    }

                    _timer.Enabled = false;
                    _timer.AutoReset = false;
                    if(_customLogAggregatorOptions.ReEntrantSafe)
                    {
                        _timer.Interval = nextInvocationTimeInMs > _MinTimeToWaitBetweenAggregationinvocationInMs ? _MinTimeToWaitBetweenAggregationinvocationInMs : nextInvocationTimeInMs;
                    }
                    else
                    {
                        double timeElapsedSinceLastInvocation = System.DateTime.UtcNow.Subtract(previousAggregationInvocationDateTime).TotalMilliseconds;
                        _timer.Interval = nextInvocationTimeInMs > (_MinTimeToWaitBetweenAggregationinvocationInMs - timeElapsedSinceLastInvocation) 
                            ? (_MinTimeToWaitBetweenAggregationinvocationInMs - timeElapsedSinceLastInvocation)
                            : nextInvocationTimeInMs;
                    }
                    

                    double executionTimeoutInMsToReturn = _AggregationMAXExecutionTimeoutInMS;

                    if (configureEventHandler)
                    {
                        //Set up the event handler to trigger aggregation
                        _timer.Elapsed += async (sender, e) => await Dispatch();
                    }
                    else
                    {
                        //Event handler to execute aggregation is already setup and it will now have to configure the cancellation token. Compute the timeout here.

                        TimeSpan deltaBetweenInvocationTimes = previousAggregationInvocationDateTime.AddMilliseconds(nextInvocationTimeInMs).Subtract(previousAggregationInvocationDateTime);
                        double computedExecutionTimeoutInMs = Math.Round((deltaBetweenInvocationTimes.TotalMilliseconds * _AggregationExecutionDurationPercentage) / 100.0, 0);
                        if (computedExecutionTimeoutInMs < executionTimeoutInMsToReturn)
                        {
                            executionTimeoutInMsToReturn = computedExecutionTimeoutInMs;
                        }
                    }

                    _timer.Start();
                    LogInformation($"{System.DateTime.Now.ToStringForLogging()}: ConfigureAndStartTimer in Dispatcher ({_dispatcherId}) for Aggregator {_customLogAggregator.Name}");

                    return executionTimeoutInMsToReturn;
                }

            }
            catch(Exception ex)
            {
                LogError(ex, $"ConfigureAndStartTimer failed with exception:{ex.Message}. Depending on the failure, the aggregator may not be trigerred again.", new Dictionary<string, string>() {
                    ["DispatcherId"] = $"{this.DispatcherId}",
                    ["AggregatorName"] = $"{_customLogAggregator.Name}",
                    ["AggregatorAuthor"] = $"{_customLogAggregator.Author}",
                    ["AggregatorDescription"] = $"{_customLogAggregator.Description}"
                });
            }

            return 0;
        }

        private async Task<bool> Dispatch()
        {
            
            Guid invocationId = Guid.NewGuid();
            LogInformation($"{System.DateTime.Now.ToStringForLogging()} : Starting Dispatcher ({_dispatcherId}) for Aggregator {_customLogAggregator.Name} Executionid: ({invocationId})");

            Stopwatch currWatch = new Stopwatch();
            currWatch.Start();

            DateTime aggregationInvocationDateTime = System.DateTime.UtcNow;


            CancellationTokenSource cts = new CancellationTokenSource();
            Task<AggregationResults> aggregationTask = _customLogAggregator.AggregationLogic(invocationId, _customAggregationLogger, _customLogAggregatorConfig, _dp, cts.Token);
            this.InvocationCount++;

            double executionTimeoutMs = _AggregationMAXExecutionTimeoutInMS;
            if (_customLogAggregatorOptions.ReEntrantSafe)
            {
                executionTimeoutMs = ConfigureAndStartTimer(invocationId, aggregationInvocationDateTime, false); //No need to reconfigure the event handler since it's already present
            }            
            
            try
            {
                cts.CancelAfter(TimeSpan.FromMilliseconds(executionTimeoutMs));
                cts.Token.Register(() => {
                    LogInformation($"{System.DateTime.Now.ToStringForLogging()} : Timedout Dispatcher ({_dispatcherId}) for Aggregator {_customLogAggregator.Name} Executionid: ({invocationId}). Time Elapsed: {executionTimeoutMs} ms. Total invocation count: {InvocationCount}");
                });

                AggregationResults aggResult = await aggregationTask;
                currWatch.Stop();

                LogInformation($"{System.DateTime.Now.ToStringForLogging()} : Completed  Dispatcher ({_dispatcherId}) for Aggregator {_customLogAggregator.Name} Executionid: ({invocationId}) with Result {aggResult}. Time Elapsed: {currWatch.ElapsedMilliseconds} ms. Total invocation count: {InvocationCount}");
            }
            catch(Exception ex)
            {
                currWatch.Stop();
                LogError(ex, $"{System.DateTime.Now.ToStringForLogging()} : Exception Dispatcher ({_dispatcherId}) for Aggregator {_customLogAggregator.Name} Executionid: ({invocationId}). Time Elapsed: {currWatch.ElapsedMilliseconds} ms. Total invocation count:{InvocationCount}");
            }
            finally
            {
                if (!_customLogAggregatorOptions.ReEntrantSafe)
                {
                    _ = ConfigureAndStartTimer(invocationId, aggregationInvocationDateTime, false); //No need to reconfigure the event handler since it's already present
                }

                cts.Dispose();
            }

            return true;
        }

        

        public void Dispose()
        {
            if(_timer != null)
            {
                _timer.Stop();
                _timer.Close();
                _timer.Dispose();
                _timer = null;
            }
        }

        private void LogError(Exception ex, string message, Dictionary<string, string> args = null)
        {
            try
            {
                LogMessage msg = new LogMessage(Guid.Empty, $"DispatcherLog=>{message}", ex, args);
                _logger.LogError(ex, JsonConvert.SerializeObject(msg));
            }
            catch (Exception) { }
        }

        private void LogInformation(string message, Dictionary<string, string> args = null)
        {
            try
            {
                LogMessage msg = new LogMessage(Guid.Empty, $"DispatcherLog=>{message}", args);
                
                _logger.LogInformation(JsonConvert.SerializeObject(msg));

            }
            catch (Exception) { }
        }


    }
}
