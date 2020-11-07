using Kusto.Data.Ingestor.Common;
using Kusto.Data.Ingestor.Common.LogHelper;
using Kusto.Data.Ingestor.DataProviders;
using Kusto.Data.Ingestor.DataProviders.Kusto;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.Eventing.Reader;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kusto.Data.Ingestor.LogAggregators.CustomLogAggregators
{
    [CustomLogAggregatorOptions(ReEntrantSafe =true)]
    public class HourlySNATConnectionsAggregator : ICustomLogAggregator
    {
        
        private string GetExtentIngestionTag(DateTime st, DateTime et, string stampLocation, string stampType)
        {
            return $"{st.ToStringKustoUtcFormat()}|{et.ToStringKustoUtcFormat()}|{stampLocation}|{stampType}";
        }

        private string GetQueryForAntaresRegionsForCurrentKustoCluster(string leaderKustoClusterName, string followerKustoClusterName)
        {
            return @$"cluster('wawseusfollower').database('wawsprod').WawsAn_regionsincluster
                    | where pdate >= ago(3d)
                    | where ClusterName =~ '{leaderKustoClusterName}' or ClusterName =~ '{followerKustoClusterName}'
                    | where LocationName != ''
                    | summarize by LocationName
                    | order by LocationName asc";
        }

        //private string GetQueryForStampTypes(DateTime st, DateTime et)
        //{
        //    return $@"AntaresConfigurationTracking
        //    | where PreciseTimeStamp >= datetime('{GetKustoDateTimeFormat(st)}')-12h and PreciseTimeStamp <= datetime('{GetKustoDateTimeFormat(et)}')
        //    | where EventStampType != '' and EventStampType endswith 'Stamp'
        //    | summarize by EventStampType ";
        //}

        private string GetQueryForSnatData(DateTime st, DateTime et, string stampLocation, string stampType)
        {
            if (string.IsNullOrWhiteSpace(stampLocation) || string.IsNullOrWhiteSpace(stampType))
            {
                return string.Empty;
            }
            return $"GetInstanceHourlyPendingAndFailedSnatDetails(stampLocation='{stampLocation}', stampType='{stampType}', st=datetime('{st.ToStringKustoUtcFormat()}'), et=datetime('{et.ToStringKustoUtcFormat()}'))";
        }

        public async Task<AggregationResults> AggregationLogic(Guid aggregationRunIdGuid, ICustomAggregationLogger logger, ICustomLogAggregatorConfig config, DefaultDataProviders dp,
                                                               CancellationToken cancellationToken)
        {
            DateTime st = DateTime.Parse(DateTime.UtcNow.AddHours(-2).ToString("yyyy-MM-dd HH:00:00"));
            DateTime et = st.AddMinutes(59); //This is to make sure we do not cross the 1 hour mark and when doing a bin, the values do not roll over to the next hour.
            string callerName = $"{Name}+AggregationLogic";

            DataTable locationList = await dp.Kusto.ExecuteQueryAsync(cancellationToken, aggregationRunIdGuid, callerName, GetQueryForAntaresRegionsForCurrentKustoCluster(config.LeaderKustoCluster, config.FollowerKustoCluster), logger: logger);

            bool firstPass = true;
            List<Task<KustoIngestionResult>> ingestionCommandTaskList = new List<Task<KustoIngestionResult>>();
            
            foreach (DataRow location in locationList.Rows)
            {
                string currLocation = location["LocationName"].ToString();
                if (firstPass)
                {
                    firstPass = false;

                    //Await the first set of ingestions to ensure that the data is cached by Kusto. this takes about 10 to 15 minutes.
                    try
                    {
                        Task<KustoIngestionResult> firstStampIngestionResultTask = dp.Kusto.IngestFromQuery(cancellationToken, aggregationRunIdGuid, callerName,
                                                                                                        tableNameToIngestInto: "SNAT_HourlyAgg",
                                                                                                        queryToIngestFrom: GetQueryForSnatData(st, et, currLocation, "Stamp"),
                                                                                                        extentTag: GetExtentIngestionTag(st, et, currLocation, "Stamp"),
                                                                                                        logger: logger);

                        Task<KustoIngestionResult> firstMiniStampIngestionResultTask = dp.Kusto.IngestFromQuery(cancellationToken, aggregationRunIdGuid, callerName,
                                                                                                            tableNameToIngestInto: "SNAT_HourlyAgg",
                                                                                                            queryToIngestFrom: GetQueryForSnatData(st, et, currLocation, "MiniStamp"),
                                                                                                            extentTag: GetExtentIngestionTag(st, et, currLocation, "MiniStamp"),
                                                                                                            logger: logger);
                        Task.WaitAll(firstStampIngestionResultTask, firstMiniStampIngestionResultTask);

                        logger.LogInformation(new LogMessage(aggregationRunIdGuid, 
                            $"Data ingestion status for {currLocation} region. Stamps:{firstStampIngestionResultTask.Result.State} MiniStamps:{firstMiniStampIngestionResultTask.Result.State}"));
                        

                        if (cancellationToken.IsCancellationRequested)
                        {
                            logger.LogInformation(new LogMessage(aggregationRunIdGuid, $"Aggregation {aggregationRunIdGuid} cancelled due to cancelation token request."));
                            return AggregationResults.Cancelled;
                        }
                    }
                    catch(KustoClientException keFirstPass)
                    {
                        logger.LogError(new LogMessage(aggregationRunIdGuid, $"Kusto client exception. {keFirstPass.Message}", keFirstPass));
                    }
                    catch(Exception ex)
                    {
                        logger.LogError(new LogMessage(aggregationRunIdGuid, $"Exception. {ex.Message}", ex));
                    }
                    
                }
                else
                {
                    //Create tasks to ingest data for rest of the operations since the data by now should already be in Kusto in-memory cache
                    Task<KustoIngestionResult> currMiniStampIngestionJob  = dp.Kusto.IngestFromQuery(cancellationToken, aggregationRunIdGuid, callerName,
                                                                                                        tableNameToIngestInto: "SNAT_HourlyAgg",
                                                                                                        queryToIngestFrom: GetQueryForSnatData(st, et, currLocation, "MiniStamp"),
                                                                                                        extentTag: GetExtentIngestionTag(st, et, currLocation, "MiniStamp"),
                                                                                                        logger: logger);
                    ingestionCommandTaskList.Add(currMiniStampIngestionJob);

                    Task<KustoIngestionResult> currStampIngestionJob = dp.Kusto.IngestFromQuery(cancellationToken, aggregationRunIdGuid, callerName,
                                                                                                        tableNameToIngestInto: "SNAT_HourlyAgg",
                                                                                                        queryToIngestFrom: GetQueryForSnatData(st, et, currLocation, "Stamp"),
                                                                                                        extentTag: GetExtentIngestionTag(st, et, currLocation, "Stamp"),
                                                                                                        logger: logger);
                    ingestionCommandTaskList.Add(currStampIngestionJob);
                    
                }
                
            }

            // At this point all ingestion tasks have been launched
            //Even if we do not get their result back, Kusto will handle ingestion async if they were issued.
            //It is safe to exit if required since everything now, is retry and bookkeeping/error handling

            if (cancellationToken.IsCancellationRequested)
            {
                return AggregationResults.Cancelled;
            }

            try
            {
                IEnumerable<KustoIngestionResult> ingestionResults = await Task.WhenAll(ingestionCommandTaskList.ToArray());
                Dictionary<KustoIngestionResultStates, int> resultCount = new Dictionary<KustoIngestionResultStates, int>();
                foreach(KustoIngestionResult kiResult in ingestionResults)
                {
                    if(!resultCount.ContainsKey(kiResult.IngestionState))
                    {
                        resultCount[kiResult.IngestionState] = 0;
                    }

                    resultCount[kiResult.IngestionState]++;
                }
                string resultStatusCountString = string.Join(",", resultCount.Select(kvp => $"{kvp.Key} ingestion queries:{kvp.Value}").ToArray());

                logger.LogInformation(new LogMessage(aggregationRunIdGuid, $"Ingestion result : {resultStatusCountString}"));
            }
            catch(KustoClientException kcEx)
            {
                logger.LogError(new LogMessage(aggregationRunIdGuid, $"Kusto client exception. {kcEx.Message}", kcEx));
            }
            catch(Exception ex)
            {
                logger.LogError(new LogMessage(aggregationRunIdGuid, $"Exception. {ex.Message}", ex));
            }

            return AggregationResults.Completed;
        }

        public string Author
        {
            get => "nmallick";
        }

        public string Description
        {
            get => $"Pushes pending and failed SNAT connection count per instance along with sites hosted on them every hour into SNAT_HourlyAgg table. Data pushed is between ago(2h) and ago(1h). This is to ensure that all information to aggregate is in Kusto hotcache.";
        }

        public string Name
        {
            get => $"HourlyPendingAndFailedSNATConnectionAggregator";
        }

        public TimeSpan GetNextInvocationInterval(Guid aggregationRunIdGuid, ICustomAggregationLogger logger, ICustomLogAggregatorConfig config, DateTime previousInvocationDateTimeUtc)
        {
            //Trigger this aggregation every hour
            return previousInvocationDateTimeUtc.AddHours(1).Subtract(previousInvocationDateTimeUtc);
        }
    }
}
