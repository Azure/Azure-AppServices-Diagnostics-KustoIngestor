using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using Kusto.Data.Ingestor.Common;
using Kusto.Data.Ingestor.Common.LogHelper;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kusto.Data.Ingestor.DataProviders.Kusto
{
    
    public class KustoClientException : Exception
    {
        public override string Source
        {
            get => base.Source;
            set => base.Source = $"{(string.IsNullOrWhiteSpace(value) ? "KustoClient" : value)}:{base.Source}";
        }

        public KustoClientException(string message, string source) : base(message)
        {
            this.Source = source;
        }
        public KustoClientException(string message, string source, Exception innerException) : base(message, innerException)
        {
            this.Source = source;
        }
    }

    /// <summary>
    /// <inheritdoc/>
    /// </summary>
    public class KustoClient : IKustoClient
    {
        private readonly KustoDataProviderConfiguration _config;
        private readonly string _kustoApiQueryEndpoint;
        private readonly string _kustoCluster;
        private readonly string _kustoClusterForIngestion;
        private readonly string _appKey;
        private readonly string _clientId;
        private readonly string _aadAuthority;
        private readonly string _aadKustoResource;
        private readonly string _aadTenantId;
        private readonly ILogger<KustoClient> _logger;
        private readonly string _defaultKustoDatabase;

        private static ConcurrentDictionary<Tuple<string, string>, ICslQueryProvider> QueryProviderMapping;
        private static ConcurrentDictionary<Tuple<string, string>, ICslAdminProvider> AdminProviderMapping;
        private static ConcurrentDictionary<Tuple<string, string>, IKustoIngestClient> DirectIngestProviderMapping;


        public KustoClient(KustoDataProviderConfiguration config, ILogger<KustoClient> logger, IConfiguration rootConfig)
        {
            if (QueryProviderMapping == null)
            {
                QueryProviderMapping = new ConcurrentDictionary<Tuple<string, string>, ICslQueryProvider>();
            }

            if (AdminProviderMapping == null)
            {
                AdminProviderMapping = new ConcurrentDictionary<Tuple<string, string>, ICslAdminProvider>();
            }

            if(DirectIngestProviderMapping == null)
            {
                DirectIngestProviderMapping = new ConcurrentDictionary<Tuple<string, string>, IKustoIngestClient>();
            }

            _logger = logger;
            _config = config;
            _kustoApiQueryEndpoint = config.KustoApiEndpoint + ":443";
            _appKey = config.AppKey;
            _clientId = config.ClientId;
            _aadAuthority = config.AADAuthority;
            _aadTenantId = config.AADTenantId;
            _aadKustoResource = config.AADKustoResource;
            _kustoCluster = string.Empty;
            _kustoClusterForIngestion = string.Empty;

            #region Extract region specific Kusto cluster 

            string rootConfigElementName = Constants.AppSettingPrefix.EndsWith('_') ? Constants.AppSettingPrefix[0..^1] : Constants.AppSettingPrefix;
            string currentHostingRegion = Constants.RegionPrefix.EndsWith('_') ? Constants.RegionPrefix[0..^1] : Constants.RegionPrefix;

            IEnumerable<IConfigurationSection> regionConfig = rootConfig.GetSection(rootConfigElementName)?.GetSection(currentHostingRegion)?.GetChildren();
            if (regionConfig?.Any() == true)
            {
                foreach (IConfigurationSection conf in regionConfig)
                {
                    if (conf.Key.Equals("KustoCluster", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(conf.Value))
                    {
                        if (conf.Value.EndsWith("follower", StringComparison.OrdinalIgnoreCase))
                        {
                            _kustoCluster = conf.Value.Trim().ToLower();
                            _kustoClusterForIngestion = _kustoCluster.Replace("follower", string.Empty, StringComparison.OrdinalIgnoreCase);
                        }
                        else
                        {
                            _kustoClusterForIngestion = conf.Value.Trim().ToLower();
                            _kustoCluster = $"{_kustoClusterForIngestion}follower";
                        }
                    }
                    else if (conf.Key.Equals("DefaultKustoDatabase", StringComparison.OrdinalIgnoreCase) && !string.IsNullOrWhiteSpace(conf.Value))
                    {
                        _defaultKustoDatabase = conf.Value.Trim().ToLower();
                    }
                }
            }
            #endregion

            if(string.IsNullOrWhiteSpace(_kustoCluster) || string.IsNullOrWhiteSpace(_kustoClusterForIngestion) || string.IsNullOrWhiteSpace(_defaultKustoDatabase))
            {
                LogError(Guid.Empty, null, $"Error creating Kusto Client. Failed to fetch kusto cluster and database defaults.", null, new Dictionary<string, string>() { 
                    ["KustoCluster"] = $"{(string.IsNullOrWhiteSpace(_kustoCluster)?"Missing": _kustoCluster)}",
                    ["KustoClusterForIngestion"] = $"{(string.IsNullOrWhiteSpace(_kustoClusterForIngestion) ? "Missing" : _kustoClusterForIngestion)}",
                    ["DefaultKustoDatabase"] = $"{(string.IsNullOrWhiteSpace(_defaultKustoDatabase) ? "Missing" : _defaultKustoDatabase)}"
                });
                throw new KustoClientException($"Error creating Kusto Client. Failed to fetch kusto cluster and database defaults. Failed settings, KustoCluster:{string.IsNullOrWhiteSpace(_kustoCluster)} KustoClusterForIngestion:{string.IsNullOrWhiteSpace(_kustoClusterForIngestion)} DefaultKustoDatabase:{string.IsNullOrWhiteSpace(_defaultKustoDatabase)}", "KustoClient constructor.");
            }
            else
            {
                LogInformation(Guid.Empty, $"Kusto client configured.", null, new Dictionary<string, string>() {
                    ["KustoCluster"] = $"{_kustoCluster}",
                    ["KustoClusterForIngestion"] = $"{_kustoClusterForIngestion}",
                    ["DefaultKustoDatabase"] = $"{_defaultKustoDatabase}"
                }, true);
            }
        }


        private KustoConnectionStringBuilder GetConnectionStringBuilder(string cluster, string database)
        {
            if (string.IsNullOrWhiteSpace(cluster) || string.IsNullOrWhiteSpace(database))
            {
                throw new KustoClientException($"Cannot create connection string for empty {((string.IsNullOrWhiteSpace(cluster)) ? "cluster" : "database")}.", "KustoConnectionStringBuilder");
            }
            return new KustoConnectionStringBuilder(_kustoApiQueryEndpoint.Replace("{cluster}", cluster), database)
            {
                FederatedSecurity = true,
                ApplicationClientId = _clientId,
                ApplicationKey = _appKey,
                Authority = _aadTenantId
            };
        }

        private IKustoIngestClient DirectIngestClient(string database, string cluster = null)
        {
            if (string.IsNullOrWhiteSpace(cluster))
            {
                cluster = _kustoClusterForIngestion;
            }
            var key = Tuple.Create(cluster, database);
            if(!DirectIngestProviderMapping.ContainsKey(key))
            {
                var directIngestProvider = KustoIngestFactory.CreateDirectIngestClient(this.GetConnectionStringBuilder(cluster, database));
                if(!DirectIngestProviderMapping.TryAdd(key, directIngestProvider))
                {
                    directIngestProvider.Dispose();
                }
            }

            return DirectIngestProviderMapping[key];
        }

        private ICslAdminProvider AdminClient(string database = "", string cluster = null)
        {
            database = string.IsNullOrWhiteSpace(database) ? _defaultKustoDatabase : database;

            if (string.IsNullOrWhiteSpace(cluster))
            {
                cluster = _kustoClusterForIngestion;
            }
            var key = Tuple.Create(cluster, database);
            if (!AdminProviderMapping.ContainsKey(key))
            {
                var adminProvider = KustoClientFactory.CreateCslAdminProvider(this.GetConnectionStringBuilder(cluster, database));
                if (!AdminProviderMapping.TryAdd(key, adminProvider))
                {
                    adminProvider.Dispose();
                }
            }

            return AdminProviderMapping[key];
        }

        private ICslQueryProvider QueryClient(string database = "", string cluster = null)
        {
            database = string.IsNullOrWhiteSpace(database) ? _defaultKustoDatabase : database;

            if (string.IsNullOrWhiteSpace(cluster))
            {
                cluster = _kustoCluster;
            }
            var key = Tuple.Create(cluster, database);
            if (!QueryProviderMapping.ContainsKey(key))
            {
                var queryProvider = KustoClientFactory.CreateCslQueryProvider(this.GetConnectionStringBuilder(cluster, database));
                if (!QueryProviderMapping.TryAdd(key, queryProvider))
                {
                    queryProvider.Dispose();
                }
            }

            return QueryProviderMapping[key];
        }

        private string GetKustoClientId(Guid aggregationRunIdGuid, string callerName, string cluster, string database, bool isRequestForAdminCommand)
        {
            return $"Diagnostics.{Constants.AppSettingPrefix}{Constants.RegionPrefix}{cluster}.{database}|{callerName ?? "KustoClient"};{(isRequestForAdminCommand ? "AdminCommand" : "Query")}##{aggregationRunIdGuid}_{Guid.NewGuid()}";
        }
        private ClientRequestProperties GetClientRequestProperties(string kustoClientRequestId, string cluster, int timeoutSeconds = 60)
        {
            ClientRequestProperties clientRequestProperties = new ClientRequestProperties()
            {
                ClientRequestId = kustoClientRequestId,
                Application = Constants.ApplicationName
            };

            clientRequestProperties.SetOption("servertimeout", new TimeSpan(0, 0, timeoutSeconds));
            if (cluster.StartsWith("waws", StringComparison.OrdinalIgnoreCase))
            {
                clientRequestProperties.SetOption(ClientRequestProperties.OptionQueryConsistency, ClientRequestProperties.OptionQueryConsistency_Weak);
            }

            return clientRequestProperties;
        }


        private void LogOperationCancelledMessage(Guid aggregationRunIdGuid, string cancelledFrom, long elapsedMilliseconds, string kustoClientId, ICustomAggregationLogger logger = null)
        {
            LogInformation(aggregationRunIdGuid, $"Cancelled operation {cancelledFrom}.", logger, 
                new Dictionary<string, string>() {
                    ["KustoClientId"] = kustoClientId,
                    ["TimeTaken"] = $"{elapsedMilliseconds}"
                });            
        }

        private KustoIngestionResult GetIngestionResultForCancellation(Guid aggregationRunIdGuid,
                                                                       Guid ingestionCommandOperationId,
                                                                       KustoIngestionResultStates ingestionState,
                                                                       string ingestionStateStr, string cancelledFrom,
                                                                       long elapsedMilliseconds, string kustoClientId,
                                                                       string database = "",
                                                                       ICustomAggregationLogger logger = null)
        {
            database = string.IsNullOrWhiteSpace(database) ? _defaultKustoDatabase : database;
            LogOperationCancelledMessage(aggregationRunIdGuid, cancelledFrom, elapsedMilliseconds, kustoClientId, logger);
            return new KustoIngestionResult()
            {
                OperationId = ingestionCommandOperationId,
                IngestionState = ingestionState,
                State = ingestionStateStr??ingestionState.ToString(),
                Database = database
            };
        }

        public async Task<KustoIngestionResult> IngestFromDataTable(CancellationToken ct, Guid aggregationRunIdGuid, string callerName,
                                                                    string tableNameToIngestInto,
                                                                    DataTable dataTableToIngestFrom, string extentTag,
                                                                    int timeoutSeconds = 600,
                                                                    int ingestionStatusPolingIntervalSeconds = 5,
                                                                    string database = "", ICustomAggregationLogger logger = null)
        {
            if (!ct.CanBeCanceled)
            {
                throw new ArgumentException("Cancellation token invalid. Please supply a cancellation token that can be cancelled.", "ct");
            }

            if (aggregationRunIdGuid == Guid.Empty)
            {
                throw new ArgumentNullException("Supply an operation id for tracking purposes.");
            }

            database = string.IsNullOrWhiteSpace(database) ? _defaultKustoDatabase : database;

            Stopwatch timeTakenStopWatch = new Stopwatch();
            string currCallerName = callerName ?? "IngestFromDataTable";
            KustoIngestionResult ingestionResult = new KustoIngestionResult();
            Guid ingestionCommandOperationId = Guid.Empty;

            try
            {
                timeTakenStopWatch.Start();
                if (ct.IsCancellationRequested)
                {
                    timeTakenStopWatch.Stop();
                    return GetIngestionResultForCancellation(aggregationRunIdGuid, ingestionCommandOperationId, KustoIngestionResultStates.NotStarted, "NotStarted", "IngestFromDataTable", timeTakenStopWatch.ElapsedMilliseconds, ingestionCommandOperationId.ToString(), database, logger);
                }

                ingestionCommandOperationId = await IngestFromDataTableWithoutWaiting(ct, aggregationRunIdGuid, currCallerName, tableNameToIngestInto, dataTableToIngestFrom, extentTag, database);

                if(ct.IsCancellationRequested)
                {
                    timeTakenStopWatch.Stop();
                    if (ingestionCommandOperationId == Guid.Empty)
                    {
                        return GetIngestionResultForCancellation(aggregationRunIdGuid, ingestionCommandOperationId, KustoIngestionResultStates.NotStarted, "NotStarted", "IngestFromDataTable", timeTakenStopWatch.ElapsedMilliseconds, ingestionCommandOperationId.ToString(), database, logger);
                    }
                    else
                    {
                        return GetIngestionResultForCancellation(aggregationRunIdGuid, ingestionCommandOperationId, KustoIngestionResultStates.InProgress, "InProgress", "IngestFromDataTable", timeTakenStopWatch.ElapsedMilliseconds, ingestionCommandOperationId.ToString(), database, logger);
                    }
                }

                ingestionResult = await PollForIngestionResult(ct, aggregationRunIdGuid, currCallerName, ingestionCommandOperationId,
                    Convert.ToInt32(TimeSpan.FromSeconds(timeoutSeconds).Subtract(timeTakenStopWatch.Elapsed).TotalSeconds), ingestionStatusPolingIntervalSeconds, database, logger);
            }            
            catch (Exception te) when (te is TimeoutException
                                       || te is TaskCanceledException)
            {
                timeTakenStopWatch.Stop();
                if (ingestionCommandOperationId != Guid.Empty)
                {
                    ingestionResult.IngestionState = KustoIngestionResultStates.InProgress;
                    ingestionResult.OperationId = ingestionCommandOperationId;
                }
                else
                {
                    LogError(aggregationRunIdGuid, te, $"Exception : {te.Message}", logger
                        , new Dictionary<string, string>() {
                            ["KustoClientId"] = $"{ingestionCommandOperationId}",
                            ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                            ["CallerName"] = currCallerName
                        });                    
                    throw new KustoClientException($"Exception while ingesting data. AggregationRunId:{aggregationRunIdGuid}", currCallerName, te);
                }
            }
            catch (Exception ex)
            {
                timeTakenStopWatch.Stop();
                LogError(aggregationRunIdGuid, ex, $"Exception : {ex.Message}.", logger
                        , new Dictionary<string, string>()
                        {
                            ["KustoClientId"] = $"{ingestionCommandOperationId}",
                            ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                            ["CallerName"] = currCallerName
                        });
                throw new KustoClientException($"Exception while ingesting data. AggregationRunId:{aggregationRunIdGuid}", currCallerName, ex);
            }
            finally
            {
                timeTakenStopWatch.Stop();
            }

            LogInformation(aggregationRunIdGuid, "IngestFromQuery ran to completion.", logger
                , new Dictionary<string, string>() {
                    ["KustoClientId"] = $"{ingestionCommandOperationId}",
                    ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                    ["CallerName"] = currCallerName
                });

            return ingestionResult;
        }

        public async Task<Guid> IngestFromDataTableWithoutWaiting(CancellationToken ct, Guid aggregationRunIdGuid, string callerName,
                                                                  string tableNameToIngestInto, DataTable dataTableToIngestFrom,
                                                                  string extentTag, string database = "",
                                                                  ICustomAggregationLogger logger = null)
        {
            #region Parameter validation
            if (!ct.CanBeCanceled)
            {
                throw new ArgumentException("Cancellation token invalid. Please supply a cancellation token that can be cancelled.", "ct");
            }

            if (aggregationRunIdGuid == Guid.Empty)
            {
                throw new ArgumentNullException("Supply an operation id for tracking purposes.");
            }

            if (string.IsNullOrWhiteSpace(extentTag))
            {
                //Technically this is not required by Kusto but it is a good practive to assign a tag to an extent, hence forcing it here.
                throw new ArgumentNullException("Cannot ingest data with empty extent tag. Please generate a string tag to uniquely identify this chunk of data being ingested.");
            }

            if (string.IsNullOrWhiteSpace(tableNameToIngestInto))
            {
                throw new ArgumentNullException("Table name where the data should be ingested is required.");
            }

            if (dataTableToIngestFrom == null )
            {
                throw new ArgumentNullException("DataTable that supplies data to ingest cannot be null.");
            }
            
            if ( dataTableToIngestFrom.Rows.Count < 1)
            {
                throw new ArgumentNullException("DataTable that supplies data to ingest cannot be empty.");
            }
            #endregion

            database = string.IsNullOrWhiteSpace(database) ? _defaultKustoDatabase : database;

            Stopwatch timeTakenStopWatch = new Stopwatch();
            string currCallerName = callerName ?? "IngestFromDataTableWithoutWaiting";

            Guid ingestionCommandOperationId = Guid.Empty;
            Guid sourceIdGuid = Guid.NewGuid();
            try
            {
                timeTakenStopWatch.Start();

                DataReaderDescription drDesc = new DataReaderDescription()
                {
                    SourceId = sourceIdGuid,
                    DataReader = dataTableToIngestFrom.CreateDataReader()
                };

                List<string> extentTagList = new List<string>() { extentTag };

                KustoIngestionProperties kip = new KustoIngestionProperties(database, tableNameToIngestInto)
                {
                    DropByTags = extentTagList,
                    IngestByTags = extentTagList,
                    IngestIfNotExists = extentTagList,
                    AdditionalProperties = new Dictionary<string, string>()
                                            {
                                                ["persistDetails"] = "true"
                                            }
                };
                
                if(ct.IsCancellationRequested)
                {
                    timeTakenStopWatch.Stop();
                    LogOperationCancelledMessage(aggregationRunIdGuid, "IngestFromDataTableWithoutWaiting", timeTakenStopWatch.ElapsedMilliseconds, ingestionCommandOperationId.ToString(), logger);
                    return ingestionCommandOperationId;
                }
                IKustoIngestionResult ingestionResult = await DirectIngestClient(database, _kustoClusterForIngestion).IngestFromDataReaderAsync(drDesc, kip);
                ingestionCommandOperationId = ingestionResult.GetIngestionStatusBySourceId(sourceIdGuid).OperationId;

            }
            catch(TaskCanceledException)
            {
                timeTakenStopWatch.Stop();
                //Swallow the exception if we have a valid guid since the ingestion command was successfully submitted to Kusto and it is now InProgress
                if (ingestionCommandOperationId == Guid.Empty)
                {
                    throw;
                }
            }
            catch (Exception ex)
            {
                timeTakenStopWatch.Stop();
                LogError(aggregationRunIdGuid, ex, $"Exception : {ex.Message}.", logger
                    , new Dictionary<string, string>() {
                        ["KustoClientId"] = $"{sourceIdGuid}",
                        ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                        ["CallerName"] = currCallerName,
                    });
                
                throw new KustoClientException($"Exception while ingesting data. AggregationRunId:{aggregationRunIdGuid}", currCallerName, ex);
            }
            finally
            {
                timeTakenStopWatch.Stop();
            }

            LogInformation(aggregationRunIdGuid, "IngestFromDataTableWithoutWaiting ran to completion.", logger
                , new Dictionary<string, string>()
                {
                    ["KustoClientId"] = $"{sourceIdGuid}",
                    ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                    ["CallerName"] = currCallerName,
                });

            return ingestionCommandOperationId;
        }

        public async Task<Guid> IngestFromQueryWithoutWaiting(CancellationToken ct, Guid aggregationRunIdGuid, string callerName,
                                                                  string tableNameToIngestInto, string queryToIngestFrom,
                                                                  string extentTag, string database = "",
                                                                  ICustomAggregationLogger logger = null)
        {
            #region Parameter validation
            if (!ct.CanBeCanceled)
            {
                throw new ArgumentException("Cancellation token invalid. Please supply a cancellation token that can be cancelled.", "ct");
            }

            if (aggregationRunIdGuid == Guid.Empty)
            {
                throw new ArgumentNullException("Supply an operation id for tracking purposes.");
            }

            if (string.IsNullOrWhiteSpace(extentTag))
            {
                //Technically this is not required by Kusto but it is a good practive to assign a tag to an extent, hence forcing it here.
                throw new ArgumentNullException("Cannot ingest data with empty extent tag. Please generate a string tag to uniquely identify this chunk of data being ingested.");
            }

            if (string.IsNullOrWhiteSpace(tableNameToIngestInto))
            {
                throw new ArgumentNullException("Table name where the data should be ingested is required.");
            }

            if (string.IsNullOrWhiteSpace(queryToIngestFrom))
            {
                throw new ArgumentNullException("Query that generates data to ingest cannot be empty.");
            }
            #endregion

            database = string.IsNullOrWhiteSpace(database) ? _defaultKustoDatabase : database;

            Stopwatch timeTakenStopWatch = new Stopwatch();
            string currCallerName = callerName ?? "IngestFromQueryWithoutWaiting";
            string kustoClientRequestId = GetKustoClientId(aggregationRunIdGuid, currCallerName, _kustoClusterForIngestion, database, isRequestForAdminCommand: true);

            Guid ingestionCommandOperationId = Guid.Empty;
            try
            {
                timeTakenStopWatch.Start();
                string ingestionCommand = $".set-or-append async {tableNameToIngestInto} with (tags='[\"ingest-by:{extentTag}\", \"drop-by:{extentTag}\"]', ingestIfNotExists='[\"{extentTag}\"]', persistDetails=true) <| {queryToIngestFrom}";

                if (ct.IsCancellationRequested)
                {
                    timeTakenStopWatch.Stop();
                    LogOperationCancelledMessage(aggregationRunIdGuid, "IngestFromDataTableWithoutWaiting", timeTakenStopWatch.ElapsedMilliseconds, kustoClientRequestId, logger);
                    return ingestionCommandOperationId;
                }

                DataTable ingestionCommandResult = await ExecuteAdminCommandAsync(ct, aggregationRunIdGuid, currCallerName, ingestionCommand, runAgainstIngestionCluster: true, database: database, logger: logger);
                if (ingestionCommandResult == null || ingestionCommandResult?.Rows?.Count < 1)
                {
                    timeTakenStopWatch.Stop();
                    LogOperationCancelledMessage(aggregationRunIdGuid, "IngestFromQueryWithoutWaiting", timeTakenStopWatch.ElapsedMilliseconds, kustoClientRequestId, logger);
                    return ingestionCommandOperationId;
                }
                else
                {                    
                    var ingestionCommandRow = ingestionCommandResult.Rows.OfType<DataRow>().Single();
                    ingestionCommandOperationId = new Guid(ingestionCommandRow["OperationId"].ToString());
                }
            }
            catch (TaskCanceledException)
            {
                timeTakenStopWatch.Stop();
                //Swallow the exception if we have a valid guid since the ingestion command was successfully submitted to Kusto and it is now InProgress
                if (ingestionCommandOperationId == Guid.Empty)
                {
                    throw;
                }
            }
            catch (Exception ex)
            {
                timeTakenStopWatch.Stop();

                LogError(aggregationRunIdGuid, ex, $"Exception : {ex.Message}.", logger
                    , new Dictionary<string, string>()
                    {
                        ["KustoClientId"] = $"{kustoClientRequestId}",
                        ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                        ["CallerName"] = currCallerName,
                    });
                
                throw new KustoClientException($"Exception while ingesting data. AggregationRunId:{aggregationRunIdGuid}", currCallerName, ex);
            }
            finally
            {
                timeTakenStopWatch.Stop();
            }

            LogInformation(aggregationRunIdGuid, "IngestFromQueryWithoutWaiting ran to completion.", logger
                    , new Dictionary<string, string>()
                    {
                        ["KustoClientId"] = $"{kustoClientRequestId}",
                        ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                        ["CallerName"] = currCallerName,
                    });

            return ingestionCommandOperationId;
        }

        private KustoIngestionResult GetIngestionResultFromDataRow(DataRow operationIdPollRow)
        {
            if (operationIdPollRow == null || operationIdPollRow.Table?.Columns?.Count < 1)
            {
                return null;
            }


            KustoIngestionResult ingestionResult = new KustoIngestionResult();
            string state = operationIdPollRow["State"].ToString();
            if (state == "Completed")
            {
                ingestionResult.IngestionState = KustoIngestionResultStates.Completed;
            }
            else if (state == "InProgress")
            {
                ingestionResult.IngestionState = KustoIngestionResultStates.InProgress;
            }
            else if (state == "Throttled")
            {
                ingestionResult.IngestionState = KustoIngestionResultStates.Throttled;
            }
            else if (state == "Failed")
            {
                ingestionResult.IngestionState = KustoIngestionResultStates.Failed;
            }
            else if (state == "PartiallySucceeded")
            {
                ingestionResult.IngestionState = KustoIngestionResultStates.PartiallySucceeded;
            }
            else
            {
                ingestionResult.IngestionState = KustoIngestionResultStates.Other;
            }

            ingestionResult.OperationId = new Guid(operationIdPollRow["OperationId"].ToString());
            ingestionResult.Operation = operationIdPollRow["Operation"].ToString();
            ingestionResult.NodeId = operationIdPollRow["NodeId"].ToString();
            _ = DateTime.TryParse(operationIdPollRow["StartedOn"].ToString(), out ingestionResult.StartedOn);
            _ = DateTime.TryParse(operationIdPollRow["LastUpdatedOn"].ToString(), out ingestionResult.LastUpdatedOn);            
            _ = TimeSpan.TryParse(operationIdPollRow["Duration"].ToString(),out ingestionResult.Duration);
            ingestionResult.State = state;
            ingestionResult.Status = operationIdPollRow["Status"].ToString();
            ingestionResult.RootActivityId = new Guid(operationIdPollRow["RootActivityId"].ToString());
            ingestionResult.ShouldRetry = Convert.ToBoolean(operationIdPollRow["ShouldRetry"].ToString());
            ingestionResult.Database = operationIdPollRow["Database"].ToString();
            ingestionResult.Principal = operationIdPollRow["Principal"].ToString();
            ingestionResult.User = operationIdPollRow["User"].ToString();

            return ingestionResult;
        }

        public async Task<KustoIngestionResult> PollForIngestionResult(CancellationToken ct, Guid aggregationRunIdGuid, string callerName, Guid ingestionCommandOperationId, int timeoutSeconds = 600, int ingestionStatusPolingIntervalSeconds = 5, string database = "", ICustomAggregationLogger logger = null)
        {
            if (!ct.CanBeCanceled)
            {
                throw new ArgumentException("Cancellation token invalid. Please supply a cancellation token that can be cancelled.", "ct");
            }

            if (aggregationRunIdGuid == Guid.Empty)
            {                
                throw new ArgumentNullException("Supply an operation id for tracking purposes.");
            }

            database = string.IsNullOrWhiteSpace(database) ? _defaultKustoDatabase : database;

            string currCallerName = callerName ?? "PollForIngestionResult";
            string kustoClientRequestId = GetKustoClientId(aggregationRunIdGuid, currCallerName, _kustoClusterForIngestion, database, isRequestForAdminCommand: true);

            if (ingestionCommandOperationId == Guid.Empty)
            {
                //This can happen if cancellation was requested right before polling started, handle this gracefully and terminate execution.
                //return GetIngestionResultForCancellation(aggregationRunIdGuid, ingestionCommandOperationId, KustoIngestionResultStates.InProgress, "PollForIngestionResult", 0, kustoClientRequestId, database, logger);
                throw new ArgumentNullException("Cannot poll ingestion status for empty operation id");
            }

            Stopwatch timeTakenStopWatch = new Stopwatch();            
            KustoIngestionResult ingestionResult = null;
            try
            {
                long timeoutMiliseconds = timeoutSeconds * 1000;
                timeTakenStopWatch.Start();
                var kustoAdminClient = AdminClient(database, _kustoClusterForIngestion);
                bool continuePolling = true;
                string showOperationCommand = $".show operations {ingestionCommandOperationId}";
                double polingInterval = (double)ingestionStatusPolingIntervalSeconds;


                while (continuePolling)
                {
                    DataTable operationIdPollResult = await ExecuteAdminCommandAsync(ct, aggregationRunIdGuid, currCallerName, showOperationCommand, true, database: database, logger: logger);
                    if (operationIdPollResult == null || operationIdPollResult?.Rows?.Count < 0 )
                    {
                        continuePolling = false;
                        timeTakenStopWatch.Stop();
                        return GetIngestionResultForCancellation(aggregationRunIdGuid, ingestionCommandOperationId, KustoIngestionResultStates.InProgress, "InProgress", "PollForIngestionResult", timeTakenStopWatch.ElapsedMilliseconds, kustoClientRequestId, database, logger);
                    }

                    DataRow operationIdPollRow = operationIdPollResult.Rows.OfType<DataRow>().Single();
                    string state = operationIdPollRow["State"].ToString(); // "InProgress", "Completed", "Throttled", or "Failed"
                    if (state == "InProgress")
                    {
                        Thread.Sleep(TimeSpan.FromSeconds(polingInterval));
                        polingInterval *= 1.1;//Gradually delay poling interval. If the ingestion is taking time, it'll probably take longer. No point aggressively poling for status.
                        if (timeTakenStopWatch.ElapsedMilliseconds > timeoutMiliseconds)
                        {
                            continuePolling = false;
                            throw new TimeoutException($"Timeout period of {timeoutSeconds} seconds elapsed while polling operation id {ingestionCommandOperationId} for ingestion to complete. AggregationRunId:{aggregationRunIdGuid}");
                        }
                    }
                    else
                    {
                        //Completed, PartiallySucceeded, Throttled and Failed states.
                        continuePolling = false;
                        ingestionResult = this.GetIngestionResultFromDataRow(operationIdPollRow);
                    }
                }

            }
            catch (Exception te) when (te is TimeoutException
                                       || te is TaskCanceledException)
            {
                timeTakenStopWatch.Stop();
                throw te;
            }
            catch (Exception ex)
            {
                timeTakenStopWatch.Stop();

                LogError(aggregationRunIdGuid, ex, $"Exception : {ex.Message}.", logger
                    , new Dictionary<string, string>()
                    {
                        ["KustoClientId"] = $"{kustoClientRequestId}",
                        ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                        ["CallerName"] = currCallerName,
                    });
                
                throw new KustoClientException($"Exception while polling for ingestion result. AggregationRunId:{aggregationRunIdGuid}", currCallerName, ex);
            }
            finally
            {
                timeTakenStopWatch.Stop();
            }

            LogInformation(aggregationRunIdGuid, "PollForIngestionResult ran to completion.", logger
                    , new Dictionary<string, string>()
                    {
                        ["KustoClientId"] = $"{kustoClientRequestId}",
                        ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                        ["CallerName"] = currCallerName,
                    });

            return ingestionResult;

        }

        public async Task<KustoIngestionResult> IngestFromQuery(CancellationToken ct, Guid aggregationRunIdGuid, string callerName, string tableNameToIngestInto, string queryToIngestFrom, string extentTag, int timeoutSeconds = 600, int ingestionStatusPolingIntervalSeconds = 5, string database = "", ICustomAggregationLogger logger = null)
        {
            if (!ct.CanBeCanceled)
            {
                throw new ArgumentException("Cancellation token invalid. Please supply a cancellation token that can be cancelled.", "ct");
            }
            
            if (aggregationRunIdGuid == Guid.Empty)
            {
                throw new ArgumentNullException("Supply an operation id for tracking purposes.");
            }

            database = string.IsNullOrWhiteSpace(database) ? _defaultKustoDatabase : database;

            Stopwatch timeTakenStopWatch = new Stopwatch();
            string currCallerName = callerName ?? "IngestFromQuery";
            string kustoClientRequestId = GetKustoClientId(aggregationRunIdGuid, currCallerName, _kustoClusterForIngestion, database, isRequestForAdminCommand: true);
            KustoIngestionResult ingestionResult = new KustoIngestionResult();
            Guid ingestionCommandOperationId = Guid.Empty;
            try
            {
                timeTakenStopWatch.Start();

                if(ct.IsCancellationRequested)
                {
                    timeTakenStopWatch.Stop();
                    return GetIngestionResultForCancellation(aggregationRunIdGuid,ingestionCommandOperationId, KustoIngestionResultStates.NotStarted, "NotStarted", "IngestFromQuery", timeTakenStopWatch.ElapsedMilliseconds, kustoClientRequestId, database, logger);
                }

                ingestionCommandOperationId = await IngestFromQueryWithoutWaiting(ct, aggregationRunIdGuid, currCallerName, tableNameToIngestInto, queryToIngestFrom, extentTag, database);
                if (ct.IsCancellationRequested)
                {
                    timeTakenStopWatch.Stop();
                    if (ingestionCommandOperationId == Guid.Empty)
                    {
                        return GetIngestionResultForCancellation(aggregationRunIdGuid, ingestionCommandOperationId, KustoIngestionResultStates.NotStarted, "NotStarted", "IngestFromQuery", timeTakenStopWatch.ElapsedMilliseconds, ingestionCommandOperationId.ToString(), database, logger);
                    }
                    else
                    {
                        return GetIngestionResultForCancellation(aggregationRunIdGuid, ingestionCommandOperationId, KustoIngestionResultStates.InProgress, "InProgress", "IngestFromQuery", timeTakenStopWatch.ElapsedMilliseconds, ingestionCommandOperationId.ToString(), database, logger);
                    }
                }
                ingestionResult = await PollForIngestionResult(ct, aggregationRunIdGuid, currCallerName, ingestionCommandOperationId,
                    Convert.ToInt32(TimeSpan.FromSeconds(timeoutSeconds).Subtract(timeTakenStopWatch.Elapsed).TotalSeconds), ingestionStatusPolingIntervalSeconds, database, logger);
            }
            catch (Exception te) when (te is TimeoutException
                                       || te is TaskCanceledException)
            {
                timeTakenStopWatch.Stop();
                if (ingestionCommandOperationId != Guid.Empty)
                {
                    ingestionResult.IngestionState = KustoIngestionResultStates.InProgress;
                    ingestionResult.OperationId = ingestionCommandOperationId;
                }
                else
                {
                    LogError(aggregationRunIdGuid, te, $"Exception : {te.Message}", logger
                    , new Dictionary<string, string>()
                    {
                        ["KustoClientId"] = $"{kustoClientRequestId}",
                        ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                        ["CallerName"] = currCallerName,
                    });

                    throw new KustoClientException($"Exception while ingesting data. AggregationRunId:{aggregationRunIdGuid}", currCallerName, te);
                }
            }
            catch (Exception ex)
            {
                timeTakenStopWatch.Stop();

                LogError(aggregationRunIdGuid, ex, $"Exception : {ex.Message}", logger
                    , new Dictionary<string, string>()
                    {
                        ["KustoClientId"] = $"{kustoClientRequestId}",
                        ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                        ["CallerName"] = currCallerName,
                    });

                throw new KustoClientException($"Exception while ingesting data. AggregationRunId:{aggregationRunIdGuid}", currCallerName, ex);
            }
            finally
            {
                timeTakenStopWatch.Stop();
            }

            LogInformation(aggregationRunIdGuid, $"IngestFromQuery ran to completion.", logger
                    , new Dictionary<string, string>()
                    {
                        ["KustoClientId"] = $"{kustoClientRequestId}",
                        ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                        ["CallerName"] = currCallerName,
                    });

            return ingestionResult;
        }

        public async Task<DataTable> ExecuteAdminCommandAsync(CancellationToken ct, Guid aggregationRunIdGuid, string callerName, string adminCommand, bool runAgainstIngestionCluster = false, int timeoutSeconds = 60, string database = "", ICustomAggregationLogger logger = null)
        {
            if (!ct.CanBeCanceled)
            {
                throw new ArgumentException("Cancellation token invalid. Please supply a cancellation token that can be cancelled.", "ct");
            }

            if (aggregationRunIdGuid == Guid.Empty)
            {
                throw new ArgumentNullException("Supply an operation id for tracking purposes.");
            }

            database = string.IsNullOrWhiteSpace(database) ? _defaultKustoDatabase : database;

            Stopwatch timeTakenStopWatch = new Stopwatch();
            DataSet dataSet = null;
            string currKustoCluster = runAgainstIngestionCluster ? _kustoClusterForIngestion : _kustoCluster;
            string currCallername = callerName ?? "ExecuteAdminCommandAsync";
            string kustoClientRequestId = GetKustoClientId(aggregationRunIdGuid, currCallername, currKustoCluster, database, isRequestForAdminCommand: true);
            try
            {
                timeTakenStopWatch.Start();
                var kustoAdminClient = AdminClient(database, currKustoCluster);
                if(ct.IsCancellationRequested)
                {
                    timeTakenStopWatch.Stop();
                    LogOperationCancelledMessage(aggregationRunIdGuid, "ExecuteAdminCommandAsync", timeTakenStopWatch.ElapsedMilliseconds, kustoClientRequestId, logger);
                }
                else
                {
                    var result = await kustoAdminClient.ExecuteControlCommandAsync(database, adminCommand, this.GetClientRequestProperties(kustoClientRequestId, currKustoCluster, timeoutSeconds));
                    dataSet = result.ToDataSet();
                }
            }
            catch (Exception ex)
            {
                timeTakenStopWatch.Stop();
                LogError(aggregationRunIdGuid, ex, $"Exception : {ex.Message}.", logger
                    , new Dictionary<string, string>()
                    {
                        ["KustoClientId"] = $"{kustoClientRequestId}",
                        ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                        ["CallerName"] = currCallername,
                    });
                
                throw new KustoClientException($"Exception while executing admin query. AggregationRunId:{aggregationRunIdGuid}", currCallername, ex);
            }
            finally
            {
                timeTakenStopWatch.Stop();
            }

            LogInformation(aggregationRunIdGuid, "ExecuteAdminCommandAsync ran to completion.", logger
                    , new Dictionary<string, string>()
                    {
                        ["KustoClientId"] = $"{kustoClientRequestId}",
                        ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                        ["CallerName"] = currCallername,
                    });

            var datatable = dataSet?.Tables?[0];
            if (datatable == null)
            {
                datatable = new DataTable();
            }
            return datatable;
        }

        public async Task<DataTable> ExecuteQueryAsync(CancellationToken ct, Guid aggregationRunIdGuid, string callerName, string query, int timeoutSeconds = 60, string database = "", ICustomAggregationLogger logger = null)
        {
            if(!ct.CanBeCanceled)
            {
                throw new ArgumentException("Cancellation token invalid. Please supply a cancellation token that can be cancelled.", "ct");
            }

            if (aggregationRunIdGuid == Guid.Empty)
            {
                throw new ArgumentNullException("Supply an operation id for tracking purposes.");
            }

            database = string.IsNullOrWhiteSpace(database) ? _defaultKustoDatabase : database;

            Stopwatch timeTakenStopWatch = new Stopwatch();
            DataSet dataSet = null;
            string currCallerName = callerName ?? "ExecuteQueryAsync";
            string kustoClientRequestId = GetKustoClientId(aggregationRunIdGuid, currCallerName, _kustoCluster, database, isRequestForAdminCommand: false);
            try
            {
                timeTakenStopWatch.Start();
                var kustoClient = QueryClient(database);
                if(ct.IsCancellationRequested)
                {
                    timeTakenStopWatch.Stop();
                    LogOperationCancelledMessage(aggregationRunIdGuid, "ExecuteQueryAsync", timeTakenStopWatch.ElapsedMilliseconds, kustoClientRequestId, logger);
                }
                else
                {
                    var result = await kustoClient.ExecuteQueryAsync(database, query, this.GetClientRequestProperties(kustoClientRequestId, _kustoCluster, timeoutSeconds));
                    dataSet = result.ToDataSet();
                }
                
            }
            catch (Exception ex)
            {
                timeTakenStopWatch.Stop();

                LogError(aggregationRunIdGuid, ex, $"Exception : {ex.Message}.", logger
                    , new Dictionary<string, string>()
                    {
                        ["KustoClientId"] = $"{kustoClientRequestId}",
                        ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                        ["CallerName"] = currCallerName,
                    });

                throw new KustoClientException($"Exception while executing query. AggregationRunId:{aggregationRunIdGuid}", currCallerName, ex);
            }
            finally
            {
                timeTakenStopWatch.Stop();
            }

            LogInformation(aggregationRunIdGuid, "ExecuteQueryAsync ran to completion.", logger
                    , new Dictionary<string, string>()
                    {
                        ["KustoClientId"] = $"{kustoClientRequestId}",
                        ["TimeTaken"] = $"{timeTakenStopWatch.ElapsedMilliseconds}",
                        ["CallerName"] = currCallerName,
                    });

            var datatable = dataSet?.Tables?[0];
            if (datatable == null)
            {
                datatable = new DataTable();
            }
            return datatable;
        }

        private void LogError(Guid aggregationRunId, Exception ex, string message, ICustomAggregationLogger logger = null, Dictionary<string, string> args = null)
        {
            try
            {
                LogMessage msg = new LogMessage(aggregationRunId, message, ex, args);
                _logger.LogError(ex, JsonConvert.SerializeObject(msg));

                logger?.LogError(msg);
            }
            catch (Exception) { }
        }

        private void LogInformation(Guid aggregationRunId, string message, ICustomAggregationLogger logger = null, Dictionary<string, string> args = null, bool forceLog = false)
        {
            try
            {
                LogMessage msg = new LogMessage(aggregationRunId, message, args);
                if (_config.DebugLogsEnabled || forceLog)
                {
                    _logger.LogInformation(JsonConvert.SerializeObject(msg));
                }
                logger?.LogInformation(msg);
            }
            catch (Exception) { }
        }
    }
}
