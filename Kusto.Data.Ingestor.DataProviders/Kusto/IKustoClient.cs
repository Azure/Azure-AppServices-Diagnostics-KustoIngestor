using Kusto.Data.Ingestor.Common.LogHelper;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kusto.Data.Ingestor.DataProviders.Kusto
{
    public interface IKustoClient
    {
        /// <summary>
        /// Executes a query against the specified databse and returns the results. Use for readonly operations.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="aggregationRunIdGuid">Unique identifier to represent current aggregation operation.</param>
        /// <param name="callerName">Name of the calling function.</param>
        /// <param name="query">Query to execute.</param>
        /// <param name="timeoutSeconds">Timeout duration for the query.</param>
        /// <param name="database">Database name against which to execute the query.</param>
        /// <param name="logger">Logger</param>
        /// <returns>An exception of type <see cref="KustoClientException"/> if the command fails to execute, else returns a <see cref="DataTable"/> with results of the query.
        /// </returns>
        public Task<DataTable> ExecuteQueryAsync(CancellationToken ct, Guid aggregationRunIdGuid, string callerName, string query, int timeoutSeconds = 60, string database = "", ICustomAggregationLogger logger = null);

        /// <summary>
        /// Executes an admin command against the specified databse and returns the results.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="aggregationRunIdGuid">Unique identifier to represent current aggregation operation.</param>
        /// <param name="callerName">Name of the calling function.</param>
        /// <param name="adminCommand">Command to execute.</param>
        /// <param name="runAgainstIngestionCluster">Target this command to the kusto cluster where data is to be ingested.</param>
        /// <param name="timeoutSeconds">Timeout duration for the admin command.</param>
        /// <param name="database">Database name against which to execute the query.</param>
        /// <param name="logger">Logger</param>
        /// <returns>An exception of type <see cref="KustoClientException"/> if the command fails to execute, else returns a <see cref="DataTable"/> with results of the query.
        /// </returns>
        public Task<DataTable> ExecuteAdminCommandAsync(CancellationToken ct, Guid aggregationRunIdGuid, string callerName, string adminCommand, bool runAgainstIngestionCluster = false, int timeoutSeconds = 60, string database = "", ICustomAggregationLogger logger = null);

        /// <summary>
        /// <para>Contents of <paramref name="dataTableToIngestFrom"/> are ingested into table <paramref name="tableNameToIngestInto"/>. The table to ingest into should be in <paramref name="database"/>.<br/>
        /// Runs a .set-or-append async command and does not wait for the ingestion to complete. The ingestion may/may not be sucessful. Use this for fire and forget scenarios.</para>
        /// To wait for the ingestion result, use <see cref="IngestFromDataTable"/>. 
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="aggregationRunIdGuid">Unique identifier to represent current aggregation operation.</param>
        /// <param name="callerName">Name of the calling function.</param>
        /// <param name="tableNameToIngestInto">Table name into which the data will be ingested.</param>
        /// <param name="dataTableToIngestFrom">Contents of this DataTable will be ingested into the specified table. Datatypes of colums should match the datatypes of the specified Kusto table.</param>
        /// <param name="extentTag">A string used to tag the current data being ingested. Data with the same tag is not re-inserted to avoid data duplication. Tags can later be used to identify this extent/chunk of data for cleanup.</param>
        /// <param name="database">Database name into which the data will be ingested.</param>
        /// <param name="logger">Logger</param>
        /// <returns>An exception of type <see cref="KustoClientException"/> if the command fails to execute, else returns the operationId for the ingestion command. Operation Id can later be used to poll for the status of ingestion via <see cref="PollForIngestionResult"/>.<br/>
        /// The final ingestion may/may not succeed.</returns>
        public Task<Guid> IngestFromDataTableWithoutWaiting(CancellationToken ct, Guid aggregationRunIdGuid, string callerName, string tableNameToIngestInto, DataTable dataTableToIngestFrom, string extentTag, string database = "", ICustomAggregationLogger logger = null);

        /// <summary>
        /// <para>Contents of <paramref name="dataTableToIngestFrom"/> are ingested into table <paramref name="tableNameToIngestInto"/>. The table to ingest into should be in <paramref name="database"/>.<br/>
        /// Runs a .set-or-append async command. Uses the operation id returned to poll for and return the final ingestion stauts. Use this operation id to confirm if an ingestion completed/failed and take appropriate actions for cleanup.</para>
        /// For fire and forget type of scenarios, use <see cref="IngestFromDataTableWithoutWaiting"/>.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="aggregationRunIdGuid">Unique identifier to represent current aggregation operation.</param>
        /// <param name="callerName">Name of the calling function.</param>
        /// <param name="tableNameToIngestInto">Table name into which the data will be ingested.</param>
        /// <param name="dataTableToIngestFrom">Contents of this DataTable will be ingested into the specified table. Datatypes of colums should match the datatypes of the specified Kusto table.</param>
        /// <param name="extentTag">A string used to tag the current data being ingested. Data with the same tag is not re-inserted to avoid data duplication. Tags can later be used to identify this extent/chunk of data for cleanup.</param>
        /// <param name="timeoutSeconds">Timeout duration for the ingestion.</param>
        /// <param name="ingestionStatusPolingIntervalSeconds">Time interval to wait between each poll to the cluster while checking for ingestion completion. Poling interval gradually increases automatically.</param>
        /// <param name="database">Database name into which the data will be ingested.</param>
        /// <param name="logger">Logger</param>
        /// <returns>An exception of type <see cref="KustoClientException"/> if the command fails to execute, else returns the ingestion result as <see cref="KustoIngestionResult"/>.</returns>
        public Task<KustoIngestionResult> IngestFromDataTable(CancellationToken ct, Guid aggregationRunIdGuid, string callerName, string tableNameToIngestInto, DataTable dataTableToIngestFrom, string extentTag, int timeoutSeconds = 600, int ingestionStatusPolingIntervalSeconds = 5, string database = "", ICustomAggregationLogger logger = null);

        /// <summary>
        /// <para>Results of <paramref name="queryToIngestFrom"/> are ingested into table <paramref name="tableNameToIngestInto"/>. The table to ingest into should be in <paramref name="database"/>.<br/>
        /// Runs a .set-or-append async command and does not wait for the ingestion to complete. The ingestion may/may not be sucessful. Use this for fire and forget scenarios.</para>
        /// To wait for the ingestion result, use <see cref="IngestFromQuery"/>.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="aggregationRunIdGuid">Unique identifier to represent current aggregation operation.</param>
        /// <param name="callerName">Name of the calling function.</param>
        /// <param name="tableNameToIngestInto">Table name into which the data will be ingested.</param>
        /// <param name="queryToIngestFrom">Results returned by this query is ingested into the specified table.</param>
        /// <param name="extentTag">A string used to tag the current data being ingested. Data with the same tag is not re-inserted to avoid data duplication. Tags can later be used to identify this extent/chunk of data for cleanup.</param>
        /// <param name="database">Database name into which the data will be ingested.</param>
        /// <param name="logger">Logger</param>
        /// <returns>An exception of type <see cref="KustoClientException"/> if the command fails to execute, else returns the operationId for the ingestion command. Operation Id can later be used to poll for the status of ingestion via <see cref="PollForIngestionResult"/>.<br/>
        /// The final ingestion may/may not succeed.</returns>
        public Task<Guid> IngestFromQueryWithoutWaiting(CancellationToken ct, Guid aggregationRunIdGuid, string callerName, string tableNameToIngestInto, string queryToIngestFrom, string extentTag, string database = "", ICustomAggregationLogger logger = null);

        /// <summary>
        /// <para>Results of <paramref name="queryToIngestFrom"/> are ingested into table <paramref name="tableNameToIngestInto"/>. The table to ingest into should be in <paramref name="database"/>.<br/>
        /// Runs a .set-or-append async command. Uses the operation id returned to poll for and return the final ingestion stauts. Use this operation id to confirm if an ingestion completed/failed and take appropriate actions for cleanup.</para>
        /// For fire and forget type of scenarios, use <see cref="IngestFromQueryWithoutWaiting"/>.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="aggregationRunIdGuid">Unique identifier to represent current aggregation operation.</param>
        /// <param name="callerName">Name of the calling function.</param>
        /// <param name="tableNameToIngestInto">Table name into which the data will be ingested.</param>
        /// <param name="queryToIngestFrom">Results returned by this query is ingested into the specified table.</param>
        /// <param name="extentTag">A string used to tag the current data being ingested. Data with the same tag is not re-inserted to avoid data duplication. Tags can later be used to identify this extent/chunk of data for cleanup.</param>
        /// <param name="timeoutSeconds">Timeout duration for the ingestion.</param>
        /// <param name="ingestionStatusPolingIntervalSeconds">Time interval to wait between each poll to the cluster while checking for ingestion completion. Poling interval gradually increases automatically.</param>
        /// <param name="database">Database name into which the data is being ingested.</param>
        /// <param name="logger">Logger</param>
        /// <returns>An exception of type <see cref="KustoClientException"/> if the command fails to execute, else returns the ingestion result as <see cref="KustoIngestionResult"/>.
        /// </returns>
        public Task<KustoIngestionResult> IngestFromQuery(CancellationToken ct, Guid aggregationRunIdGuid, string callerName, string tableNameToIngestInto, string queryToIngestFrom, string extentTag, int timeoutSeconds = 600, int ingestionStatusPolingIntervalSeconds = 5, string database = "", ICustomAggregationLogger logger = null);

        /// <summary>
        /// Polls the cluster to check for ingestion outcome using the operation id obtained when ingestion was initiated.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="aggregationRunIdGuid">Unique identifier to represent current aggregation operation.</param>
        /// <param name="callerName">Name of the calling function.</param>
        /// <param name="ingestionCommandOperationId">OperationId Guid returned by ingestion operation</param>
        /// <param name="timeoutSeconds">Timeout duration for the entire polling operation.</param>
        /// <param name="ingestionStatusPolingIntervalSeconds">Time interval to wait between each poll to the cluster while checking for ingestion completion. Poling interval gradually increases automatically.</param>
        /// <param name="database">Database name into which the data is being ingested.</param>
        /// <param name="logger">Logger</param>
        /// <returns>An object of type <see cref="KustoIngestionResult"/> if the ingestion enters a definitive state. If the ingestion is taking longer than the timeout period specified, throws a <see cref="TimeoutException"/>. If an error was encountered while retrieving the status of ingestion, throws a <see cref="KustoClientException"/>.</returns>
        public Task<KustoIngestionResult> PollForIngestionResult(CancellationToken ct, Guid aggregationRunIdGuid, string callerName, Guid ingestionCommandOperationId, int timeoutSeconds = 600, int ingestionStatusPolingIntervalSeconds = 5, string database = "", ICustomAggregationLogger logger = null);
    }

    public enum KustoIngestionResultStates
    {
        NotStarted,
        InProgress,
        Completed,
        PartiallySucceeded,
        Throttled,
        Failed,
        /// <summary>
        /// Check the string State field for raw state value.
        /// </summary>
        Other
    }

    public class KustoIngestionResult : OperationsShowCommandResult
    {
        public bool IsIngestionSuccessful
        {
            get => this.IngestionState == KustoIngestionResultStates.Completed;
        }

        /// <summary>
        /// Enum describing one of the possible states in which ingestion concluded. Takes one of the values from <see cref="KustoIngestionResultStates"/>
        /// </summary>
        public KustoIngestionResultStates IngestionState;

        /// <summary>
        /// String representation of <see cref="IngestionState"/>.
        /// </summary>
        public new string State { get => base.State; set => base.State = value; }



    }
}
