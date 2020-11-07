using Kusto.Data.Ingestor.Common.LogHelper;
using Kusto.Data.Ingestor.DataProviders;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kusto.Data.Ingestor
{
    /** 
     * <summary>
     * <para>Implement this interface for your log aggregator to be invoked. Your class must have the default constructor. </para>
     * <para>If your logic is not re-entrant, decorate the class implementing this interface with <seealso cref="CustomLogAggregatorOptionsAttribute"/></para>
     * </summary>
     * <remarks>
     * <para>As a pre-requisite, run the following command to grant this app permission to ingest data in the target table <br/>
     *   .add table TABLE_NAME ingestors ('aadapp=9979f1cc-4fa5-4626-b9d5-f9173713c4f0;72f988bf-86f1-41af-91ab-2d7cd011db47') 'AppLensLogAggregator'</para>
     * </remarks>
     */

    public interface ICustomLogAggregator
    {
        /// <summary>
        /// Name of the aggregator
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Author name
        /// </summary>
        public string Author { get; }

        /// <summary>
        /// A short description of what this aggregator is supposed to do.
        /// </summary>
        public string Description { get; }

        /// <summary>
        /// Time duration after which the aggregation logic will be trigerred again.
        /// </summary>
        /// <param name="aggregationRunIdGuid">Unique GUID identifying the current invocation.</param>
        /// <param name="logger">Logger <see cref="ICustomAggregationLogger"/></param>
        /// <param name="config">Configuration to be used by the aggregator</param>
        /// <param name="previousInvocationDateTimeUtc">Timestamp when the aggregation logic was last invoked.</param>
        /// <returns></returns>
        public TimeSpan GetNextInvocationInterval(Guid aggregationRunIdGuid, ICustomAggregationLogger logger, ICustomLogAggregatorConfig config, DateTime previousInvocationDateTimeUtc);

        /// <summary>
        /// Implement logic to perform aggregation in this method. The method is allowed to execute for 80% of the time from its current invocation to the next scheduled invocation or up to 1 hour, whichever is lesser.
        /// If the method is not reentrant safe, the maximum time allotted for execution is one hour before a cancellation request is raised.
        /// </summary>
        /// <param name="aggregationRunIdGuid">Unique GUID identifying the current invocation</param>
        /// <param name="logger">Logger</param>
        /// <param name="config">Configuration to be used by the aggregator</param>
        /// <param name="dp">Data providers supported</param>
        /// <param name="cancellationToken">Cancellation token in case the aggregation runs for a long time. Implement cancellation at safe points within the logic to avoid data inconsistancy</param>
        /// <returns></returns>
        public Task<AggregationResults> AggregationLogic(Guid aggregationRunIdGuid, ICustomAggregationLogger logger, ICustomLogAggregatorConfig config, DefaultDataProviders dp, CancellationToken cancellationToken);
    }

    public enum AggregationResults
    {
        Cancelled,
        Completed,
        Failed,
        Fault,
        PartiallySucceeded,        
        TimedOut,        
        Transient,
        Other
    }
}
