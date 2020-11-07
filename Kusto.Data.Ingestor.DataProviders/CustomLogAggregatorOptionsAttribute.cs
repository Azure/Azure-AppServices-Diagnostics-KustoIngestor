using System;
using System.Collections.Generic;
using System.Text;

namespace Kusto.Data.Ingestor.DataProviders
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false,Inherited =true)]
    public class CustomLogAggregatorOptionsAttribute:Attribute
    {
        /// <summary>
        /// Use this if your aggregation logic is not re-entrant safe and needs for the current aggregation to complete/fail before it is trigerred again. Defaults to true
        /// </summary>
        public bool ReEntrantSafe { get; set; } = true;
    }

    public interface ICustomLogAggregatorOptions
    {
        public bool ReEntrantSafe { get; }
    }

    public class CustomLogAggregatorOptions : ICustomLogAggregatorOptions
    {
        /// <summary>
        /// Specifies if the aggregation logic is re-entrant safe. False indicates for the current aggregation to complete/fail before it is trigerred again. Defaults to true.
        /// </summary>
        public bool ReEntrantSafe { get; set; } = true;
    }
}
