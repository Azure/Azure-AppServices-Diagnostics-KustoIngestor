using Kusto.Data.Ingestor.DataProviders.Kusto;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kusto.Data.Ingestor.DataProviders
{
    public class DataSourcesConfiguration
    {
        public KustoDataProviderConfiguration KustoConfiguration { get; set; }
    }
}
