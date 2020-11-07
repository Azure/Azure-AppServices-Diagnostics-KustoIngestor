using System;
using System.Collections.Generic;
using System.Text;

namespace Kusto.Data.Ingestor.DataProviders
{
    public interface IDataProviderConfiguration
    {
        bool Enabled { get; set; }
        Dictionary<string, string> HealthCheckInputs { get; }
        void PostInitialize();
        void Validate();
    }
}
