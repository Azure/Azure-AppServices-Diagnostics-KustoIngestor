using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kusto.Data.Ingestor.DataProviders
{
    public interface IDataSourcesConfigurationService
    {
        DataSourcesConfiguration Config { get; }
    }

    public class DataSourcesConfigurationService : IDataSourcesConfigurationService
    {
        private DataSourcesConfiguration _config;

        public DataSourcesConfiguration Config => _config;

        public DataSourcesConfigurationService(IHostingEnvironment env, IConfiguration config)
        {
            IConfigurationFactory factory = GetDataProviderConfigurationFactory(env, config);
            _config = factory.LoadConfigurations();
        }

        public static IConfigurationFactory GetDataProviderConfigurationFactory(IHostingEnvironment env, IConfiguration config)
        {
            return new AppSettingsDataProviderConfigurationFactory(env, config);
        }
    }
}
