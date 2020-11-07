using Kusto.Data.Ingestor.DataProviders.Kusto;
using Microsoft.Extensions.Logging;


namespace Kusto.Data.Ingestor.DataProviders
{
    public class DefaultDataProviders
    {
        private IKustoClient _kustoClient;
        public DefaultDataProviders(ILogger<DefaultDataProviders> logger, IKustoClient kustoClient)
        {
            _kustoClient = kustoClient;
        }

        public IKustoClient Kusto
        {
            get { return _kustoClient; }
        }
    }
}
