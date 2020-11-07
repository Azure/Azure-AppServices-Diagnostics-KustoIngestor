using System;
using System.Collections.Generic;
using System.Text;

namespace Kusto.Data.Ingestor
{
    public enum HostingRegions
    {
        CentralUS,
        EastAsia,
        EastUS,
        NorthEurope,
        Unknown,
        WestEurope,
        WestUS
    }
    public interface ICustomLogAggregatorConfig
    {
        public HostingRegions HostingRegion { get; }
        public string LeaderKustoCluster { get; }
        public string FollowerKustoCluster { get; }
        public string DefaultKustoDatabase { get; }
    }

    public class CustomLogAggregatorConfig : ICustomLogAggregatorConfig
    {
        public CustomLogAggregatorConfig(string hostingRegion, string leaderKustoCluster, string followerKustoCluster, string defaultKustoDatabase)
        {
            string emptyParamNameList = string.Empty;
            emptyParamNameList += string.IsNullOrWhiteSpace(hostingRegion) ? " HostingRegion" : string.Empty;
            emptyParamNameList += string.IsNullOrWhiteSpace(leaderKustoCluster) ? " LeaderKustoCluster" : string.Empty;
            emptyParamNameList += string.IsNullOrWhiteSpace(followerKustoCluster) ? " FollowerKustoCluster" : string.Empty;
            emptyParamNameList += string.IsNullOrWhiteSpace(defaultKustoDatabase) ? " DefaultKustoDatabase" : string.Empty;
            if (!string.IsNullOrWhiteSpace(emptyParamNameList))
            {
                throw new ArgumentNullException($"Failed to initialize LogAggregatorConfig. Empty parameter supplied : {emptyParamNameList}");
            }

            if(!Enum.TryParse<HostingRegions>(hostingRegion, true, out this._hostingRegion))
            {
                this._hostingRegion = HostingRegions.Unknown;
            }

            this._leaderKustoCluster = leaderKustoCluster;
            this._followerKustoCluster = followerKustoCluster;
            this._defaultKustoDatabase = defaultKustoDatabase;
        }
        private readonly HostingRegions _hostingRegion;
        public HostingRegions HostingRegion
        {
            get => _hostingRegion;
        }

        private readonly string _leaderKustoCluster;
        public string LeaderKustoCluster
        {
            get => _leaderKustoCluster;
        }

        private readonly string _followerKustoCluster;
        public string FollowerKustoCluster
        {
            get => _followerKustoCluster;
        }

        private readonly string _defaultKustoDatabase;
        public string DefaultKustoDatabase
        {
            get => _defaultKustoDatabase;
        }
    }
}
