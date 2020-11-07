using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;
using System.Text.RegularExpressions;

namespace Kusto.Data.Ingestor.DataProviders.Kusto
{
    [DataSourceConfiguration("Kusto")]
    public class KustoDataProviderConfiguration : IDataProviderConfiguration
    {
        /// <summary>
        /// Client Id
        /// </summary>
        [ConfigurationName("ClientId")]
        [Required]
        public string ClientId { get; set; }

        /// <summary>
        /// App Key
        /// </summary>
        [ConfigurationName("AppKey")]
        [Required]
        public string AppKey { get; set; }

        /// <summary>
        /// Tenant to authenticate with
        /// </summary>
        [ConfigurationName("AADAuthority")]
        [Required]
        public string AADAuthority { get; set; }

        [ConfigurationName("AADTenantId")]
        [Required]
        public string AADTenantId { get; set; }

        /// <summary>
        /// Resource to issue token
        /// </summary>
        [ConfigurationName("AADKustoResource")]
        [Required]
        public string AADKustoResource { get; set; }

        private bool _debugLogsEnabled = false;

        /// <summary>
        /// When set to true, detailed informational logs are emitted. 
        /// </summary>
        [ConfigurationName("DebugLogsEnabled")]
        public bool DebugLogsEnabled
        {
            get { return _debugLogsEnabled; }
            set { _debugLogsEnabled = Convert.ToBoolean(value) == true; }
        }

        public string KustoCluster
        {
            get
            {
                if(!string.IsNullOrWhiteSpace(AADKustoResource))
                {
                    var m = Regex.Match(AADKustoResource, @"https://(?<cluster>\w+).");
                    if (m.Success)
                    {
                        return m.Groups["cluster"].Value;
                    }
                    else
                    {
                        throw new ArgumentException(nameof(AADKustoResource) + " not correctly formatted.");
                    }
                }
                else
                {
                    throw new Exception($"Empty AADKustoResource. Check if appSettings.json file is present in {Environment.CurrentDirectory} and has a setting named AADKustoResource of the form https://<KustoCluster>follower.kusto.windows.net.");
                }
            }
        }

        public string KustoApiEndpoint
        {
            get
            {
                return AADKustoResource.Replace(KustoCluster, "{cluster}");
            }
        }


        private bool _enabled = true;
        bool IDataProviderConfiguration.Enabled
        {
            get { return true; }
            set { _enabled = value; }
        }        

        Dictionary<string, string> IDataProviderConfiguration.HealthCheckInputs => new Dictionary<string, string>();

        void IDataProviderConfiguration.PostInitialize()
        {
            ;
        }

        void IDataProviderConfiguration.Validate()
        {
        }
    }
}
