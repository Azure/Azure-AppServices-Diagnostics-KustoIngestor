using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kusto.Data.Ingestor
{
    public class HostingStopException : Exception
    {
    }
    public class ApplicationHostService : IHostedService
    {
        readonly ILogger<ApplicationHostService> _logger;
        readonly IConfiguration _configuration;
        readonly IHostingEnvironment _hostingEnvironment;

        public IConfiguration GetConfiguration()
        {
            return _configuration;
        }

        public ApplicationHostService(ILogger<ApplicationHostService> logger, IConfiguration configuration, IHostingEnvironment hostingEnvironment)
        {
            _logger = logger;
            _configuration = configuration;
            _hostingEnvironment = hostingEnvironment;
        }
        async Task IHostedService.StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Application host started");
            await Task.CompletedTask;
            throw new HostingStopException();
        }

        async Task IHostedService.StopAsync(CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            _logger.LogInformation("Application host stopped");
        }
    }
}
