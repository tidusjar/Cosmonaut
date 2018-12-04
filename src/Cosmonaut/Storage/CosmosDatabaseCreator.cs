using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

namespace Cosmonaut.Storage
{
    internal class CosmosDatabaseCreator : IDatabaseCreator
    {
        private readonly CosmosClient _cosmosClient;

        public CosmosDatabaseCreator(CosmosClient cosmosClient)
        {
            _cosmosClient = cosmosClient;
        }

        public async Task<bool> EnsureCreatedAsync(string databaseId)
        {
            var response = await _cosmosClient.Databases.CreateDatabaseIfNotExistsAsync(databaseId);

            return true;//response.StatusCode == HttpStatusCode.OK;
        }
    }
}