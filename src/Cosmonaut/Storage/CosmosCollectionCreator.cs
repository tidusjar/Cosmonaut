using System;
using System.Net;
using System.Threading.Tasks;
using Cosmonaut.Extensions;
using Microsoft.Azure.Cosmos;

namespace Cosmonaut.Storage
{
    internal class CosmosCollectionCreator : ICollectionCreator
    {
        private readonly CosmosClient _cosmosClient;
        
        public CosmosCollectionCreator(CosmosClient cosmosClient)
        {
            _cosmosClient = cosmosClient;
        }

        public async Task<bool> EnsureCreatedAsync<TEntity>(
            string databaseId,
            string collectionId,
            int collectionThroughput,
            IndexingPolicy indexingPolicy = null) where TEntity : class
        {
            var response = await _cosmosClient.Databases[databaseId].Containers
                .CreateContainerIfNotExistsAsync(new CosmosContainerSettings
                {
                    Id = collectionId,
                    IndexingPolicy = indexingPolicy ?? CosmosConstants.DefaultIndexingPolicy,
                    PartitionKey = typeof(TEntity).GetPartitionKeyDefinitionForEntity()
                }, collectionThroughput);

            return true;//response.StatusCode == HttpStatusCode.OK
        }
    }
}