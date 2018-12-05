using System;
using Microsoft.Azure.Cosmos;

namespace Cosmonaut.Exceptions
{
    public class CosmosCollectionThroughputUpdateException : Exception
    {
        public CosmosCollectionThroughputUpdateException(CosmosContainer collection) : base($"Failed to update hroughput of collection {collection.Id}")
        {

        }
    }
}