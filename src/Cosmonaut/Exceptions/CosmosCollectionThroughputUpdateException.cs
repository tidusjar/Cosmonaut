using System;

namespace Cosmonaut.Exceptions
{
    public class CosmosCollectionThroughputUpdateException : Exception
    {
        public CosmosCollectionThroughputUpdateException(DocumentCollection collection) : base($"Failed to update hroughput of collection {collection.Id}")
        {

        }
    }
}