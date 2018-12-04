using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

namespace Cosmonaut.Storage
{
    public interface ICollectionCreator
    {
        Task<bool> EnsureCreatedAsync<TEntity>(string databaseId, string collectionId, int collectionThroughput, IndexingPolicy indexingPolicy = null) where TEntity : class;
    }
}