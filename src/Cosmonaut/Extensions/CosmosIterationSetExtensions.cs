using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Cosmonaut.Diagnostics;

namespace Cosmonaut.Extensions
{
    public static class CosmosIterationSetExtensions
    {
        public static async Task<List<TEntity>> ToListAsync<TEntity>(
            this CosmosResultSetIterator<TEntity> setIterator,
            CancellationToken cancellationToken = default)
        {
            var results = new List<TEntity>();
            while (setIterator.HasMoreResults)
            {
                var items = await setIterator.InvokeExecuteNextAsync(() => setIterator.FetchNextSetAsync(cancellationToken),
                    setIterator.ToString(), target: "v3sdk"/*GetAltLocationFromQueryable(queryable)*/);
                results.AddRange(items);
            }
            return results;
        }
    }
}