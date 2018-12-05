using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos;

namespace Cosmonaut.Response
{
    public class CosmosMultipleResponse<TEntity> where TEntity : class
    {
        public bool IsSuccess => !FailedEntities.Any();

        public List<CosmosItemResponse<TEntity>> FailedEntities { get; } = new List<CosmosItemResponse<TEntity>>();

        public List<CosmosItemResponse<TEntity>> SuccessfulEntities { get; } = new List<CosmosItemResponse<TEntity>>();

        internal void AddResponse(CosmosItemResponse<TEntity> response)
        {
            if (response == null)
                return;

            if (((int)response.StatusCode >= 200) && ((int)response.StatusCode <= 299))
            {
                SuccessfulEntities.Add(response);
                return;
            }

            FailedEntities.Add(response);
        }
    }
}