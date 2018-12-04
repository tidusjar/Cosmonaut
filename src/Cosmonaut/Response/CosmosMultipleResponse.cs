using System.Collections.Generic;
using System.Linq;

namespace Cosmonaut.Response
{
    public class CosmosMultipleResponse<TEntity> where TEntity : class
    {
        public bool IsSuccess => !FailedEntities.Any();

        public List<CosmonautResponse<TEntity>> FailedEntities { get; } = new List<CosmonautResponse<TEntity>>();

        public List<CosmonautResponse<TEntity>> SuccessfulEntities { get; } = new List<CosmonautResponse<TEntity>>();

        internal void AddResponse(CosmonautResponse<TEntity> response)
        {
            if (response == null)
                return;

            if (response.IsSuccess)
            {
                SuccessfulEntities.Add(response);
                return;
            }

            FailedEntities.Add(response);
        }
    }
}