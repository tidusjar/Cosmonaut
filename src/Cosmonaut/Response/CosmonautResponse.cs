using System;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace Cosmonaut.Response
{
    public class CosmonautResponse<TEntity> where TEntity : class
    {
        public bool IsSuccess => CosmosOperationStatus == CosmosOperationStatus.Success;

        public CosmosOperationStatus CosmosOperationStatus { get; } = CosmosOperationStatus.Success;

        public CosmosResponse<TEntity> CosmosResponse { get; }

        public TEntity Entity { get; }

        public Exception Exception { get; }

        public CosmonautResponse(CosmosResponse<TEntity> cosmosResponse)
        {
            CosmosResponse = cosmosResponse;
        }

        public CosmonautResponse(TEntity entity, CosmosResponse<TEntity> cosmosResponse)
        {
            CosmosResponse = cosmosResponse;
            Entity = entity;
        }

        public CosmonautResponse(TEntity entity, Exception exception, CosmosOperationStatus statusType)
        {
            CosmosOperationStatus = statusType;
            Entity = entity;
            Exception = exception;
        }

        public static implicit operator TEntity(CosmonautResponse<TEntity> response)
        {
            if (response?.Entity != null)
                return response.Entity;

            if (!string.IsNullOrEmpty(response?.CosmosResponse?.Resource?.ToString()))
                return JsonConvert.DeserializeObject<TEntity>(response.CosmosResponse.Resource.ToString());

            return null;
        }
    }
}