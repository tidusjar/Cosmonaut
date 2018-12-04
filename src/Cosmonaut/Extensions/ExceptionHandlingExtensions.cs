using System.Net;
using Cosmonaut.Response;

namespace Cosmonaut.Extensions
{
    public static class ExceptionHandlingExtensions
    {
        internal static CosmonautResponse<TEntity> DocumentClientExceptionToCosmosResponse<TEntity>(DocumentClientException exception, TEntity entity) where TEntity : class
        {
            switch (exception.StatusCode)
            {
                case HttpStatusCode.NotFound:
                    return new CosmonautResponse<TEntity>(entity, exception, CosmosOperationStatus.ResourceNotFound);
                case (HttpStatusCode) CosmosConstants.TooManyRequestsStatusCode:
                    return new CosmonautResponse<TEntity>(entity, exception, CosmosOperationStatus.RequestRateIsLarge);
                case HttpStatusCode.PreconditionFailed:
                    return new CosmonautResponse<TEntity>(entity, exception, CosmosOperationStatus.PreconditionFailed);
                case HttpStatusCode.Conflict:
                    return new CosmonautResponse<TEntity>(entity, exception, CosmosOperationStatus.Conflict);
            }

            throw exception;
        }

        internal static CosmonautResponse<TEntity> ToCosmosResponse<TEntity>(this DocumentClientException exception) where TEntity : class
        {
            return ToCosmosResponse<TEntity>(exception, null);
        }

        internal static CosmonautResponse<TEntity> ToCosmosResponse<TEntity>(this DocumentClientException exception, TEntity entity) where TEntity : class
        {
            return DocumentClientExceptionToCosmosResponse(exception, entity);
        }
    }
}