using System.Linq;

namespace Cosmonaut.Unit
{
    public interface IFakeDocumentQuery<T> : IDocumentQuery<T>, IOrderedQueryable<T>
    {

    }
}