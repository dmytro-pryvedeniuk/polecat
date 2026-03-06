using JasperFx.Events.Tags;
using Polecat.Events.Dcb;

namespace Polecat.Batching;

/// <summary>
///     Batches multiple Load/Query operations into a single database roundtrip
///     using SQL Server's multiple result sets.
/// </summary>
public interface IBatchedQuery
{
    /// <summary>
    ///     The parent query session that created this batch.
    /// </summary>
    IQuerySession Parent { get; }

    Task<T?> Load<T>(Guid id) where T : class;
    Task<T?> Load<T>(string id) where T : class;
    Task<T?> Load<T>(int id) where T : class;
    Task<T?> Load<T>(long id) where T : class;

    Task<IReadOnlyList<T>> LoadMany<T>(params Guid[] ids) where T : class;
    Task<IReadOnlyList<T>> LoadMany<T>(params string[] ids) where T : class;

    IBatchedQueryable<T> Query<T>() where T : class;

    /// <summary>
    ///     Execute a batch query plan (specification pattern).
    /// </summary>
    Task<T> QueryByPlan<T>(IBatchQueryPlan<T> plan);

    /// <summary>
    ///     Fetch events matching a tag query and aggregate them into type T with a DCB consistency boundary.
    ///     At SaveChangesAsync time, will throw DcbConcurrencyException if new matching events were appended.
    /// </summary>
    Task<IEventBoundary<T>> FetchForWritingByTags<T>(EventTagQuery query) where T : class;

    Task Execute(CancellationToken token = default);
}
