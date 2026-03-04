using System.Data.Common;
using Polecat.Serialization;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Reads two data columns (outer and inner) and applies a compiled projection function.
///     For LEFT JOIN, the inner column may be NULL.
/// </summary>
internal class JoinListHandler<TOuter, TInner, TResult> : IQueryHandler<IReadOnlyList<TResult>>
    where TOuter : class
    where TInner : class
{
    private readonly ISerializer _serializer;
    private readonly Func<TOuter, TInner?, TResult> _projection;
    private readonly bool _isLeftJoin;

    public JoinListHandler(ISerializer serializer, Func<TOuter, TInner?, TResult> projection, bool isLeftJoin)
    {
        _serializer = serializer;
        _projection = projection;
        _isLeftJoin = isLeftJoin;
    }

    public async Task<IReadOnlyList<TResult>> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        var list = new List<TResult>();
        while (await reader.ReadAsync(token))
        {
            var outerJson = reader.GetString(0);
            var outer = _serializer.FromJson<TOuter>(outerJson);

            TInner? inner = null;
            if (!reader.IsDBNull(1))
            {
                var innerJson = reader.GetString(1);
                inner = _serializer.FromJson<TInner>(innerJson);
            }

            list.Add(_projection(outer, inner));
        }

        return list;
    }
}
