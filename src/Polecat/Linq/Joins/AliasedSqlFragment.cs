using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Linq.Joins;

/// <summary>
///     Wraps an ISqlFragment and applies table alias replacement when generating SQL.
/// </summary>
internal class AliasedSqlFragment : ISqlFragment
{
    private readonly ISqlFragment _inner;
    private readonly string _alias;

    public AliasedSqlFragment(ISqlFragment inner, string alias)
    {
        _inner = inner;
        _alias = alias;
    }

    public void Apply(ICommandBuilder builder)
    {
        var aliasing = new AliasingCommandBuilder(builder, _alias);
        _inner.Apply(aliasing);
    }
}
