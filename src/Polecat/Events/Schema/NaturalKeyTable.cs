using JasperFx.Events;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     Weasel table definition for pc_natural_key_{aggregatetype} — stores natural key
///     to stream id mappings for domain-meaningful lookups.
/// </summary>
internal class NaturalKeyTable : Table
{
    public NaturalKeyTable(EventGraph events, NaturalKeyDefinition naturalKey)
        : base(new SqlServerObjectName(events.DatabaseSchemaName,
            $"pc_natural_key_{naturalKey.AggregateType.Name.ToLowerInvariant()}"))
    {
        var columnType = naturalKey.InnerType == typeof(int) ? "int"
            : naturalKey.InnerType == typeof(long) ? "bigint"
            : "nvarchar(200)";

        AddColumn("natural_key_value", columnType).AsPrimaryKey().NotNull();

        if (events.StreamIdentity == StreamIdentity.AsGuid)
        {
            AddColumn("stream_id", "uniqueidentifier").NotNull();
        }
        else
        {
            AddColumn("stream_key", "varchar(250)").NotNull();
        }

        if (events.TenancyStyle == TenancyStyle.Conjoined)
        {
            AddColumn("tenant_id", "varchar(250)").NotNull();
        }

        AddColumn("is_archived", "bit").NotNull().DefaultValue(0);
    }
}
