using System.Data.Common;
using JasperFx.Events.Projections;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Daemon.Progress;

/// <summary>
///     Inserts an initial projection progression row into pc_event_progression.
/// </summary>
internal class InsertProjectionProgress : Polecat.Internal.IStorageOperation
{
    private readonly EventGraph _events;
    private readonly EventRange _range;

    public InsertProjectionProgress(EventGraph events, EventRange range)
    {
        _events = events;
        _range = range;
    }

    public Type DocumentType => typeof(ShardState);
    public OperationRole Role() => OperationRole.Insert;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append($"""
            INSERT INTO {_events.ProgressionTableName} (name, last_seq_id, last_updated)
            VALUES (@name, @seq, SYSDATETIMEOFFSET());
            """);

        builder.AddParameters(new Dictionary<string, object?>
        {
            ["name"] = _range.ShardName.Identity,
            ["seq"] = _range.SequenceCeiling
        });
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
