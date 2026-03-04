using System.Data.Common;
using Polecat.Events;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

/// <summary>
///     Updates the snapshot and snapshot_version columns on pc_streams
///     for a given stream. Used by snapshot projections to cache aggregate state.
/// </summary>
internal class UpdateSnapshotOperation : IStorageOperation
{
    private readonly EventGraph _events;
    private readonly object _streamId;
    private readonly string _tenantId;
    private readonly string _snapshotJson;
    private readonly long _snapshotVersion;

    public UpdateSnapshotOperation(EventGraph events, object streamId, string tenantId,
        string snapshotJson, long snapshotVersion)
    {
        _events = events;
        _streamId = streamId;
        _tenantId = tenantId;
        _snapshotJson = snapshotJson;
        _snapshotVersion = snapshotVersion;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append($"""
            UPDATE {_events.StreamsTableName}
            SET snapshot = @snapshot, snapshot_version = @snapshot_version
            WHERE id = @id AND tenant_id = @tenant_id;
            """);
        builder.AddParameters(new Dictionary<string, object?>
        {
            ["snapshot"] = _snapshotJson, ["snapshot_version"] = _snapshotVersion,
            ["id"] = _streamId, ["tenant_id"] = _tenantId
        });
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
