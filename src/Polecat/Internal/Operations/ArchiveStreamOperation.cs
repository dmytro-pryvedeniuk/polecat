using System.Data.Common;
using Polecat.Events;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

internal class ArchiveStreamOperation : IStorageOperation
{
    private readonly EventGraph _events;
    private readonly object _streamId;
    private readonly string _tenantId;

    public ArchiveStreamOperation(EventGraph events, object streamId, string tenantId)
    {
        _events = events;
        _streamId = streamId;
        _tenantId = tenantId;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append($"""
            UPDATE {_events.StreamsTableName} SET is_archived = 1
            WHERE id = @id AND tenant_id = @tenant_id;
            UPDATE {_events.EventsTableName} SET is_archived = 1
            WHERE stream_id = @id AND tenant_id = @tenant_id;
            """);
        builder.AddParameters(new Dictionary<string, object?> { ["id"] = _streamId, ["tenant_id"] = _tenantId });
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
