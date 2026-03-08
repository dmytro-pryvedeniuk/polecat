using System.Data.Common;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Projections;

/// <summary>
///     Storage operation that marks a natural key mapping as archived
///     when the corresponding stream is archived.
/// </summary>
internal class NaturalKeyArchiveOperation : Polecat.Internal.IStorageOperation
{
    private readonly string _tableName;
    private readonly object _streamId;
    private readonly bool _isGuidStream;

    public NaturalKeyArchiveOperation(string tableName, object streamId, bool isGuidStream)
    {
        _tableName = tableName;
        _streamId = streamId;
        _isGuidStream = isGuidStream;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var streamColumn = _isGuidStream ? "stream_id" : "stream_key";

        builder.Append($"UPDATE {_tableName} SET is_archived = 1 WHERE {streamColumn} = ");
        builder.AppendParameter(_streamId);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
