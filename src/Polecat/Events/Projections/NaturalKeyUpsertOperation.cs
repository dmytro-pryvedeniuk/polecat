using System.Data.Common;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Projections;

/// <summary>
///     Storage operation that upserts a natural key → stream id mapping
///     using SQL Server MERGE statement.
/// </summary>
internal class NaturalKeyUpsertOperation : Polecat.Internal.IStorageOperation
{
    private readonly string _tableName;
    private readonly object _naturalKeyValue;
    private readonly object _streamId;
    private readonly bool _isGuidStream;

    public NaturalKeyUpsertOperation(string tableName, object naturalKeyValue, object streamId, bool isGuidStream)
    {
        _tableName = tableName;
        _naturalKeyValue = naturalKeyValue;
        _streamId = streamId;
        _isGuidStream = isGuidStream;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Upsert;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var streamColumn = _isGuidStream ? "stream_id" : "stream_key";

        builder.Append($"""
            MERGE {_tableName} AS target
            USING (VALUES (
            """);
        builder.AppendParameter(_naturalKeyValue);
        builder.Append(", ");
        builder.AppendParameter(_streamId);
        builder.Append($"""
            , 0)) AS source (natural_key_value, {streamColumn}, is_archived)
            ON target.natural_key_value = source.natural_key_value
            WHEN MATCHED THEN UPDATE SET {streamColumn} = source.{streamColumn}, is_archived = 0
            WHEN NOT MATCHED THEN INSERT (natural_key_value, {streamColumn}, is_archived) VALUES (source.natural_key_value, source.{streamColumn}, source.is_archived);
            """);
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
