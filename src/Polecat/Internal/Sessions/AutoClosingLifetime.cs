using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Polecat.Internal.Sessions;

/// <summary>
///     Opens a fresh connection per call and closes it when done.
///     For readers, uses CommandBehavior.CloseConnection so the connection
///     is closed when the reader is disposed.
///     Default lifetime for query sessions.
/// </summary>
internal class AutoClosingLifetime : IConnectionLifetime
{
    private readonly ConnectionFactory _connectionFactory;

    public AutoClosingLifetime(ConnectionFactory connectionFactory, int commandTimeout)
    {
        _connectionFactory = connectionFactory;
        CommandTimeout = commandTimeout;
    }

    public int CommandTimeout { get; }

    public async Task<int> ExecuteAsync(SqlCommand command, CancellationToken token)
    {
        await using var conn = _connectionFactory.Create();
        await conn.OpenAsync(token);
        command.Connection = conn;
        command.CommandTimeout = CommandTimeout;
        return await command.ExecuteNonQueryAsync(token);
    }

    public async Task<object?> ExecuteScalarAsync(SqlCommand command, CancellationToken token)
    {
        await using var conn = _connectionFactory.Create();
        await conn.OpenAsync(token);
        command.Connection = conn;
        command.CommandTimeout = CommandTimeout;
        return await command.ExecuteScalarAsync(token);
    }

    public async Task<DbDataReader> ExecuteReaderAsync(SqlCommand command, CancellationToken token)
    {
        var conn = _connectionFactory.Create();
        try
        {
            await conn.OpenAsync(token);
            command.Connection = conn;
            command.CommandTimeout = CommandTimeout;
            return await command.ExecuteReaderAsync(CommandBehavior.CloseConnection, token);
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    public async Task<DbDataReader> ExecuteReaderAsync(SqlBatch batch, CancellationToken token)
    {
        var conn = _connectionFactory.Create();
        try
        {
            await conn.OpenAsync(token);
            batch.Connection = conn;
            batch.Timeout = CommandTimeout;
            return await batch.ExecuteReaderAsync(CommandBehavior.CloseConnection, token);
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    public void Dispose()
    {
        // No persistent connection to dispose
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
