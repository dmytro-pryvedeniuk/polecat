using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Polecat.Internal.Sessions;

/// <summary>
///     Abstracts SQL Server connection lifetime management.
///     All SQL execution flows through this interface.
/// </summary>
internal interface IConnectionLifetime : IAsyncDisposable, IDisposable
{
    int CommandTimeout { get; }
    Task<int> ExecuteAsync(SqlCommand command, CancellationToken token);
    Task<object?> ExecuteScalarAsync(SqlCommand command, CancellationToken token);
    Task<DbDataReader> ExecuteReaderAsync(SqlCommand command, CancellationToken token);
    Task<DbDataReader> ExecuteReaderAsync(SqlBatch batch, CancellationToken token);
}

/// <summary>
///     A connection lifetime that maintains a persistent connection with optional transaction.
///     Used by document sessions that need SaveChangesAsync transactional semantics.
/// </summary>
internal interface IAlwaysConnectedLifetime : IConnectionLifetime
{
    SqlConnection Connection { get; }
    SqlTransaction? Transaction { get; set; }
    ValueTask BeginTransactionAsync(CancellationToken token);
    ValueTask BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken token);
}
