using Microsoft.Data.SqlClient;
using Polly;
using Polly.Retry;

namespace Polecat.Resilience;

/// <summary>
///     Default Polly resilience pipeline for SQL Server transient error handling.
/// </summary>
internal static class PolecatResilienceDefaults
{
    public static ResiliencePipelineBuilder AddPolecatDefaults(this ResiliencePipelineBuilder builder)
    {
        return builder.AddRetry(new RetryStrategyOptions
        {
            ShouldHandle = new PredicateBuilder()
                .Handle<SqlException>(ex => IsTransient(ex))
                .Handle<TimeoutException>(),
            MaxRetryAttempts = 3,
            Delay = TimeSpan.FromMilliseconds(50),
            BackoffType = DelayBackoffType.Exponential
        });
    }

    /// <summary>
    ///     SQL Server transient error numbers:
    ///     1205  = deadlock victim
    ///     -2    = timeout
    ///     20    = transport-level error
    ///     64    = login failed (transient)
    ///     40613 = Azure SQL database not available
    ///     40197 = Azure SQL service error
    ///     40501 = Azure SQL service busy
    ///     49918/49919/49920 = Azure SQL elastic pool capacity
    /// </summary>
    private static bool IsTransient(SqlException ex) =>
        ex.Number is 1205 or -2 or 20 or 64 or 40613 or 40197 or 40501 or 49918 or 49919 or 49920
        || ex.IsTransient;
}
