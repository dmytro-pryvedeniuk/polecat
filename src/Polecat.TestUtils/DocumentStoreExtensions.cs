using Microsoft.Data.SqlClient;

namespace Polecat.TestUtils;

public static class DocumentStoreExtensions
{
    extension(DocumentStore Store)
    {
        public async Task WaitForProjectionAsync()
        {
            const int maxRetries = 3;
            using var daemon = await Store.BuildProjectionDaemonAsync();

            // Retry on transient SQL Server connection errors from daemon internals
            for (var attempt = 0; attempt < maxRetries; attempt++)
            {
                try
                {
                    await daemon.StartAllAsync();
                    await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
                    return;
                }
                catch (AggregateException ex) when (IsTransientErrorFromDeamonInternals(ex))
                {
                    SqlConnection.ClearAllPools();
                    if (attempt < maxRetries - 1)
                        await Task.Delay(200);
                    else
                        throw;
                }
            }
        }

        public async Task WaitForConditionAsync(Func<Task<bool>> predicate, TimeSpan timeout)
        {
            if (await predicate())
                return;

            using var daemon = await Store.BuildProjectionDaemonAsync();
            var cts = new CancellationTokenSource(timeout);

            while (!await predicate())
            {
                await daemon.StartAllAsync();
                await Task.Delay(500, cts.Token);
            }
        }

        private static bool IsTransientErrorFromDeamonInternals(AggregateException ex)
        {
            return ex.Flatten().InnerExceptions.Any(ex =>
                ex is SqlException || 
                ex is OperationCanceledException ||
                ex is InvalidOperationException && ex.Message.Contains("Operation cancelled by user")
            );
        }
    }
}
