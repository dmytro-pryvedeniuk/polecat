using Polly;

namespace Polecat.Resilience;

/// <summary>
///     Default Polly resilience pipeline transient error handling.
/// </summary>
internal static class PolecatResilienceDefaults
{
    public static ResiliencePipelineBuilder AddPolecatDefaults(this ResiliencePipelineBuilder builder)
    {
        return builder;
    }
}
