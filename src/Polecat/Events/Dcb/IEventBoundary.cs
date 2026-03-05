using JasperFx.Events;

namespace Polecat.Events.Dcb;

/// <summary>
///     A writable handle for Dynamic Consistency Boundary operations.
///     Unlike IEventStream which is tied to a single stream, IEventBoundary
///     works across streams using tag-based consistency.
/// </summary>
public interface IEventBoundary<out T> where T : class
{
    /// <summary>
    ///     The current aggregate state built from events matching the tag query.
    /// </summary>
    T? Aggregate { get; }

    /// <summary>
    ///     The highest sequence number seen when the boundary was established.
    /// </summary>
    long LastSeenSequence { get; }

    /// <summary>
    ///     The events loaded by the tag query.
    /// </summary>
    IReadOnlyList<IEvent> Events { get; }

    /// <summary>
    ///     Append a single event. The event must have tags set via WithTag().
    /// </summary>
    void AppendOne(object @event);

    /// <summary>
    ///     Append multiple events. Each event must have tags set via WithTag().
    /// </summary>
    void AppendMany(params object[] events);

    /// <summary>
    ///     Append multiple events. Each event must have tags set via WithTag().
    /// </summary>
    void AppendMany(IEnumerable<object> events);
}
