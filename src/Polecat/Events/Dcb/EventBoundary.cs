using JasperFx.Events;
using Polecat.Internal;

namespace Polecat.Events.Dcb;

internal class EventBoundary<T> : IEventBoundary<T> where T : class
{
    private readonly DocumentSessionBase _session;
    private readonly EventGraph _events;

    public EventBoundary(DocumentSessionBase session, EventGraph events, T? aggregate,
        IReadOnlyList<IEvent> loadedEvents, long lastSeenSequence)
    {
        _session = session;
        _events = events;
        Aggregate = aggregate;
        Events = loadedEvents;
        LastSeenSequence = lastSeenSequence;
    }

    public T? Aggregate { get; }
    public long LastSeenSequence { get; }
    public IReadOnlyList<IEvent> Events { get; }

    public void AppendOne(object @event)
    {
        var wrapped = _events.BuildEvent(@event);
        RouteEventByTags(wrapped);
    }

    public void AppendMany(params object[] events)
    {
        foreach (var e in events) AppendOne(e);
    }

    public void AppendMany(IEnumerable<object> events)
    {
        foreach (var e in events) AppendOne(e);
    }

    private void RouteEventByTags(IEvent wrapped)
    {
        var tags = wrapped.Tags;
        if (tags == null || tags.Count == 0)
        {
            throw new InvalidOperationException(
                "Events appended via IEventBoundary must have tags set via WithTag(). " +
                "Polecat uses tags to route events to the appropriate stream(s).");
        }

        foreach (var tag in tags)
        {
            var registration = _events.FindTagType(tag.TagType);
            if (registration == null) continue;

            var aggregateType = registration.AggregateType;
            if (aggregateType == null) continue;

            var streamId = tag.Value;
            StreamAction? stream = null;

            if (streamId is Guid guidId)
            {
                if (!_session.WorkTracker.TryFindStream(guidId, out stream))
                {
                    stream = StreamAction.Start(_events, guidId, Array.Empty<IEvent>());
                    stream.AggregateType = aggregateType;
                    _session.WorkTracker.AddStream(stream);
                }
            }
            else if (streamId is string stringId)
            {
                if (!_session.WorkTracker.TryFindStream(stringId, out stream))
                {
                    stream = StreamAction.Start(_events, stringId, Array.Empty<IEvent>());
                    stream.AggregateType = aggregateType;
                    _session.WorkTracker.AddStream(stream);
                }
            }

            if (stream != null)
            {
                if (stream.Id != Guid.Empty)
                {
                    wrapped.StreamId = stream.Id;
                }
                else if (stream.Key != null)
                {
                    wrapped.StreamKey = stream.Key;
                }

                stream.AddEvent(wrapped);
            }
        }
    }
}
