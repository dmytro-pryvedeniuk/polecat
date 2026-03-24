using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using JasperFx.Events.Projections.Composite;
using JasperFx.Events.Subscriptions;
using Polecat.Events;
using Polecat.Events.Projections;
using Polecat.Projections.Flattened;
using Polecat.Subscriptions;

namespace Polecat.Projections;

/// <summary>
///     Projection registration and configuration for Polecat.
///     Extends ProjectionGraph to integrate with the JasperFx async daemon framework.
/// </summary>
public class PolecatProjectionOptions
    : ProjectionGraph<IProjection, IDocumentSession, IQuerySession>
{
    private readonly EventGraph _events;
    private StoreOptions? _storeOptions;
    private IInlineProjection<IDocumentSession>[]? _inlineProjections;

    internal PolecatProjectionOptions(EventGraph events) : base(events, "polecat")
    {
        _events = events;
    }

    internal void SetStoreOptions(StoreOptions options) => _storeOptions = options;

    /// <summary>
    ///     Opt into a performance optimization that directs Polecat to use a session-level
    ///     identity map for aggregates fetched via FetchForWriting() or FetchLatest().
    ///     See <see cref="EventGraph.UseIdentityMapForAggregates"/> for details.
    /// </summary>
    public bool UseIdentityMapForAggregates
    {
        get => _events.UseIdentityMapForAggregates;
        set => _events.UseIdentityMapForAggregates = value;
    }

    protected override void onAddProjection(object projection)
    {
        if (projection is ProjectionBase pb)
        {
            foreach (var eventType in pb.IncludedEventTypes)
            {
                _events.AddEventType(eventType);
            }
        }

        if (projection is FlatTableProjection flatTable)
        {
            flatTable.Compile(_events);
        }
    }

    /// <summary>
    ///     Register a self-aggregating type for snapshot projection with explicit identity type.
    ///     The aggregate type must have Apply/Create methods matching event types.
    /// </summary>
    public void Snapshot<T, TId>(SnapshotLifecycle lifecycle)
        where T : notnull, new()
        where TId : notnull
    {
        var projection = new SingleStreamProjection<T, TId>();
        var mapped = lifecycle.Map();
        projection.Lifecycle = mapped;
        projection.AssembleAndAssertValidity();

        foreach (var eventType in projection.IncludedEventTypes)
        {
            _events.AddEventType(eventType);
        }

        All.Add((IProjectionSource<IDocumentSession, IQuerySession>)projection);
    }

    /// <summary>
    ///     Register a self-aggregating type for snapshot projection.
    ///     Uses Guid as the default stream identity type.
    ///     The aggregate type must have Apply/Create methods matching event types.
    /// </summary>
    public void Snapshot<T>(SnapshotLifecycle lifecycle)
        where T : notnull, new()
    {
        Snapshot<T, Guid>(lifecycle);
    }

    /// <summary>
    ///     Register a composite projection that orchestrates multiple projections
    ///     in ordered stages. Stages run sequentially; projections within a stage
    ///     run in parallel. Composite projections always run asynchronously.
    /// </summary>
    public void CompositeProjectionFor(string name, Action<PolecatCompositeProjection> configure)
    {
        var composite = new PolecatCompositeProjection(name, _storeOptions!);
        configure(composite);
        composite.AssembleAndAssertValidity();

        // Register event types from all child projections
        foreach (var child in composite.AllProjections())
        {
            if (child is ProjectionBase pb)
            {
                foreach (var eventType in pb.IncludedEventTypes)
                {
                    _events.AddEventType(eventType);
                }
            }
        }

        All.Add((IProjectionSource<IDocumentSession, IQuerySession>)composite);
    }

    /// <summary>
    ///     Register a subscription for push-based event processing.
    /// </summary>
    public void Subscribe(Subscriptions.ISubscription subscription, Action<ISubscriptionOptions>? configure = null)
    {
        var source = subscription as ISubscriptionSource<IDocumentSession, IQuerySession>
            ?? new SubscriptionWrapper(subscription);

        if (source is ISubscriptionOptions options)
            configure?.Invoke(options);

        registerSubscription(source);
    }

    /// <summary>
    ///     Register a subscription by type. The subscription must have a parameterless constructor.
    /// </summary>
    public void Subscribe<T>(Action<ISubscriptionOptions>? configure = null)
        where T : Subscriptions.ISubscription, new()
    {
        Subscribe(new T(), configure);
    }

    /// <summary>
    /// Find a registered natural key definition for the given aggregate type, if any.
    /// </summary>
    public NaturalKeyDefinition? FindNaturalKeyDefinition(Type aggregateType)
    {
        if (TryFindAggregate(aggregateType, out var projection))
        {
            return projection.NaturalKeyDefinition;
        }

        return null;
    }

    /// <summary>
    ///     Build the inline projection instances. Called once at DocumentStore construction.
    /// </summary>
    internal IInlineProjection<IDocumentSession>[] BuildInlineProjections()
    {
        if (_inlineProjections != null) return _inlineProjections;

        // Ensure any FlatTableProjections are compiled
        foreach (var source in All)
        {
            if (source is FlatTableProjection flatTable)
            {
                flatTable.Compile(_events);
            }
        }

        var inlineList = All
            .Where(x => x.Lifecycle == ProjectionLifecycle.Inline)
            .Select(x => x.BuildForInline())
            .ToList();

        // Add natural key inline projections for any aggregate projections with natural keys
        foreach (var source in All)
        {
            if (source is IAggregateProjection { NaturalKeyDefinition: not null } aggProjection)
            {
                inlineList.Add(new NaturalKeyProjection(aggProjection.NaturalKeyDefinition, _events));
            }
        }

        _inlineProjections = inlineList.ToArray();

        return _inlineProjections;
    }
}

/// <summary>
///     Lifecycle for snapshot projections, mirroring Marten's SnapshotLifecycle.
/// </summary>
public enum SnapshotLifecycle
{
    Inline,
    Async
}

public static class SnapshotLifecycleExtensions
{
    public static ProjectionLifecycle Map(this SnapshotLifecycle lifecycle) => lifecycle switch
    {
        SnapshotLifecycle.Inline => ProjectionLifecycle.Inline,
        SnapshotLifecycle.Async => ProjectionLifecycle.Async,
        _ => throw new ArgumentOutOfRangeException(nameof(lifecycle))
    };
}
