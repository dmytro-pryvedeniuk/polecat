# EF Core Projections

Polecat integrates with Entity Framework Core, allowing you to use DbContext within your event projections. This is provided by the `Polecat.EntityFrameworkCore` package.

## Installation

```shell
dotnet add package Polecat.EntityFrameworkCore
```

## Single Stream Projection with EF Core

```cs
public class OrderDbContext : DbContext
{
    public DbSet<OrderReadModel> Orders { get; set; } = null!;

    public OrderDbContext(DbContextOptions<OrderDbContext> options)
        : base(options) { }
}

public class OrderEfProjection : EfCoreSingleStreamProjection<OrderReadModel, OrderDbContext>
{
    public OrderReadModel Create(OrderCreated e, OrderDbContext db)
    {
        var model = new OrderReadModel
        {
            Status = "Created",
            Amount = e.Amount
        };
        db.Orders.Add(model);
        return model;
    }

    public void Apply(OrderShipped e, OrderReadModel current, OrderDbContext db)
    {
        current.Status = "Shipped";
        current.ShippedDate = e.ShippedAt;
    }
}
```

## Multi Stream Projection with EF Core

```cs
public class CustomerEfProjection : EfCoreMultiStreamProjection<CustomerDashboard, Guid, AppDbContext>
{
    public CustomerEfProjection()
    {
        Identity<OrderCreated>(e => e.CustomerId);
    }

    public CustomerDashboard Create(OrderCreated e, AppDbContext db)
    {
        return new CustomerDashboard { TotalOrders = 1, TotalSpent = e.Amount };
    }

    public void Apply(OrderCreated e, CustomerDashboard current, AppDbContext db)
    {
        current.TotalOrders++;
        current.TotalSpent += e.Amount;
    }
}
```

## Event Projection with EF Core

```cs
public class AuditEfProjection : EfCoreEventProjection
{
    public void Project(IEvent<OrderCreated> @event, AppDbContext db)
    {
        db.AuditEntries.Add(new AuditEntry
        {
            Action = "OrderCreated",
            Timestamp = @event.Timestamp
        });
    }
}
```

## Schema Migration with AddEntityTablesFromDbContext

By default, EF Core entity tables must be created separately (e.g., via `dotnet ef database update` or `Database.GenerateCreateScript()`). If you want Polecat's Weasel migration pipeline to manage your EF Core entity tables alongside Polecat's own schema objects, use `AddEntityTablesFromDbContext`:

```cs
var store = DocumentStore.For(opts =>
{
    opts.ConnectionString = connectionString;
    opts.AddEntityTablesFromDbContext<OrderDbContext>();
});
```

This reads the entity model from your `DbContext` and registers each table with Polecat's Weasel migration system. When `ApplyAllConfiguredChangesToDatabaseAsync()` runs, EF Core entity tables will be created and migrated automatically.

### Schema Handling

`AddEntityTablesFromDbContext` respects explicit schema configuration in your EF Core model:

- **Entities with an explicit schema** (via `HasDefaultSchema()` or `ToTable("name", "schema")`) will retain their configured schema
- **Entities without an explicit schema** will use the SQL Server default schema (`dbo`)

```cs
// This DbContext places entities in a custom schema
public class MyDbContext : DbContext
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("my_schema");
        modelBuilder.Entity<Order>().ToTable("orders");
    }
}

var store = DocumentStore.For(opts =>
{
    opts.ConnectionString = connectionString;
    opts.DatabaseSchemaName = "polecat_schema";

    // The "orders" table will be created in "my_schema", NOT "polecat_schema"
    opts.AddEntityTablesFromDbContext<MyDbContext>();
});
```

### Combining with Projections

You can use `AddEntityTablesFromDbContext` alongside EF Core projections for a fully integrated setup:

```cs
var store = DocumentStore.For(opts =>
{
    opts.ConnectionString = connectionString;
    opts.Projections.Add<OrderEfProjection, Order, OrderDbContext>(
        opts, new OrderEfProjection(), ProjectionLifecycle.Inline);

    // Also register the entity tables for Weasel migration
    opts.AddEntityTablesFromDbContext<OrderDbContext>();
});

// This single call now creates both Polecat event tables and EF Core entity tables
await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
```

## How It Works

1. Polecat creates a placeholder `SqlConnection` and `DbContext` for each projection batch
2. During `SaveChangesAsync`, the placeholder connection is swapped for the real connection and transaction
3. `DbContext.SaveChangesAsync()` is called as a `ITransactionParticipant` within Polecat's transaction
4. Both Polecat document operations and EF Core changes commit atomically

## Registration

```cs
opts.Projections.Add<OrderEfProjection>(ProjectionLifecycle.Inline);
// or
opts.Projections.Add<OrderEfProjection>(ProjectionLifecycle.Async);
```

## Lifecycle Support

EF Core projections support all lifecycle modes:

| Mode | Description |
| :--- | :--- |
| Inline | DbContext changes commit in same transaction as events |
| Async | Daemon processes events and applies DbContext changes |
| Live | Aggregate built on demand with DbContext |

## Tenanted Projections

EF Core projections work with multi-tenancy -- the DbContext receives the correct connection for the tenant's database.

::: tip
The `ITransactionParticipant` pattern ensures that EF Core's `SaveChanges` runs within Polecat's transaction boundary, providing atomic consistency between event-sourced projections and EF Core-managed tables.
:::
