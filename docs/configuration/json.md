# JSON Serialization

Polecat uses **System.Text.Json** exclusively for all JSON serialization. Newtonsoft.Json is not supported.

## Default Configuration

Out of the box, Polecat uses camelCase property naming:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");

    // These are the defaults:
    opts.Serializer(new JsonSerializerOptions
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    });
});
```

## Serializer Options

Configure serialization behavior through `StoreOptions`:

### Enum Storage

```cs
// Store enums as integers (default)
opts.UseSystemTextJsonSerializerOptions(o =>
{
    o.Converters.Add(new JsonStringEnumConverter()); // Store as strings instead
});
```

### Casing

```cs
// CamelCase (default) - "firstName"
opts.Serializer(new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
});

// Keep original casing - "FirstName"
opts.Serializer(new JsonSerializerOptions
{
    PropertyNamingPolicy = null
});
```

### Non-Public Members

```cs
// Include non-public properties in serialization
opts.UseSystemTextJsonSerializerOptions(o =>
{
    o.IncludeFields = true;
});
```

## SQL Server 2025 JSON Type

Polecat stores all document bodies and event data using SQL Server 2025's native `json` data type by default. This provides:

- Native JSON validation at the database level
- Efficient JSON path queries via `JSON_VALUE()` and `JSON_QUERY()`
- `JSON_MODIFY()` for partial updates (used by the [patching API](/documents/partial-updates-patching))
- Smaller storage footprint compared to `nvarchar(max)`

::: tip
The `json` type in SQL Server 2025 is analogous to PostgreSQL's `jsonb` type used by Marten, but without the binary storage optimization.
:::

### Falling Back to nvarchar(max)

If you are running against a pre-2025 SQL Server instance that does not support the native `json` data type, disable it with:

```cs
opts.UseNativeJsonType = false;
```

When set to `false`, Polecat uses `nvarchar(max)` for all JSON columns instead. All JSON querying, patching, and projection features continue to work identically with either column type.
