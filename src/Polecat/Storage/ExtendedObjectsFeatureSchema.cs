using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer;

namespace Polecat.Storage;

/// <summary>
///     Wraps additional schema objects (e.g., EF Core entity tables) so they
///     participate in Weasel schema migration alongside Polecat's own tables.
/// </summary>
internal class ExtendedObjectsFeatureSchema : FeatureSchemaBase
{
    private readonly IReadOnlyList<ISchemaObject> _objects;

    public ExtendedObjectsFeatureSchema(IReadOnlyList<ISchemaObject> objects)
        : base("ExtendedObjects", new SqlServerMigrator())
    {
        _objects = objects;
    }

    public override Type StorageType => typeof(ExtendedObjectsFeatureSchema);

    protected override IEnumerable<ISchemaObject> schemaObjects()
    {
        return _objects;
    }
}
