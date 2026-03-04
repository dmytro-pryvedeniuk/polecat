using System.Data.Common;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal;

public interface IStorageOperation : Weasel.Core.IStorageOperation
{
    object? DocumentId => null;
    void ConfigureCommand(ICommandBuilder builder);
}
