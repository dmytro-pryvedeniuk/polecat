using System.Linq.Expressions;

namespace Polecat.Linq.Joins;

/// <summary>
///     Data holder parsed from GroupJoin + SelectMany expression arguments.
/// </summary>
internal class GroupJoinData
{
    public Expression InnerSourceExpression { get; set; } = null!;
    public LambdaExpression OuterKeySelector { get; set; } = null!;
    public LambdaExpression InnerKeySelector { get; set; } = null!;
    public LambdaExpression GroupJoinResultSelector { get; set; } = null!;
    public Type OuterElementType { get; set; } = null!;
    public Type InnerElementType { get; set; } = null!;
    public bool IsLeftJoin { get; set; }
    public LambdaExpression? SelectManyResultSelector { get; set; }
    public LambdaExpression? SelectManyCollectionSelector { get; set; }

    /// <summary>
    ///     OrderBy expressions captured after SelectMany, stored as (lambda, descending).
    ///     These reference the projected anonymous type and must be resolved
    ///     by mapping through the result selector.
    /// </summary>
    public List<(LambdaExpression Expression, bool Descending)> OrderByExpressions { get; } = [];
}
