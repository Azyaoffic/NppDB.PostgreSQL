using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using NppDB.Comm;
using static PostgreSQLParser;

namespace NppDB.PostgreSQL
{
    internal static class PostgreSQLAnalyzerHelper
    {

        public static bool In<T>(this T x, params T[] set)
        {
            return set.Contains(x);
        }

        public static void _AnalyzeForDoubleQuotes(IdentifierContext context, ParsedTreeCommand command)
        {
            if (_AnalyzeForDoubleQuotes(context)) 
            {
                if (context.Parent != null && context.Parent is CollabelContext collabel)
                {
                    if (collabel != null && collabel.Parent is Target_labelContext targetLbl)
                    {
                        if (targetLbl.AS() == null)
                        {
                            command.AddWarning(context, ParserMessageType.DOUBLE_QUOTES);
                        }
                    }
                }
                else if (context.Parent != null && context.Parent is Target_labelContext targetLb)
                {
                    if (targetLb.AS() == null)
                    {
                        command.AddWarning(context, ParserMessageType.DOUBLE_QUOTES);
                    }
                }
                else
                {
                    command.AddWarning(context, ParserMessageType.DOUBLE_QUOTES);
                }
            }
        }

        public static bool _AnalyzeForDoubleQuotes(IdentifierContext context)
        {
            if (context.GetText()[0] == '"' && context.GetText()[context.GetText().Length - 1] == '"')
            {
                return true;
            }
            return false;
        }

        private static int CountTables(IList<Table_refContext> ctxs, int count)
        {
            if (ctxs != null && ctxs.Count > 0)
            {
                foreach (var ctx in ctxs)
                {
                    count++;
                    if (ctx._tables != null && ctx._tables.Count > 0)
                    {
                        count = CountTables(ctx._tables, count);
                    }
                }
            }
            return count;
        }

        public static bool IsHavingClauseUsingGroupByTerm(Having_clauseContext ctx)
        {
            if (ctx.Parent != null && ctx.Parent is Simple_select_pramaryContext pramaryContext)
            {
                Group_clauseContext group_clauseContext = pramaryContext.group_clause();
                if (HasText(group_clauseContext) && HasText(group_clauseContext.group_by_list()))
                {
                    ColumnrefContext columnRef = (ColumnrefContext)FindFirstTargetType(ctx, typeof(ColumnrefContext));
                    if (HasText(columnRef))
                    {
                        Group_by_listContext group_by_listContext = group_clauseContext.group_by_list();
                        HashSet<string> hashSet = GetGroupingTerms(group_by_listContext._grouping_term);
                        if (columnRef.GetText().In(hashSet.ToArray()))
                        {
                            return true;
                        }
                    }

                }
            }
            return false;
        }

        public static HashSet<string> GetGroupingTerms(IList<Group_by_itemContext> group_By_ItemContexts) 
        {
            HashSet<string> groupingTerms = new HashSet<string>();
            if (group_By_ItemContexts == null) return groupingTerms;
            foreach (Group_by_itemContext item in group_By_ItemContexts)
            {
                if (HasText(item))
                {
                    groupingTerms.Add(item.GetText());
                }
            }
            return groupingTerms;
        }

        public static bool IsColumnAliasWithAggregateFunctionUsedInGroupBy(Group_clauseContext ctx, Target_elContext[] columns) 
        {
            if (ctx != null && columns.Length > 0) 
            {
                if (HasText(ctx.group_by_list())) 
                {
                    HashSet<string> groupingTerms = GetGroupingTerms(ctx.group_by_list()._grouping_term);
                    if (groupingTerms.Count > 0)
                    {
                        foreach (Target_elContext targetEl in columns)
                        {
                            if (targetEl is Target_labelContext labelContext)
                            {
                                if (HasText(labelContext.collabel()))
                                {
                                    if (labelContext.collabel().GetText().In(groupingTerms.ToArray()))
                                    {
                                        return true;
                                    }
                                }
                                else if (HasText(labelContext.identifier()))
                                {
                                    if (labelContext.identifier().GetText().In(groupingTerms.ToArray()))
                                    {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return false;
        }

        public static bool AllHaveAggregateFunction(IParseTree[] contexts) 
        {
            foreach (IParseTree context in contexts)
            {
                if (!HasAggregateFunction(context)) return false;
            }
            return true;
        }

        public static bool HasAggregateFunction(IParseTree context)
        {
            return HasSpecificAggregateFunction(context, "sum", "avg", "min", "max", "count");
        }

        public static bool HasSpecificAggregateFunctionAndArgument(IParseTree context, string argument, params string[] functionNames)
        {
            if (context is Func_applicationContext ctx &&
                ctx.func_name().GetText().ToLower().In(functionNames) &&
                (ctx.func_arg_list()?.GetText().ToLower() == argument ||
                ctx.func_arg_expr()?.GetText().ToLower() == argument ||
                (HasText(ctx.STAR()) && argument == "*")))
            {
                return true;
            }

            for (var n = 0; n < context.ChildCount; ++n)
            {
                var child = context.GetChild(n);
                if (!(child is Simple_select_pramaryContext))
                {
                    var result = HasSpecificAggregateFunctionAndArgument(child, argument, functionNames);
                    if (result) return true;
                }
            }
            return false;
        }

        public static bool HasSpecificAggregateFunction(IParseTree context, params string[] functionNames)
        {
            if (context is Select_no_parensContext)
            {
                return false;
            }
            if (context is Func_applicationContext ctx &&
                ctx.func_name().GetText().ToLower().In(functionNames))
            {
                return true;
            }

            for (var n = 0; n < context.ChildCount; ++n)
            {
                var child = context.GetChild(n);
                if (!(child is Simple_select_pramaryContext))
                {
                    var result = HasSpecificAggregateFunction(child, functionNames);
                    if (result) return true;
                }
            }
            return false;
        }

        public static bool HasAndOrExprWithoutParens(IParseTree context)
        {
            A_expr_orContext a_ExprOR = (A_expr_orContext)FindFirstTargetType(context, typeof(A_expr_orContext));

            while (a_ExprOR != null)
            {
                int ORCount = 0;
                int ANDCount = 0;
                ORCount += a_ExprOR.OR().Length;
                foreach (A_expr_andContext aExprAnd in a_ExprOR.a_expr_and())
                {
                    ANDCount += aExprAnd.AND().Length;
                }
                if (ORCount > 0 && ANDCount > 0) 
                {
                    return true;
                }
                a_ExprOR = (A_expr_orContext)FindFirstTargetType(a_ExprOR, typeof(A_expr_orContext));
            }
            return false;
        }

        public static void FindUsedOperands(IParseTree context, IList<IToken> results)
        {
            for (var n = 0; n < context.ChildCount; ++n)
            {
                var child = context.GetChild(n);
                if (DoesFieldExist(child, "_operands")) 
                {
                    object operandsValue = GetFieldValue(child, "_operands");
                    if (operandsValue is IList<IToken> operandsList)
                    {
                        foreach (IToken token in operandsList)
                        {
                            results.Add(token);
                        }
                    }
                }
                FindUsedOperands(child, results);
            }
        }
        public static bool IsLogical(IParseTree context)
        {
            for (var n = 0; n < context.ChildCount; ++n)
            {
                var child = context.GetChild(n);
                IList<dynamic> lhs = FindSimilarNotNullExistingFields(child, "lhs");
                IList<dynamic> rhs = FindSimilarNotNullExistingFields(child, "rhs");
                IList<IToken> operands = FindOperands(child);
                IList<IParseTree> rhsCleaned = new List<IParseTree>();
                foreach (dynamic expr in rhs) 
                {
                    if ((expr is IList && expr.GetType().IsGenericType && expr.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(List<>))))
                    {
                        foreach (var item in expr)
                        {
                            if (item is IParseTree tree && HasText(tree))
                            {
                                C_expr_exprContext value = (C_expr_exprContext)FindFirstTargetType(tree, typeof(C_expr_exprContext));
                                if (value != null && value.ChildCount > 0 && !(value.GetChild(0) is AexprconstContext) && tree.GetText() != value.GetChild(0).GetText())
                                {
                                    rhsCleaned.Add(tree);
                                }
                            }
                        }
                    }
                    else
                    {
                        if (expr is IParseTree tree)
                        {
                            rhsCleaned.Add(tree);
                        }
                    }
                }
                if (rhsCleaned.Count != operands.Count)
                {
                    return false;
                }
                bool result = IsLogical(child);
                if (!result)
                {
                    return false;
                }
            }
            return true;
        }

        private static IList<IToken> FindOperands(IParseTree child)
        {
            IList<IToken> operands = new List<IToken>();
            if (DoesFieldExist(child, "_operands"))
            {
                object operandsValue = GetFieldValue(child, "_operands");
                if (operandsValue is IList<IToken> operandsList)
                {
                    foreach (IToken token in operandsList)
                    {
                        operands.Add(token);
                    }
                }
            }
            if (DoesFieldExist(child, "_operands1"))
            {
                object operandsValue = GetFieldValue(child, "_operands1");
                if (operandsValue is IList<Sub_typeContext> operandsList)
                {
                    foreach (Sub_typeContext sub in operandsList)
                    {
                        if (sub.ChildCount > 0 && sub.GetChild(0)?.Payload is IToken token)
                        {
                            operands.Add(token);
                        }
                    }
                }
            }
            return operands;
        }
        public static object GetFieldValue(dynamic obj, string field)
        {
            FieldInfo fieldInfo = ((Type)obj.GetType()).GetField(field);
            return fieldInfo?.GetValue(obj);
        }

        public static bool DoesFieldExist(dynamic obj, string field)
        {
            FieldInfo[] fieldInfos = ((Type)obj.GetType()).GetFields();
            return fieldInfos.Where(p => p.Name.Equals(field)).Any();
        }

        public static IList<dynamic> FindSimilarNotNullExistingFields(dynamic obj, string field)
        {
            FieldInfo[] fieldInfos = ((Type)obj.GetType()).GetFields();
            return fieldInfos.Where(p => p.Name.Contains(field) && p.GetValue(obj) != null).Select(p => p.GetValue(obj)).ToList();
        }

        public static IParseTree FindParentOfAnyType(IParseTree context, IList<Type> targets)
        {
            var parent = context.Parent;
            if (parent == null)
            {
                return null;
            }
            foreach (Type target in targets)
            {
                if (target.IsAssignableFrom(parent.GetType()))
                {
                    return parent;
                }
            }
            var result = FindParentOfAnyType(parent, targets);
            if (result != null)
            {
                return result;
            }
            return null;
        }

        public static bool IsSelectPramaryContextSelectStar(Simple_select_pramaryContext[] contexts)
        {
            if (contexts != null && contexts.Length > 0)
            {
                Simple_select_pramaryContext ctx = contexts[0];
                if (IsSelectPramaryContextSelectStar(ctx))
                {
                    return true;
                }
            }
            return false;
        }

        public static bool IsSelectPramaryContextSelectStar(Simple_select_pramaryContext context)
        {
            if (context != null && HasSelectStar(context))
            {
                return true;
            }
            return false;
        }

        public static bool HasOuterJoin(Simple_select_pramaryContext context)
        {
            if (context.from_clause()?.from_list()?._tables != null && context.from_clause()?.from_list()?._tables.Count > 0)
            {
                foreach (Table_refContext table in context.from_clause()?.from_list()?._tables)
                {
                    if (table.CROSS().Length > 0)
                    {
                        return true;
                    }
                    foreach (Join_typeContext joinType in table.join_type())
                    {
                        string joinTypeText = joinType.GetText().ToLower();
                        if (joinTypeText.Contains("full")
                            || joinTypeText.Contains("left")
                            || joinTypeText.Contains("right")
                            || joinTypeText.Contains("outer"))
                        {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        public static bool HasSelectStar(Simple_select_pramaryContext ctx)
        {
            if (ctx.opt_target_list()?.target_list()?.target_el() != null
                && ctx?.opt_target_list()?.target_list()?.target_el()?.Length > 0) 
            {
                foreach (var item in ctx?.opt_target_list()?.target_list()?.target_el())
                {
                    if (item is Target_starContext) 
                    {
                        return true;
                    }
                }
            }
            return false;
        }
        
        public static bool HasQualifiedSelectStar(Simple_select_pramaryContext ctx)
        {
            var targets = ctx?.opt_target_list()?.target_list()?.target_el();
            if (targets == null || targets.Length == 0) return false;

            foreach (var t in targets)
            {
                if (t is Target_labelContext lbl && HasText(lbl.a_expr()))
                {
                    var colRefs = new List<IParseTree>();
                    FindAllTargetTypes(lbl.a_expr(), typeof(ColumnrefContext), colRefs);

                    foreach (var tree in colRefs)
                    {
                        var colRef = tree as ColumnrefContext;
                        var indir = colRef?.indirection();
                        if (indir == null) continue;

                        foreach (var el in indir.indirection_el())
                        {
                            if (el?.DOT() != null && el?.STAR() != null)
                            {
                                return true; // matches u.* / schema.table.* etc.
                            }
                        }
                    }
                }
            }
            return false;
        }

        public static IParseTree FindFirstTargetTypeParent(IParseTree context, Type target)
        {
            if (context == null) return null;

            var parent = context.Parent;
            if (target.IsAssignableFrom(parent.GetType()))
            {
                return parent;
            }
            var result = FindFirstTargetTypeParent(parent, target);
            if (result != null)
            {
                return result;
            }
            return null;
        }
        
        public static bool HasRandomOrderingFunction(IParseTree context)
        {
            List<IParseTree> funcs = new List<IParseTree>();
            FindAllTargetTypes(context, typeof(Func_applicationContext), funcs);

            foreach (var item in funcs)
            {
                if (item is Func_applicationContext func && HasText(func.func_name()))
                {
                    var nameText = func.func_name().GetText().ToLower();
                    var parts = nameText.Split('.');
                    var name = parts.Length > 0 ? parts[parts.Length - 1] : nameText;

                    if (name == "random" || name == "rand")
                        return true;
                }
            }
            return false;
        }


        public static IParseTree FindFirstTargetType(IParseTree context, Type target)
        {
            for (var n = 0; n < context.ChildCount; ++n)
            {
                var child = context.GetChild(n);
                if (target.IsAssignableFrom(child.GetType()))
                {
                    return child;
                }
                var result = FindFirstTargetType(child, target);
                if (result != null)
                {
                    return result;
                }
            }
            return null;
        }

        public static void FindAllTargetTypes(IParseTree context, Type target, IList<IParseTree> results)
        {
            for (var n = 0; n < context.ChildCount; ++n)
            {
                var child = context.GetChild(n);
                if (target.IsAssignableFrom(child.GetType()))
                {
                    results.Add(child);
                }
                FindAllTargetTypes(child, target, results);
            }
        }

        public static bool HasAExprConst(Sortby_listContext ctx)
        {
            foreach (SortbyContext sortBy in ctx.sortby())
            {
                var result = FindFirstTargetType(sortBy, typeof(AexprconstContext));
                if (result != null)
                {
                    return true;
                }
            }
            return false;
        }

        public static bool HasGroupByClause(Simple_select_pramaryContext ctx)
        {
            return ctx.group_clause() != null && !string.IsNullOrEmpty(ctx.group_clause().GetText());
        }

        public static bool HasAllErrorNodes(IParseTree ctx)
        {
            if (!HasText(ctx)) return false;
            for (int i = 0; i < ctx.ChildCount; i++)
            {
                if (!(ctx.GetChild(i) is ErrorNodeImpl)) 
                {
                    return false;
                }
            }
            return true;
        }

        public static bool WhereClauseIdentifiersMatchTableIdentifier(Simple_select_pramaryContext ctx)
        {
            Where_clauseContext where_clauseContext = ctx.where_clause();

            IList<IParseTree> whereColumnRefs = new List<IParseTree>();
            FindAllTargetTypes(where_clauseContext, typeof(ColumnrefContext), whereColumnRefs);

            Non_ansi_joinContext non_ansi_joinContext = ctx.from_clause().from_list().non_ansi_join();
            IList<IParseTree> nonAnsiJoinTableRefs = new List<IParseTree>();
            FindAllTargetTypes(non_ansi_joinContext, typeof(Table_refContext), nonAnsiJoinTableRefs);

            HashSet<String> whereColumnRef_identifiers = new HashSet<string>();
            foreach (IParseTree whereColumnRef in whereColumnRefs) 
            {
                if (whereColumnRef is ColumnrefContext columnref && HasText(columnref)) 
                {
                    whereColumnRef_identifiers.Add(columnref.colid().GetText().ToLower());
                }
            }
            HashSet<String> nonAnsiJoinTableRef_identifiers = new HashSet<string>();
            foreach (IParseTree nonAnsiJoinTableRef in nonAnsiJoinTableRefs)
            {
                if (nonAnsiJoinTableRef is Table_refContext tableRef && HasText(tableRef))
                {
                    if (HasText(tableRef.opt_alias_clause()) && !tableRef.opt_alias_clause().GetText().ToLower().In(whereColumnRef_identifiers.ToArray()))
                    {
                        return false;
                    }

                    if (!HasText(tableRef.opt_alias_clause()) && !tableRef.relation_expr().GetText().ToLower().In(whereColumnRef_identifiers.ToArray()))
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        public static bool HasNonAnsiJoin(Simple_select_pramaryContext ctx)
        {
            return HasText(ctx.from_clause()) && HasText(ctx.from_clause().from_list()) && HasText(ctx.from_clause().from_list().non_ansi_join());
        }

        public static bool HasDistinctClause(Simple_select_pramaryContext ctx)
        {
            return ctx.distinct_clause() != null && !string.IsNullOrEmpty(ctx.distinct_clause().GetText());
        }

        public static bool HasText(IParseTree ctx)
        {
            return ctx != null && !string.IsNullOrEmpty(ctx.GetText());
        }

        public static int CountTablesInFromClause(Simple_select_pramaryContext ctx)
        {
            if (ctx.from_clause()?.from_list()?._tables != null) 
            {
                return CountTables(ctx.from_clause().from_list()._tables, 0);
            }
            return 0;
        }

        public static Target_elContext[] GetColumns(Simple_select_pramaryContext ctx)
        {
            if (ctx?.opt_target_list()?.target_list()?.target_el() != null) 
            {
                return ctx.opt_target_list().target_list().target_el();
            }
            if (ctx?.target_list()?.target_el() != null) 
            {
                return ctx.target_list().target_el();
            }
            return new Target_elContext[0];
        }

        public static int CountGroupingTerms(Simple_select_pramaryContext ctx)
        {
            if (ctx.group_clause()?.group_by_list()?._grouping_term != null)
            {
                return ctx.group_clause().group_by_list()._grouping_term.Count;
            }
            return 0;
        }

        public static int CountInsertColumns(InsertstmtContext ctx)
        {
            if (ctx?.insert_rest()?.insert_column_list()?._insert_columns != null)
            {
                return ctx.insert_rest().insert_column_list()._insert_columns.Count;
            }
            return 0;
        }

        public static bool HasDuplicateColumns(Target_elContext[] columns) 
        {
            List<String> columnNames = new List<String>();
            foreach (Target_elContext column in columns) 
            {
                if (column.ChildCount <= 1) 
                {
                    columnNames.Add(column.GetText());
                    continue;
                }
                columnNames.Add(column.GetChild(column.ChildCount - 1).GetText());
            }
            HashSet<String> columnNamesSet = new HashSet<String>(columnNames);
            return columnNames.Count != columnNamesSet.Count;
        }

        public static bool HasEqualityWithTextPattern(ParserRuleContext ctx)
        {
            if (HasText(ctx) && (
                    ctx.GetText().Contains("%") ||
                    ctx.GetText().Contains("_") ||
                    ctx.GetText().Contains("|") ||
                    ctx.GetText().Contains("*") ||
                    ctx.GetText().Contains("+") ||
                    ctx.GetText().Contains("?") ||
                    ctx.GetText().Contains("{") ||
                    ctx.GetText().Contains("}") ||
                    ctx.GetText().Contains("(") ||
                    ctx.GetText().Contains(")") ||
                    ctx.GetText().Contains("[") ||
                    ctx.GetText().Contains("]") ||
                    ctx.GetText().Contains("*")
            ))
            {
                C_expr_exprContext value = (C_expr_exprContext)FindFirstTargetType(ctx, typeof(C_expr_exprContext));
                if (HasText(value) && value.ChildCount > 0 && value.GetChild(0) is AexprconstContext)
                {
                    return true;
                }
            }
            return false;
        }

        public static int CountHavings(IParseTree context, int count)
        {
            if (string.Equals(context.GetText(), "having", StringComparison.OrdinalIgnoreCase))
            {
                return count + 1;
            }
            for (var n = 0; n < context.ChildCount; ++n)
            {
                var child = context.GetChild(n);
                count = CountHavings(child, count);
            }
            return count;
        }

        public static int CountWheres(IParseTree context, int count)
        {
            if (string.Equals(context.GetText(), "where", StringComparison.OrdinalIgnoreCase))
            {
                return count + 1;
            }
            for (var n = 0; n < context.ChildCount; ++n)
            {
                var child = context.GetChild(n);
                count = CountWheres(child, count);
            }
            return count;
        }

        public static int CountWhereClauses(IParseTree context)
        {
            IList<IParseTree> whereClauses = new List<IParseTree>();
            FindAllTargetTypes(context, typeof(Where_clauseContext), whereClauses);
            return whereClauses.Count;
        }

        public static bool ColumnHasAlias(Target_elContext column) 
        {
            return column != null && column.ChildCount > 1;
        }

        public static bool HasMissingColumnAlias(Target_elContext[] columns)
        {
            foreach (Target_elContext column in columns) 
            {
                if (!ColumnHasAlias(column)) 
                {
                    C_expr_exprContext value = (C_expr_exprContext) FindFirstTargetType(column, typeof(C_expr_exprContext));
                    if (value != null && value.ChildCount > 0)
                    {
                        if (value.GetChild(0) is AexprconstContext || value.GetChild(0) is Func_exprContext)
                        {
                            return true;
                        }
                    }
                }
                
            }
            return false;
        }

        public static Simple_select_pramaryContext FindSelectPramaryContext(IParseTree context) 
        {
            if (context != null)
            {
                if (context is Simple_select_pramaryContext ctx)
                {
                    return ctx;
                }
                for (var n = 0; n < context.ChildCount; ++n)
                {
                    var child = context.GetChild(n);
                    var result = FindSelectPramaryContext(child);
                    if (result is Simple_select_pramaryContext) 
                    {
                        return result;
                    }
                }
            }
            return null;
        }

        public static bool HasSubqueryColumnMismatch(IParseTree context, Simple_select_pramaryContext subQuery) 
        {
            Target_elContext[] subQueryColumns = GetColumns(subQuery);
            int subQueryColumnCount = subQueryColumns.Count();
            IList<IParseTree> c_expr_Contexts = new List<IParseTree>();
            FindAllTargetTypes(context, typeof(C_exprContext), c_expr_Contexts);
            c_expr_Contexts = c_expr_Contexts.Where(c_expr_ctx =>
                ((C_exprContext)c_expr_ctx).Start.Type != OPEN_PAREN
                || ((C_exprContext)c_expr_ctx).Stop.Type != CLOSE_PAREN)
            .ToList();
            return c_expr_Contexts.Count != subQueryColumnCount;
        }

        public static bool IsValueNegative(String text) 
        {
            if (float.TryParse(text, out float floatOut) && floatOut < 0) 
            {
                return true;
            }
            if (double.TryParse(text, out double doubleOut) && doubleOut < 0) 
            {
                return true;
            }
            if (int.TryParse(text, out int intOut) && intOut < 0) 
            {
                return true;
            }
            return false;
        }

        public static bool HasWhereClauseWithMultipleAllowed(IParseTree context, IParseTree subquery)
        {
            Simple_select_pramaryContext value = (Simple_select_pramaryContext)FindFirstTargetType(context, typeof(Simple_select_pramaryContext));
            if (HasText(value))
            {
                A_expr_inContext inContext = (A_expr_inContext)FindFirstTargetType(context, typeof(A_expr_inContext));
                if (HasText(inContext) && inContext.IN_P() != null)
                {
                    return true;
                }
            }

            IParseTree exprParent = FindParentOfAnyType(subquery, new List<Type> { typeof(A_expr_compareContext) });
            if (HasText(exprParent) && exprParent is A_expr_compareContext compareContext && compareContext.sub_type() != null)
            {
                return true;
            }
            return false;
        }

        public static bool HasWhereClauseWithMultipleAllowed(IParseTree context)
        {
            Simple_select_pramaryContext value = (Simple_select_pramaryContext)FindFirstTargetType(context, typeof(Simple_select_pramaryContext));
            if (HasText(value))
            {
                A_expr_inContext inContext = (A_expr_inContext)FindFirstTargetType(context, typeof(A_expr_inContext));
                if (HasText(inContext) && inContext.IN_P() != null) 
                {
                    return true;
                }
            }

            IParseTree exprParent = FindParentOfAnyType(context, new List<Type> { typeof(A_expr_compareContext) });
            if (HasText(exprParent) && exprParent is A_expr_compareContext compareContext && compareContext.sub_type() != null) 
            {
                return true;
            }
            return false;
        }
        public static ParserRuleContext FindLimitClause(Select_no_parensContext ctx)
        {
            if (HasText(ctx.opt_select_limit()))
            {
                return ctx.opt_select_limit().select_limit().limit_clause();
            }
            if (HasText(ctx.select_limit()))
            {
                return ctx.select_limit().limit_clause();
            }
            return null;
        }

        public static Limit_clauseContext ShouldAddFetchMultipleRowsWarning(Select_no_parensContext subQuery) 
        {
            ParserRuleContext optlimit_Clause = FindLimitClause(subQuery);
            if (HasText(optlimit_Clause) && optlimit_Clause is Limit_clauseContext limitClause)
            {
                if (limitClause.WITH() != null
                        || (HasText(limitClause.select_fetch_first_value()) && limitClause.select_fetch_first_value().GetText() != "1")
                        || (HasText(limitClause.select_limit_value()) && limitClause.select_limit_value().GetText() != "1")
                    )
                {
                    return limitClause;
                }
            }
            return null;
        }

    }
}
