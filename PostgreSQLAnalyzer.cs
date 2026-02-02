using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using NppDB.Comm;
using static NppDB.PostgreSQL.PostgreSQLAnalyzerHelper;
using static PostgreSQLParser;

namespace NppDB.PostgreSQL
{
    public static class PostgreSQLAnalyzer
    {
        public static int CollectCommands(
            this RuleContext context,
            CaretPosition caretPosition,
            string tokenSeparator,
            int commandSeparatorTokenType,
            out IList<ParsedTreeCommand> commands)
        {
            commands = new List<ParsedTreeCommand>();
            commands.Add(new ParsedTreeCommand
            {
                StartOffset = -1,
                StopOffset = -1,
                StartLine = -1,
                StopLine = -1,
                StartColumn = -1,
                StopColumn = -1,
                Text = "",
                Context = null
            });
            return _CollectCommands(context, caretPosition, tokenSeparator, commandSeparatorTokenType, commands, -1, null, new List<StringBuilder>());
        }

        private static void _AnalyzeRuleContext(RuleContext context, ParsedTreeCommand command)
        {
            switch (context.RuleIndex)
            {
                case RULE_simple_select_pramary:
                    {
                        if (context is Simple_select_pramaryContext ctx)
                        {
                            int tableCount = CountTablesInFromClause(ctx);
                            Target_elContext[] columns = GetColumns(ctx);
                            int columnCount = columns.Count();
                            int groupingTermCount = CountGroupingTerms(ctx);
                            bool hasGroupByClause = HasGroupByClause(ctx);
                            int whereCount = CountWheres(ctx, 0);
                            int whereClauseCount = CountWhereClauses(ctx);
                            bool hasWhereClause = HasText(ctx.where_clause());
                            bool hasFromClause = HasText(ctx.from_clause());
                            bool hasNonAnsiJoin = HasNonAnsiJoin(ctx);
                            int joinCount = CountJoinsInFromClause(ctx);
                            int subqueryCount = CountSubqueriesInSelect(ctx);
                            bool hasAggregates = HasAggregateFunction(ctx);

                            if (IsExcessivelyComplexSelect(joinCount, subqueryCount, columnCount, hasGroupByClause, hasAggregates))
                            {
                                command.AddWarning(ctx, ParserMessageType.COMPLEX_SELECT_STATEMENT);
                            }

                            if (HasMissingColumnAlias(columns))
                            {
                                command.AddWarning(ctx, ParserMessageType.MISSING_COLUMN_ALIAS_IN_SELECT_CLAUSE);
                            }
                            if (hasFromClause)
                            {
                                From_clauseContext from_clauseContext = ctx.from_clause();
                                if (HasText(from_clauseContext.from_list())
                                    && HasAllErrorNodes(from_clauseContext.from_list())
                                    && from_clauseContext.from_list().GetText().ToLower().Contains("join"))
                                {
                                    command.AddWarning(ctx.where_clause(), ParserMessageType.MISSING_EXPRESSION_IN_JOIN_CLAUSE);
                                }
                            }

                            if (hasWhereClause)
                            {
                                if (whereCount > whereClauseCount)
                                {
                                    command.AddWarning(ctx.where_clause(), ParserMessageType.MULTIPLE_WHERE_USED);
                                }
                            }
                            if (hasNonAnsiJoin)
                            {
                                if (!hasWhereClause)
                                {
                                    command.AddWarning(ctx.where_clause(), ParserMessageType.MISSING_EXPRESSION_IN_JOIN_CLAUSE);
                                }
                                else if (hasWhereClause && !WhereClauseIdentifiersMatchTableIdentifier(ctx))
                                {
                                    command.AddWarning(ctx.where_clause(), ParserMessageType.MISSING_EXPRESSION_IN_JOIN_CLAUSE);
                                }
                            }
                            if (HasDuplicateColumns(columns))
                            {
                                command.AddWarning(columns[0], ParserMessageType.DUPLICATE_SELECTED_COLUMN_IN_SELECT_CLAUSE);
                            }
                            if (!hasGroupByClause)
                            {
                                if (columnCount > 1 && HasAggregateFunction(ctx))
                                {
                                    command.AddWarning(ctx, ParserMessageType.AGGREGATE_FUNCTION_WITHOUT_GROUP_BY_CLAUSE);
                                }
                            }
                            else if (hasGroupByClause)
                            {
                                if (columnCount != 0 && columnCount - 1 != groupingTermCount && columnCount != groupingTermCount)
                                {
                                    command.AddWarning(ctx.group_clause(), ParserMessageType.MISSING_COLUMN_IN_GROUP_BY_CLAUSE);
                                }
                                if (IsColumnAliasWithAggregateFunctionUsedInGroupBy(ctx.group_clause(), columns) || HasAggregateFunction(ctx.group_clause()))
                                {
                                    command.AddWarning(ctx.group_clause(), ParserMessageType.AGGREGATE_FUNCTION_IN_GROUP_BY_CLAUSE);
                                }
                            }
                            if (HasOuterJoin(ctx) && HasSpecificAggregateFunctionAndArgument(ctx, "*", "count"))
                            {
                                command.AddWarning(ctx, ParserMessageType.COUNT_FUNCTION_WITH_OUTER_JOIN);
                            }
                            if (HasDistinctClause(ctx) && hasGroupByClause)
                            {
                                command.AddWarning(ctx.distinct_clause(), ParserMessageType.DISTINCT_KEYWORD_WITH_GROUP_BY_CLAUSE);
                            }
                            if (HasSelectStar(ctx) || HasQualifiedSelectStar(ctx))
                            {
                                command.AddWarning(ctx, ParserMessageType.SELECT_ALL_IN_SELECT_CLAUSE);

                                if (HasSelectStar(ctx) && tableCount > 1)
                                {
                                    command.AddWarning(ctx, ParserMessageType.SELECT_ALL_WITH_MULTIPLE_JOINS);
                                }
                            }
                        }
                        break;
                    }
                case RULE_having_clause: 
                    {
                        if (context is Having_clauseContext ctx && HasText(ctx))
                        {
                            if (!HasText(ctx.a_expr()))
                            {
                                command.AddWarning(ctx, ParserMessageType.MISSING_EXPRESSION_IN_HAVING_CLAUSE);
                                break;
                            }
                            A_expr_unary_notContext a_expr_unary_notContext = (A_expr_unary_notContext)FindFirstTargetType(ctx.a_expr(), typeof(A_expr_unary_notContext));
                            if (HasText(ctx.a_expr()) &&
                                HasText(a_expr_unary_notContext) &&
                                a_expr_unary_notContext.ChildCount > 0 &&
                                a_expr_unary_notContext.GetChild(0) is ErrorNodeImpl)
                            {
                                command.AddWarning(ctx.a_expr(), ParserMessageType.MISSING_EXPRESSION_IN_HAVING_CLAUSE);
                                break;
                            }
                            if (!HasAggregateFunction(ctx) && !IsHavingClauseUsingGroupByTerm(ctx))
                            {
                                command.AddWarning(ctx, ParserMessageType.HAVING_CLAUSE_WITHOUT_AGGREGATE_FUNCTION);
                            }
                            int havingCountInHavingClause = CountHavings(ctx, 0);
                            if (havingCountInHavingClause > 1 || (havingCountInHavingClause > 0 && // Sometimes second having part an go into parents' window_clause
                                ctx.Parent is Simple_select_pramaryContext parent && HasText(parent)
                                && HasText(parent.window_clause()) && CountHavings(parent.window_clause(), 0) > 0))
                            {
                                command.AddWarning(ctx, ParserMessageType.MULTIPLE_HAVING_USED);
                            }
                            if (HasAndOrExprWithoutParens(ctx))
                            {
                                command.AddWarning(ctx, ParserMessageType.AND_OR_MISSING_PARENTHESES_IN_HAVING_CLAUSE);
                            }
                            if (!IsLogical(ctx))
                            {
                                command.AddWarning(ctx, ParserMessageType.NOT_LOGICAL_OPERAND);
                            }
                        }
                        break;
                    }
                case RULE_from_list:
                    {
                        if (context is From_listContext ctx && HasText(ctx))
                        {
                            if (ctx._tables != null && ctx._tables.Count > 0) 
                            {
                                foreach (Table_refContext table in ctx._tables)
                                {
                                    if (HasText(table.select_with_parens()) && !HasText(table.opt_alias_clause()))
                                    {
                                        command.AddWarning(ctx, ParserMessageType.MISSING_ALIAS_IN_FROM_SUBQUERY);
                                    }
                                }
                            }
                        }
                        break;
                    }
                case RULE_where_clause: 
                case RULE_where_or_current_clause: 
                    {
                        A_exprContext aExpr = null;
                        if (!HasText(context)) break;
                        if (context is Where_clauseContext ctx && HasText(ctx))
                        {
                            aExpr = ctx.a_expr();
                            if (!HasText(aExpr))
                            {
                                command.AddWarning(ctx, ParserMessageType.MISSING_EXPRESSION_IN_WHERE_CLAUSE);
                                break;
                            }
                        }
                        if (context is Where_or_current_clauseContext ctx2 && HasText(ctx2))
                        {
                            aExpr = ctx2.a_expr();
                            if (!HasText(aExpr))
                            {
                                command.AddWarning(ctx2, ParserMessageType.MISSING_EXPRESSION_IN_WHERE_CLAUSE);
                                break;
                            }
                        }
                        A_expr_unary_notContext a_expr_unary_notContext = (A_expr_unary_notContext)FindFirstTargetType(aExpr, typeof(A_expr_unary_notContext));
                        if (HasText(aExpr) &&
                            HasText(a_expr_unary_notContext) &&
                            a_expr_unary_notContext.ChildCount > 0 &&
                            a_expr_unary_notContext.GetChild(0) is ErrorNodeImpl)
                        {
                            command.AddWarning(aExpr, ParserMessageType.MISSING_EXPRESSION_IN_WHERE_CLAUSE);
                            break;
                        }
                        if (HasAggregateFunction(aExpr))
                        {
                            command.AddWarning(aExpr, ParserMessageType.AGGREGATE_FUNCTION_IN_WHERE_CLAUSE);
                        }
                        if (HasAndOrExprWithoutParens(aExpr))
                        {
                            command.AddWarning(aExpr, ParserMessageType.AND_OR_MISSING_PARENTHESES_IN_WHERE_CLAUSE);
                        }
                        if (!IsLogical(aExpr))
                        {
                            command.AddWarning(aExpr, ParserMessageType.NOT_LOGICAL_OPERAND);
                        }
                        C_expr_existsContext value = (C_expr_existsContext)FindFirstTargetType(aExpr, typeof(C_expr_existsContext));
                        if (!HasText(value) || (HasText(value) && value.EXISTS() == null))
                        {
                            List<IParseTree> subQueries = new List<IParseTree>();
                            FindAllTargetTypes(aExpr, typeof(Simple_select_pramaryContext), subQueries);
                            A_expr_inContext a_expr_inContext = (A_expr_inContext)FindFirstTargetType(aExpr, typeof(A_expr_inContext));
                            A_expr_compareContext a_expr_compareContext = (A_expr_compareContext)FindFirstTargetType(aExpr, typeof(A_expr_compareContext));
                            Simple_select_pramaryContext subQuery = (Simple_select_pramaryContext)FindFirstTargetType(aExpr, typeof(Simple_select_pramaryContext));
                            if (subQuery != null) 
                            {
                                if (a_expr_inContext != null && a_expr_inContext.ChildCount > 1)
                                {
                                    if (HasSubqueryColumnMismatch(a_expr_inContext.a_expr_unary_not(), subQuery))
                                    {
                                        command.AddWarning(subQuery, ParserMessageType.SUBQUERY_COLUMN_COUNT_MISMATCH);
                                    }
                                } 
                                else if (a_expr_compareContext != null && a_expr_compareContext.ChildCount > 1)
                                {
                                    if (HasSubqueryColumnMismatch(a_expr_compareContext.lhs, subQuery))
                                    {
                                        command.AddWarning(subQuery, ParserMessageType.SUBQUERY_COLUMN_COUNT_MISMATCH);
                                    }
                                }
                                
                                if (HasSelectStar(subQuery))
                                {
                                    command.AddWarning(aExpr, ParserMessageType.SELECT_ALL_IN_SUB_QUERY);
                                }
                            }
                        }
                        break;
                    }
                case RULE_select_clause: 
                    {
                        if (context is Select_clauseContext ctx)
                        {
                            if ((ctx.union != null || ctx.except != null) && ctx.first_intersect != null && ctx.second_intersect != null) 
                            {
                                if (IsSelectPramaryContextSelectStar(ctx.first_intersect?.simple_select_pramary()) 
                                    || IsSelectPramaryContextSelectStar(ctx.second_intersect?.simple_select_pramary()))
                                {
                                    command.AddWarning(ctx, ParserMessageType.SELECT_ALL_IN_UNION_STATEMENT);
                                }
                            }
                        }
                        break;
                    }
                case RULE_simple_select_intersect: 
                    {
                        if (context is Simple_select_intersectContext ctx)
                        {
                            if (ctx.intersect != null && ctx.first_pramary != null && ctx.second_pramary != null) 
                            {
                                if (IsSelectPramaryContextSelectStar(ctx.first_pramary) 
                                    || IsSelectPramaryContextSelectStar(ctx.second_pramary))
                                {
                                    command.AddWarning(ctx, ParserMessageType.SELECT_ALL_IN_UNION_STATEMENT);
                                }
                            }
                        }
                        break;
                    }
                case RULE_updatestmt: 
                case RULE_deletestmt: 
                case RULE_insertstmt: 
                    {
                        if ((context is UpdatestmtContext && HasText(context)) 
                            || (context is DeletestmtContext && HasText(context))
                            || (context is InsertstmtContext && HasText(context))
                        )
                        {
                            List<IParseTree> subQueries = new List<IParseTree>();
                            FindAllTargetTypes(context, typeof(Select_no_parensContext), subQueries);
                            foreach (Select_no_parensContext subQuery in subQueries)
                            {
                                if (HasText(subQuery.opt_sort_clause()) && !(HasText(subQuery.opt_select_limit()) || HasText(subQuery.select_limit())))
                                {
                                    command.AddWarning(subQuery.opt_sort_clause(), ParserMessageType.ORDER_BY_CLAUSE_IN_SUB_QUERY_WITHOUT_LIMIT);
                                }
                                if (!HasWhereClauseWithMultipleAllowed(context, subQuery))
                                {
                                    Limit_clauseContext limit_clauseContext = ShouldAddFetchMultipleRowsWarning(subQuery);
                                    if (HasText(limit_clauseContext))
                                    {
                                        command.AddWarning(limit_clauseContext, ParserMessageType.FETCH_CLAUSE_MIGHT_RETURN_MULTIPLE_ROWS);
                                    }
                                }
                            }
                        }
                        if (context is InsertstmtContext ctx)
                        {
                            int insertColumnCount = CountInsertColumns(ctx);
                            if (insertColumnCount == 0 && ctx?.insert_rest()?.DEFAULT() == null)
                            {
                                command.AddWarning(ctx, ParserMessageType.INSERT_STATEMENT_WITHOUT_COLUMN_NAMES);
                            }
                            Simple_select_pramaryContext select_PramaryContext = FindSelectPramaryContext(ctx);
                            if (select_PramaryContext != null && HasSelectStar(select_PramaryContext))
                            {
                                command.AddWarning(ctx, ParserMessageType.SELECT_ALL_IN_INSERT_STATEMENT);
                            }
                        }
                        
                        if (context is UpdatestmtContext updCtx)
                        {
                            if (!HasText(updCtx.where_or_current_clause()))
                            {
                                command.AddWarning(updCtx, ParserMessageType.UPDATE_STATEMENT_WITHOUT_WHERE_CLAUSE);
                            }
                        }
                        else if (context is DeletestmtContext delCtx)
                        {
                            if (!HasText(delCtx.where_or_current_clause()))
                            {
                                command.AddWarning(delCtx, ParserMessageType.DELETE_STATEMENT_WITHOUT_WHERE_CLAUSE);
                            }
                        }
                        
                        break;
                    }
                case RULE_select_no_parens: 
                    {
                        if (context is Select_no_parensContext ctx && HasText(ctx))
                        {
                            Simple_select_pramaryContext value = (Simple_select_pramaryContext)FindFirstTargetType(ctx, typeof(Simple_select_pramaryContext));
                            Target_elContext[] columns2 = GetColumns(value);
                            ParserRuleContext optlimitClause = FindLimitClause(ctx);
                            if (value != null && AllHaveAggregateFunction(columns2) && HasText(optlimitClause) && optlimitClause is Limit_clauseContext limitCtx)
                            {
                                if (limitCtx.LIMIT() != null)
                                {

                                    command.AddWarning(limitCtx.select_limit_value(), ParserMessageType.ONE_ROW_IN_RESULT_WITH_LIMIT);
                                }
                                if (limitCtx.FETCH() != null)
                                {
                                    command.AddWarning(limitCtx.select_fetch_first_value(), ParserMessageType.ONE_ROW_IN_RESULT_WITH_LIMIT);
                                }
                            }
                            List<IParseTree> subQueries = new List<IParseTree>();
                            FindAllTargetTypes(ctx, typeof(Select_no_parensContext), subQueries);
                            foreach (Select_no_parensContext subQuery in subQueries)
                            {
                                if (HasText(subQuery.opt_sort_clause()) && !(HasText(subQuery.opt_select_limit()) || HasText(subQuery.select_limit())))
                                {
                                    command.AddWarning(subQuery.opt_sort_clause(), ParserMessageType.ORDER_BY_CLAUSE_IN_SUB_QUERY_WITHOUT_LIMIT);
                                }
                                if (!HasWhereClauseWithMultipleAllowed(ctx, subQuery)) 
                                {
                                    Limit_clauseContext limit_clauseContext = ShouldAddFetchMultipleRowsWarning(subQuery);
                                    if (HasText(limit_clauseContext)) 
                                    {
                                        command.AddWarning(limit_clauseContext, ParserMessageType.FETCH_CLAUSE_MIGHT_RETURN_MULTIPLE_ROWS);
                                    }
                                }
                            }
                        }
                        break;
                    }
                case RULE_select_limit: 
                    {
                        if (context is Select_limitContext ctx && HasText(ctx))
                        {
                            Select_no_parensContext selectNoParensParent = null;
                            if (ctx.Parent is Opt_select_limitContext optLimit) 
                            {
                                selectNoParensParent = (Select_no_parensContext) optLimit.Parent;
                            }
                            if (ctx.Parent is Select_no_parensContext selectParent) 
                            {
                                selectNoParensParent = selectParent;
                            }
                            if (!HasText(selectNoParensParent?.opt_sort_clause()) && (HasText(ctx.offset_clause()) || HasText(ctx.limit_clause())))
                            {
                                command.AddWarning(ctx, ParserMessageType.FETCH_LIMIT_OFFSET_CLAUSE_WITHOUT_ORDER_BY_CLAUSE);
                            }
                            if (HasText(ctx.limit_clause())) 
                            {
                                Limit_clauseContext limitCtx = ctx.limit_clause();
                                if (limitCtx.LIMIT() != null)
                                {
                                    if (HasText(limitCtx.select_limit_value()) && limitCtx.select_limit_value().ALL() == null)
                                    {
                                        if (IsValueNegative(limitCtx.select_limit_value().a_expr().GetText()))
                                        {
                                            command.AddWarning(limitCtx.select_limit_value(), ParserMessageType.LIMIT_CONSTRAINT);
                                        }
                                    }
                                }
                                if (limitCtx.FETCH() != null)
                                {
                                    if (HasText(limitCtx.select_fetch_first_value()))
                                    {
                                        if (IsValueNegative(limitCtx.select_fetch_first_value().GetText()))
                                        {
                                            command.AddWarning(limitCtx.select_fetch_first_value(), ParserMessageType.LIMIT_CONSTRAINT);
                                        }
                                    }
                                }
                            }
                            if (HasText(ctx.offset_clause())) 
                            {
                                Offset_clauseContext offsetCtx = ctx.offset_clause();
                                if (offsetCtx.OFFSET() != null)
                                {
                                    if (HasText(offsetCtx.select_offset_value()))
                                    {
                                        if (IsValueNegative(offsetCtx.select_offset_value().a_expr().GetText()))
                                        {
                                            command.AddWarning(offsetCtx.select_offset_value(), ParserMessageType.LIMIT_CONSTRAINT);
                                        }
                                    }
                                    else if (HasText(offsetCtx.select_fetch_first_value()))
                                    {
                                        if (IsValueNegative(offsetCtx.select_fetch_first_value().GetText()))
                                        {
                                            command.AddWarning(offsetCtx.select_fetch_first_value(), ParserMessageType.LIMIT_CONSTRAINT);
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    }
                case RULE_sortby_list: 
                    {
                        if (context is Sortby_listContext ctx)
                        {
                            if (HasRandomOrderingFunction(ctx))
                                command.AddWarning(ctx, ParserMessageType.ORDER_BY_RANDOM_FUNCTION);
                            
                            
                            List<IParseTree> aexprconsts = new List<IParseTree>();
                            FindAllTargetTypes(ctx, typeof(AexprconstContext), aexprconsts);
                            foreach (IParseTree item in aexprconsts)
                            {
                                if (item is AexprconstContext aexprconst) 
                                {
                                    if (aexprconst != null && !string.IsNullOrEmpty(aexprconst.GetText()))
                                    {
                                        command.AddWarning(ctx, ParserMessageType.ORDERING_BY_ORDINAL);
                                        break;
                                    }
                                }
                            }
                        }
                        break;
                    }
                case RULE_func_application: 
                    {
                        if (context is Func_applicationContext ctx)
                        {
                            if (ctx.func_name().GetText().ToLower() == "sum" && ctx.func_arg_list().GetText() == "1")
                            {
                                command.AddWarning(ctx, ParserMessageType.USE_COUNT_FUNCTION);
                            }
                        }
                        break;
                    }
                case RULE_a_expr_like: 
                    {
                        if (context is A_expr_likeContext ctx && HasText(ctx))
                        {
                            var rhsText = ctx.rhs?.GetText();
                            if (!string.IsNullOrEmpty(rhsText))
                            {
                                var q = rhsText.IndexOf('\'');
                                if (q >= 0 && q + 1 < rhsText.Length)
                                {
                                    var first = rhsText[q + 1];
                                    if (first == '%' || first == '_')
                                    {
                                        command.AddWarning(ctx, ParserMessageType.LEADING_WILDCARD_IN_LIKE_EXPRESSION);
                                    }
                                }
                            }

                            if (HasText(ctx.rhs) && ctx._ILIKE == null)
                            {
                                C_expr_exprContext value = (C_expr_exprContext)FindFirstTargetType(ctx.rhs, typeof(C_expr_exprContext));
                                if (value != null && value.ChildCount > 0 && value.GetChild(0) is AexprconstContext)
                                {
                                    if (!ctx.rhs.GetText().Contains("%") && !ctx.rhs.GetText().Contains("_")) 
                                    {
                                        if ((ctx._SIMILAR != null
                                            && !ctx.rhs.GetText().Contains("|")
                                            && !ctx.rhs.GetText().Contains("*")
                                            && !ctx.rhs.GetText().Contains("+")
                                            && !ctx.rhs.GetText().Contains("?")
                                            && !ctx.rhs.GetText().Contains("{")
                                            && !ctx.rhs.GetText().Contains("}")
                                            && !ctx.rhs.GetText().Contains("(")
                                            && !ctx.rhs.GetText().Contains(")")
                                            && !ctx.rhs.GetText().Contains("[")
                                            && !ctx.rhs.GetText().Contains("]")
                                            && !ctx.rhs.GetText().Contains("*")) || ctx._SIMILAR == null)
                                        {
                                            command.AddWarning(ctx, ParserMessageType.MISSING_WILDCARDS_IN_LIKE_EXPRESSION);
                                        }
                                    }
                                }
                                if (value.ChildCount > 0 && value.GetChild(0) is ColumnrefContext)
                                {
                                    command.AddWarning(ctx, ParserMessageType.COLUMN_LIKE_COLUMN);
                                }
                                
                            }
                        }
                        break;
                    }
                case RULE_a_expr_between: 
                    {
                        if (context is A_expr_betweenContext ctx && HasText(ctx))
                        {
                            if ((HasText(ctx.rhs) && ctx.rhs.GetText().ToLower().Equals("null")) ||
                                    (HasText(ctx.between_r_h_s) && ctx.between_r_h_s.GetText().ToLower().Equals("null")))
                            {
                                command.AddWarning(ctx, ParserMessageType.EQUALITY_WITH_NULL);
                            }
                        }
                        break;
                    }
                case RULE_a_expr_compare: 
                    {
                        if (context is A_expr_compareContext ctx && HasText(ctx))
                        {
                            ParserRuleContext rhs = FindCompareRhs(ctx);
                            if (ctx._operands.Count > 0 && (HasEqualityWithTextPattern(rhs) || HasEqualityWithTextPattern(ctx.lhs)))
                            {
                                command.AddWarning(ctx, ParserMessageType.EQUALITY_WITH_TEXT_PATTERN);
                            }
                            if ((HasText(rhs) && rhs.GetText().ToLower().Equals("null")) ||
                                    (HasText(ctx.lhs) && ctx.lhs.GetText().ToLower().Equals("null")))
                            {
                                if (ctx.LT() != null || ctx.GT() != null || ctx.LESS_EQUALS() != null || ctx.GREATER_EQUALS() != null)
                                {
                                    command.AddWarning(ctx, ParserMessageType.COMPARING_WITH_NULL);
                                }
                                if (ctx.EQUAL() != null)
                                {
                                    command.AddWarning(ctx, ParserMessageType.EQUALITY_WITH_NULL);
                                }
                                if (ctx.NOT_EQUALS() != null)
                                {
                                    command.AddWarning(ctx, ParserMessageType.NOT_EQUALITY_WITH_NULL);
                                }
                            }

                            if (HasText(ctx.subquery_Op())) 
                            {
                                MathopContext mathOperand = (MathopContext)FindFirstTargetType(ctx.subquery_Op(), typeof(MathopContext));
                                Sub_typeContext subType = ctx.sub_type();
                                if (HasText(subType) && HasText(mathOperand))
                                {
                                    if (mathOperand.EQUAL() != null && subType.ALL() != null)
                                    {
                                        command.AddWarning(ctx, ParserMessageType.EQUALS_ALL);
                                    }
                                    if (mathOperand.NOT_EQUALS() != null && (subType.ANY() != null || subType.SOME() != null))
                                    {
                                        command.AddWarning(ctx, ParserMessageType.NOT_EQUALS_ANY);
                                    }
                                }
                            }
                            
                        }
                        break;
                    }
                case RULE_a_expr_mul: 
                    {
                        if (context is A_expr_mulContext ctx && HasText(ctx))
                        {
                            if (HasText(ctx.lhs) && ctx._rhs != null && ctx._rhs.Count > 0 && HasText(ctx._rhs[0]) && ctx.SLASH() != null &&
                                ctx.lhs.GetText().ToLower().Contains("sum") &&
                                ctx._rhs[0].GetText().ToLower().Contains("count"))
                            {
                                command.AddWarning(ctx, ParserMessageType.USE_AVG_FUNCTION);
                            }
                            
                        }
                        break;
                    }
                case RULE_typename:
                {
                    if (context is TypenameContext ctx && HasText(ctx))
                    {
                        var typeText = ctx.GetText().ToLower();

                        var lastPart = typeText;
                        var dotPos = typeText.LastIndexOf('.');
                        if (dotPos >= 0 && dotPos < typeText.Length - 1)
                        {
                            lastPart = typeText.Substring(dotPos + 1);
                        }

                        var isFloatType =
                            lastPart.StartsWith("float") ||
                            lastPart == "real" ||
                            lastPart == "doubleprecision";

                        if (isFloatType)
                        {
                            var inDdl =
                                FindFirstTargetTypeParent(ctx, typeof(CreatestmtContext)) != null ||
                                FindFirstTargetTypeParent(ctx, typeof(CreateforeigntablestmtContext)) != null ||
                                FindFirstTargetTypeParent(ctx, typeof(CreatedomainstmtContext)) != null ||
                                FindFirstTargetTypeParent(ctx, typeof(AltertablestmtContext)) != null ||
                                FindFirstTargetTypeParent(ctx, typeof(AlterdomainstmtContext)) != null;

                            if (inDdl)
                            {
                                command.AddWarning(ctx, ParserMessageType.USE_FLOAT_DATA_TYPE);
                            }
                        }
                    }
                    break;
                }
            }
        }

        private static ParserRuleContext FindCompareRhs(A_expr_compareContext ctx)
        {
            if (HasText(ctx.rhs1)) 
            {
                return ctx.rhs1;
            }
            if (HasText(ctx.rhs2)) 
            {
                return ctx.rhs2;
            }
            if (HasText(ctx.rhs3)) 
            {
                return ctx.rhs3;
            }
            return null;
        }

        private static int _CollectCommands(
            RuleContext context,
            CaretPosition caretPosition,
            string tokenSeparator,
            int commandSeparatorTokenType,
            in IList<ParsedTreeCommand> commands,
            int enclosingCommandIndex,
            in IList<StringBuilder> functionParams,
            in IList<StringBuilder> transactionStatementsEncountered)
        {
            if (context is RootContext && commands.Last().Context == null)
            {
                commands.Last().Context = context; // base starting branch
                if (context.ChildCount > 0)
                {
                    var errorFreeCount = 0;
                    var errorCount = 0;
                    ParserRuleContext notErrorChild = null;
                    for (var i = 0; i < context.ChildCount; ++i)
                    {
                        var c = context.GetChild(i);
                        if (c is ErrorNodeImpl)
                        {
                            errorCount++;
                        }
                        else
                        {
                            notErrorChild = c as ParserRuleContext;
                            errorFreeCount++;
                        }
                        if (errorFreeCount > 1)
                        {
                            break;
                        }
                    }
                    if (errorFreeCount < 2 && errorCount > 0)
                    {
                        commands.Last().AddAnalyzeErrors(notErrorChild, ParserMessageType.PARSING_ERROR);
                    }
                }
            }
            _AnalyzeRuleContext(context, commands.Last());
            for (var i = 0; i < context.ChildCount; ++i)
            {
                var child = context.GetChild(i);
                if (child is IdentifierContext identifier)
                {
                    _AnalyzeForDoubleQuotes(identifier, commands.Last());
                }
                if (child is ITerminalNode terminalNode)
                {
                    var token = terminalNode.Symbol;
                    var tokenLength = token.StopIndex - token.StartIndex + 1;
                    if (transactionStatementsEncountered?.Count % 2 == 0 && (token.Type == TokenConstants.EOF || token.Type == commandSeparatorTokenType))
                    {

                        if (enclosingCommandIndex == -1 && (
                            caretPosition.Line > commands.Last().StartLine && caretPosition.Line < token.Line ||
                            caretPosition.Line == commands.Last().StartLine && caretPosition.Column >= commands.Last().StartColumn && (caretPosition.Line < token.Line || caretPosition.Column <= token.Column) ||
                            caretPosition.Line == token.Line && caretPosition.Column <= token.Column))
                        {
                            enclosingCommandIndex = commands.Count - 1;
                        }

                        if (token.Type == TokenConstants.EOF)
                        {
                            continue;
                        }
                        if (string.IsNullOrWhiteSpace(commands.Last().Text))
                        {
                            commands.Last().AddWarning(token, ParserMessageType.UNNECESSARY_SEMICOLON);
                        }
                        commands.Add(new ParsedTreeCommand());
                    }
                    else
                    {
                        if (commands.Last().StartOffset == -1)
                        {
                            commands.Last().StartLine = token.Line;
                            commands.Last().StartColumn = token.Column;
                            commands.Last().StartOffset = token.StartIndex;
                        }
                        commands.Last().StopLine = token.Line;
                        commands.Last().StopColumn = token.Column + tokenLength;
                        commands.Last().StopOffset = token.StopIndex;

                        if (functionParams is null)
                        {
                            commands.Last().Text = commands.Last().Text + child.GetText() + tokenSeparator;
                        }
                        else if (context.RuleIndex != RULE_function_or_procedure ||
                            token.Type != OPEN_PAREN &&
                            child.GetText() != "(" &&
                            token.Type != CLOSE_PAREN &&
                            child.GetText() != ")" &&
                            token.Type != COMMA &&
                            child.GetText() != ",")
                        {
                            functionParams.Last().Append(child.GetText() + tokenSeparator);
                        }
                    }
                }
                else
                {
                    var ctx = child as RuleContext;


                    if (ctx?.RuleIndex == RULE_transactionstmt)
                    {
                        var statements = new[] { "BEGIN", "END" };
                        if (statements.Any(s => ctx?.GetText().IndexOf(s, StringComparison.OrdinalIgnoreCase) >= 0))
                        {
                            transactionStatementsEncountered?.Add(new StringBuilder(ctx?.GetText()));
                        }
                    };


                    if (ctx?.RuleIndex == RULE_empty_grouping_set) continue;

                    enclosingCommandIndex = _CollectCommands(ctx, caretPosition, tokenSeparator, commandSeparatorTokenType, commands, enclosingCommandIndex, functionParams, transactionStatementsEncountered);
                }
            }
            return enclosingCommandIndex;
        }
    }
}
