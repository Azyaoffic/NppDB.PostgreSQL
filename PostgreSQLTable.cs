using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Windows.Forms;
using Npgsql;
using NppDB.Comm;

namespace NppDB.PostgreSQL
{
    public class PostgreSqlTable : TreeNode, IRefreshable, IMenuProvider
    {
        public string TypeName { get; set; } = "TABLE";
        public string FuncOid { get; set; }
        public PostgreSqlTable()
        {
            SelectedImageKey = ImageKey = @"Table";
        }

        public void Refresh()
        {
            var conn = GetDbConnect();
            using (var cnn = conn.GetConnection())
            {
                TreeView.Enabled = false;
                TreeView.Cursor = Cursors.WaitCursor;
                try
                {
                    cnn.Open();
                    Nodes.Clear();

                    var columns = new List<PostgreSqlColumn>();

                    if (GetSchema().Foreign)
                    {
                        var columnCount = CollectColumns(cnn, ref columns, new List<string>(), new List<string>(), new List<string>());
                        if (columnCount == 0) return;
                    }
                    else if (TypeName == "FUNCTION")
                    {
                        var columnCount = CollectFunctionColumns(cnn, ref columns);
                        if (columnCount == 0) return;
                    }
                    else
                    {
                        var primaryKeyColumnNames = CollectPrimaryKeys(cnn, ref columns);
                        var foreignKeyColumnNames = CollectForeignKeys(cnn, ref columns);
                        var indexedColumnNames = CollectIndices(cnn, ref columns);

                        var columnCount = CollectColumns(cnn, ref columns, primaryKeyColumnNames, foreignKeyColumnNames, indexedColumnNames);
                        if (columnCount == 0) return;
                    }

                    var maxLength = columns.Max(c => c.ColumnName.Length);
                    columns.ForEach(c => c.AdjustColumnNameFixedWidth(maxLength));
                    Nodes.AddRange(columns.ToArray<TreeNode>());
                }
                catch (Exception ex)
                {
                    MessageBox.Show(ex.Message, @"Exception");
                }
                finally
                {
                    cnn.Close();
                    TreeView.Enabled = true;
                    TreeView.Cursor = null;
                }
            }
        }

        private int CollectFunctionColumns(NpgsqlConnection connection, ref List<PostgreSqlColumn> columns)
        {
            var count = 0;
            const string query = "select pg_get_function_arguments(p.oid) as function_arguments " +
                                 "from pg_proc p " +
                                 "left join pg_namespace n on p.pronamespace = n.oid " +
                                 "where n.nspname = '{0}' and p.proname = '{1}' and p.oid = '{2}'";
            using (var command = new NpgsqlCommand(string.Format(query, GetSchemaName(), Text, FuncOid), connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var functionArguments = reader["function_arguments"].ToString();
                        var functionArgumentsArray = functionArguments.Split(',');
                        foreach (var functionArgument in functionArgumentsArray)
                        {
                            var argumentNameAndType = functionArgument.Trim().Split(' ');
                            if (argumentNameAndType.Length > 1)
                            {
                                if (string.IsNullOrEmpty(argumentNameAndType[0]) ||
                                    string.IsNullOrEmpty(argumentNameAndType[1])) continue;
                                var postgreSqlColumnInfo = new PostgreSqlColumn(argumentNameAndType[0], argumentNameAndType[1].ToUpper(), 0, 0);
                                columns.Insert(count++, postgreSqlColumnInfo);
                            }
                            else if (argumentNameAndType.Length == 1)
                            {
                                if (string.IsNullOrEmpty(argumentNameAndType[0])) continue;
                                var postgreSqlColumnInfo = new PostgreSqlColumn(argumentNameAndType[0].ToUpper(), "", 0, 0);
                                columns.Insert(count++, postgreSqlColumnInfo);
                            }
                        }
                    }
                }
            }
            return count;
        }

        private int CollectColumns(NpgsqlConnection connection, ref List<PostgreSqlColumn> columns,
            in List<string> primaryKeyColumnNames,
            in List<string> foreignKeyColumnNames,
            in List<string> indexedColumnNames
            )
        {
            var count = 0;
            const string query = "SELECT attr.attname AS column_name, " +
                                 "pg_catalog.format_type(attr.atttypid, attr.atttypmod) AS data_type, " +
                                 "pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS column_default, " +
                                 "NOT(attr.attnotnull) AS is_nullable " + "FROM pg_catalog.pg_attribute AS attr " +
                                 "LEFT JOIN pg_catalog.pg_attrdef d ON (attr.attrelid, attr.attnum) = (d.adrelid, d.adnum) " +
                                 "JOIN pg_catalog.pg_class AS cls ON cls.oid = attr.attrelid " +
                                 "JOIN pg_catalog.pg_namespace AS ns ON ns.oid = cls.relnamespace " +
                                 "JOIN pg_catalog.pg_type AS tp ON tp.oid = attr.atttypid " +
                                 "WHERE ns.nspname = '{0}' " +
                                 "AND cls.relname = '{1}' " +
                                 "AND attr.attnum >= 1 AND NOT attr.attisdropped " + "ORDER BY attr.attnum";

            using (var command = new NpgsqlCommand(string.Format(query, GetSchemaName(), Text), connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var columnName = reader["column_name"].ToString();
                        var dataTypeName = reader["data_type"].ToString().ToUpper();
                        var columnDefaultObj = reader["column_default"];
                        var isNullable = Convert.ToBoolean(reader["is_nullable"]);

                        var options = 0;
                        if (!isNullable) options += 1;
                        if (indexedColumnNames.Contains(columnName)) options += 10;
                        if (primaryKeyColumnNames.Contains(columnName)) options += 100;
                        if (foreignKeyColumnNames.Contains(columnName)) options += 1000;

                        var columnInfoNode = new PostgreSqlColumn(columnName, GetDataTypeName(reader), 0, options);

                        var tooltipText = new StringBuilder();
                        tooltipText.AppendLine($"Column: {columnName}");
                        tooltipText.AppendLine($"Type: {dataTypeName}");
                        tooltipText.AppendLine($"Nullable: {(isNullable ? "Yes" : "No")}");

                        if (!(columnDefaultObj is DBNull) && columnDefaultObj != null)
                        {
                            tooltipText.AppendLine($"Default: {columnDefaultObj}");
                        }
                        if (primaryKeyColumnNames.Contains(columnName))
                            tooltipText.AppendLine("Primary Key Member");
                        if (foreignKeyColumnNames.Contains(columnName))
                            tooltipText.AppendLine("Foreign Key Member");

                        columnInfoNode.ToolTipText = tooltipText.ToString().TrimEnd();

                        columns.Insert(count++, columnInfoNode);
                    }
                }
            }
            return count;
        }

        private static string GetDataTypeName(NpgsqlDataReader reader)
        {
            var dataType = reader["data_type"].ToString();
            return dataType.ToUpper();
        }

        private List<string> CollectPrimaryKeys(NpgsqlConnection connection, ref List<PostgreSqlColumn> columns)
        {
            const string query = "SELECT distinct c.conname as constraint_name, " +
                                 "pg_get_constraintdef(c.oid) as constraint_definition " +
                                 "FROM pg_catalog.pg_constraint c " +
                                 "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid " + "JOIN pg_catalog.pg_class AS cls ON cls.oid = c.conrelid " + "WHERE c.contype IN('p') " +
                                 "AND c.connamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '{0}') " + "AND cls.relname = '{1}'";

            var names = new List<string>();
            using (var command = new NpgsqlCommand(string.Format(query, GetSchemaName(), Text), connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var primaryKeyName = reader["constraint_name"].ToString();
                        var primaryKeyDef = reader["constraint_definition"].ToString();

                        var pkNode = new PostgreSqlColumn(primaryKeyName, primaryKeyDef, 1, 0);

                        var tooltipText = new StringBuilder();
                        tooltipText.AppendLine($"Primary Key Constraint: {primaryKeyName}");
                        tooltipText.AppendLine($"Definition: {primaryKeyDef}");
                        pkNode.ToolTipText = tooltipText.ToString().TrimEnd();

                        columns.Add(pkNode);

                        var primaryKeyTargetMatch = Regex.Match(primaryKeyDef, @"PRIMARY KEY \((.+)\)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
                        if (primaryKeyTargetMatch.Success && primaryKeyTargetMatch.Groups.Count > 1)
                        {
                            names.AddRange(primaryKeyTargetMatch.Groups[1].ToString().Split(',').Select(s => s.Trim().Trim('"')));
                        }
                    }
                }
            }
            return names;
        }

        private List<string> CollectForeignKeys(NpgsqlConnection connection, ref List<PostgreSqlColumn> columns)
        {
            const string query = "SELECT distinct c.conname as constraint_name, " +
                                 "pg_get_constraintdef(c.oid) as constraint_definition " +
                                 "FROM pg_catalog.pg_constraint c " +
                                 "JOIN pg_catalog.pg_class AS cls ON cls.oid = c.conrelid " + "WHERE c.contype IN('f') " +
                                 "AND c.connamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '{0}') " + "AND cls.relname = '{1}'";

            var names = new List<string>();
            using (var command = new NpgsqlCommand(string.Format(query, GetSchemaName(), Text), connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var foreignKeyName = reader["constraint_name"].ToString();
                        var foreignKeyDef = reader["constraint_definition"].ToString();
                        var foreignKeyDefFormatted = foreignKeyDef;

                        var fkColumnNameMatch = Regex.Match(foreignKeyDef, @"FOREIGN KEY \((.+)\) REFERENCES", RegexOptions.IgnoreCase | RegexOptions.Singleline);
                        var fkTargetMatch = Regex.Match(foreignKeyDef, @"REFERENCES (.+)", RegexOptions.IgnoreCase | RegexOptions.Singleline);
                        if (fkColumnNameMatch.Success && fkColumnNameMatch.Groups.Count > 1)
                        {
                            var fkColumnList = fkColumnNameMatch.Groups[1].ToString().Trim();
                            names.AddRange(fkColumnList.Split(',').Select(s => s.Trim().Trim('"')));
                            if (fkTargetMatch.Success && fkTargetMatch.Groups.Count > 1)
                            {
                                foreignKeyDefFormatted = $"({fkColumnList}) -> {fkTargetMatch.Groups[1].ToString().Trim()}";
                            }
                        }

                        var fkNode = new PostgreSqlColumn(foreignKeyName, foreignKeyDefFormatted, 2, 0);

                        var tooltipText = new StringBuilder();
                        tooltipText.AppendLine($"Foreign Key Constraint: {foreignKeyName}");
                        tooltipText.AppendLine($"Definition: {foreignKeyDef}");
                        fkNode.ToolTipText = tooltipText.ToString().TrimEnd();

                        columns.Add(fkNode);
                    }
                }
            }
            return names;
        }

        private List<string> CollectIndices(NpgsqlConnection connection, ref List<PostgreSqlColumn> columns)
        {
            const string query = "select indexname, indexdef from pg_catalog.pg_indexes where schemaname = '{0}' and tablename = '{1}';";

            var names = new List<string>();
            var processedIndexNames = new HashSet<string>();

            using (var command = new NpgsqlCommand(string.Format(query, GetSchemaName(), Text), connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var indexName = reader["indexname"].ToString();
                        var indexDef = reader["indexdef"].ToString();

                        if (processedIndexNames.Contains(indexName)) continue;

                        var indexDefFormatted = indexDef;
                        var isUnique = indexDef.StartsWith("CREATE UNIQUE INDEX", StringComparison.OrdinalIgnoreCase);

                        var indexDefMatch = Regex.Match(indexDef, @"\((.+)\)", RegexOptions.Singleline);
                        if (indexDefMatch.Success && indexDefMatch.Groups.Count > 1)
                        {
                            var indexColumns = indexDefMatch.Groups[1].ToString().Trim();
                            names.AddRange(indexColumns.Split(',').Select(s => s.Trim().Trim('"')));
                            indexDefFormatted = $"({indexColumns})";
                        }

                        var indexNode = new PostgreSqlColumn(indexName, indexDefFormatted, isUnique ? 4 : 3, 0);

                        var tooltipText = new StringBuilder();
                        tooltipText.AppendLine($"Index: {indexName}");
                        tooltipText.AppendLine($"Type: {(isUnique ? "Unique" : "Non-Unique")}");
                        tooltipText.AppendLine($"Definition: {indexDef}");
                        indexNode.ToolTipText = tooltipText.ToString().TrimEnd();

                        columns.Add(indexNode);
                        processedIndexNames.Add(indexName);
                    }
                }
            }

            return names.Distinct().ToList();
        }

        public virtual ContextMenuStrip GetMenu()
        {
            var menuList = new ContextMenuStrip { ShowImageMargin = false };
            var connect = GetDbConnect();
            menuList.Items.Add(new ToolStripButton("Refresh", null, (s, e) => { Refresh(); }));
            if (connect?.CommandHost == null) return menuList;
            menuList.Items.Add(new ToolStripSeparator());

            var host = connect.CommandHost;
            var schemaName = GetSchemaName();
            var tableNameWithSchema = $"\"{schemaName}\".\"{Text}\"";

            if (TypeName != "FUNCTION")
            {
                menuList.Items.Add(new ToolStripButton($"Select all rows", null, (s, e) =>
                {
                    host.Execute(NppDbCommandType.NEW_FILE, null);
                    var id = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                    var query = $"SELECT * FROM \"{schemaName}\".\"{Text}\";";
                    host.Execute(NppDbCommandType.APPEND_TO_CURRENT_VIEW, new object[] { query });
                    host.Execute(NppDbCommandType.CREATE_RESULT_VIEW, new[] { id, connect, connect.CreateSqlExecutor() });
                    host.Execute(NppDbCommandType.EXECUTE_SQL, new[] { id, query });
                }));
                menuList.Items.Add(new ToolStripButton($"Select random 100 rows", null, (s, e) =>
                {
                    host.Execute(NppDbCommandType.NEW_FILE, null);
                    var id = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                    var query = $"SELECT * FROM \"{schemaName}\".\"{Text}\" FETCH FIRST 100 ROWS ONLY;";
                    host.Execute(NppDbCommandType.APPEND_TO_CURRENT_VIEW, new object[] { query });
                    host.Execute(NppDbCommandType.CREATE_RESULT_VIEW, new[] { id, connect, connect.CreateSqlExecutor() });
                    host.Execute(NppDbCommandType.EXECUTE_SQL, new[] { id, query });
                }));
            }
            if (TypeName == "MATERIALIZED_VIEW")
            {
                menuList.Items.Add(new ToolStripButton("Refresh materialized view", null, (s, e) =>
                {
                    var mvName = $"\"{Text}\"";
                    var message = $"Are you sure you want to refresh the materialized view {mvName}?";
                    if (MessageBox.Show(message, @"Confirm Refresh", MessageBoxButtons.YesNo,
                            MessageBoxIcon.Question) != DialogResult.Yes) return;
                    var query = $"REFRESH MATERIALIZED VIEW {tableNameWithSchema};";
                    var id = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                    host.Execute(NppDbCommandType.EXECUTE_SQL, new[] { id, query });
                }));
                menuList.Items.Add(new ToolStripButton("Drop materialized view", null, (s, e) =>
                {
                    var mvName = $"\"{Text}\"";
                    var message = $"Are you sure you want to drop the materialized view {mvName}?\nThis action cannot be undone.";
                    if (MessageBox.Show(message, @"Confirm Drop Materialized View", MessageBoxButtons.YesNo,
                            MessageBoxIcon.Warning) != DialogResult.Yes) return;
                    var query = $"DROP MATERIALIZED VIEW {tableNameWithSchema};";
                    var id = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                    host.Execute(NppDbCommandType.EXECUTE_SQL, new[] { id, query });
                    System.Threading.Thread.Sleep(500);
                    if (Parent is IRefreshable parentGroupNode)
                    {
                        parentGroupNode.Refresh();
                    }
                    else if (TreeView != null)
                    {
                        Remove();
                    }
                }));
            }
            else if (TypeName != "FOREIGN_TABLE")
            {
                if (schemaName != "information_schema" && schemaName != "pg_catalog")
                {
                    menuList.Items.Add(new ToolStripButton($"Drop {TypeName.ToLower()} (RESTRICT)", null, (s, e) =>
                    {
                        var objectName = Text;
                        var message = $"Are you sure you want to drop the {TypeName.ToLower()} '{objectName}' RESTRICT?\n" +
                                      "This action cannot be undone and will fail if other objects depend on this {TypeName.ToLower()}.";
                        if (MessageBox.Show(message, $@"Confirm Drop {TypeName}", MessageBoxButtons.YesNo,
                                MessageBoxIcon.Warning) != DialogResult.Yes) return;
                        var paramsQuery = (TypeName == "FUNCTION") ? CollectFunctionParams(connect) : "";
                        var query = $"DROP {TypeName} {tableNameWithSchema}{paramsQuery} RESTRICT;";
                        var id = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                        host.Execute(NppDbCommandType.EXECUTE_SQL, new[] { id, query });
                        System.Threading.Thread.Sleep(500);
                        if (Parent is IRefreshable parentGroupNode)
                        {
                            parentGroupNode.Refresh();
                        }
                        else if (TreeView != null)
                        {
                            Remove();
                        }
                    }));

                    menuList.Items.Add(new ToolStripButton($"Drop {TypeName.ToLower()} (CASCADE)", null, (s, e) =>
                    {
                        var objectName = Text;
                        var message = $"Are you sure you want to drop the {TypeName.ToLower()} '{objectName}' CASCADE?\n" +
                                      "WARNING: This will also drop all dependent objects automatically.\n" +
                                      "This action cannot be undone.";
                        if (MessageBox.Show(message, $@"Confirm Drop {TypeName} with Cascade", MessageBoxButtons.YesNo,
                                MessageBoxIcon.Exclamation) != DialogResult.Yes) return;
                        var paramsQuery = (TypeName == "FUNCTION") ? CollectFunctionParams(connect) : "";
                        var query = $"DROP {TypeName} {tableNameWithSchema}{paramsQuery} CASCADE;";
                        var id = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                        host.Execute(NppDbCommandType.EXECUTE_SQL, new[] { id, query });
                        System.Threading.Thread.Sleep(500);
                        if (Parent is IRefreshable parentGroupNode)
                        {
                            parentGroupNode.Refresh();
                        }
                        else if (TreeView != null)
                        {
                            Remove();
                        }
                    }));
                }
            }

            var dummy = new ToolStripButton("Dummy", null, (s, e) => { });
            dummy.Visible = false;
            menuList.Items.Add(dummy);
            return menuList;
        }

        private string CollectFunctionParams(PostgreSqlConnect connect)
        {
            var paramsQuery = "()";
            using (var cnn = connect.GetConnection())
            {
                try
                {
                    cnn.Open();
                    var columns = new List<PostgreSqlColumn>();
                    CollectFunctionColumns(cnn, ref columns);
                    if (columns.Count > 0)
                    {
                        paramsQuery = "(";
                        for (var i = 0; i < columns.Count; i++)
                        {
                            var column = columns[i];
                            if (column.ColumnType == "")
                            {
                                paramsQuery += column.ColumnName;
                            }
                            else
                            {
                                paramsQuery += column.ColumnType;
                            }
                            if (i + 1 < columns.Count)
                            {
                                paramsQuery += ",";
                            }
                        }
                        paramsQuery += ")";
                    }
                }
                catch (Exception)
                {
                    // ignored
                }
                finally
                {
                    cnn.Close();
                }
            }

            return paramsQuery;
        }

        private PostgreSqlConnect GetDbConnect()
        {
            var connect = Parent.Parent.Parent as PostgreSqlConnect;
            return connect;
        }

        private PostgreSqlSchema GetSchema()
        {
            return Parent.Parent as PostgreSqlSchema;
        }

        private string GetSchemaName()
        {
            return GetSchema().Schema;
        }
    }
}
