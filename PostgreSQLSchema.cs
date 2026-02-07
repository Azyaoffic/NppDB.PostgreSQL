using System;
using System.Data;
using System.Windows.Forms;
using Npgsql;
using NppDB.Comm;

namespace NppDB.PostgreSQL
{
    public class PostgreSqlSchema : TreeNode, IRefreshable, IMenuProvider
    {
        public string Schema { get; set; }
        public bool Foreign { get; set; }
        public PostgreSqlSchema()
        {
            SelectedImageKey = ImageKey = "Schema";
        }

        public void Refresh()
        {
            Nodes.Clear();
            var connect = GetDbConnect();
            if (connect == null) return;

            using (var checkConn = connect.GetConnection())
            {
                try
                {
                    checkConn.Open();

                    var tableGroup = new PostgreSqlTableGroup();
                    if (SchemaGroupHasChildren(checkConn, Schema, "BASE TABLE"))
                        tableGroup.Nodes.Add(new TreeNode(""));
                    Nodes.Add(tableGroup);

                    var foreignTableGroup = new PostgreSqlForeignTableGroup();
                    if (SchemaGroupHasChildren(checkConn, Schema, "FOREIGN TABLE"))
                        foreignTableGroup.Nodes.Add(new TreeNode(""));
                    Nodes.Add(foreignTableGroup);

                    var viewGroup = new PostgreSqlViewGroup();
                    if (SchemaGroupHasChildren(checkConn, Schema, "VIEW"))
                        viewGroup.Nodes.Add(new TreeNode(""));
                    Nodes.Add(viewGroup);

                    var matViewGroup = new PostgreSqlMaterializedViewGroup();
                    if (SchemaGroupHasChildren(checkConn, Schema, "MATERIALIZED VIEW"))
                        matViewGroup.Nodes.Add(new TreeNode(""));
                    Nodes.Add(matViewGroup);

                    var functionGroup = new PostgreSqlFunctionGroup();
                    if (SchemaGroupHasChildren(checkConn, Schema, "FUNCTION"))
                        functionGroup.Nodes.Add(new TreeNode(""));
                    Nodes.Add(functionGroup);
                }
                catch (Exception ex)
                {
                     Console.WriteLine($"Error refreshing schema {Schema}: {ex.Message}");
                     if(Nodes.Count == 0)
                     {
                         Nodes.Add(new PostgreSqlTableGroup());
                         Nodes.Add(new PostgreSqlForeignTableGroup());
                         Nodes.Add(new PostgreSqlViewGroup());
                         Nodes.Add(new PostgreSqlMaterializedViewGroup());
                         Nodes.Add(new PostgreSqlFunctionGroup());
                     }
                }
            }
        }

        private static bool SchemaGroupHasChildren(NpgsqlConnection conn, string schemaName, string objectType)
        {
            if (conn == null || conn.State != ConnectionState.Open)
            {
                Console.WriteLine("DEBUG: SchemaGroupHasChildren returning TRUE (Connection not open or null)");
                return true;
            }

            string query;
            switch (objectType)
            {
                case "BASE TABLE":          query = "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE' LIMIT 1"; break;
                case "VIEW":                query = "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'VIEW' LIMIT 1"; break;
                case "FOREIGN TABLE":       query = "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'FOREIGN TABLE' LIMIT 1"; break;
                case "MATERIALIZED VIEW":   query = "SELECT 1 FROM pg_catalog.pg_matviews WHERE schemaname = $1 LIMIT 1"; break;
                case "FUNCTION":            query = "SELECT 1 FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1 AND p.prokind = 'f' LIMIT 1"; break;
                default:
                     Console.WriteLine($"DEBUG: SchemaGroupHasChildren returning TRUE (Unknown objectType: {objectType})");
                     return true;
            }

            try
            {
                using (var cmd = new NpgsqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue(schemaName);

                    var result = cmd.ExecuteScalar();
                    var hasChildren = (result != null && result != DBNull.Value);

                    return hasChildren;
                }
            }
            catch (Exception ex)
            {
                var errorMsg = $"DEBUG: EXCEPTION checking {objectType} for schema '{schemaName}':\n\n{ex.Message}";
                Console.WriteLine(errorMsg + "\n" + ex.StackTrace);

                return false;
            }
        }

        public ContextMenuStrip GetMenu()
        {
            var menuList = new ContextMenuStrip { ShowImageMargin = false };
            menuList.Items.Add(new ToolStripButton("Refresh", null, (s, e) => { Refresh(); }));
            GetDbConnect();
            return menuList;
        }

        private PostgreSqlConnect GetDbConnect()
        {
            return Parent as PostgreSqlConnect;
        }
    }
}