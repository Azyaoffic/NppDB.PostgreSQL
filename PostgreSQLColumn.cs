using System.Drawing;
using System.Windows.Forms;
using NppDB.Comm;

namespace NppDB.PostgreSQL
{
    public class PostgreSqlColumn : TreeNode, IMenuProvider
    {
        private readonly int _nodeType;

        public string ColumnName { get; }
        public string ColumnType { get; }

        public PostgreSqlColumn(string columnName, string columnType, int type, int options)
        {
            NodeFont = new Font("Consolas", 8F, FontStyle.Regular);

            ColumnName = columnName;
            ColumnType = columnType;
            _nodeType = type;

            AdjustColumnNameFixedWidth(0);

            switch (type)
            {
                case 1:
                    SelectedImageKey = ImageKey = "Primary_Key";
                    break;
                case 2:
                    SelectedImageKey = ImageKey = "Foreign_Key";
                    break;
                case 3:
                    SelectedImageKey = ImageKey = "Index";
                    break;
                case 4:
                    SelectedImageKey = ImageKey = "Unique_Index";
                    break;
                default:
                    // FK, PK, Indexed, Not Null
                    SelectedImageKey = ImageKey = $"Column_{options:0000}";
                    break;
            }
        }

        public void AdjustColumnNameFixedWidth(int fixedWidth)
        {
            Text = ColumnName.PadRight(fixedWidth) + (string.IsNullOrEmpty(ColumnType) ? "" : "  " + ColumnType);
        }

        public ContextMenuStrip GetMenu()
        {
            var menuList = new ContextMenuStrip { ShowImageMargin = false };
            var connect = GetDbConnect();
            menuList.Items.Add(new ToolStripSeparator());

            if (connect?.CommandHost == null) return menuList;

            // only real columns and not constraints/indexes
            if (_nodeType != 0) return menuList;

            var tableNode = GetParentTableNode();
            if (tableNode == null) return menuList;

            if (tableNode.TypeName == "FUNCTION") return menuList;

            var allowAlter = !IsUnderView();
            var host = connect.CommandHost;

            var schemaName = GetSchemaName();
            var tableQuoted = $"{QuotePostgreSql(schemaName)}.{QuotePostgreSql(tableNode.Text)}";
            var columnQuoted = QuotePostgreSql(ColumnName);

            menuList.Items.Add(new ToolStripButton("Select distinct values", null, (s, e) =>
                {
                    host.Execute(NppDbCommandType.NEW_FILE, null);
                    var query = $"SELECT DISTINCT {columnQuoted} FROM {tableQuoted} ORDER BY {columnQuoted};";
                    host.Execute(NppDbCommandType.APPEND_TO_CURRENT_VIEW, new object[] { query });
                }
            ));

            menuList.Items.Add(new ToolStripSeparator());

            menuList.Items.Add(new ToolStripButton("Create ALTER COLUMN query", null, (s, e) =>
                {
                    host.Execute(NppDbCommandType.NEW_FILE, null);
                    var query = $"ALTER TABLE {tableQuoted} ALTER COLUMN {columnQuoted} TYPE <DATA_TYPE>;";
                    host.Execute(NppDbCommandType.APPEND_TO_CURRENT_VIEW, new object[] { query });
                })
                { Enabled = allowAlter });

            menuList.Items.Add(new ToolStripButton("Create DROP COLUMN query", null, (s, e) =>
                {
                    host.Execute(NppDbCommandType.NEW_FILE, null);
                    var query = $"ALTER TABLE {tableQuoted} DROP COLUMN {columnQuoted};";
                    host.Execute(NppDbCommandType.APPEND_TO_CURRENT_VIEW, new object[] { query });
                })
                { Enabled = allowAlter });

            return menuList;
        }

        private PostgreSqlConnect GetDbConnect()
        {
            TreeNode n = this;
            while (n.Parent != null) n = n.Parent;
            return n as PostgreSqlConnect;
        }

        private static string QuotePostgreSql(string name)
        {
            return $"\"{(name ?? string.Empty).Replace("\"", "\"\"")}\"";
        }

        private PostgreSqlTable GetParentTableNode()
        {
            TreeNode n = this;
            while (n != null)
            {
                if (n is PostgreSqlTable t) return t;
                n = n.Parent;
            }
            return null;
        }

        private PostgreSqlSchema GetParentSchemaNode()
        {
            TreeNode n = this;
            while (n != null)
            {
                if (n is PostgreSqlSchema s) return s;
                n = n.Parent;
            }
            return null;
        }

        private string GetSchemaName()
        {
            var schemaNode = GetParentSchemaNode();
            if (schemaNode == null) return "public";
            return string.IsNullOrEmpty(schemaNode.Schema) ? schemaNode.Text : schemaNode.Schema;
        }

        private bool IsUnderView()
        {
            TreeNode n = this;
            while (n != null)
            {
                if (n is PostgreSqlTable t)
                {
                    // allow TABLE and FOREIGN_TABLE; disable for views/materialized views/functions
                    return (t.TypeName != "TABLE" && t.TypeName != "FOREIGN_TABLE");
                }
                n = n.Parent;
            }
            return false;
        }
    }
}
