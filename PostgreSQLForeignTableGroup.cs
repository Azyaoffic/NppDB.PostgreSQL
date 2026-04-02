using System.Windows.Forms;
using Npgsql;

namespace NppDB.PostgreSQL
{
    public class PostgreSqlForeignTableGroup : PostgreSqlTableGroup
    {
        public PostgreSqlForeignTableGroup()
        {
            Query = "SELECT c.relname AS table_name FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '{0}' AND c.relkind = 'f' ORDER BY table_name";
            Text = "Foreign Tables";
            SelectedImageKey = ImageKey = "Group";
        }

        protected override TreeNode CreateTreeNode(NpgsqlDataReader reader)
        {
            var tableNode = new PostgreSqlTable
            {
                Text = reader["table_name"].ToString(),
                TypeName = "FOREIGN TABLE"
            };
            
            tableNode.SelectedImageKey = tableNode.ImageKey = "ForeignTable";

            return tableNode;
        }
    }
}