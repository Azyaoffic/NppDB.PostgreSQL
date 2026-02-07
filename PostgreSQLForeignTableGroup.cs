using System.Windows.Forms;
using Npgsql;

namespace NppDB.PostgreSQL
{
    public class PostgreSqlForeignTableGroup : PostgreSqlTableGroup
    {
        public PostgreSqlForeignTableGroup()
        {
            Query = "SELECT table_name FROM information_schema.tables WHERE table_schema='{0}' AND table_type = 'FOREIGN TABLE' ORDER BY table_name";
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