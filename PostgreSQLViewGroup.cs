using System.Windows.Forms;
using Npgsql;

namespace NppDB.PostgreSQL
{
    internal class PostgreSqlViewGroup : PostgreSqlTableGroup
    {
        public PostgreSqlViewGroup()
        {
            Query = "SELECT table_name " +
                    "FROM information_schema.tables tbls " +
                    "WHERE table_schema='{0}' " +
                    "AND table_type='VIEW' " +
                    "ORDER BY table_name;";
            Text = "Views";
        }

        protected override TreeNode CreateTreeNode(NpgsqlDataReader reader)
        {
            var viewNode = new PostgreSQLView
            {
                Text = reader["table_name"].ToString(),
                TypeName = "VIEW"
            };

            viewNode.SelectedImageKey = viewNode.ImageKey = "View";

            return viewNode;
        }
    }
}