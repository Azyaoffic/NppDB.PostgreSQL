using System.Windows.Forms;
using Npgsql;

namespace NppDB.PostgreSQL
{
    internal class PostgreSqlMaterializedViewGroup : PostgreSqlTableGroup
    {
        public PostgreSqlMaterializedViewGroup()
        {
            Query = "SELECT c.relname AS table_name " +
                    "FROM pg_namespace AS n " +
                    "INNER JOIN pg_class AS c " +
                    "ON n.oid=c.relnamespace " +
                    "WHERE relkind='m' " + "AND n.nspname = '{0}' " +
                    "ORDER BY table_name;";
            Text = "Materialized Views";
        }

        protected override TreeNode CreateTreeNode(NpgsqlDataReader reader)
        {
            var matViewNode = new PostgreSQLView
            {
                Text = reader["table_name"].ToString(),
                TypeName = "MATERIALIZED VIEW"
            };
            
            matViewNode.SelectedImageKey = matViewNode.ImageKey = "MaterializedView";

            return matViewNode;
        }
    }
}