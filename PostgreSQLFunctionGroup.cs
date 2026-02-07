using System.Windows.Forms;
using Npgsql;

namespace NppDB.PostgreSQL
{
    internal class PostgreSqlFunctionGroup : PostgreSqlTableGroup
    {
        public PostgreSqlFunctionGroup()
        {
            Query = "select p.proname as function_name, p.oid as function_oid " + "from pg_proc p " +
                    "left join pg_namespace n on p.pronamespace = n.oid " +
                    "where n.nspname = '{0}' and p.prokind = 'f' " + "order by function_name;";
            Text = "Functions";
        }

        protected override TreeNode CreateTreeNode(NpgsqlDataReader reader)
        {
            var functionNode = new PostgreSQLView
            {
                Text = reader["function_name"].ToString(),
                FuncOid = reader["function_oid"].ToString(),
                TypeName = "FUNCTION"
            };
            
            functionNode.SelectedImageKey = functionNode.ImageKey = "Function";

            return functionNode;
        }

        protected override bool NodeHasChildrenCheck(NpgsqlConnection conn, string schemaName, string functionName)
        {
            return true;
        }
    }
}