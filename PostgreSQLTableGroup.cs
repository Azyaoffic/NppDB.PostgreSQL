using System;
using System.Windows.Forms;
using Npgsql;
using NppDB.Comm;

namespace NppDB.PostgreSQL
{
    public class PostgreSqlTableGroup : TreeNode, IRefreshable, IMenuProvider
    {
        public PostgreSqlTableGroup()
        {
            Query = "SELECT table_name FROM information_schema.tables WHERE table_schema='{0}' AND table_type in ('BASE TABLE') ORDER BY table_name";
            Text = "Base Tables";
            SelectedImageKey = ImageKey = "Group";
        }

        protected string Query { get; set; }

        protected virtual TreeNode CreateTreeNode(NpgsqlDataReader reader)
        {
            var tableNode = new PostgreSqlTable
            {
                Text = reader["table_name"].ToString()
            };
            return tableNode;
        }

        public void Refresh()
        {
            if (!(Parent is PostgreSqlSchema schemaNode)) return;
            if (!(schemaNode.Parent is PostgreSqlConnect connNode)) return;

            using (var cnn = connNode.GetConnection())
            {
                TreeView.Cursor = Cursors.WaitCursor;
                TreeView.Enabled = false;
                try
                {
                    cnn.Open();
                    Nodes.Clear();
                    var formattedQuery = string.Format(Query, schemaNode.Text);
                    using (var command = new NpgsqlCommand(formattedQuery, cnn))
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var childNode = CreateTreeNode(reader);
                                var childName = childNode.Text;

                                if (NodeHasChildrenCheck(cnn, schemaNode.Text, childName))
                                {
                                    childNode.Nodes.Add(new TreeNode(""));
                                }

                                Nodes.Add(childNode);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    MessageBox.Show(ex.Message, @"Exception");
                }
                finally
                {
                    TreeView.Enabled = true;
                    TreeView.Cursor = null;
                }
            }
        }

        protected virtual bool NodeHasChildrenCheck(NpgsqlConnection conn, string schemaName, string tableOrViewName)
        {
            const string query = "SELECT 1 FROM information_schema.columns WHERE table_schema = @schema AND table_name = @table LIMIT 1";
            try
            {
                using (var cmd = new NpgsqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue("@schema", schemaName);
                    cmd.Parameters.AddWithValue("@table", tableOrViewName);
                    var result = cmd.ExecuteScalar();
                    return (result != null && result != DBNull.Value);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error checking columns for {schemaName}.{tableOrViewName}: {ex.Message}");
                return true;
            }
        }

        public ContextMenuStrip GetMenu()
        {
            var menuList = new ContextMenuStrip { ShowImageMargin = false };
             menuList.Items.Add(new ToolStripButton("Refresh", null, (s, e) => { Refresh(); }));
             return menuList;
        }
    }
}