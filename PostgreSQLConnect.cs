using System;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Windows.Forms;
using System.Xml.Serialization;
using Npgsql;
using NppDB.Comm;
using NppDB.PostgreSQL.Properties;

namespace NppDB.PostgreSQL
{
    [XmlRoot]
    [ConnectAttr(Id = "PostgreSQLConnect", Title = "PostgreSQL")]
    public class PostgreSqlConnect : TreeNode, IDbConnect, IRefreshable, IMenuProvider, IIconProvider, INppDBCommandClient
    {
        [XmlElement]
        public string Title { set => Text = value; get => Text; }
        [XmlElement]
        public string ServerAddress { set; get; }
        public string Account { set; get; }
        public string Port { set; get; }
        public string Database { set; get; }
        public string ConnectionName { set; get; }
        [XmlIgnore]
        public string Password { set; get; }
        private NpgsqlConnection _connection;
        private List<PostgreSqlExecutor> Executors { set; get; }
        private string _serverVersion;

        public PostgreSqlConnect()
        {
            AppContext.SetSwitch("Npgsql.EnableSqlRewriting", false);
            Executors = new List<PostgreSqlExecutor>();
        }

        public bool IsOpened => _connection != null && _connection.State == ConnectionState.Open;

        public string DatabaseSystemName =>
            !string.IsNullOrEmpty(_serverVersion) ? $"PostgreSQL {_serverVersion}" : "PostgreSQL";

        public SqlDialect Dialect => SqlDialect.POSTGRE_SQL;

        [XmlIgnore]
        public INppDbCommandHost CommandHost { get; set; }

        public void SetCommandHost(INppDbCommandHost host)
        {
            CommandHost = host;
        }

        public void Attach()
        {
            var id = CommandHost.Execute(NppDbCommandType.GET_ATTACHED_BUFFER_ID, null);
            if (id != null)
            {
                CommandHost.Execute(NppDbCommandType.NEW_FILE, null);
                CommandHost.Execute(NppDbCommandType.SET_SQL_LANGUAGE, null);
            }
            id = CommandHost.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
            CommandHost.Execute(NppDbCommandType.CREATE_RESULT_VIEW, new[] { id, this, CreateSqlExecutor() });
        }

        public ISqlExecutor CreateSqlExecutor()
        {
            var executor = new PostgreSqlExecutor(GetConnection);
            Executors.Add(executor);
            return executor;
        }

        internal NpgsqlConnection GetConnection()
        {
            var builder = GetConnectionStringBuilder();
            builder["Pwd"] = Password;
            return new NpgsqlConnection(builder.ConnectionString);
        }

        public bool CheckLogin()
        {
            var dlg = new frmPostgreSQLConnect { VisiblePassword = false };
            var isEditingExisting = !string.IsNullOrEmpty(ServerAddress);
            if (isEditingExisting)
            {
                dlg.ConnectionName = ConnectionName;
                dlg.Server = ServerAddress;
                dlg.Port = Port;
                dlg.Database = Database;
                dlg.FocusPassword();
                dlg.SetConnNameVisible(true);
            }
            else
            {
                dlg.SetConnNameVisible(true);
            }
            
            var dialogResult = dlg.ShowDialog();
            if (dialogResult != DialogResult.OK)
            {
                return false;
            }

            ConnectionName = dlg.ConnectionName;
            ServerAddress = dlg.Server;
            Port = dlg.Port;
            Database = dlg.Database;
            Account = dlg.Username;
            Password = dlg.Password;
            Title = !string.IsNullOrEmpty(ConnectionName) ? ConnectionName : GetDefaultTitle();

            return true;
        }

        public void Connect()
        {
            if (_connection == null) _connection = new NpgsqlConnection();

            var curConnStrBuilder = GetConnectionStringBuilder();

            var needsConnectionStringUpdate = string.IsNullOrEmpty(_connection.ConnectionString);
            if (!needsConnectionStringUpdate)
            {
                try {
                    var existingBuilder = new NpgsqlConnectionStringBuilder(_connection.ConnectionString);
                    needsConnectionStringUpdate = existingBuilder.Host != curConnStrBuilder.Host ||
                                                existingBuilder.Port != curConnStrBuilder.Port ||
                                                existingBuilder.Database != curConnStrBuilder.Database ||
                                                existingBuilder.Username != curConnStrBuilder.Username;
                } catch {
                    needsConnectionStringUpdate = true;
                }
            }

            if (needsConnectionStringUpdate)
            {
                curConnStrBuilder["Password"] = Password;
                _connection.ConnectionString = curConnStrBuilder.ConnectionString;
                _serverVersion = null;
            }

            if (_connection.State == ConnectionState.Open)
            {
                if (_serverVersion == null) FetchServerVersionInternal();
                 return;
            }

            try
            {
                _connection.Open();
                FetchServerVersionInternal();
            }
            catch (Exception ex)
            {
                _serverVersion = null;
                throw new ApplicationException("connect fail", ex);
            }
        }

        private void FetchServerVersionInternal()
        {
            if (_connection == null || _connection.State != ConnectionState.Open)
            {
                _serverVersion = null;
                return;
            }
            try
            {
                using (var cmd = new NpgsqlCommand("SHOW server_version;", _connection))
                {
                    var versionResult = cmd.ExecuteScalar();
                    _serverVersion = versionResult?.ToString();
                    Console.WriteLine($@"Fetched PostgreSQL Version: {_serverVersion}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($@"Error fetching PostgreSQL version: {ex.Message}");
                 _serverVersion = null;
            }
        }

        internal NpgsqlConnectionStringBuilder GetConnectionStringBuilder()
        {
            var builder = new NpgsqlConnectionStringBuilder
            {
                Username = Account,
                Host = ServerAddress,
                Port = int.Parse(Port),
                Database = Database,
                IncludeErrorDetail = true,
            };
            return builder;
        }

        public string ConnectAndAttach()
        {
            if (IsOpened) return "CONTINUE";

            if (!CheckLogin())
            {
                return "FAIL";
            }

            try
            {
                Connect();
                Attach();
                Refresh();
                return "FRESH_NODES";
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message + (ex.InnerException != null ? " : " + ex.InnerException.Message : ""));
            }
            return "FAIL";
        }

        public void Disconnect()
        {
            foreach (var executor in Executors)
            {
                executor.Stop();
            }
            Executors.Clear();
            NpgsqlConnection.ClearAllPools();
            _serverVersion = null;
            if (_connection == null || _connection.State == ConnectionState.Closed) return;

            _connection.Close();
        }

        public string GetDefaultTitle()
        {
            return !string.IsNullOrEmpty(ConnectionName) ? ConnectionName : !string.IsNullOrEmpty(Database) ? Database : "Untitled";
        }

        public Bitmap GetIcon()
        {
            return Resources.PostgreSQL;
        }

        public ContextMenuStrip GetMenu()
        {
            var menuList = new ContextMenuStrip { ShowImageMargin = false };
            var connect = this;
            var host = CommandHost;
            if (host != null)
            {
                menuList.Items.Add(new ToolStripButton("Open a new query window", null, (s, e) =>
                {
                    try
                    {
                        host.Execute(NppDbCommandType.NEW_FILE, null);
                        host.Execute(NppDbCommandType.SET_SQL_LANGUAGE, null);
                        var idObj = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                        if (idObj == null) return;
                        var bufferId = (IntPtr)idObj;
                        host.Execute(NppDbCommandType.CREATE_RESULT_VIEW, new object[] { bufferId, connect, CreateSqlExecutor() });
                    }
                    catch (Exception ex) { Console.WriteLine($@"Error in 'Open new query': {ex.Message}"); }
                }));

                if (host.Execute(NppDbCommandType.GET_ATTACHED_BUFFER_ID, null) == null)
                {
                    menuList.Items.Add(new ToolStripButton("Attach to the open query window", null, (s, e) =>
                    {
                        try
                        {
                            var idObj = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                            if (idObj == null) { Console.WriteLine(@"Attach failed: Could not get Activated Buffer ID."); return; }
                            var bufferId = (IntPtr)idObj;

                            host.Execute(NppDbCommandType.SET_SQL_LANGUAGE, null);

                            host.Execute(NppDbCommandType.CREATE_RESULT_VIEW, new object[] { bufferId, connect, CreateSqlExecutor() });
                        }
                        catch (Exception attachEx) { Console.WriteLine($@"Error during Attach: {attachEx.Message}"); }
                    }));
                }
                else
                {
                     menuList.Items.Add(new ToolStripButton("Detach from the query window", null, (s, e) => { try { host.Execute(NppDbCommandType.DESTROY_RESULT_VIEW, null); } catch (Exception ex) { Console.WriteLine($@"Error during Detach: {ex.Message}"); } }));
                }
                menuList.Items.Add(new ToolStripSeparator());
            }

            menuList.Items.Add(new ToolStripButton("Refresh the database connection", null, (s, e) => { try { Refresh(); } catch (Exception ex) { Console.WriteLine($@"Error during Refresh: {ex.Message}"); } }));
            return menuList;
        }

        public void Refresh()
        {
            if (IsOpened && _serverVersion == null)
            {
                FetchServerVersionInternal();
            }

            using (var conn = GetConnection())
            {
                TreeView.Cursor = Cursors.WaitCursor;
                TreeView.Enabled = false;
                try
                {
                    conn.Open();
                    Nodes.Clear();
                    Console.WriteLine(@"addschemas");
                    AddSchemas(conn);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    Nodes.Clear();
                }
                finally
                {
                    conn.Close();
                    TreeView.Enabled = true;
                    TreeView.Cursor = null;
                }
            }
        }

        internal List<string> GetForeignSchemas(NpgsqlConnection conn)
        {
            var result = new List<string>();
            const string query = "SELECT DISTINCT foreign_table_schema FROM information_schema.foreign_tables;";
            using (var command = new NpgsqlCommand(query, conn))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schemaName = reader["foreign_table_schema"].ToString();
                        result.Add(schemaName);
                    }
                }
            }
            return result;
        }

        internal void AddSchemas(NpgsqlConnection conn)
        {
            const string query = "SELECT nspname FROM pg_namespace ORDER BY nspname;";
            var foreignSchemas = GetForeignSchemas(conn);

            using (var command = new NpgsqlCommand(query, conn))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schemaName = reader["nspname"].ToString();

                        var dbSchemaNode = new PostgreSqlSchema
                        {
                            Text = schemaName,
                            Schema = schemaName,
                            Foreign = foreignSchemas.Contains(schemaName)
                        };

                        dbSchemaNode.Nodes.Add(new TreeNode(""));

                        Nodes.Add(dbSchemaNode);
                    }
                }
            }
        }

        public void Reset()
        {
            Database = ""; ServerAddress = ""; Account = ""; Port = "";
            Password = "";
            Disconnect();
            _connection = null;
        }
    }
}
