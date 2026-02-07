namespace NppDB.PostgreSQL
{
    internal class PostgreSQLView: PostgreSqlTable
    {
        public PostgreSQLView()
        {
            TypeName = "VIEW";
            SelectedImageKey = ImageKey = "View";
        }
    }
}
