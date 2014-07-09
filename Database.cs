using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Web;
using System.Text;
using System.Data.Common;

public sealed class Database : IDisposable
{
    #region Variables

    private IDbConnection Connection;
    private IDbCommand Command;
    private IDbTransaction Transaction;

    private static Regex rxParams = new Regex(@"(?<!@)@\w+", RegexOptions.Compiled);
    private static Func<IDataRecord, dynamic> binderDefault = x =>
    {
        dynamic resultBinder = new ExpandoObject();

        for (int i = 0; i < x.FieldCount; i++)
            ((IDictionary<string, object>)resultBinder).Add(x.GetName(i).ToLower(), x.IsDBNull(i) ? string.Empty : x.GetValue(i));

        return resultBinder;
    };


    #endregion

    #region Constructor e Dispose

    public Database(string key)
    {
        this.Connection = this.CreateDbConnection(key);

        if (this.Connection == null)
            new InvalidOperationException("Cannot open a connection without specifying a data source or server.");
    }

    public void Dispose()
    {
        this.Close();
    }

    #endregion

    #region Conection

    public void Open(bool useTransaction = false)
    {
        if (this.Connection.State != ConnectionState.Open)
            this.Connection.Open();
    }

    public void Close()
    {
        if (this.Connection.State != ConnectionState.Closed)
        {
            this.Command.Dispose();
            this.Command = null;

            this.Transaction.Rollback();
            this.Transaction.Dispose();
            this.Transaction = null;

            this.Connection.Close();
            this.Connection.Dispose();
            this.Connection = null;
        }
    }

    #endregion

    #region Transaction

    public void BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        if (this.Transaction == null)
            this.Transaction = this.Connection.BeginTransaction(isolationLevel);
    }

    public void Commit()
    {
        if (this.Transaction != null)
            this.Transaction.Commit();
    }

    public void Rollback()
    {
        if (this.Transaction != null)
            this.Transaction.Rollback();
    }

    #endregion

    #region Public Methods

    public dynamic All(string tableName)
    {
        if (string.IsNullOrEmpty(tableName))
            throw new ArgumentNullException("tableName");

        string sql = string.Format("SELECT * FROM [{0}]", tableName);

        IEnumerable<dynamic> result = SelectMethod(sql, null, binderDefault);

        return result.ToList();
    }

    public List<T> All<T>(string tableName, Func<IDataRecord, T> binder = null)
    {
        if (string.IsNullOrEmpty(tableName))
            throw new ArgumentNullException("tableName");

        string sql = string.Format("SELECT * FROM [{0}]", tableName);

        IEnumerable<T> result = SelectMethod<T>(sql, null, binder);

        return result.ToList();
    }

    public int Count(string tableName, dynamic dynamicObject = null)
    {
        if (string.IsNullOrEmpty(tableName))
            throw new ArgumentNullException("tableName");

        string sql = string.Format("SELECT COUNT(0) FROM [{0}] ", tableName);

        if (dynamicObject != null && ((IDictionary<string, object>)dynamicObject).Count > 0)
        {
            sql += this.GenerateWhere(dynamicObject);

            this.PrepareCommand(sql);

            MatchCollection matches = rxParams.Matches(sql);

            if (matches != null && matches.Count > 0)
            {
                foreach (Match match in matches)
                    this.AddParameter(match.Value, dynamicObject);
            }
        }
        else
            this.PrepareCommand(sql);

        int rowsCount;

        rowsCount = Convert.ToInt32(this.Command.ExecuteScalar());

        this.Command.Dispose();
        this.Command = null;

        return rowsCount;
    }

    public int Count(string tableName, string condition, dynamic dynamicObject = null)
    {
        if (string.IsNullOrEmpty(tableName))
            throw new ArgumentNullException("tableName");

        if (string.IsNullOrEmpty(condition))
            throw new ArgumentNullException("condition");

        string sql = string.Format("SELECT COUNT(0) FROM [{0}] WHERE {1}", tableName, condition);

        if (dynamicObject == null || ((IDictionary<string, object>)dynamicObject).Count == 0)
            throw new ArgumentNullException("dynamicObject");

        this.PrepareCommand(sql);

        MatchCollection matches = rxParams.Matches(sql);

        if (matches != null && matches.Count > 0)
        {
            foreach (Match match in matches)
                this.AddParameter(match.Value, dynamicObject);
        }

        int rowsCount;

        rowsCount = Convert.ToInt32(this.Command.ExecuteScalar());

        this.Command.Dispose();
        this.Command = null;

        return rowsCount;
    }

    public dynamic Find(string tableName, dynamic dynamicObject)
    {
        if (string.IsNullOrEmpty(tableName))
            throw new ArgumentNullException("tableName");

        string sql = string.Format("SELECT * FROM [{0}] ", tableName);

        if (dynamicObject == null || ((IDictionary<string, object>)dynamicObject).Count == 0)
            throw new ArgumentNullException("dynamicObject");

        sql += this.GenerateWhere(dynamicObject);

        IEnumerable<dynamic> result = SelectMethod(sql, dynamicObject, binderDefault);

        return result.First();
    }

    public dynamic Find(string tableName, string condition, dynamic dynamicObject)
    {
        if (string.IsNullOrEmpty(tableName))
            throw new ArgumentNullException("tableName");

        if (string.IsNullOrEmpty(tableName))
            throw new ArgumentNullException("condition");

        string sql = string.Format("SELECT * FROM [{0}] WHERE {1}", tableName, condition);

        if (dynamicObject == null || ((IDictionary<string, object>)dynamicObject).Count == 0)
            throw new ArgumentNullException("dynamicObject");

        IEnumerable<dynamic> result = SelectMethod(sql, dynamicObject, binderDefault);

        return result.First();
    }

    public T Find<T>(string tableName, dynamic dynamicObject, Func<IDataRecord, T> binder = null)
    {
        if (string.IsNullOrEmpty(tableName))
            throw new ArgumentNullException("tableName");

        string sql = string.Format("SELECT * FROM [{0}] ", tableName);

        if (dynamicObject == null || ((IDictionary<string, object>)dynamicObject).Count == 0)
            throw new ArgumentNullException("dynamicObject");

        sql += this.GenerateWhere(dynamicObject);

        IEnumerable<dynamic> result = SelectMethod(sql, dynamicObject, binder);

        return result.First();
    }

    public T Find<T>(string tableName, string condition, dynamic dynamicObject, Func<IDataRecord, T> binder = null)
    {
        if (string.IsNullOrEmpty(tableName))
            throw new ArgumentNullException("tableName");

        if (string.IsNullOrEmpty(tableName))
            throw new ArgumentNullException("condition");

        string sql = string.Format("SELECT * FROM [{0}] WHERE {1}", tableName, condition);

        if (dynamicObject == null || ((IDictionary<string, object>)dynamicObject).Count == 0)
            throw new ArgumentNullException("dynamicObject");

        IEnumerable<dynamic> result = SelectMethod(sql, dynamicObject, binder);

        return result.First();
    }

    public dynamic Select(string commandText, dynamic dynamicObject = null)
    {
        IEnumerable<dynamic> result = SelectMethod(commandText, dynamicObject, binderDefault);

        return result.ToList();
    }

    public List<T> Select<T>(string commandText, dynamic dynamicObject = null, Func<IDataRecord, T> binder = null)
    {
        IEnumerable<T> result = SelectMethod(commandText, dynamicObject, binder);

        return result.ToList();
    }

    public int Insert(string commandText, dynamic dynamicObject, bool returnIdentity = true)
    {
        return this.Execute(commandText, dynamicObject, returnIdentity);
    }

    public int Update(string commandText, dynamic dynamicObject)
    {
        return this.Execute(commandText, dynamicObject, false);
    }

    public int Delete(string commandText, dynamic dynamicObject = null)
    {
        return this.Execute(commandText, dynamicObject, false);
    }
    
    #endregion

    #region Private Methods

    private DbConnection CreateDbConnection(string key)
    {
        DbConnection connection = null;

        if (string.IsNullOrEmpty(key))
            throw new ArgumentNullException("key");

        if (ConfigurationManager.ConnectionStrings[key] != null)
        {
            try
            {
                DbProviderFactory factory = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings[key].ProviderName);

                connection = factory.CreateConnection();
                connection.ConnectionString = ConfigurationManager.ConnectionStrings[key].ConnectionString;
            }
            catch
            {
                if (connection != null)
                    connection = null;
            }
        }
        else
            throw new ArgumentOutOfRangeException("key");

        return connection;
    }

    private IEnumerable<T> SelectMethod<T>(string commandText, dynamic dynamicObject, Func<IDataRecord, T> binder)
    {
        if (string.IsNullOrEmpty(commandText))
            throw new ArgumentNullException("commandText");

        MatchCollection matches = rxParams.Matches(commandText);

        this.PrepareCommand(commandText);

        if (matches != null && matches.Count > 0)
        {
            if (dynamicObject == null || ((IDictionary<string, object>)dynamicObject).Count == 0)
                throw new ArgumentNullException("dynamicObject");

            foreach (Match match in matches)
                this.AddParameter(match.Value, dynamicObject);
        }

        IDataReader reader = this.Command.ExecuteReader();

        if (!reader.IsClosed)
        {
            try
            {
                while (reader.Read())
                    yield return binder(reader);
            }
            finally
            {
                reader.Close();
                reader.Dispose();
                reader = null;

                this.Command.Dispose();
                this.Command = null;
            }
        }
    }

    private int Execute(string commandText, dynamic dynamicObject = null, bool returnIdentity = false)
    {
        if (string.IsNullOrEmpty(commandText))
            throw new ArgumentNullException("commandText");

        MatchCollection matches = rxParams.Matches(commandText);

        if (returnIdentity)
            commandText += " SELECT @@IDENTITY";

        this.PrepareCommand(commandText);

        if (matches != null && matches.Count > 0)
        {
            if (dynamicObject == null || ((IDictionary<string, object>)dynamicObject).Count == 0)
                throw new ArgumentNullException("dynamicObject");

            foreach (Match match in matches)
                this.AddParameter(match.Value, dynamicObject);
        }

        int rowsAffected;

        if (returnIdentity)
            rowsAffected = Convert.ToInt32(this.Command.ExecuteScalar());
        else
            rowsAffected = this.Command.ExecuteNonQuery();

        this.Command.Dispose();
        this.Command = null;

        return rowsAffected;
    }

    private void PrepareCommand(string commandText, CommandType commandType = CommandType.Text)
    {
        if (string.IsNullOrEmpty(commandText))
            throw new ArgumentNullException("commandText");

        this.Command = this.Connection.CreateCommand();

        this.Command.CommandText = commandText;
        this.Command.CommandType = commandType;
    }

    private void AddParameter(string parameterName, dynamic dynamicObject)
    {
        IDictionary<string, object> values = (IDictionary<string, object>)dynamicObject;
        object value;

        if (values.ContainsKey(parameterName.Substring(1)))
            value = values[parameterName.Substring(1)];
        else
            throw new ArgumentOutOfRangeException(parameterName);

        IDbDataParameter parameter = this.Command.CreateParameter();
        parameter.ParameterName = parameterName;
        parameter.Value = value;

        this.Command.Parameters.Add(parameter);
    }

    private string GenerateWhere(dynamic dynamicObject)
    {
        if (dynamicObject != null && ((IDictionary<string, object>)dynamicObject).Count > 0)
        {
            StringBuilder sbWhere = new StringBuilder();

            foreach (KeyValuePair<string, object> item in dynamicObject)
                sbWhere.Append((sbWhere.Length == 0 ? string.Format("{0} = @{0}", item.Key) : string.Format(" AND {0} = @{0}", item.Key)));

            if (sbWhere.Length > 0)
                sbWhere.Insert(0, "WHERE ");

            return sbWhere.ToString();
        }

        return string.Empty;
    }

    #endregion
}
