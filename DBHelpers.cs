using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace DBHelper
{
    public class DbHelpers
    {
        #region members


        private DbConnection _connection;
        private string _connectionstring = "";
        private DbProviderFactory _factory = null;
        private TransactionScope _transactionScope;
        #endregion members

        #region properties



        public DbConnection connection
        {
            get { return _connection; }
        }

        public string connectionstring
        {
            get { return _connectionstring; }
        }

        #endregion properties

        public void Close()
        {
            if (_connection.State == ConnectionState.Open)
                _connection.Close();
        }

        public DbHelpers(string connectString, Providers providerList)
        {

            switch (providerList)
            {
                case Providers.SqlServer:
                    _factory = SqlClientFactory.Instance;
                    break;

                case Providers.OleDB:
                    _factory = OleDbFactory.Instance;
                    break;

                case Providers.ODBC:
                    _factory = OdbcFactory.Instance;
                    break;


            }

            _connectionstring = connectString;
            _connection = CreateConnection();


        }

        public DbHelpers(string connectionStringName)
        {
            ConnectionStringSettings css = ConfigurationManager.ConnectionStrings[connectionStringName];

            if (css == null)
                throw new ArgumentException("The connection string you specified does not exist in your configuration file.");

            _factory = DbProviderFactories.GetFactory(css.ProviderName);
            _connectionstring = css.ConnectionString;
            _connection = CreateConnection();

        }


        public TransactionScope CreateTransactionScope()
        {
            return _transactionScope = new TransactionScope();
        }

        public DbConnection CreateConnection()
        {
            DbConnection connection = _factory.CreateConnection();
            connection.ConnectionString = _connectionstring;
            return connection;
        }

        public DbCommand CreateCommand(string query, CommandType commandtype)
        {
            DbCommand command = _factory.CreateCommand();
            command.CommandText = query;
            command.CommandType = commandtype;
            command.Connection = _connection;
            return command;
        }
        public DbCommand CreateCommand(string query, CommandType commandtype, DbConnection connection)
        {
            DbCommand command = _factory.CreateCommand();
            command.CommandText = query;
            command.CommandType = commandtype;
            command.Connection = connection;
            return command;
        }

        public int AddParameter(DbCommand command, string name, object value)
        {
            DbParameter parm = _factory.CreateParameter();
            parm.ParameterName = name;
            parm.Value = value;
            return command.Parameters.Add(parm);
        }
        public int AddParameter(DbCommand command, string name, object value, DbType tip, int size = 0)
        {
            DbParameter parm = _factory.CreateParameter();
            parm.ParameterName = name;
            parm.Value = value;
            parm.DbType = tip;
            if (size > 0)
                parm.Size = size;
            return command.Parameters.Add(parm);
        }

        public int ExecuteNonQuery(DbCommand command)
        {
            int i = -1;
            try
            {
                if (connection.State == System.Data.ConnectionState.Closed)
                {
                    connection.Open();
                }

                i = command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw (ex);
            }
            return i;
        }

        public T ExecuteScalar<T>(DbCommand command, Converter<object, T> converter)
        {

            var value = command.ExecuteScalar();
            return converter(value);
        }
        public T ExecuteScalar<T>(DbCommand command)
        {
            return ExecuteScalar<T>(command, GetTypeConverter<T>());
        }

        public DbDataReader ExecuteReader(DbCommand command)
        {
            DbDataReader reader = null;
            try
            {
                if (connection.State == System.Data.ConnectionState.Closed)
                {
                    connection.Open();
                }

                reader = connection.State == System.Data.ConnectionState.Open ? command.ExecuteReader(CommandBehavior.CloseConnection) : command.ExecuteReader();
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                command.Parameters.Clear();
            }

            return reader;
        }
        DbDataReader ExecuteReader(DbCommand command, DbConnection connection)
        {
            DbDataReader reader = null;
            try
            {
                if (connection.State == System.Data.ConnectionState.Closed)
                {
                    connection.Open();
                }

                reader = connection.State == System.Data.ConnectionState.Open ? command.ExecuteReader(CommandBehavior.CloseConnection) : command.ExecuteReader();
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                command.Parameters.Clear();
            }

            return reader;
        }

        public DataTable ExecuteDataTable(DbCommand command, int startRecord, int maxRecords)
        {
            DbDataAdapter adapter = _factory.CreateDataAdapter();
            adapter.SelectCommand = command;

            DataTable dt = new DataTable();

            try
            {
                if (startRecord >= 0 || maxRecords >= 0)
                    adapter.Fill(startRecord, maxRecords, dt);
                else
                    adapter.Fill(dt);
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                command.Parameters.Clear();
                command.CommandText = "";
            }
            return dt;
        }
        public DataTable ExecuteDataTable(DbCommand command)
        {
            return ExecuteDataTable(command, 0, 0);
        }

        public DataSet ExecuteDataSet(DbCommand command, int startRecord, int maxRecords)
        {
            DbDataAdapter adapter = _factory.CreateDataAdapter();
            adapter.SelectCommand = command;

            DataSet ds = new DataSet();

            try
            {
                if (startRecord >= 0 || maxRecords >= 0)
                    adapter.Fill(ds, startRecord, maxRecords, Guid.NewGuid().ToString());
                else
                    adapter.Fill(ds);
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                command.Parameters.Clear();
                command.CommandText = "";
            }
            return ds;
        }
        public DataSet ExecuteDataSet(DbCommand command)
        {
            return ExecuteDataSet(command, 0, 0);
        }

        private T[] ExecuteArray<T>(DbCommand command, Converter<object, T> converter, int startRecord, int maxRecords)
        {
            List<T> list = new List<T>();

            using (DbDataReader reader = ExecuteReader(command))
            {
                FillFromReader(reader, startRecord, maxRecords, r =>
                {
                    list.Add(
                        converter(r.GetValue(0))
                    );
                });

                reader.Close();
            }

            return list.ToArray();
        }
        private T[] ExecuteArray<T>(DbCommand command, Converter<object, T> converter)
        {
            return ExecuteArray<T>(command, converter, 0, 0);
        }
        public T[] ExecuteArray<T>(DbCommand command, int startRecord, int maxRecords)
        {
            return ExecuteArray<T>(command, GetTypeConverter<T>(), startRecord, maxRecords);
        }
        public T[] ExecuteArray<T>(DbCommand command)
        {
            return ExecuteArray<T>(command, GetTypeConverter<T>());
        }

        private List<T> ExecuteList<T>(DbCommand command, Converter<DbDataReader, T> converter, int startRecord, int maxRecords)
        {
            var list = new List<T>();

            using (DbDataReader reader = ExecuteReader(command))
            {
                FillFromReader(reader, startRecord, maxRecords, r =>
                {
                    list.Add(converter(reader));
                });

                reader.Close();
            }

            return list;
        }
        public List<T> ExecuteList<T>(DbCommand command, int startRecord, int maxRecords)
            where T : new()
        {
            var converter = GetDataReaderConverter<T>();
            return ExecuteList<T>(command, converter, startRecord, maxRecords);
        }
        public List<T> ExecuteList<T>(DbCommand command, Converter<DbDataReader, T> converter)
        {
            return ExecuteList<T>(command, converter,0,0);
        }
        public List<T> ExecuteList<T>(DbCommand command)
            where T : new()
        {
            var converter = GetDataReaderConverter<T>();
            return ExecuteList<T>(command, converter, 0, 0);
        }

        public T ExecuteObject<T>(DbCommand command, Converter<DbDataReader, T> converter)
        {
            T o;

            using (DbDataReader reader = ExecuteReader(command))
            {
                if (reader.Read())
                    o = converter(reader);
                else
                    o = default(T);

                reader.Close();
            }

            return o;
        }
        public T ExecuteObject<T>(DbCommand command)
            where T : new()
        {
            var converter = GetDataReaderConverter<T>();
            return ExecuteObject<T>(command, converter);
        }
        
        protected virtual Converter<object, T> GetTypeConverter<T>()
        {
            return (object o) => (T)DBConvert.To<T>(o);
        }
        protected virtual Converter<DbDataReader, T> GetDataReaderConverter<T>()
            where T : new()
        {
            return new DataReaderConverter<T>().Convert;
        }
        protected static void FillFromReader(DbDataReader reader, int startRecord, int maxRecords, Action<DbDataReader> action)
        {
            if (startRecord < 0)
                throw new ArgumentOutOfRangeException("startRecord", "StartRecord must be zero or higher.");

            while (startRecord > 0)
            {
                if (!reader.Read())
                    return;

                startRecord--;
            }

            if (maxRecords > 0)
            {
                int i = 0;

                while (i < maxRecords && reader.Read())
                {
                    action(reader);
                    i++;
                }
            }
            else
            {
                while (reader.Read())
                    action(reader);
            }
        }


        #region enums

        /// <summary>
        /// A list of data providers
        /// </summary>
        public enum Providers
        {
            SqlServer,
            OleDB,
            ODBC
        }

        #endregion enums
    }
}
