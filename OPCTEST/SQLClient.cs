using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace SqlClientLib
{
    public class SQLClient
    {
        //string sConnectStr = "Data Source=.;Initial Catalog=Runtime;Integrated Security=True"
        string sconnectstr = "Data Source=.;Initial Catalog=Test;Persist Security Info=True;User ID=sa;Password=root";
        public string sConnectStr
        {
            get
            {
                return sconnectstr;
            }
            set
            {
                sconnectstr = value;
            }
            
        }
        SqlConnection sqlcnt= new SqlConnection();
        public bool ConnectSql()
        {
            try
            {
                sqlcnt = new SqlConnection(sconnectstr);
                sqlcnt.Open();
                return true;
            }
            catch(Exception e)
            {
                Console.WriteLine($"Open Sql Server Fail:{e.Message}");
                return false;
            }

            
        }
        public bool sqlExecuteNonQuery(string sQueryString,out int iUpdateRows, CommandType comdtype)
        {
            //bool bSqlOpen=ConnectSql();
            try {
                //SqlConnection sqlcnt = new SqlConnection(sconnectstr);
               // sqlcnt.Open();
                SqlCommand sqlcomm = sqlcnt.CreateCommand();
                    sqlcomm.CommandType = comdtype;
                    sqlcomm.CommandText = sQueryString;
                    sqlcomm.CommandTimeout = 0;
                    iUpdateRows=sqlcomm.ExecuteNonQuery();
                    //sqlcnt.Close();
                    return true;
               
            }
            catch(Exception e)
            {
                Console.WriteLine($"Execute Query Sql  Fail:{e.Message}.");
                //sqlcnt.Close();
                iUpdateRows= 0;
                return false;

            }
        }
        public bool sqlBulkCopyData(string sTableName,DataTable dt)
        {
            try
            {
                SqlBulkCopy sqlblcopy = new SqlBulkCopy(sconnectstr, SqlBulkCopyOptions.UseInternalTransaction);
                sqlblcopy.DestinationTableName = sTableName;
                for (int i = 0; i < dt.Columns.Count; i++)
                {
                    sqlblcopy.ColumnMappings.Add(dt.Columns[i].ColumnName, dt.Columns[i].ColumnName);
                }
                sqlblcopy.WriteToServer(dt);
                return true;
            }
            catch(Exception e)
            {
                Console.WriteLine($"Bulk Copy Data To Sql  Fail:{e.Message}.");
                return false;
            }
            
        }
        public void SqlClose()
        {
            sqlcnt.Close();
        }

    }
}
