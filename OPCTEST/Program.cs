using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OPCAutomation;
using System.Net;
using System.Windows;
using System.Threading;
using System.Runtime.InteropServices;
using OPCClientLib;
using SqlClientLib;
using ExcelFunctionLib;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using LogFunctionLib;

namespace OPCTEST
{
    class Program
    {
        static OPCClient opcKepClient;

        static string sConFilePath;
        static string sLogFilePath;
        static string sServerName;
        static string sSqlStr;
        static int iDataRefrTimes;
        static int iSqlRefrTimes;
        static bool bReConnect;
        static int iReConnectTimes;
        static List<OPCItemsStruct> lOPCItemsStrt = new List<OPCItemsStruct>();
        static System.Timers.Timer AsyncReadTimer;
        static System.Timers.Timer ActiveTimer;
        static System.Timers.Timer SqlInsertTimer;
        static double iAlive = -1;// 心跳信号

        [DllImport("user32.dll", EntryPoint = "FindWindow")]
        extern static IntPtr FindWindow(string lpClassName, string lpWindowName);
        [DllImport("user32.dll", EntryPoint = "GetSystemMenu")]
        extern static IntPtr GetSystemMenu(IntPtr hWnd, IntPtr bRevert);
        [DllImport("user32.dll", EntryPoint = "RemoveMenu")]
        extern static IntPtr RemoveMenu(IntPtr hMenu, uint uPosition, uint uFlags);
        static void closebtn()
        {
            //IntPtr windowHandle = FindWindow(null, Process.GetCurrentProcess().MainModule.FileName);  
            IntPtr windowHandle = FindWindow(null, "OPCClient");
            IntPtr closeMenu = GetSystemMenu(windowHandle, IntPtr.Zero);
            uint SC_CLOSE = 0xF060;
            RemoveMenu(closeMenu, SC_CLOSE, 0x0);
        }

        static void Main(string[] args)
        {
            string sUserCommand = "";

            Console.Title = "OPCClient";
            closebtn();

            //config初始化
            Initialize();
            LogFunction.WriteLog(sLogFilePath, "Message:Service Start.");
            LogFunction.WriteLog(sLogFilePath, "Message:Initialize Compalte.");


            //Console关闭事件
            Console.CancelKeyPress += new ConsoleCancelEventHandler(CloseConsole);
            //开启OPCClient线程
            Action OPCClientaction = new Action(OPCClient);
            Task threadOPCClient = Task.Run(OPCClientaction);
            //threadOPCClient.Start();
            LogFunction.WriteLog(sLogFilePath, "Message:OPCClient Thread Start.");

            //开启SqlInsert线程
            Action SqlInsertaction = new Action(SqlInsert);
            Task threadSqlInsert = Task.Run(SqlInsertaction);
            //threadSqlInsert.Start();
            LogFunction.WriteLog(sLogFilePath, "Message:SqlInsert Thread Start.");

            while (true)
            {

                sUserCommand =Console.ReadLine();
                if (sUserCommand == "Exit")
                {
                    if(opcKepClient.opc_connected == true)
                    opcKepClient.DisConnectServer();
                    LogFunction.WriteLog(sLogFilePath, "Message:Service Stop.");
                    break;
                }

            }

        }
        protected static void CloseConsole(object sender, ConsoleCancelEventArgs e)
        {
            if (opcKepClient.opc_connected == true)
                opcKepClient.DisConnectServer();
            LogFunction.WriteLog(sLogFilePath, "Message:Service Stop.");
        }
        static void Initialize()
        {
            sConFilePath = ConfigurationSettings.AppSettings["Config.FilePath"];
            sLogFilePath = ConfigurationSettings.AppSettings["Log.FilePath"];
            sServerName = ConfigurationSettings.AppSettings["ServerName"];
            sSqlStr = ConfigurationSettings.AppSettings["SqlStr"];
            iDataRefrTimes = int.Parse(ConfigurationSettings.AppSettings["DataRefrTimes"]);
            iSqlRefrTimes = int.Parse(ConfigurationSettings.AppSettings["SqlRefrTimes"]);
            bReConnect = bool.Parse(ConfigurationSettings.AppSettings["bReConnect"]);
            iReConnectTimes = int.Parse(ConfigurationSettings.AppSettings["ReConnectTimes"]);

            opcKepClient = new OPCClient(sLogFilePath);
            string[] sConFilePaths = sConFilePath.Split(new char[] { ';' });
            foreach (string sConFilePath in sConFilePaths)
            {
                string[] sFlieNames = sConFilePath.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
                string sFlieName = sFlieNames[sFlieNames.Length - 1];
                sFlieName = sFlieName.Replace(".xlsx", "");

                OPCItemsStruct OPCItemsStrt = new OPCItemsStruct();
                OPCItemsStrt.sGroupName = sFlieName;

                DataSet dsOPCGroup = ExcelFunction.ExcelRead(sConFilePath);
                DataTable dtOPCGroup = dsOPCGroup.Tables[0];
                int iRowCounts = dtOPCGroup.Rows.Count;

                OPCItemsStrt.sItems = new string[iRowCounts];
                for (int i = 0; i < iRowCounts; i++)
                {
                    if (dtOPCGroup.Rows[i][0] is System.DBNull == false)
                    {
                        OPCItemsStrt.sItems[i] = sFlieName + "." + dtOPCGroup.Rows[i][0].ToString();
                    }
                }
                lOPCItemsStrt.Add(OPCItemsStrt);
            }
        }    //创建OPCclient
        static void OPCClient()
        {
            //opcKepClient. GetLocalServer();
            //创建OPCGroups
            CreatGroups();
            //定时异步读取
            AsyncReadTimer = new System.Timers.Timer(iDataRefrTimes);
            AsyncReadTimer.Elapsed += new System.Timers.ElapsedEventHandler(AsyncRead);
            AsyncReadTimer.AutoReset = true;
            if (opcKepClient.opc_connected == true)
            {
                AsyncReadTimer.Enabled = true;
                Console.WriteLine("Updata From OPC Server => Start");
                LogFunction.WriteLog(sLogFilePath, "Message:\"Updata From OPC Server\" Start.");
                SqlInsertTimer.Enabled = true;
                Console.WriteLine("Insert To Sql => Start");
                LogFunction.WriteLog(sLogFilePath, "Message:\"Insert To Sql\" Start.");
            }

            //Alive Check
            ActiveTimer = new System.Timers.Timer(iReConnectTimes);
            ActiveTimer.Elapsed +=new System.Timers.ElapsedEventHandler(ActiveCheck);
            ActiveTimer.AutoReset = true;
            ActiveTimer.Enabled = bReConnect;

            //opcKepClient.OPCGroupDataChang += new OPCClient.OPCGroupDataChangHandler(InsertSql);
            //opcKepClient.OPCGroupAsyncRead += new OPCClient.OPCGroupAsyncReadHandler(InsertSql);
        }


        static void SqlInsert()
        {
            //Thread.Sleep(1000);
            SqlInsertTimer = new System.Timers.Timer(iSqlRefrTimes);
            SqlInsertTimer.Elapsed += new System.Timers.ElapsedEventHandler(InsertSql);
            SqlInsertTimer.AutoReset = true;
        }
        private static void AsyncRead(object sender,System.Timers.ElapsedEventArgs e)
        {
            AsyncReadTimer.Enabled = false;
            int iTransactionID = 0;
            foreach (OPCItemsStruct OPCItemsStrt in lOPCItemsStrt)
            {

                Array Result;
                //if (OPCItemsStrt.sItems.Length > 100)
                //{

                //}
               // else
                //{
                    iTransactionID++;
                    opcKepClient.AsyncRead(OPCItemsStrt.sItems, OPCItemsStrt.sGroupName, iTransactionID, out Result);
                    //Thread.Sleep(100);
                //}


            }

            AsyncReadTimer.Enabled = true;


        }

        private static void ActiveCheck(object sender, System.Timers.ElapsedEventArgs e)
        {
           if (opcKepClient.opc_connected == false)
            {

                //opcKepClient.DisConnectServer();
                //opcKepClient.ConnectServer("", "Kepware.KEPServerEX.V5");
                ActiveTimer.Enabled = false;

                AsyncReadTimer.Enabled = false;
                Console.WriteLine("Updata From OPC Server =>Stop");

                SqlInsertTimer.Enabled = false;
                Console.WriteLine("Insert To Sql => Stop");
                CreatGroups();
                if (opcKepClient.opc_connected == true)
                {
                    AsyncReadTimer.Enabled = true;
                    Console.WriteLine("Updata From OPC Server =>Strat");

                    SqlInsertTimer.Enabled = true;
                    Console.WriteLine("Insert To Sql => Strat");
                }
                ActiveTimer.Enabled = true;
            }
            //iAlive = opcKepClient.lKepItem[5].Value;

        }

        private static void  InsertSql(object sender, System.Timers.ElapsedEventArgs e)
        {
            SqlInsertTimer.Enabled = false;
            SQLClient sqlclnt = new SQLClient();
            DataTable dtItems = new DataTable("OPCItems");
            dtItems.Columns.AddRange(new DataColumn[] 
            {
                        /*new DataColumn("ItemID",typeof(string)),
                        new DataColumn("ItemValue",typeof(object)),
                        new DataColumn("ItemQualitie",typeof(int)),
                        new DataColumn("ItemTimeStamp",typeof(DateTime)),
                        new DataColumn("UpdateTime",typeof(DateTime)),*/

                        new DataColumn("ItemID"),
                        new DataColumn("ItemValue"),
                        new DataColumn("ItemQualitie"),
                        new DataColumn("ItemTimeStamp"),
                        new DataColumn("UpdateTime"),
            });

            sqlclnt.sConnectStr= sSqlStr;
            bool bsqlcnt = sqlclnt.ConnectSql();
            if (bsqlcnt == true)
            {
                DateTime UpdataTime = DateTime.Now;
                for (int i = 0; i < opcKepClient.lKepItem.Count; i++)
                {
                    //int Index =(int) ClientHandles.GetValue(i);
                    string ItemID = opcKepClient.lKepItem[i].ItemID;
                    var ItemValue = opcKepClient.lKepItem[i].Value;
                    int? ItemQuality = opcKepClient.lKepItem[i].Quality;
                    DateTime? ItemTimeStamp = opcKepClient.lKepItem[i].TimeStamp.AddHours(8) ;
                    DataRow drItem = dtItems.NewRow();
                    drItem[0] = opcKepClient.lKepItem[i].ItemID;
                    drItem[1] = opcKepClient.lKepItem[i].Value;
                    drItem[2]= opcKepClient.lKepItem[i].Quality;
                    drItem[3] = opcKepClient.lKepItem[i].TimeStamp.AddHours(8);
                    drItem[4] = DateTime.Now;
                    dtItems.Rows.Add(drItem);

                    /*string sqldelet = string.Format("delete from {0} where {1}='{2}' ", "Runtimes", "ItemID", ItemID);
                    string sqlInsert = string.Format("INSERT INTO {0} VALUES ('{1}','{2}','{3}','{4}','{5}') ", "Runtimes",
                        ItemID, ItemValue, ItemQuality, ItemTimeStamp, UpdataTime);

                    sqlclnt.sqlExecuteNonQuery(sqldelet);
                    sqlclnt.sqlExecuteNonQuery(sqlInsert);*/

                    /*string sqlUpdata = string.Format("update {0} set {3}='{4}',{5}='{6}',{7}='{8}',{9}='{10}' where {1}='{2}'", "Runtimes", "ItemID", ItemID,
                        "ItemValue", ItemValue, "ItemQualitie", ItemQuality, "ItemTimeStamp", ItemTimeStamp, "UpdateTime", DateTime.Now);
                    int iUpdateRows;
                    sqlclnt.sqlExecuteNonQuery(sqlUpdata,out iUpdateRows);
                    if (iUpdateRows == 0)
                    {
                        string sqlInsert = string.Format("INSERT INTO {0} VALUES ('{1}','{2}','{3}','{4}','{5}') ", "Runtimes",
                            ItemID, ItemValue, ItemQuality, ItemTimeStamp, DateTime.Now);
                        sqlclnt.sqlExecuteNonQuery(sqlInsert, out iUpdateRows);
                    }*/

                }

            }
            int iUpdateRows;
            string sqldelet = string.Format("delete from {0} where 1=1 ", "Runtimes");
            sqlclnt.sqlExecuteNonQuery(sqldelet,out iUpdateRows, CommandType.Text);
            sqlclnt.sqlBulkCopyData("Runtimes",dtItems);
            sqlclnt.SqlClose();
            SqlInsertTimer.Enabled = true;

        }
        /*private static void InsertSql(int NumItems, ref Array ClientHandles, ref Array ItemValues, ref Array Qualities, ref Array TimeStamps)
        {
            
            for (int i = 1; i <= NumItems; i++)
            {
                int Index = (int)ClientHandles.GetValue(i);
                string ItemID = opcKepClient.lKepItem[Index].ItemID;
                var ItemValue = opcKepClient.lKepItem[Index].Value;
                int? ItemQuality = opcKepClient.lKepItem[Index].Quality;
                DateTime? ItemTimeStamp = opcKepClient.lKepItem[Index].TimeStamp;
                string sqldelet = string.Format("delete from {0} where {1}='{2}' ", "Runtimes", "ItemID", ItemID);
                //string sqlInsert = string.Format("INSERT INTO {0}({1},{2},{3},{4},{5}) VALUES ({6},{7},{8},{9},{10})", "Runtimes", "ItemID",
                //    "ItemValue", "ItemQualitie", "ItemTimeStamp", "UpdateTime", ItemID, ItemValue, ItemQuality, ItemTimeStamp, DateTime.Now);
                string sqlInsert = string.Format("INSERT INTO {0} VALUES ('{1}','{2}','{3}','{4}','{5}') ", "Runtimes",
                    ItemID, ItemValue, ItemQuality, ItemTimeStamp, DateTime.Now);
                SQLClient sqlclnt = new SQLClient();
                //sqlclnt.sConnectStr="Data Source=.;Initial Catalog=Runtime;Integrated Security=True";
                sqlclnt.sqlExecuteNonQuery(sqldelet);
                sqlclnt.sqlExecuteNonQuery(sqlInsert);
            }

        }*/
        private static void CreatGroups()
        {
            opcKepClient.ConnectServer("", sServerName);
            //Thread.Sleep(1000);
            if (opcKepClient.opc_connected == true)
            {
                opcKepClient.itmHandleClient = 0;
                opcKepClient.lKepItem = new List<OPCItem>();
                foreach (OPCItemsStruct OPCItemsStrt in lOPCItemsStrt)
                {
                    opcKepClient.CreateGroup(OPCItemsStrt.sGroupName, 250, true, OPCItemsStrt.sItems.ToList());
                }
                /*AsyncReadTimer.Enabled = true;
                Console.WriteLine("Updata From OPC Server => Start");
                SqlInsertTimer.Enabled = true;
                Console.WriteLine("Insert To Sql => Start");
                */
            }
        }

    }
}
