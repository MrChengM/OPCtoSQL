using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using OPCClientLib;
using SqlClientLib;
using System.Configuration;
using ExcelFunctionLib;
using System.Threading;
using OPCAutomation;
using LogFunctionLib;

namespace OPCTESTService
{
    public partial class OPCTESTService : ServiceBase
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
        public OPCTESTService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            //LogFunction.WriteLog(sLogFilePath, "Message:Service Start.");
            //Config初始化
            Initialize();
            LogFunction.WriteLog(sLogFilePath, "Message:Service Start.");
            LogFunction.WriteLog(sLogFilePath, "Message:Initialize Compalte.");

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
        }

        protected override void OnStop()
        {
            opcKepClient.Dispose();
            LogFunction.WriteLog(sLogFilePath, "Message:Service Stop.");


        }
        /// <summary>
        /// 初始化参数
        /// </summary>
        private void Initialize()
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
        }
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
            ActiveTimer.Elapsed += new System.Timers.ElapsedEventHandler(ActiveCheck);
            ActiveTimer.AutoReset = true;
            ActiveTimer.Enabled = bReConnect;

        }


        static void SqlInsert()
        {
            //Thread.Sleep(1000);
            SqlInsertTimer = new System.Timers.Timer(iSqlRefrTimes);
            SqlInsertTimer.Elapsed += new System.Timers.ElapsedEventHandler(InsertSql);
            SqlInsertTimer.AutoReset = true;
        }
        private static void AsyncRead(object sender, System.Timers.ElapsedEventArgs e)
        {
            AsyncReadTimer.Enabled = false;
            int iTransactionID = 0;
            foreach (OPCItemsStruct OPCItemsStrt in lOPCItemsStrt)
            {

                Array Result;
                iTransactionID++;
                opcKepClient.AsyncRead(OPCItemsStrt.sItems, OPCItemsStrt.sGroupName, iTransactionID, out Result);

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
                LogFunction.WriteLog(sLogFilePath, "Message:\"Updata From OPC Server\" Stop.");

                SqlInsertTimer.Enabled = false;
                Console.WriteLine("Insert To Sql => Stop");
                LogFunction.WriteLog(sLogFilePath, "Message:\"Insert To Sql\" Stop.");
                CreatGroups();
                if (opcKepClient.opc_connected == true)
                {
                    AsyncReadTimer.Enabled = true;
                    Console.WriteLine("Updata From OPC Server =>Strat");
                    LogFunction.WriteLog(sLogFilePath, "Message:\"Updata From OPC Server\" Strat.");

                    SqlInsertTimer.Enabled = true;
                    Console.WriteLine("Insert To Sql => Strat");
                    LogFunction.WriteLog(sLogFilePath, "Message:\"Insert To Sql\" Strat.");
                }
                ActiveTimer.Enabled = true;
            }
            //iAlive = opcKepClient.lKepItem[5].Value;

        }

        private static void InsertSql(object sender, System.Timers.ElapsedEventArgs e)
        {
            SqlInsertTimer.Enabled = false;
            SQLClient sqlclnt = new SQLClient(sLogFilePath);
            DataTable dtItems = new DataTable("OPCItems");
            dtItems.Columns.AddRange(new DataColumn[]
            {
                        new DataColumn("ItemID"),
                        new DataColumn("ItemValue"),
                        new DataColumn("ItemQualitie"),
                        new DataColumn("ItemTimeStamp"),
                        new DataColumn("UpdateTime"),
            });

            sqlclnt.sConnectStr = sSqlStr;
            bool bsqlcnt = sqlclnt.ConnectSql();
            if (bsqlcnt == true)
            {
                DateTime UpdataTime = DateTime.Now;
                for (int i = 0; i < opcKepClient.lKepItem.Count; i++)
                {
                    string ItemID = opcKepClient.lKepItem[i].ItemID;
                    var ItemValue = opcKepClient.lKepItem[i].Value;
                    int? ItemQuality = opcKepClient.lKepItem[i].Quality;
                    DateTime? ItemTimeStamp = opcKepClient.lKepItem[i].TimeStamp.AddHours(8);
                    DataRow drItem = dtItems.NewRow();
                    drItem[0] = opcKepClient.lKepItem[i].ItemID;
                    drItem[1] = opcKepClient.lKepItem[i].Value;
                    drItem[2] = opcKepClient.lKepItem[i].Quality;
                    drItem[3] = opcKepClient.lKepItem[i].TimeStamp.AddHours(8);
                    drItem[4] = DateTime.Now;
                    dtItems.Rows.Add(drItem);
                }

            }
            int iUpdateRows;
            string sqldelet = string.Format("delete from {0} where 1=1 ", "Runtimes");
            sqlclnt.sqlExecuteNonQuery(sqldelet, out iUpdateRows, CommandType.Text);
            sqlclnt.sqlBulkCopyData("Runtimes", dtItems);
            sqlclnt.SqlClose();
            SqlInsertTimer.Enabled = true;

        }
 
        private static void CreatGroups()
        {
            opcKepClient.ConnectServer("", sServerName);
            if (opcKepClient.opc_connected == true)
            {
                opcKepClient.itmHandleClient = 0;
                opcKepClient.lKepItem = new List<OPCItem>();
                foreach (OPCItemsStruct OPCItemsStrt in lOPCItemsStrt)
                {
                    opcKepClient.CreateGroup(OPCItemsStrt.sGroupName, 250, true, OPCItemsStrt.sItems.ToList());
                }
            }
        }
    }
}
