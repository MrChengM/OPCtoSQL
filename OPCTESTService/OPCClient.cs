using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using OPCAutomation;
using LogFunctionLib;

namespace OPCClientLib
{
    public class OPCClient:IDisposable
    {
        #region 私有变量
        /// <summary>
        /// OPCServer Object
        /// </summary>
        private OPCServer KepServer=new OPCServer();
        /// <summary>
        /// OPCGroups Object
        /// </summary>
        private OPCGroups KepGroups;
        /// <summary>
        /// OPCGroup Object
        /// </summary>
        private OPCGroup KepGroup;
        /// <summary>
        /// OPCItems Object
        /// </summary>
        private OPCItems KepItems;
        /// <summary>
        /// OPCItem Object
        /// </summary>
        public List<OPCItem> lKepItem = new List<OPCItem>();
        /// <summary>
        /// 主机IP
        /// </summary>
        private string strHostIP = "";
        /// <summary>
        /// 主机名称
        /// </summary>
        private string strHostName = "";
        /// <summary>
        /// 服务名；
        /// </summary>
        public List<String> lServerName = new List<string>();
        /// <summary>
        /// 连接状态
        /// </summary>
        public bool opc_connected = false;
        /// <summary>
        /// 客户端句柄
        /// </summary>
        public int itmHandleClient = 0;
        /// <summary>
        /// 服务端句柄
        /// </summary>
        private int itmHandleServer = 0;
        private string sLogFilePath;
        #endregion
        #region 方法
        /// <summary>
        /// 枚举本地OPC服务器
        /// </summary>

        public delegate void OPCGroupDataChangHandler(int NumItems,ref Array ClientHandles, ref Array ItemValues, ref Array Qualities, ref Array TimeStamps);
        public event OPCGroupDataChangHandler OPCGroupDataChang;
        public delegate void OPCGroupAsyncReadHandler(int NumItems, ref Array ClientHandles, ref Array ItemValues, ref Array Qualities, ref Array TimeStamps);
        public event OPCGroupAsyncReadHandler OPCGroupAsyncRead;
        public delegate void OPCGroupAsyncWriteHandler(int NumItems, ref Array ClientHandles,ref Array Error);
        public event OPCGroupAsyncWriteHandler OPCGroupAsyncWrite;

        public OPCClient(string slogfilepath)
        {
            sLogFilePath = slogfilepath;

        }
        public void GetLocalServer()
        {
            try
            {
                //KepServer = new OPCServer();
                object serverList = KepServer.GetOPCServers(strHostName);

                foreach (string turn in (Array)serverList)
                {
                    lServerName.Add(turn);
                    Console.WriteLine($"OPC Servers Name:{ turn }");
                    LogFunction.WriteLog(sLogFilePath, $"Messagess:OPC Servers Name Add\"{ turn }\".");
                }

                Console.Write($"OPC Servers Name:{ lServerName[0]}");
                LogFunction.WriteLog(sLogFilePath, $"Messagess:OPC Servers Name:{ lServerName[0]}.");
            }
            catch (Exception err)
            {
                Console.Write($"OPC SEVER error：{err.Message}.");
                LogFunction.WriteLog(sLogFilePath, $"Error:OPC SEVER error：{err.Message}.");
            }

        }
        #endregion
        /// <summary>
        /// 连接OPC服务
        /// </summary>
        /// <param name="remoteServerIP">OPCServerIP</param>
        /// <param name="remoteServerName">OPCServer名称</param>
        public void ConnectServer(string remoteServerIP, string remoteServerName)
        {
            try
            {
                KepServer.Connect(remoteServerName, remoteServerIP);
                if (KepServer.ServerState == (int)OPCServerState.OPCRunning)
                {
                    opc_connected = true;
                    KepServer.ServerShutDown += new DIOPCServerEvent_ServerShutDownEventHandler(ShutDown);
                    Console.WriteLine($"Connected to {KepServer.ServerName} successfully");
                    LogFunction.WriteLog(sLogFilePath, $"Message:Connected to {KepServer.ServerName} successfully.");

                }
                else
                {
                    //这里你可以根据返回的状态来自定义显示信息，请查看自动化接口API文档
                    Console.WriteLine($"States :{KepServer.ServerState.ToString()}");
                    LogFunction.WriteLog(sLogFilePath, $"Message:{KepServer.ServerState.ToString()}");
                }
            }
            catch (Exception err)
            {
                Console.WriteLine($"Connected to OPCServer error：{ err.Message}");
                LogFunction.WriteLog(sLogFilePath, $"Error:Connected to OPCServer error：{ err.Message}");

                opc_connected =false;
            }
            finally
            {

            }

        }
        /// <summary>
        /// 断开OPC服务
        /// </summary>
        public void DisConnectServer()
        {
            opc_connected = false;
            KepServer.Disconnect();

        }
        /// <summary>
        /// 创建组
        /// </summary>
        public void CreateGroup(string sGroupName,int iUpdataRate,bool bIsSubscribed, List<string> lItemsID)
        {

            KepGroups = KepServer.OPCGroups;
            KepServer.OPCGroups.DefaultGroupIsActive = true;
            KepServer.OPCGroups.DefaultGroupDeadband = 0;

            KepGroup = KepGroups.Add(sGroupName);
            KepGroup.UpdateRate = iUpdataRate;
            KepGroup.IsActive = true;
            KepGroup.IsSubscribed = bIsSubscribed;
            //SetGroupProperty();
            Console.WriteLine($"New a KepGroup:{KepGroup.Name}");
            LogFunction.WriteLog(sLogFilePath, $"Message:New a KepGroup\"{KepGroup.Name}\".");


            KepItems = KepGroup.OPCItems;
            //KepItems.DefaultIsActive = true;
            int _itmHandleClient = itmHandleClient;
            List<OPCItem> _lkepitem = new List<OPCItem>();
            foreach (string str in lItemsID)
            {
                try
                {
                    _lkepitem.Add(KepItems.AddItem(str, _itmHandleClient));
                    _itmHandleClient++;
                    //Console.WriteLine($"New a lKepItem:{str}");
                    //LogFunction.WriteLog(sLogFilePath, $"Message:New a lKepItem\"{str}\".");
                }
                catch (Exception err)
                {
                    Console.WriteLine($"new KepGroup error:GroupName {sGroupName}  Message {err.Message} ");
                    LogFunction.WriteLog(sLogFilePath, $"Error:new KepGroup error \"GroupName {sGroupName}  Message {err.Message} \".");
                    continue;
                }

            }
            itmHandleClient = _itmHandleClient;
            lKepItem.AddRange(_lkepitem);


            KepGroup.DataChange += new DIOPCGroupEvent_DataChangeEventHandler(KepGroup_DataChange);
            KepGroup.AsyncWriteComplete += new DIOPCGroupEvent_AsyncWriteCompleteEventHandler(KepGroup_AsyncWriteComplete);
            KepGroup.AsyncReadComplete += new DIOPCGroupEvent_AsyncReadCompleteEventHandler(KepGroup_AsyncReadComplete);
        }
        /// <summary>
        /// OPC分支展开
        /// </summary>
        /// <param name="oPCBrowser"></param>
        public void RecurBrowse(OPCBrowser oPCBrowser)
        {
            //展开分支
            oPCBrowser.ShowBranches();
            //展开叶子
            oPCBrowser.ShowLeafs(true);
            foreach (object turn in oPCBrowser)
            {
                Console.WriteLine(turn.ToString());
            }
        }


        public void  SyncRead(string[] sItemsID,string sGroupName,out Array Value,out object Qualities, out object TimeStamps,out Array Result)
        {
            try
            {
                int iNumItem = sItemsID.Length;
                int[] iServerHandler = GetServerHandles(sItemsID);
                Array aServerHandler = (Array)iServerHandler;
                Array aValue;
                Array aError;
                object oQualities;
                object oTimeStamps;
                KepGroup = KepGroups.GetOPCGroup(sGroupName);
                KepGroup.SyncRead(1, iNumItem, aServerHandler, out aValue, out aError, out oQualities, out oTimeStamps);
                //Console.WriteLine($"lKepItem:{ sItemsID[0]},{(int)aValue.GetValue(0)},{Qualities},{ TimeStamps}");
                //Console.WriteLine($"lKepItem:{(int)aValue.GetValue(0)}");
                Value = aValue;
                Qualities = oQualities;
                TimeStamps = oTimeStamps;
                Result = aError;

            }
            catch
            {
               // Console.WriteLine("************** " + "SyncRead Error" + " **************");
                Value = null;
                Qualities = null;
                TimeStamps = null;
                Result = null;

            }

        }
        public void SyncWrite(string[] sItemsID, object[] ivalue, string sGroupName,out Array Result)
        {
            try
            {
                int iNumItem = sItemsID.Length;
                int[] iServerHandler = GetServerHandles(sItemsID);
                object[] oValue = new object[ivalue.Length + 1];
                for (int i = 0; i < ivalue.Length; i++)
                {
                    oValue[i + 1] = ivalue[i];
                }
                Array aServerHandler = (Array)iServerHandler;
                Array aValue = (Array)oValue;
                Array aError;
                KepGroup = KepGroups.GetOPCGroup(sGroupName);
                KepGroup.SyncWrite(iNumItem, ref aServerHandler, ref aValue, out aError);
                Result = aError;
            }
            catch
            {
               // Console.WriteLine("************** " + "SyncWrite Error" + " **************");
                Result = null;

            }
          
        }
        public void AsyncWrite(string[] sItemsID, object[] ivalue, string sGroupName, int iTransactionID,out Array Result)
        {
            try
            {
                int iNumItem = sItemsID.Length;
                int[] iServerHandler = GetServerHandles(sItemsID);
                object[] oValue = new object[ivalue.Length + 1];
                for (int i = 0; i < ivalue.Length; i++)
                {
                    oValue[i + 1] = ivalue[i];
                }
                Array aServerHandler = (Array)iServerHandler;
                Array aValue = (Array)oValue;
                Array aError;
                
                int iCanceID;
                KepGroup = KepGroups.GetOPCGroup(sGroupName);
                KepGroup.AsyncWrite(iNumItem, ref aServerHandler, ref aValue, out aError, iTransactionID, out iCanceID);
                Result = aError;
            }
            catch
            {
               // Console.WriteLine("************** " + "AsyncWrite Error" + " **************");
                Result = null;
            }
            
        }
        public void AsyncRead(string[] sItemsID, string sGroupName, int iTransactionID,out Array Result)
        {
            try
            {
                int iNumItem = sItemsID.Length;
                int[] iServerHandler = GetServerHandles(sItemsID);
                Array aServerHandler = (Array)iServerHandler;
                Array aError;
                //int iTransactionID = 299;
                int iCanceID;
                KepGroup = KepGroups.GetOPCGroup(sGroupName);
                KepGroup.AsyncRead(iNumItem, aServerHandler, out aError, iTransactionID, out iCanceID);
                Result = aError;
            }
            catch
            {
                //Console.WriteLine("************** " + "AsyncRead Error" + " **************");
                Result = null;
            }
           
            
        }
        void KepGroup_DataChange(int TransactionID, int NumItems, ref Array ClientHandles, ref Array ItemValues, ref Array Qualities, ref Array TimeStamps)
        {
            if (OPCGroupDataChang != null)
            {
                OPCGroupDataChang(NumItems, ref ClientHandles, ref ItemValues, ref Qualities, ref TimeStamps);

            }
        }
        void ShutDown(string Reason)
        {
            DisConnectServer();
            Console.WriteLine($"ServerShutDown:{Reason}");
            LogFunction.WriteLog(sLogFilePath, $"Meagges:Server Shut Down\"{ Reason}\".");
            
        }

        void KepGroup_AsyncReadComplete(int TransactionID, int NumItems, ref Array ClientHandles, ref Array ItemValues, ref Array Qualities, ref Array TimeStamps, ref Array Errors)
        {
            if (OPCGroupAsyncRead != null)
            {
                OPCGroupAsyncRead(NumItems, ref ClientHandles, ref ItemValues, ref Qualities, ref TimeStamps);

            }
        }
        void KepGroup_AsyncWriteComplete(int TransactionID, int NumItems, ref Array ClientHandles, ref Array Errors)
        {
            if (OPCGroupAsyncWrite != null)
            {
                OPCGroupAsyncWrite(NumItems, ref ClientHandles, ref Errors);
            }
            for (int i = 1; i <= NumItems; i++)
            {
                int sWriteStatus = (int)Errors.GetValue(i);
                int iIndex = (int)ClientHandles.GetValue(i);
                //Console.WriteLine($"lKepItem Write Status:{lKepItem[iIndex].ItemID},{sWriteStatus}");

            }
        }
        /// <summary>
        /// 通过itemID查找ServerHandle
        /// </summary>
        /// <param name="sItemsID"></param>
        /// <returns></returns>
        int[] GetServerHandles(string[] sItemsID)
        {

            List<int> lServerHandles = new List<int>();
            lServerHandles.Add(0);//由于OPC库数组下标从1开始， List第一位补0
            foreach (string sItemID in sItemsID)
            {
                var vServerHandles = from p in lKepItem where p.ItemID == sItemID select p.ServerHandle;

                if (vServerHandles.Count() != 0)
                {
                    foreach (var vServerHandle in vServerHandles)
                    {
                        lServerHandles.Add(vServerHandle);
                    }
                }
                else
                {
                    //lServerHandles = null;
                    Console.WriteLine($"Error Message:The ItemID { sItemID} invlid. ");
                    LogFunction.WriteLog(sLogFilePath, $"Error:The ItemID { sItemID} invlid. ");
                    //break;
                }

            }
            int[] _iServerHandles = lServerHandles.ToArray();
            return _iServerHandles;
        }
        public OPCItem[] GetOPCItems(string[] sItemsID)
        {

            List<OPCItem> _lOPCItems = new List<OPCItem>();
            foreach (string sItemID in sItemsID)
            {
                var vServerHandles = from p in lKepItem where p.ItemID == sItemID select p;

                if (vServerHandles.Count() != 0)
                {
                    foreach (var vServerHandle in vServerHandles)
                    {
                        _lOPCItems.Add(vServerHandle);
                    }
                }
                else
                {
                    //_lOPCItems = null;
                    Console.WriteLine($"Error Message:The ItemID { sItemID} invlid. ");
                    LogFunction.WriteLog(sLogFilePath, $"Error:The ItemID { sItemID} invlid. ");
                    //break;
                }

            }
            OPCItem[] _iOPCItems = _lOPCItems.ToArray();
            return _iOPCItems;
        }

        public void Dispose()
        {
            if (opc_connected)
                KepServer.Disconnect();

        }
    }
    public struct OPCItemsStruct
    {
        public string sGroupName { get; set; }
        public string[] sItems { get; set; }
    }
}
