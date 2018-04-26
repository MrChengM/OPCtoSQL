using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.OleDb;
using System.Windows;
using Excel = Microsoft.Office.Interop.Excel;
using System.Reflection;
using System.IO;

namespace ExcelFunctionLib
{
   public class ExcelFunction //Excel转换成DataTable;
    {
        /// <summary>
        /// 读Excel表（数据库方式）
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public static DataSet ExcelRead(string filePath)
        {
            string connStr = "";
            string fileType = System.IO.Path.GetExtension(filePath);
            if (string.IsNullOrEmpty(fileType)) return null;
            if (fileType == ".xls")
                connStr = "Provider=Microsoft.Jet.OLEDB.4.0;" + "Data Source=" + filePath + ";" + ";Extended Properties=\"Excel 8.0;HDR=YES;IMEX=1\"";
            else
                connStr = "Provider=Microsoft.ACE.OLEDB.12.0;" + "Data Source=" + filePath + ";" + ";Extended Properties=\"Excel 12.0;HDR=YES;IMEX=1\"";
            string sql_F = "Select * FROM [{0}]";

            OleDbConnection conn = null;
            OleDbDataAdapter da = null;
            DataTable dtSheetName = null;
            DataSet ds = new DataSet();
            try
            {
                // 初始化连接，并打开
                conn = new OleDbConnection(connStr);
                conn.Open();
                // 获取数据源的表定义元数据                        
                string SheetName = "";
                dtSheetName = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                // 初始化适配器
                da = new OleDbDataAdapter();
                for (int i = 0; i < dtSheetName.Rows.Count; i++)
                {
                    SheetName = (string)dtSheetName.Rows[i]["TABLE_NAME"];
                    if (SheetName.Contains("$") && !SheetName.Replace("'", "").EndsWith("$"))
                    {
                        continue;
                    }
                    da.SelectCommand = new OleDbCommand(String.Format(sql_F, SheetName), conn);
                    DataSet dsItem = new DataSet();
                    da.Fill(dsItem, SheetName);
                    ds.Tables.Add(dsItem.Tables[0].Copy());
                }
            }
            catch (Exception ex)
            {
                conn.Close();
                //da.Dispose();
                //conn.Dispose();
                Console.WriteLine("Excel Input failed: " + ex.Message);
            }
            finally
            {
                // 关闭连接
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                    da.Dispose();
                    conn.Dispose();
                }
            }
            return ds;
        }
        /// <summary>
        /// 写Excel数据表（COM方式）
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="outputstring"></param>
        public static void ExcelWrite(string outfilename, DataTable dt)
        {
            Excel.Application xapps = new Excel.Application();
            Excel.Workbook xbook = xapps.Workbooks.Add(Missing.Value);
            Excel.Worksheet xsheet = xbook.Sheets[1];
            Excel.Range xrng;

            try
            {

                string sfilename = @outfilename;
                if (File.Exists(sfilename))
                {
                    File.Delete(sfilename);
                }

                //DataTable导入到Worksheet.
                int iRowCounts = dt.Rows.Count;
                int iColCounts = dt.Columns.Count;

                string[,] sDatas = new string[iRowCounts, iColCounts];
                //string[] sRowDatas = new string[iColCounts];

                for (int i = 1; i <= iColCounts; i++)
                {
                    xrng = xsheet.Range[xsheet.Cells[1, i], xsheet.Cells[1, i]];
                    xrng.Value = dt.Columns[i - 1].ToString();
                }
                for (int i = 1; i <= iRowCounts; i++)
                {
                    for (int j = 1; j <= iColCounts; j++)
                    {
                        sDatas[i - 1, j - 1] = dt.Rows[i - 1][j - 1].ToString();
                        //sRowDatas[j - 1] = dt.Rows[i - 1][j - 1].ToString();
                        //xrng = xsheet.Range[xsheet.Cells[i+1,j], xsheet.Cells[i+1,j]];
                        //xrng.Value = dt.Rows[i-1][j-1].ToString();
                    }
                    //xrng = xsheet.Range[xsheet.Cells[i+1, 1], xsheet.Cells[i+1, iColCounts]];
                    //xrng.Value= sRowDatas;
                }
                xrng = xsheet.Range[xsheet.Cells[2, 1], xsheet.Cells[iRowCounts+1, iColCounts]];
                xrng.Value = sDatas;

                //保存文件
                xbook.SaveAs(sfilename, Missing.Value, Missing.Value, Missing.Value, false, Missing.Value, Excel.XlSaveAsAccessMode.xlNoChange, Missing.Value, Missing.Value, Missing.Value);
            }
            catch(Exception ex)
            {
                Console.WriteLine("Write to Excel Error： " + ex.Message);
                xbook.Close();
                xsheet = null;
                xapps.Quit();
                xapps = null;
            }
            finally
            {
                xbook.Close();
                xsheet = null;
                xapps.Quit();
                xapps = null;
            }
        }

    }
}
