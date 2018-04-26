using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace LogFunctionLib
{
   public class LogFunction
    {
        public static void WriteLog(string sFilePath,string sLogMessage)
        {

            DateTime dt = DateTime.Now;
            int i = 0;
            string sDataNow = dt.Year.ToString() + dt.Month.ToString() + dt.Day.ToString();
            string fileName = string.Format("{0}_log_{1}.txt", sDataNow, "00");
            string sfilePath = @sFilePath + fileName;
            FileStream fs;
            if (File.Exists(sfilePath))
            {
                fs = new FileStream(sfilePath, FileMode.Append, FileAccess.Write);
            }
            else
            {
                fs = new FileStream(sfilePath, FileMode.CreateNew, FileAccess.Write);
            }
            StreamWriter sw = new StreamWriter(fs);
            sLogMessage = dt + " " + sLogMessage;
            sw.WriteLine(sLogMessage);
            sw.Close();
            sw.Dispose();
        }
    }
}
