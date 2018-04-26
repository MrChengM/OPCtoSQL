%SystemRoot%\Microsoft.NET\Framework\v4.0.30319\installutil.exe D:\C#Projects\OPCTEST\OPCTESTService\bin\Debug\OPCTESTService.exe
Net Start ServiceTest
sc config ServiceTest start= auto