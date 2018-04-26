%SystemRoot%\Microsoft.NET\Framework\v4.0.30319\installutil.exe D:\C#Projects\OPCTEST\OPCTEST\bin\Debug\OPCTEST.exe
Net Start ServiceTest
sc config ServiceTest start= auto