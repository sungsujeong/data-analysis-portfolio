Set args = Wscript.Arguments
Set objWshShell = CreateObject("Wscript.Shell")
UserProfile = objWshShell.ExpandEnvironmentStrings("%USERPROFILE%")
Downloadfolder = UserProfile & "\downloads\"

Dim FSO
Set FSO = CreateObject("Scripting.FileSystemObject")
ReportName = args.Item(0)
ReportURL = args.Item(1)
RootFolder = args.Item(2)
TodayDate = Year(Now()) & Right("0" & Month(Now()), 2) & Right("0" & Day(Now()), 2)
TargetFolder = RootFolder & ReportName & "_CurrentQuarter_" & TodayDate

If Not FSO.FolderExists(TargetFolder) Then
    FSO.CreateFolder(TargetFolder)
End If

LogFile = TargetFolder & "\Log_" & TodayDate & ".txt"
If Not FSO.FileExists(LogFile) Then
    FSO.CreateTextFile(LogFile)
End If

d = DatePart("q", Now())
    If (CInt(d) = 1) Then
	Q4 = "1231" + Cstr(Year(Now))
	URL = Replace(ReportURL, "XXXXXXXX", Q4)
	FileName = "Call_Cert3510_" + "1231" + Right(Year(Now)-1, 2)
	Call OpenURLDownLoadFile(URL, FileName, DownloadFolder, TargetFolder)
     Elseif (CInt(d) = 2) Then
	Q1 = "0331" + Cstr(Year(Now))
	URL = Replace(ReportURL, "XXXXXXXX", Q1)
	FileName = "Call_Cert3510_" + "0331" + Right(Year(Now), 2)
	Call OpenURLDownLoadFile(URL, FileName, DownloadFolder, TargetFolder)
     Elseif (CInt(d) = 3) Then
	Q2 = "0630" + Cstr(Year(Now))
	URL = Replace(ReportURL, "XXXXXXXX", Q2)
	FileName = "Call_Cert3510_" + "0630" + Right(Year(Now), 2)
	Call OpenURLDownLoadFile(URL, FileName, DownloadFolder, TargetFolder)
     Elseif (CInt(d) = 4) Thenc
	Q3 = "0930" + Cstr(Year(Now))
	URL = Replace(ReportURL, "XXXXXXXX", Q3)
	FileName = "Call_Cert3510_" + "0930" + Right(Year(Now), 2)
	Call OpenURLDownLoadFile(URL, FileName, DownloadFolder, TargetFolder)	
     End If

Function OpenURLDownLoadFile(URL, FileName, DownloadFolder, sTargetFolder)
On Error Resume Next
Set logObj = FSO.OpenTextFile(LogFile, 8, True)
logObj.WriteLine Now & vbTab & "- Started downloading file: " & FileName & " ..."
    
Set IE = CreateObject("InternetExplorer.Application")
With IE
   IE.Visible = True
   IE.Navigate URL
    
   Do While .Busy
      WScript.Sleep 100
   Loop
   Set evtChng = .Document.createEvent("HTMLEvents")
      evtChng.initEvent "Click", True, False
If Not (IE.Document.getElementById("Download_SDF_3") Is Nothing) Then
  IE.Document.getElementById("Download_SDF_3").Click
  IE.Document.dispatchEvent evtChng
  WScript.Sleep 2000
  objWshShell.SendKeys "%{S}"
  WScript.Sleep 5000
  SrcFileName = DownloadFolder & FileName & ".SDF"
  sTargetFolder = sTargetFolder & "\"
  Do While (FSO.FileExists(SrcFileName) = False)
    WScript.Sleep 100
  Loop
  If FSO.FileExists(SrcFileName) Then
    	FSO.CopyFile SrcFileName, sTargetFolder, True
        FSO.DeleteFile SrcFileName
        logObj.WriteLine Now & vbTab & "- Downloaded file successfully: " & sTargetFolder & FileName
  Else
     logObj.WriteLine Now & vbTab & "- Cannot find file"
  End If
End If
IE.Quit
End With
    Set IE = Nothing
If Err.Number <> 0 Then
      logObj.WriteLine Now & vbTab & "- Error:" & Err.Description & " while downloading file: " & FileName
      Err.Clear
End If
End Function
