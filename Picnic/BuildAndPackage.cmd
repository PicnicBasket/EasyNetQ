powershell -NoProfile -ExecutionPolicy unrestricted -File .\psake.ps1
.nuget\NuGet.exe pack .\easynetq.nuspec -BasePath ..\source
