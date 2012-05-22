#This build assumes the following directory structure
#
#  \Build          - This is where the project build code lives
#  \BuildArtifacts - This folder is created if it is missing and contains output of the build
#  \Code           - This folder contains the source code or solutions you want to build
#
Properties {
	$build_dir = Split-Path $psake.build_script_file
	$code_dir = "$build_dir\..\"
	$build_script = "$build_dir\..\source\EasyNetQ.sln"
	$keyfile = "$build_dir\..\autofac\src\SharedKey.snk"
	$repo_name = "EasyNetQ"
}

FormatTaskName (("-"*25) + "[{0}]" + ("-"*25))

Task Default -Depends Build

Task Build -Depends Clean, Init {	
	Write-Host "Building $repo_name" -ForegroundColor Green
	#Write-Host $build_script -ForegroundColor Magenta
	Exec { msbuild $build_script /m /t:Build /p:Configuration=Release }
}

Task Clean {	
	Write-Host "Build script: $build_script"
	Write-Host "Cleaning $repo_name" -ForegroundColor Green
	Exec { msbuild $build_script /t:Clean /p:Configuration=Release /v:quiet } 
}

task Init {  
    
}
