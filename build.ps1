$ClientImportPath="github.com/hazelcast/hazelcast-go-client"
$ListOfPackages = New-Object System.Collections.ArrayList
go list $ClientImportPath/... | Select-String -pattern ".*/test|.*/compatibility|.*/rc|.*/sample" -notmatch | %{$ListOfPackages.add($_)}
$PackageList=$ListOfPackages -join ","

#run linter
cd $env:gopath\src\$ClientImportPath
.\linter.ps1

If ($lastExitCode -ne 0) {
    Exit 1
}

If (Test-Path -PathType Container "$env:gopath\src\github.com\apache\thrift\") {
    Write-Host "thrift already exists, not downloading."
} Else {
    go get github.com/apache/thrift/lib/go/thrift
    cd $env:gopath\src\github.com\apache\thrift
    git fetch --tags --quiet
    git checkout 0.10.0    
}

cd $env:gopath\src\$ClientImportPath
go build

cd $env:gopath
.\start-rc.ps1

Start-Sleep -s 11

go get github.com/t-yuki/gocover-cobertura
go get github.com/tebeka/go2xunit

go list github.com/hazelcast/hazelcast-go-client/... | ForEach-Object {
    If (!($_ -like "*vendor*")) {
        Write-Host "testing... $_"
        go test -race -covermode=atomic -v -coverprofile="tmp.out" -coverpkg $PackageList $_ | Tee-Object -FilePath "test.out" -Append
    }
}

Get-Content -Path .\test.out | go2xunit -output tests.xml


