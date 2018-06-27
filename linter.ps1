If (!(Test-Path -PathType Container "$env:gopath\src\github.com\alecthomas\gometalinter\")) {
    Write-Host "downloading gometalinter"
    go get -u github.com/alecthomas/gometalinter
    gometalinter --install
    If ($lastExitCode -ne 0) {
        Write-Host "error when installing gometalinter"
        Exit 1
    }
}

gometalinter ./... 2>&1 > gometalinter-err.txt

If ((Get-Content "gometalinter-err.txt") -ne $Null) {
    Write-Host "gometalinter ./...  detected problems"
    Get-Content -Path .\gometalinter-err.txt
    rm .\gometalinter-err.txt
    Exit 1
}

Write-Host "code format is ok!"
rm .\gometalinter-err.txt
Exit 0

