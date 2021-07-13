$HzVersion="3.10-SNAPSHOT"
$HazelcastTestVersion=$HzVersion
$HazelcastVersion=$HzVersion
$HazelcastEnterpriseVersion=$HzVersion
$HazelcastRcVersion="0.3-SNAPSHOT"
$SnapshotRepo="https://oss.sonatype.org/content/repositories/snapshots"
$ReleaseRepo="http://repo1.maven.apache.org/maven2"
$EnterpriseReleaseRepo="https://repository-hazelcast-l337.forge.cloudbees.com/release/"
$EnterpriseSnapshotRepo="https://repository-hazelcast-l337.forge.cloudbees.com/snapshot/"

If ($HzVersion -like "*-SNAPSHOT") {
    $Repo=$SnapshotRepo
    $EnterpriseRepo=$EnterpriseSnapshotRepo
} Else {
    $Repo=$ReleaseRepo
    $EnterpriseRepo=$EnterpriseReleaseRepo
}	

If (Test-Path -PathType Leaf "hazelcast-remote-controller-$HazelcastRcVersion.jar") {
    Write-Host "remote controller already exist, not downloading from maven."
} Else {
    Write-Host "Downloading: remote-controller jar com.hazelcast:hazelcast-remote-controller:$HazelcastRcVersion"
    mvn -q dependency:get "-DrepoUrl=$SnapshotRepo" "-Dartifact=com.hazelcast:hazelcast-remote-controller:$HazelcastRcVersion" "-Ddest=hazelcast-remote-controller-$HazelcastRcVersion.jar"
    If ($lastExitCode -ne 0) {
        Write-Host "Failed download remote-controller jar com.hazelcast:hazelcast-remote-controller:$HazelcastRcVersion"
        Exit 1
    }
} 

If (Test-Path -PathType Leaf "hazelcast-$HazelcastTestVersion-tests.jar") {
    Write-Host "hazelcast-test.jar already exists, not downloading from maven."
} Else {
    Write-Host "Downloading: hazelcast test jar com.hazelcast:hazelcast:$HazelcastTestVersion`:jar:tests"
    mvn -q dependency:get "-DrepoUrl=$Repo" "-Dartifact=com.hazelcast:hazelcast:$HazelcastTestVersion`:jar:tests" "-Ddest=hazelcast-$HazelcastTestVersion-tests.jar"
    If ($lastExitCode -ne 0) {
        Write-Host "Failed download hazelcast test jar com.hazelcast:hazelcast:$HazelcastTestVersion`:jar:tests"
        Exit 1
    }
}

$ClassPath="hazelcast-remote-controller-$HazelcastRcVersion.jar;hazelcast-$HazelcastTestVersion-tests.jar;test/javaclasses"

If ($HAZELCAST_ENTERPRISE_KEY) {
    If (Test-Path -PathType Leaf "hazelcast-enterprise-$HazelcastEnterpriseVersion.jar") {
        Write-Host "hazelcast-enterprise.jar already exists, not downloading from maven."
	} Else {
        Write-Host "Downloading: hazelcast enterprise jar com.hazelcast:hazelcast-enterprise:$HazelcastEnterpriseVersion"
        mvn -q dependency:get "-DrepoUrl=$EnterpriseRepo" "-Dartifact=com.hazelcast:hazelcast-enterprise:$HazelcastEnterpriseVersion" "-Ddest=hazelcast-enterprise-$HazelcastEnterpriseVersion.jar"
        If ($lastExitCode -ne 0) {
            Write-Host "Failed download hazelcast enterprise jar com.hazelcast:hazelcast-enterprise:$HazelcastEnterpriseVersion"
            Exit 1
        }
    }
    $ClassPath="hazelcast-enterprise-$HazelcastEnterpriseVersion.jar;"+$ClassPath
    Write-Host "Starting Remote Controller ... enterprise ..."
} Else {
    If (Test-Path -PathType Leaf "hazelcast-$HazelcastVersion.jar") {
        Write-Host "hazelcast.jar already exists, not downloading from maven."
    } Else {
        Write-Host "Downloading: hazelcast jar com.hazelcast:hazelcast:$HazelcastVersion"
        mvn -q dependency:get "-DrepoUrl=$Repo" "-Dartifact=com.hazelcast:hazelcast:$HazelcastVersion" "-Ddest=hazelcast-$HazelcastVersion.jar"
        If ($lastExitCode -ne 0) {
            Write-Host "Failed download hazelcast jar com.hazelcast:hazelcast:$HazelcastVersion"
            Exit 1
        }
    }
    $ClassPath="hazelcast-$HazelcastVersion.jar;"+$ClassPath
    Write-Host "Starting Remote Controller ... oss ..."
}

Start-Process -FilePath javaw -ArgumentList ( "-Dhazelcast.enterprise.license.key=$HAZELCAST_ENTERPRISE_KEY","-cp", "$ClassPath", "com.hazelcast.remotecontroller.Main" ) -RedirectStandardOutput "rc_stdout.txt" -RedirectStandardError "rc_stderr.txt"

