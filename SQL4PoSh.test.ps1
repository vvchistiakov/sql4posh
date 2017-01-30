Clear-Host;
$here = Split-Path -Path $MyInvocation.MyCommand.Path -Parent;
$env:PSModulePath = $env:PSModulePath.Insert(0, (Split-Path -Path $here -Parent) + ';');
$name = $MyInvocation.MyCommand.Name.Split('.')[0];
Import-Module $name -Force;

$str = Get-ConnectionString -server '.' -instance 'velo2014'  -database 'master' -trustedConnection;
$str1 = Get-ConnectionString -server '.' -instance 'velo2014' -database 'testdb' -trustedConnection;
$strole = 'Provider=sqloledb;Data Source=.\velo2014;Initial Catalog=master;Integrated Security=SSPI;';
Write-Host "Connection string: '$str'";

function test1 {
    Write-Host 'Test 1: single query';
    $res = $null;
    $query = "print 'Hello, World!'";
    $res = Invoke-SQLQuery -connectionString $str -query $query;
    $res;
}

function test2 {
    Write-Host 'Test 2: array query';
    $res = $null;
    $query = @("print 123", "print 'Hello, World!'");
    $res = $query | Invoke-SQLQuery -connectionString $str;
    $res;
}

function test3 {
    Write-Host 'Test 3: multistring query';
    $res = $null;
    $query = @" 
print 'Hello,'
print 'World!'
"@;
    $res = $query | Invoke-SQLQuery -connectionString $str;
    $res;
}

function test4 {
    Write-Host 'Test 4: array multistring query';
    $res = $null;
    $query = @(
@" 
print 'Hello,'
print 'World4!'
"@,
@"
print '123'
print '231'
"@
    )
    $res = $query | Invoke-SQLQuery -connectionString $str;
    $res;
}

function test5 {
    Write-Host 'Test 5: oledb test';
    $res = $null;
    $query = "print 'Hello, World!'";
    $res = Invoke-SQLQuery -connectionString $strole -query $query;
    $res;
}

function test6 {
    Write-Host 'Test 6: insert test';
    $res = $null;
    $query = @"
insert into testtable (testint)
values (123);
"@;
    $res = Invoke-SQLQuery -connectionString $str1 -query $query -isSQLServer;
    $res;
}
function test7 {
    Write-Host 'Test 7: insert test with transaction';
    $res = $null;
    $query = @"
insert into testtable (testint)
values (1);
insert into testtable (testint)
values ('123');
"@;
    $res = Invoke-SQLQuery -connectionString $str1 -query $query -isSQLServer -withTransact;
    $res;
}

function test8 {
    Write-Host 'Test 8: insert multiple vals from pipeline';
    $res = $null;
    $query = @(
@"
insert into testtable (testint)
values (1);
"@,
@"
insert into testtable (testint)
values ('qwer');
"@
    );

    $res = $query | Invoke-SQLQuery -connectionString $str1 -isSQLServer -withTransact;
    $res;
}

function test9 {
    Write-Host 'Test 9: return single datareader';
    $res = $null;
    $query = "sp_who";
    $res = Invoke-SQLReader -connectionString $str -query $query -isSQLServer;
    $res;
}

function test10 {
    Write-Host 'Test10: return single datareader';
    $res = $null;
    $query = "print 'Hello, World!'";
    $res = Invoke-SQLReader -connectionString $str -query $query -isSQLServer;
    $res;
}

test7