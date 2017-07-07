Clear-Host;
$here = Split-Path -Path $MyInvocation.MyCommand.Path -Parent;
$env:PSModulePath = $env:PSModulePath.Insert(0, (Split-Path -Path $here -Parent) + ';');
$name = $MyInvocation.MyCommand.Name.Split('.')[0];
Import-Module $name -Force;

$str = Get-ConnectionString -server 'sbt-oasib-001'	-database 'tempdb' -trustedConnection;

$strole = 'Provider=sqloledb;Data Source=.\sbt-oasib-001;Initial Catalog=tempdb;Integrated Security=SSPI;';

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
	$res = $query |
		Invoke-SQLQuery -connectionString $str;
	$res;
}

function test3 {
	Write-Host 'Test 3: multistring query';
	$res = $null;
	$query = @"
print 'Hello,'
print 'World!'
"@;
	$res = $query |
		Invoke-SQLQuery -connectionString $str;
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
	);
	$res = $query |
		Invoke-SQLQuery -connectionString $str;
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
	$res = Invoke-SQLQuery -connectionString $str -query $query -isSQLServer;
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
	$res = Invoke-SQLQuery -connectionString $str -query $query -isSQLServer -withTransact;
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
	$res = $query |
		Invoke-SQLQuery -connectionString $str1 -isSQLServer -withTransact;
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

function test11 {
	Write-Host 'Test11: exec query by path';
	$str = Get-ConnectionString -server 'sbt-oasib-001' -database 'DiasoftD5NT4' -trustedConnection;
	Write-Host "Connection string: '$str'";
	$res = $null;
	$query = [string](Get-Content -Path 'C:\Users\sbt-chistyakov-vv\Documents\WindowsPowerShell\Sql\AddAllPermission.sql');
	$res = Invoke-SQLReader -connectionString $str -query $query -isSQLServer;
	$res;
	$res.errors;
}

function test12 {
	Write-Host 'Test 12: Invoke-SQLQuery with param';
	$res = $null;
	$p = @{};
	$p.name = 'яить';
	$query =
	@'
select *
from sys.objects
where name like '$(name)';
print '$(name)';
'@;

	$res = $query | Invoke-SQLQuery -connectionString $str1 -isSQLServer -params $p;
	$res;
}

function test13 {
	Write-Host 'Test 13: from file';
	$res = $null;
	Write-Host "Connection string: $str";
	$query = (Get-Item -Path "$here\query.sql" | Get-Content -Raw);
	Write-Host "Query: $query";
	$res = Invoke-SQLQuery -connectionString $str -query $query -isSQLServer -verbouse;
	$res;
}

#test1
#test2
#test3
#test4
#test5
#test6
#test8
#test9
#test10
#test11
#test12
#test13
