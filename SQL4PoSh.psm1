<#
.SYNOPSIS
Create connectionstring
.DESCRIPTION
Create connection string of input parameters. If necessary, you can use the bush option 
to enter your additional data.
.PARAMETER server
Server name. Use '.\' ask local server and '.' ask local server with instance.
.PARAMETER instance
Instance name. If you use the default: you can not to set.
.PARAMETER database
Database name.
.PARAMETER user
User name that has access to the database.
.PARAMETER password
User password to connect to the database.
.PARAMETER trustedConnection
If you using the same credentials to enter Windows and connect to the database, you can use 
this option to not enter explicitly login and password.
.PARAMETER oledb
Switch to use OleDB interface connect to source.
.PARAMETER datasource
Data source for database connection.
.PARAMETER custom
Hashtable with additional parameters.
.INPUTS
String. You can pipe query string objects.
.OUTPUTS
Hashtable. Returns the number of rows for the query, as well as related information, and 
error messages.
#>
function Get-ConnectionString {
[CmdletBinding()]
	param (
		[parameter(
			Mandatory = $true,
			HelpMessage = 'Enter server name.'
		)]
		[string]$server,

		[parameter(
			HelpMessage = 'If use not default instance, enter name.'
		)]
		[string]$instance,

		[parameter(
			Mandatory = $true,
			HelpMessage = 'Enter database name.'
		)]
		[string]$database,

		[parameter(
			ParameterSetName = 'NativeUser',
			HelpMessage = 'Enter user name.'
		)]
		[string]$user,

		[parameter(
			ParameterSetName = 'NativeUser',
			HelpMessage = 'Enter password.'
		)]
		[string]$password,

		[parameter(
			ParameterSetName = 'NTLMUser',
			HelpMessage = 'If you use trusted connection method, use this switch. User/password option not use.'
		)]
		[Alias('trustedUser')]
		[switch]$trustedConnection,

		[parameter(
			HelpMessage = 'if connect with OLE DB, use this option'
		)]
		[ValidateSet('Access', 'Active Directory', 'MySQL', 'Oracle', 'Microsoft')]
		[string]$oledb,

		[parameter()]
		[string]$datasource,

		[parameter()]
		[hashtable]$custom
	)
	begin {
		$string = @{};
	}
	process {
		if (![String]::IsNullOrEmpty($oledb)) {
			switch ($oledb) {
				'Oracle' {
					$string['Provider'] = 'OraOLEDB.Oracle';
					$string['Date Source'] = $datasource;
					if ($trustedConnection.IsPresent) {
						$string.'OSAuthent' = '1';
					}
					else {
						$string['User Id'] = $user;
						$string['Password'] = $password;
					}
				}
				'Access' {
					$string['Provider'] = 'Microsoft.ACE.OLEDB.12.0';
					$string['Data Source'] = $datasource;
					if ($trustedConnection.IsPresent) {
						$string['Persist Security Info'] = 'False';
					}
					else {
						$string['Jet OLEDB:Database Password'] = $password;
					}
				}
				'Active Directory' {
					$string['Provider'] = 'ADSDSOObject';
					if (!$trustedConnection.IsPresent) {
						$string['User Id'] = $user;
						$string['Password'] = $password;
					}
				}
				'Microsoft' {
					$string['Provider'] = 'sqloledb';
					$string['Data Source'] = $datasource;
					if (![System.String]::IsNullOrEmpty()) {
						$string['Data Source'];    
					}
					$string['Initial Catalog'] = $database;
					if($trustedConnection.IsPresent) {
						$string['Integrated Security'] = 'SSPI';
					}
					else {
						$string['User Id'] = $user;
						$string['Password'] = $password;
					}
				}
				'MySQL' {
					$string['Provider'] = 'MySQLProv';
					$string['Data Source'] = $datasource;
					$string['Uid'] = $user;
					$string['Pwd'] = $password;
				}
			} 
		}
		else {
			$string.'Server' = $server;
			if (![String]::IsNullOrEmpty($instance)) {
				$string['Server'] += "\$instance";
			}
			$string['Database'] = $database;
			if ($trustedConnection.IsPresent) {
				$string['Trusted_Connection'] = 'True';
			}
			else {
				$string['User Id'] = $user;
				$string['Password'] = $password;
			}
		}

		if ($custom.Count -ne 0) {
			$string += $custom;
		}

		return [string]::Join(" ", ($string.GetEnumerator() | ForEach-Object -Process { "$($_.Key)=$($_.Value);" }));
	}
}

<#
.SYNOPSIS
Get a selection data.
.DESCRIPTION
Get a selection data from query
.PARAMETER connectionstring
Sets the string used to open a SQL database.
.PARAMETER query
Sets the text command to run against the data source.
.PARAMETER isSQLServer
Switch to connect to MS SQL Server.
.INPUTS
String. You can pipe query string objects.
.OUTPUTS 
Object {System.Data.DataSet, errors} 
#>
function Get-SQLData {
	[CmdletBinding()]
	param (
		[Parameter()]
		[string]$connetcionString,
		
		[Parameter(ValueFromPipeline = $true)]
		[string]$query,
		
		[Parameter()]
		[switch]$isSQLServer
	)
	begin {
		# Create connection
		[System.Data.Common.DbConnection]$connection = $null;
		if ($isSQLServer.IsPresent) {
		# Adding event handers for info messages
			$connection = New-Object -TypeName System.Data.SqlClient.SqlConnection;
		}
		else {
			$connection = New-Object -TypeName System.Data.OleDb.OleDbConnection;
		}

		$connection.ConnectionString = $connetcionString;
		$connection.Open();
	}

	process {
		# Create comman
		[System.Data.Common.DbCommand]$command = $connection.CreateCommand();
		$command.CommandText = $query;

		# Create Adapter
		[System.Data.Common.DataAdapter]$adapter = $null;
		if ($isSQLServer.IsPresent) {
			$adapter = New-Object -TypeName System.Data.SqlClient.SqlDataAdapter($command);
		}
		else {
			$adapter = New-Object -TypeName System.Data.OleDb.OleDbDataAdapter($command);
		}

		# Create DataSet
		[System.Data.DataSet]$dataSet = New-Object -TypeName System.Data.DataSet;
		$result = @{};
		try {
			$adapter.Fill($dataSet);
			$result.data = $dataSet.Tables;
		}
		catch {
			$result.'errors' = $_.Exception.InnerException.Errors;
		}

		return $result;
	}

	end {
		if (($connection.State -ne 'Closed') -or ($connection.State -ne 'Broken')) {
			$connection.Close();            
		}
	}
}

<#
.SYNOPSIS
Executes a SQL statement against the connection and returns the number of rows affected.
.DESCRIPTION
Executes a SQL statement against the connection and returns the number of rows affected. 
Also return errors and info message.
.PARAMETER connectionString
String used to open a SQL Server database. You can use cmdlet Get-ConnectionString to 
get format string or do it yorself (http://connectionstrings.com).
.PARAMETER query
String with sql instructions
.PRAMETER isSQLServer
Switching to use of SQL Server
.PARAMETER withTransact
Switching to the use of the transaction mechanism. One transaction is used for all requests 
sent via pipeline. if you want to use transactions for each request individually, it is 
necessary to use cmdlet's foreach.
.INPUTS
String. You can pipe query string objects.
.OUTPUTS
Hashtable. Returns the number of rows for the query, as well as related information, and 
error messages.
.EXAMPLE
$query = "print 'Hello, World!'";
Invoke-SQLQuery -connectionString $str -query $query;
.EXAMPLE
$query = @("print 123", "print 'Hello, World!'");
$query | Invoke-SQLQuery -connectionString $str -isSQLServer;
#>
function Invoke-SQLQuery {
	[CmdletBinding()]
	param (
		[Parameter(
			Mandatory = $true,
			HelpMessage = 'Enetr connection string'
		)]
		[string]$connectionString,

		[parameter(
			Mandatory = $true, 
			ValueFromPipeline = $true,
			HelpMessage = 'Enter query string'
		)]
		[AllowEmptyString()]
		[string]$query,

		[parameter(
			HelpMessage = 'Use to connect SQl Server'
		)]
		[switch]$isSQLServer,

		[parameter(
			HelpMessage = 'Use to enable transaction mechanism'
		)]
		[switch]$withTransact
	)
	
	begin {
		# Create connection
		[System.Data.Common.DbConnection]$connection = $null;
		if($isSQLServer.IsPresent) {
			$connection = New-Object -TypeName System.Data.SqlClient.SqlConnection;
			# Continue processing the rest of the statements in a command regardless of any errors produced by the server
			$connection.FireInfoMessageEventOnUserErrors = $true;
		}
		else {
			$connection = New-Object -TypeName System.Data.OleDb.OleDbConnection;
		}

		# Open connection
		try {
			$connection.ConnectionString = $connectionString;
			$connection.Open();
		}
		catch {
			Out-Default -InputObject 'Cant connect to server';
			Out-Default -InputObject $_.Exception;
			return @{'errors' = $_.Exception};
		}

		# Use transaction
		if ($withTransact.IsPresent) {
			[System.Data.Common.DbTransaction]$transaction = $connection.BeginTransaction();
		}
	}
	
	process {
		if ($connection.State -eq 'Closed') {
			return;
		}
		# Zero out result for each pipe query.
		[hashtable]$result = @{};

		[System.Data.Common.DbCommand]$command = $connection.CreateCommand();
		$command.CommandText = $query;

		# Use transaction
		if ($withTransact.IsPresent) {
			$command.Transaction = $transaction;
		}

		# Adding event handers for info messages
		[scriptblock]$scriptInfoMessage =  {
			# Add to $result.errors
			$event.MessageData.errors += $eventArgs.Errors;
			$event.MessageData.eventsCount += 1;
		}
		# Create hide event. Only this method is work!!! 
		Register-ObjectEvent -InputObject $connection -EventName 'InfoMessage' -Action $scriptInfoMessage -MessageData $result -SupportEvent;

		# Execute
		$result.rowCount = $command.ExecuteNonQuery();
		while ($result.errors.count -ne $result.eventsCount) {
      Start-Sleep -Milliseconds 10;
    }

		return $result;
	}
	
	end {
		if ($connection.State -eq 'Closed') {
			return;
		}
		# Use transaction Commit or Rollback
		if ($withTransact.IsPresent) {
			try {
				$transaction.Commit();
			}
			catch {
				try {
					Out-Default -InputObject "Can't commit this transaction. Rollback!";
					Out-Default -InputObject $_.Exception;
					$result.errors += $_.Exception;
					$transaction.Rollback();
				}
				catch {
					Out-Default -InputObject "Can't rollback transaction!";
					Out-Default -InputObject $_.Exception;
					$result.errors += $_.Exception;
				}
			}
		}

		# Close Connection
		$connection.Close();
	}
}

<#
.SYNOPSIS
Executes a SQL statement against the connection and returns query results with the number of rows affected.
.DESCRIPTION
Executes a SQL statement against the connection and returns query results withthe number of rows affected. 
Also return errors and info message.
.PARAMETER connectionString
String used to open a SQL Server database. You can use cmdlet Get-ConnectionString to 
get format string or do it yorself (http://connectionstrings.com).
.PARAMETER query
String with sql instructions
.PRAMETER isSQLServer
Switching to use of SQL Server
.PARAMETER withTransact
Switching to the use of the transaction mechanism. One transaction is used for all requests 
sent via pipeline. if you want to use transactions for each request individually, it is 
necessary to use cmdlet's foreach.
.INPUTS
String. You can pipe query string objects.
.OUTPUTS
Hashtable. Returns the number of rows for the query, as well as related information, and 
error messages.
.EXAMPLE
$query = "print 'Hello, World!'";
Invoke-SQLQuery -connectionString $str -query $query;
.EXAMPLE
$query = @("print 123", "print 'Hello, World!'");
$query | Invoke-SQLQuery -connectionString $str -isSQLServer;
#>
function Invoke-SQLReader {
	[CmdletBinding()]
	param(
		[Parameter(
			Mandatory = $true,
			HelpMessage = 'Enetr connection string'
		)]
		[string]$connectionString,

		[parameter(
			Mandatory = $true, 
			ValueFromPipeline = $true,
			HelpMessage = 'Enter query string'
		)]
		[AllowEmptyString()]
		[string]$query,

		[parameter(
			HelpMessage = 'Use to connect SQl Server'
		)]
		[switch]$isSQLServer
	)

	begin {
		# Create connection
		[System.Data.Common.DbConnection]$connection = $null;
		if($isSQLServer.IsPresent) {
			$connection = New-Object -TypeName System.Data.SqlClient.SqlConnection;
			# Continue processing the rest of the statements in a command regardless of any errors produced by the server
			$connection.FireInfoMessageEventOnUserErrors = $true;
		}
		else {
			$connection = New-Object -TypeName System.Data.OleDb.OleDbConnection;
		}

		# Open connection
		try {
			$connection.ConnectionString = $connectionString;
			$connection.Open();
		}
		catch {
			Out-Default -InputObject 'Can not connect to server';
			Out-Default -InputObject $_.Exception;
			return @{'errors' = $_.Exception};
		}

		# Use transaction
		if ($withTransact.IsPresent) {
			[System.Data.Common.DbTransaction]$transaction = $connection.BeginTransaction();
		}
	}

	process {
		if ($connection.State -eq 'Closed') {
			return;
		}
		# Zero out result for each pipe query.
		[hashtable]$result = @{};

		[System.Data.Common.DbCommand]$command = $connection.CreateCommand();
		$command.CommandText = $query;
		# Use transaction
		if ($withTransact.IsPresent) {
			$command.Transaction = $transaction;
		}
		
		# Adding event handers for info messages
		[scriptblock]$scriptInfoMessage =  {
			# Add to $result.errors
			$event.MessageData.errors += $eventArgs.Errors;
			$event.MessageData.eventsCount += 1;
		} 

		# Create hide event. Only this method is work!!! 
		Register-ObjectEvent -InputObject $connection -EventName 'InfoMessage' -Action $scriptInfoMessage -MessageData $result -SupportEvent;
		
		# Execute
		[System.Data.Common.DbDataReader]$reader = $command.ExecuteReader();
    #sync execute and events
		while ($result.errors.count -ne $result.eventsCount) {
      Start-Sleep -Milliseconds 10;
    }
		[System.Data.DataTable]$result.data = New-Object -TypeName System.Data.DataTable;
		$result.data.Load($reader);
		$reader.Close();

		return $result;
	}
	
	end {
		if ($connection.State -eq 'Closed') {
			return;
		}
		# Use transaction
		if ($withTransact.IsPresent) {
			try {
				$transaction.Commit();
			}
			catch {
				try {
					Out-Default -InputObject "Can't commit this transaction. Rollback!";
					Out-Default -InputObject $_.Exception;
					$result.errors += $_.Exception;
					$transaction.Rollback();
				}
				catch {
					Out-Default -InputObject "Can't rollback transaction!";
					Out-Default -InputObject $_.Exception;
					$result.errors += $_.Exception;
				}
			}
		}

		# Close Connection
		$connection.Close();
	}
}
