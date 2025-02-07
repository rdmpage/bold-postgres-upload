<?php

// Parse BOLD file inster into Postgres database

ini_set('memory_limit', '-1');

require_once (dirname(__FILE__) . '/pg.php');

//----------------------------------------------------------------------------------------

$headings = array();

$debug = false;
//$debug = true;

$tablename = 'boldmeta';

$row_count = 0;

$batchsize  =      10; 
$batchsize  =    1000;  // 1K
//$batchsize  =   10000;  // 10K

$batch = array();
$row_startTime = microtime(true);

$filename = "BOLD_Public.06-Sep-2024/BOLD_Public.06-Sep-2024.tsv";

$file_handle = fopen($filename, "r");
while (!feof($file_handle)) 
{
	$line = trim(fgets($file_handle));
		
	$row = explode("\t",$line);
	
	$go = is_array($row) && count($row) > 1;
	
	if ($go)
	{
		if ($row_count == 0)
		{
			$headings = $row;		
		}
		else
		{
			$data = new stdclass;
		
			foreach ($row as $k => $v)
			{
				if (trim($v) != '' && $v != "None")
				{
					$data->{$headings[$k]} = $v;
					
					// arrays
					if (preg_match('/^\[/', $v))
					{
						$v = preg_replace('/^\[/', '', $v);						
						$v = preg_replace('/\]$/', '', $v);
						$v = preg_replace('/\'/', '', $v);						
						$data->{$headings[$k]} = preg_split('/,\s+/', $v);
					}
				}
				else
				{
					$data->{$headings[$k]} = null;
				}
			}
		
			if ($debug)
			{
				print_r($data);	
			}
				
			$keys = array();
			$values = array();

			foreach ($data as $k => $v)
			{
				$keys[] = '"' . $k . '"'; // must be double quotes
				
				if (is_null($v))
				{
					$values[] = 'NULL';
				}
				elseif (is_array($v))
				{
					$arr = array();
					foreach ($v as $arr_value)
					{
						$arr[] = '"' . $arr_value . '"';
					}
					$values[] = "'{" . join(",", $arr) . "}'";
				}
				elseif(is_object($v))
				{
					$values[] = "'" . str_replace("'", "''", json_encode($v)) . "'";
				}
				else
				{				
					$values[] = "'" . str_replace("'", "''", $v) . "'";
				}	
			}
			$batch[] = '(' . join(",", $values) . ')';
		}
	}	
	
	if (count($batch) == $batchsize)
	{
		echo "Row count: $row_count\n";

		$row_endTime = microtime(true);
		$row_executionTime = $row_endTime - $row_startTime;
		$formattedTime = number_format($row_executionTime, 3, '.', '');
		echo "Took " . $formattedTime . " seconds to process " . count($batch) . " rows.\n";

		//print_r($batch);
		
		if (1)
		{
			$sql = 'INSERT INTO ' . $tablename . ' (' . join(',', $keys) . ') VALUES' . "\n";
			$sql .= join(",\n", $batch);
			$sql .= " ON CONFLICT DO NOTHING;";
			
			$startTime = microtime(true);
	
			echo "Uploading " . count($batch) . " rows to psql\n";
			
			$result = pg_query($db, $sql);
	
			$endTime = microtime(true);
			$executionTime = $endTime - $startTime;
			$formattedTime = number_format($executionTime, 3, '.', '');
			echo "Execution time: " . $formattedTime . " seconds\n\n";
			
			$row_startTime = microtime(true);
		}
		
		$batch = array();
		
		if ($row_count > 100)
		{
			//exit(); // just do a million
		}
	}	
	
	$row_count++;
}	

// left over?
if (count($batch) > 0)
{
	echo "Row count: $row_count\n";

	$row_endTime = microtime(true);
	$row_executionTime = $row_endTime - $row_startTime;
	$formattedTime = number_format($row_executionTime, 3, '.', '');
	echo "Took " . $formattedTime . " seconds to process " . count($batch) . " rows.\n";

	// print_r($batch);
	
	if (1)
	{
		$sql = 'INSERT INTO ' . $tablename . ' (' . join(',', $keys) . ') VALUES' . "\n";
		$sql .= join(",\n", $batch);
		$sql .= " ON CONFLICT DO NOTHING;";
		
		// $batch_filename = str_pad($row_count, $file_name_size, '0', STR_PAD_LEFT) . '.sql';
		//file_put_contents($batch_filename, $sql);	
		
		$startTime = microtime(true);

		echo "Uploading " . count($batch) . " rows to psql\n";
		
		$result = pg_query($db, $sql);

		$endTime = microtime(true);
		$executionTime = $endTime - $startTime;
		$formattedTime = number_format($executionTime, 3, '.', '');
		echo "Execution time: " . $formattedTime . " seconds\n\n";
		
		$row_startTime = microtime(true);
	}
	
	$batch = array();

}	


?>
