<?php

// Parse GBIF BOLD images into Postgres database

ini_set('memory_limit', '-1');

require_once (dirname(__FILE__) . '/pg.php');

//----------------------------------------------------------------------------------------
// Convert BOLD license string into a cleaned form
function clean_license($v)
{
	// fix any encoding errors
	$v = preg_replace('/\x{FFFD}/u', '-', $v);

	$terms = array();
	
	if (preg_match('/CreativeCommons/', $v))
	{
		$terms[] = 'CC';
	}

	if (preg_match('/Attribution/', $v))
	{
		$terms[] = 'BY';
	}

	if (preg_match('/Non-?Commercial/i', $v))
	{
		$terms[] = 'NC';
	}

	if (preg_match('/Share\s*-?(Alike)?/i', $v))
	{
		$terms[] = 'SA';
	}

	if (preg_match('/No Derivatives/', $v))
	{
		$terms[] = 'ND';
	}

	if (preg_match('/No Rights/', $v))
	{
		$terms[] = 'CC0';
	}

	if (preg_match('/-by-nc-nd/', $v))
	{
		$terms = ['CC', 'BY', 'NC', 'ND'];
	}
	
	return join('-', $terms);
}

//----------------------------------------------------------------------------------------
$debug = false;
//$debug = true;

$tablename = 'boldimage';

$row_count = 0;

$batchsize =  10000; 

$batchsize =  1000;

$upload = true;

$batch = array();
$row_startTime = microtime(true);

$headings = array('processid', 'title', 'identifier', 'references', 'format', 'license');

$filename = "ibol_2024_07_19/media.txt";

$file_handle = fopen($filename, "r");
while (!feof($file_handle)) 
{
	$line = trim(fgets($file_handle));
		
	$row = explode("\t",$line);
	
	if (is_array($row))
	{
		$obj = new stdclass;
	
		foreach ($row as $k => $v)
		{
			if (trim($v) != '' && $v != "None")
			{
				$obj->{$headings[$k]} = $v;
			}
			else
			{
				$obj->{$headings[$k]} = null;
			}
		}
	
		if ($debug)
		{
			print_r($obj);	
		}
		
		$img = new stdclass;
		
		$img->processid 	= null;
		$img->url 			= null;
		$img->title 		= null;
		$img->view 			= null;
		$img->mimetype  	= null;
		$img->license 		= null;
		$img->clean_license = null;
		
		foreach ($obj as $k => $v)
		{
			switch ($k)
			{
				case 'processid':
					$img->$k = $v;
					break;
					
				case 'identifier':
					$img->url = $v;
					
					// md5 of URL was used in earlier caching										
					$img->url = str_replace('#', '%23', $img->url);
					break;

				case 'title':
					$img->title = $v;
					
					$view = $img->title;
					$view = preg_replace('/' . $img->processid . '\s+/', '', $view);
					if ($view != '')
					{
						$img->view = $view;
					}
					break;

				case 'format':
					$img->mimetype = $v;
					break;
					
				case 'license':
					$img->license = $v;
					$img->clean_license = clean_license($v);
					break;
					
				default:
					break;
			}
		}
		
		
		if ($img->processid)
		{
				
			$keys = array();
			$values = array();
	
			foreach ($img as $k => $v)
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
		
		if ($upload)
		{
			$sql = 'INSERT INTO ' . $tablename . ' (' . join(',', $keys) . ') VALUES' . "\n";
			$sql .= join(",\n", $batch);
			$sql .= " ON CONFLICT DO NOTHING;";
			
			// echo $sql . "\n";
			
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
	
	if ($upload)
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
