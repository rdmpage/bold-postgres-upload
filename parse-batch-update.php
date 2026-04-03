<?php

// Parse BOLD file and generate states to update existing database (we are tweaking things)

ini_set('memory_limit', '-1');

require_once (dirname(__FILE__) . '/pg.php');
require_once (dirname(__FILE__) . '/five-tuple.php');

//----------------------------------------------------------------------------------------

$headings = array();

$debug = false;
//$debug = true;

// taxonomy						
$taxon_keys = array(
	"kingdom" 		=> "k__",
	"phylum"		=> "p__",
	"class" 		=> "c__",
	"order" 		=> "o__",
	"family" 		=> "f__",
	"subfamily" 	=> "sf__",
	"tribe" 		=> "t__",
	"genus" 		=> "g__",
	"species" 		=> "s__",
	"subspecies"	=> "ss__",
);	

$row_count = 0;
$skip_line = 643000;

$file_name_size = 8;

$upload = true;
$upload = false;

$batchsize  =      10; 
$batchsize  =    1000;  // 1K
//$batchsize  =   10000; // 10K
//$batchsize  =  100000; // 100K

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
			if ($row_count > $skip_line)
			{
				$data = new stdclass;
			
				foreach ($row as $k => $v)
				{
					if (trim($v) != '' && $v != "None")
					{
						$data->{$headings[$k]} = $v;
					}
				}
			
				if ($debug)
				{
					//print_r($data);	
				}
				
				$obj = new stdclass;
				$obj->processid = $data->processid;
				
				// taxonomic hierarchy
				$obj->lineage = array();
				$ranks = array('kingdom','phylum','class','order','family','subfamily','tribe', 'genus','species','subspecies');
				
				foreach ($ranks as $k)
				{
					if (isset($data->{$k}))
					{
						$obj->lineage[] = $taxon_keys[$k] . $data->{$k};
					}
				}
				
				// SQL
				
				// print_r($obj);
				
				if (count($obj->lineage) > 0)
				{
					$arr = array();
					foreach ($obj->lineage as $arr_value)
					{
						$arr[] = '"' . $arr_value . '"';
					}
					
				
					$sql = "UPDATE boldvector SET lineage_arr = '{" . join(",", $arr) . "}' WHERE processid='" . $obj->processid . "';"; 
					
					$batch[] = $sql;
					
					if (count($batch) > $batchsize)
					{
						echo "Uploading " . count($batch) . " rows to psql\n";
						$startTime = microtime(true);

						$batch_sql = join("", $batch)			;
						$result = pg_query($db, $batch_sql);
						
						$endTime = microtime(true);
						$executionTime = $endTime - $startTime;
						$formattedTime = number_format($executionTime, 3, '.', '');
						echo "Execution time: " . $formattedTime . " seconds\n\n";
						
						$batch = array();
					}
				}
	
			}
		}	
		
	}	
	
	$row_count++;
	
	if ($row_count % 1000 == 0)
	{
		echo "[$row_count]\n";
	}
}	

// left over?
if (count($batch) > $batchsize)
{
	echo "Uploading " . count($batch) . " rows to psql\n";
	$startTime = microtime(true);

	$batch_sql = join("", $batch)			;
	$result = pg_query($db, $batch_sql);
	
	$endTime = microtime(true);
	$executionTime = $endTime - $startTime;
	$formattedTime = number_format($executionTime, 3, '.', '');
	echo "Execution time: " . $formattedTime . " seconds\n\n";
	
	$batch = array();
}


?>
