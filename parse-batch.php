<?php

// Parse BOLD file and generate a SQL dump of just the key metadata field we are interested in.
// For example, we exclude the sequences but include the k-mers as an embedding, and include 
// the geographic coordinates.

ini_set('memory_limit', '-1');

require_once (dirname(__FILE__) . '/pg.php');
require_once (dirname(__FILE__) . '/five-tuple.php');

//----------------------------------------------------------------------------------------

$headings = array();

$debug = false;
//$debug = true;

$export_format = 'json';
$export_format = 'postgres';

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
$skip_line = 0;

$file_name_size = 8;

$upload = true;
//$upload = false;

$batchsize  =     10; 
$batchsize  = 100000;

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
				
				// create a simplified object that we will store for an analysis			
				$obj = new stdclass;
				
				foreach ($data as $k => $v)
				{
					switch($k)
					{
						// things that require special handling
						
						// coordinates
						case 'coord':
							if (preg_match('/[\(|\[](.*),\s*(.*)[\)|\]]/', $v, $m))
							{	
								$obj->geometry = new stdclass;
								$obj->geometry->type = "Point";
								$obj->geometry->coordinates = array(
									(float) $m[2],
									(float) $m[1]
									);
							}						
							break;	
	
						// remove spaces from collectors, e.g. PHLCA834-11
						case 'collectors':
							$obj->{$k} = preg_split('/\s*[,|&]\s*/', $v);
							break;
							
						// projects
						case 'recordsetcodearr':
						case 'bold_recordset_code_arr':
							$v = preg_replace('/^\[/', '', $v);						
							$v = preg_replace('/\]$/', '', $v);
							$v = preg_replace('/\'/', '', $v);
							
							$obj->{$k} = preg_split('/,\s+/', $v);
							break;
							
						default:
							$obj->{$k} = $v;
							break;
					}
				}
				
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
				
				switch ($export_format)
				{
					case 'elastic':
						break;
						
					case 'json':
						// Simple JSON dump
						echo json_encode($obj, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) . "\n";
						break;		
	
					case 'postgres':
						// SQL dump with sequence encoded as vectors
						
						$record = new stdclass;
						
						// id
						$record->processid = $obj->processid;
						
						// taxonomy
						if (isset($obj->bin_uri))
						{
							$record->bin_uri = $obj->bin_uri;
						}
						else
						{
							$record->bin_uri = null;
						}					
						
						// identification
						if (isset($obj->identification))
						{
							$record->identification = $obj->identification;
						}
						else
						{
							$record->identification = null;
						}
						
						// lineage string
						if (isset($obj->lineage))
						{
							$record->lineage = join(';', $obj->lineage);
						}
						else
						{
							$record->lineage = null;
						}
						
						// taxonomy id in BOLD
						if (isset($obj->taxid))
						{
							$record->taxid = (Integer)$obj->taxid;
						}
						else
						{
							$record->taxid = null;
						}
														
						// point 					
						if (isset($obj->geometry))
						{
							$record->coord = 'POINT(' . $obj->geometry->coordinates[0] . ' ' . $obj->geometry->coordinates[1] . ')';
						}
						else
						{
							$record->coord = null;
						}					
						
						// sequence
						if (isset($obj->marker_code))
						{						
							$record->marker_code = $obj->marker_code;		
						}	
						else
						{
							$record->marker_code = null;
						}					
	
						// genbank
						if (isset($obj->insdc_acs))
						{						
							$record->insdc_acs = $obj->insdc_acs;	
						}	
						else
						{
							$record->insdc_acs = null;
						}	
									
						// sequence as embedding
						if (isset($obj->nuc))
						{				
							$record->embedding = null;
						
							$embedding = sequence_to_vector($obj->nuc);
							if (count($embedding) == 0)
							{
								// badness
								echo "No embedding for $record->processid\n";
								echo "Sequence = " . $obj->nuc . "\n";
							}
							else
							{							
								$record->embedding = $embedding;
							}
						}
						else
						{
							$record->embedding = null;
						}
						
						if ($record->marker_code)
						{
							// SQL
							$keys = array();
							$values = array();
	
							foreach ($record as $k => $v)
							{
								$keys[] = '"' . $k . '"'; // must be double quotes
								
								if (!$v)
								{
									$values[] = 'NULL';
								}
								elseif (is_array($v))
								{
									$values[] = "'" . str_replace("'", "''", json_encode(array_values($v))) . "'";
								}
								elseif(is_object($v))
								{
									$values[] = "'" . str_replace("'", "''", json_encode($v)) . "'";
								}
								elseif (preg_match('/^POINT/', $v))
								{
									$values[] = "ST_GeomFromText('" . $v . "', 4326)";
								}
								else
								{				
									$values[] = "'" . str_replace("'", "''", $v) . "'";
								}					
							}
							
							$batch[] = '(' . join(",", $values) . ')';
						}
						break;
						
					default:
						break;
				}
			}
		}	
		
		if (count($batch) == $batchsize)
		{
			echo "Row count: $row_count\n";
	
			$row_endTime = microtime(true);
			$row_executionTime = $row_endTime - $row_startTime;
			$formattedTime = number_format($row_executionTime, 3, '.', '');
			echo "Took " . $formattedTime . " seconds to process " . count($batch) . " rows.\n";
	
			// print_r($batch);
			
			if ($upload)
			{
				$sql = 'INSERT INTO boldvector (' . join(',', $keys) . ') VALUES' . "\n";
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
			}
			
			$batch = array();
			$row_startTime = microtime(true);
			
			//if ($row_count >= 1000000)
			{
				//exit(); // just do a batch for experiments
			}
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
		$sql = 'INSERT INTO boldvector (' . join(',', $keys) . ') VALUES' . "\n";
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
		
		
	}
	
	$batch = array();
	$row_startTime = microtime(true);
}	

?>
