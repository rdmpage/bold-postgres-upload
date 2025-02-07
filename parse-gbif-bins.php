<?php

// Parse iBOL taxonomy from GBIF and output as TGF
// Note that GBIF has integer tax ids for internal nodes but not for BINSs
// so we create those using Latin32 encoding.
// The BOLD taxonomy is also not a tree as the kingdoms are identifieds by name
// but not an integer taxid. We create integer values for the kingdoms, and set the
// root of the tree to taxid=0.

require_once(dirname(__FILE__) . '/base-converter.php');

ini_set('memory_limit', '-1');

$headings = array();

$row_count = 0;

$filename = "data/ibol_bins_2024_07_19/taxon.txt";

// Darwin Core
$headings = array(
'id',
'taxonID',
'scientificNameID',
'parentNameUsageID',
'scientificNameAuthorship',
'scientificName',
'references',
'taxonRank',
'taxonRemarks'
);

// Latin32 to encode BIN numbers
$b = new BaseConverter("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ");

$nodes = array();
$edges = array();

// create root of tree connecting kingdoms
$nodes[0] = "Biota";

// need to start at a number > highest taxid in BOLD

// kingdoms
$nodes[2000000] = "Animalia";
$nodes[2000001] = "Plantae";
$nodes[2000002] = "Fungi";
$nodes[2000003] = "Protista";

// biota is ancestor of a kingdom
$edges[2000000] = 0;
$edges[2000001] = 0;
$edges[2000002] = 0;
$edges[2000003] = 0;

// roots of major subtrees
function name_to_index($string)
{
	switch ($string)
	{
		case 'Animalia':
			$index = 2000000;
			break;

		case 'Plantae':
			$index = 2000001;
			break;

		case 'Fungi':
			$index = 2000002;
			break;

		case 'Protista':
			$index = 2000003;
			break;
			
		default:
			$index = 2000004;
			break;				
	}				
	return $index;
}

$file_handle = fopen($filename, "r");
while (!feof($file_handle)) 
{
	$line = trim(fgets($file_handle));
		
	$row = explode("\t",$line);
	
	// print_r($row);
	
	$go = is_array($row) && count($row) > 1;
	
	if ($go)
	{
		$data = new stdclass;
	
		foreach ($row as $k => $v)
		{
			if ($v != '')
			{
				$data->{$headings[$k]} = $v;
			}
		}
		
		// print_r($data);
		
		// store node
		$node_index = 0;
		
		if (preg_match('/^[0-9]+$/', $data->taxonID))
		{
			$node_index = $data->taxonID;
			$nodes[$node_index] = $data->taxonID . '|' . $data->scientificName;
		}
		elseif (preg_match('/^BOLD:(.*)/', $data->taxonID, $m))
		{
			// strip prefox from BIN and convert to integer
			$bin_no_prefix = $m[1];
			$node_index = $b->decode($bin_no_prefix);
			
			$nodes[$node_index] = 'BOLD:' . $bin_no_prefix . '|' . 'BOLD:' . $bin_no_prefix;
		}
		else
		{
			// named node that is root of major subtree of life
			$node_index = name_to_index($data->taxonID);	
		}
		
		$parent_index = 0;
		if (isset($data->parentNameUsageID))
		{
			if (preg_match('/^[0-9]+$/', $data->parentNameUsageID))
			{
				$parent_index = $data->parentNameUsageID;
			}
			else
			{
				$parent_index = name_to_index($data->parentNameUsageID);
			}
			$edges[$node_index] = $parent_index;
		}
	}	
	
	$row_count++;
	
	if ($row_count == 100)
	{
		//break;
	}
	
}	


// export as TGF...

// nodes
foreach ($nodes as $index => $label)
{
	echo "$index $label\n";
}

echo "#\n";

// edges
foreach ($edges as $target => $source)
{
	echo "$source $target\n";	
}

?>
