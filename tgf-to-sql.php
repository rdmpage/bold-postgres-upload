<?php

// Read a TGF file and export SQL

require_once (dirname(__FILE__) . '/n-tree.php');

//----------------------------------------------------------------------------------------
function tgf_to_tree($filename)
{
	$t = null;
	
	echo "-- getting tree\n";
	$reader = new ReadTreeTGF($filename);
	
	if ($reader->Read())
	{		
		$t = $reader->GetTree();
		
		if (0)
		{
			$writer = new WriteDot($t);
			$output = $writer->Write();
			echo $output;
			exit();
		}
		
		// weights
		// depth
		// score
		// lca (eventually, main reason is for Kraken-like tool)
		// visit numbers (for range queries)
		// leaf numbers (for Hilbert curves)
		
		// weights
		// number of leaves descendant from node, =1 if node is a leaf
		echo "-- building weights\n";
		$t->BuildWeights($t->GetRoot());

		// depth
		// number of nodes on path to root
		echo "-- computing depth\n";
		$n = new PreorderIterator($t->GetRoot());
		$q = $n->Begin();
		while ($q)
		{
			$q->SetAttribute('depth', $n->get_stack_size());
			$q = $n->Next();
		}
		
		// score
		// by default score is weight of node (i.e., number of leaves in subtree rooted at
		// this node), divided by the depth of the node. This scheme gives more weight to 
		// nodes closer to the root
		echo "-- scoring nodes\n";
		$n = new PreorderIterator($t->GetRoot());
		$q = $n->Begin();
		while ($q)
		{
			$q->SetAttribute('score', $q->GetAttribute('weight'));				
			$q->SetAttribute('score', $q->GetAttribute('score')  / max(1, $q->GetAttribute('depth')));
			
			$q = $n->Next();
		}
				
		// leaf numbers
		// leaves are ordered from left to right, leftmost leaf = 0
		echo "-- ordering leaves\n";
		$left_count = 0;
		$n = new NodeIterator($t->GetRoot());
		$q = $n->Begin();
		while ($q)
		{
			if ($q->IsLeaf())
			{
				$q->SetAttribute('leaf_number', $left_count++);
			}

			$q = $n->Next();
		}		

		// visitor numbers
		echo "-- adding vistor numbers\n";
		$left_count = 0;
		$n = new VisitorIterator($t->GetRoot());
		$q = $n->Begin();
		while ($q)
		{
			$q = $n->Next();
		}		
		
		// OK we now have a tree with all the extra bits added
	}
	
	return $t;
}

//----------------------------------------------------------------------------------------
function tree_to_sql(&$t)
{
	$batchsize  =    1000;  // 1K
	$batchsize  =      10;
	$batch = array();
	$row_startTime = microtime(true);
	
	$row_count = 0;
	
	$tablename = 'boldtree';

	$n = new NodeIterator($t->GetRoot());
	$q = $n->Begin();
	while ($q)
	{	
		$row = new stdclass;
		
		// id
		$row->id = $q->GetId();
		
		// ancestor
		$row->anc_id = null;
		if ($q->GetAncestor())
		{
			$row->anc_id = $q->GetAncestor()->GetId();			
		}
		
		// external id and name
		$row->external_id = null;		
		// label may include external identifier if these aren't the same as the integer source->target ids
		if (preg_match('/(.*)\|(.*)/', $q->GetLabel(), $m))
		{
			$row->external_id = $m[1];
			$row->name = $m[2];
		}
		else
		{		
			$row->name = $q->GetLabel();
		}
		
		$row->weight = $q->GetAttribute('weight');
		$row->depth = $q->GetAttribute('depth');
		$row->score = $q->GetAttribute('score');

		$row->leaf_number = null;
		if (!is_null($q->GetAttribute('leaf_number')))
		{
			$row->leaf_number = $q->GetAttribute('leaf_number');
		}

		$row->left = null;
		if (!is_null($q->GetAttribute('left')))
		{
			$row->left = $q->GetAttribute('left');
		}

		$row->right = null;
		if (!is_null($q->GetAttribute('right')))
		{
			$row->right = $q->GetAttribute('right');
		}
		
		// SQL
		$keys = array();
		$values = array();

		foreach ($row as $k => $v)
		{
			$keys[] = '"' . $k . '"'; // must be double quotes
			
			if (is_null($v))
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
			else
			{				
				$values[] = "'" . str_replace("'", "''", $v) . "'";
			}					
		}
		
		$batch[] = '(' . join(",", $values) . ')';
		
		if (count($batch) == $batchsize)
		{
			echo "Row count: $row_count\n";
	
			$row_endTime = microtime(true);
			$row_executionTime = $row_endTime - $row_startTime;
			$formattedTime = number_format($row_executionTime, 3, '.', '');
			echo "Took " . $formattedTime . " seconds to process " . count($batch) . " rows.\n";
	
			
			if (1)
			{
				$sql = 'INSERT INTO ' . $tablename . ' (' . join(',', $keys) . ') VALUES' . "\n";
				$sql .= join(",\n", $batch);
				$sql .= " ON CONFLICT DO NOTHING;";
				
				$startTime = microtime(true);
		
				echo "Uploading " . count($batch) . " rows to psql\n";
				
				//$result = pg_query($db, $sql);
				
				echo $sql;
				exit();
		
				$endTime = microtime(true);
				$executionTime = $endTime - $startTime;
				$formattedTime = number_format($executionTime, 3, '.', '');
				echo "Execution time: " . $formattedTime . " seconds\n\n";
				
				$row_startTime = microtime(true);
			}
			
			$batch = array();
		}	
		
		$row_count++;
			
		$q = $n->Next();
	}
	
	// left over?
	if (count($batch) == $batchsize)
	{
		echo "Row count: $row_count\n";

		$row_endTime = microtime(true);
		$row_executionTime = $row_endTime - $row_startTime;
		$formattedTime = number_format($row_executionTime, 3, '.', '');
		echo "Took " . $formattedTime . " seconds to process " . count($batch) . " rows.\n";
		
		if (0)
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
	}	
	
}

//----------------------------------------------------------------------------------------
	
	
$filename = '';
if ($argc < 2)
{
	echo "Usage: " . basename(__FILE__) . " <filename>\n";
	exit(1);
}
else
{
	$filename = $argv[1];
}

$basename = basename($filename, '.tgf');		

echo "-- reading $filename\n";
$t = tgf_to_tree($filename);

echo "-- converting to SQL\n";
tree_to_sql($t);



