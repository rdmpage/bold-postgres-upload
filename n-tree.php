<?php

// Class to handle n-trees

error_reporting(E_ALL);

ini_set('memory_limit', '-1');

//----------------------------------------------------------------------------------------
class Node 
{
	var $id 		= 0;
	var $label 	= '';
	
	var $ancestor 	= null;
	var $child 		= null;
	var $sibling 	= null;
	
	var $weight 	= 0;
	var $depth 		= 0;
	var $score 		= 0;
	
	var $left		= 0; // PHP_INT_MAX;
	var $right		= 0;
	
	//------------------------------------------------------------------------------------
	function __construct($id = 0, $label = '')
	{
		$this->id 		= $id;	
		$this->label 	= $label;	
		
		$this->ancestor = null;
		$this->child 	= null;
		$this->sibling 	= null;
		
		$this->weight 	= 1;
		$this->depth 	= 0;
		$this->score 	= 0;	
		
		$this->visit 	= 0;	
	}
	
	//------------------------------------------------------------------------------------
	function Dump()
	{
		$padding = 10;
		
		$empty_string = '';
		for ($i = 0; $i < $padding; $i++)
		{
			$empty_string .= ' ';
		}
	
		echo str_pad($this->id, $padding, ' ', STR_PAD_LEFT);
		echo ' ' . str_pad(substr($this->label, 0, 20), 20, ' ', STR_PAD_RIGHT);
		echo ' ' . str_pad($this->weight, $padding, ' ', STR_PAD_LEFT);
		echo ' ' . str_pad($this->depth, $padding, ' ', STR_PAD_LEFT);
		//echo ' ' . str_pad($this->score, $padding, ' ', STR_PAD_LEFT);

		if (isset($this->leaf_number))
		{
			echo str_pad($this->leaf_number, $padding, ' ', STR_PAD_LEFT);
		}
		else
		{
			echo $empty_string;
		}

		echo str_pad($this->left, $padding, ' ', STR_PAD_LEFT);
		echo str_pad($this->right, $padding, ' ', STR_PAD_LEFT);
		
		// separate node attributes from tree structure
		echo " | ";
	
		echo ' ';
		if ($this->child)
		{
			echo str_pad($this->child->id, $padding, ' ', STR_PAD_LEFT);
		}
		else
		{
			echo $empty_string;
		}

		echo ' ';
		if ($this->sibling)
		{
			echo str_pad($this->sibling->id, $padding, ' ', STR_PAD_LEFT);
		}
		else
		{
			echo $empty_string;
		}

		echo ' ';
		if ($this->ancestor)
		{
			echo str_pad($this->ancestor->id, $padding, ' ', STR_PAD_LEFT);
		}
		else
		{
			echo $empty_string;
		}
			
		echo "\n";
	}	
	
	//------------------------------------------------------------------------------------
	function GetAncestor() { return $this->ancestor; }	

	//------------------------------------------------------------------------------------
	function GetAttribute($k) 
	{ 
		$v = null;
		if (isset($this->{$k}))
		{
			$v = $this->{$k};
		}
		return $v; 
	}	
	
	//------------------------------------------------------------------------------------
	function GetChild() { return $this->child; }	
	
	//------------------------------------------------------------------------------------
	// Children of node (as array)
	function GetChildren()
	{
		$children = array();
		$p = $this->child;
		if ($p)
		{
			array_push($children, $p);
			$p = $p->sibling;
			while ($p)
			{
				array_push($children, $p);
				$p = $p->sibling;
			}
		}
		return $children;
	}	

	//------------------------------------------------------------------------------------
	function GetId() { return $this->id; }	

	//------------------------------------------------------------------------------------
	function GetLabel() { return $this->label; }	
	
	//------------------------------------------------------------------------------------
	// If node is sibling get node immediately preceding it ("to the left")
	function GetLeftSibling()
	{
		$q = $this->ancestor->child;
		while ($q->sibling != $this)
		{
			$q = $q->sibling;
		}
		return $q;
	}
	
	//------------------------------------------------------------------------------------
	function GetRightMostSibling()
	{
		$p = $this;
		
		while ($p->sibling)
		{
			$p = $p->sibling;
		}
		return $p;
	}

	//------------------------------------------------------------------------------------
	function GetSibling() { return $this->sibling; }	
	
	//------------------------------------------------------------------------------------
	function IsChild()
	{
		$is_child = false;
		$q = $this->ancestor;
		if ($q)
		{
			$is_child = ($q->child === $this);
		}
		return $is_child;
	}
	
	//------------------------------------------------------------------------------------
	function IsLeaf()
	{
		return ($this->child == NULL);
	}
	
	//------------------------------------------------------------------------------------
	function SetAncestor($p)
	{
		$this->ancestor = $p;
	}
	
	//------------------------------------------------------------------------------------
	function SetAttribute($k, $v)
	{
		$this->{$k} = $v;
	}

	//------------------------------------------------------------------------------------
	function SetChild($p)
	{
		$this->child = $p;
	}
	
	//------------------------------------------------------------------------------------
	function SetId($id)
	{
		$this->id = $id;
	}
	
	//------------------------------------------------------------------------------------
	function SetLabel($label)
	{
		$this->label = $label;
	}
	
	//------------------------------------------------------------------------------------
	function SetSibling($p)
	{
		$this->sibling = $p;
	}

}


//----------------------------------------------------------------------------------------
class Tree 
{
	var $nodes = array();
	var $root = null;

	//------------------------------------------------------------------------------------
	function __construct()
	{
	
	}
	
	//------------------------------------------------------------------------------------
	// get a node from its id
	function GetNode($id)
	{
		if (isset($this->nodes[$id]))
		{
			return $this->nodes[$id];
		}
		else
		{
			return null;
		}
	}
	
	//------------------------------------------------------------------------------------
	// how many nodes do we have?
	function GetNumNodes()
	{
		return count($this->nodes);
	}
	
	//------------------------------------------------------------------------------------
	// how many leaves do we have?
	function GetNumLeaves()
	{
		$n = 0;
		foreach ($this->nodes as $id => $node)
		{
			if ($node->IsLeaf())
			{
				$n++;
			}
		}
		
		return $n;
	}
	
	//------------------------------------------------------------------------------------
	// root
	function GetRoot()
	{
		if (!$this->root)
		{		
			foreach ($this->nodes as $id => $node)
			{
				if (!$node->GetAncestor())
				{
					$this->root = $node;
					break;
				}
			}
		}
		return $this->root;
	}
	
	//------------------------------------------------------------------------------------
	// create a new node, avoid id collisions
	function AddNode($id = null, $label = '')
	{
		$ok = true;
		$node = null;
		
		if (is_null($id))
		{
			$id = $this->getNumNodes();			
		}
		else
		{
			if ($this->GetNode($id))
			{
				$ok = false; // we already have a node with this id
			}
		}
		
		if ($ok)
		{		
			$node = new Node($id, $label);
			$this->nodes[$id] = $node;
		}
		
		return $node;
	
	}
	
	//------------------------------------------------------------------------------------
	function AddEdge($source_id, $target_id)
	{
		$p = $this->GetNode($source_id);
		$q = $this->GetNode($target_id);
		
		if ($p && $q)
		{
			// p is ancestor, q is a descendamt
			
			$q->SetAncestor($p);
			
			$r = $p->GetChild();
			
			if ($r)
			{
				$r = $r->GetRightMostSibling();
				$r->SetSibling($q);
			}
			else
			{
				$p->SetChild($q);
			}
		}
	
	}
	
	//------------------------------------------------------------------------------------
	function BuildWeights($p)
	{
		if ($p)
		{
			$p->weight = 0;
						
			$this->BuildWeights($p->GetChild());
			$this->BuildWeights($p->GetSibling());
			
			if ($p->Isleaf())
			{
				$p->weight = 1;
			}
			if ($p->GetAncestor())
			{
				$p->GetAncestor()->weight += $p->weight;
			}
		}
	}

	//------------------------------------------------------------------------------------
	function Dump()
	{		
	
		echo "Tree dump\n";
		$n = new NodeIterator ($this->root);
		$a = $n->Begin();
		while ($a != NULL)
		{
			$a->Dump();
			$a = $n->Next();
		}
	}


}

//----------------------------------------------------------------------------------------
// Postorder iterator
class NodeIterator
{
	var $root;
	var $cur;
	var $stack;
	
	//------------------------------------------------------------------------------------
	// Starting point
	function __construct($subtree_root)
	{
		$this->root 	= $subtree_root;
		$this->stack 	= array();
		$this->cur 		= null;
	}
	
	//------------------------------------------------------------------------------------
	// The first node of the tree
	function Begin()
	{
		$this->cur = $this->root;
		while ($this->cur->GetChild())
		{
			array_push($this->stack, $this->cur);			
			$this->cur = $this->cur->GetChild();
		}
		return $this->cur;	
	}
	
	//------------------------------------------------------------------------------------
	// The next node in the tree, or NULL if all nodes have been visited.
	function Next()
	{
		if (count($this->stack) == 0)
		{
			$this->cur = null;
		}
		else
		{
			if ($this->cur->GetSibling())
			{
				$p = $this->cur->GetSibling();
				while ($p->GetChild())
				{
					array_push($this->stack, $p);
					$p = $p->GetChild();
				}
				$this->cur = $p;
			}
			else
			{
				$this->cur = array_pop($this->stack);
			}
		}
		return $this->cur;
	}
	
	//------------------------------------------------------------------------------------
	function get_stack_size()
	{
		return count($this->stack);
	}
	
}

//----------------------------------------------------------------------------------------
// Preorder iterator
class PreorderIterator extends NodeIterator
{
	//------------------------------------------------------------------------------------
	function Begin()
	{
		$this->cur = $this->root;
		return $this->cur;	
	}
	
	//------------------------------------------------------------------------------------
	function Next()
	{
		if ($this->cur->GetChild())
		{
			array_push($this->stack, $this->cur);
			$this->cur = $this->cur->GetChild();
		}
		else
		{
			while (!empty($this->stack)
				&& ($this->cur->GetSibling() == NULL))
			{
				$this->cur = array_pop($this->stack);
			}
			if (empty($this->stack))
			{
				$this->cur = NULL;
			}
			else
			{
				$this->cur = $this->cur->GetSibling();
			}
		}
		return $this->cur;
	}
	
}

//----------------------------------------------------------------------------------------
class VisitorIterator extends PreorderIterator
{
	var $visit_number = 0;
	
	//------------------------------------------------------------------------------------
	function Begin()
	{
		$this->cur = $this->root;
		
		$this->visit_number = 0;
		$this->cur->SetAttribute('left', $this->visit_number++);
		$this->cur->SetAttribute('right', $this->visit_number);
		return $this->cur;	
	}
	
	//------------------------------------------------------------------------------------
	function Next()
	{
		if ($this->cur->GetChild())
		{
			array_push($this->stack, $this->cur);
			$this->cur = $this->cur->GetChild();
			
			$this->cur->SetAttribute('left', $this->visit_number++);
			$this->cur->SetAttribute('right', $this->visit_number);
		}
		else
		{
			while (!empty($this->stack)
				&& ($this->cur->GetSibling() == NULL))
			{
				$this->cur = array_pop($this->stack);
				$this->cur->SetAttribute('right', $this->visit_number++);

			}
			if (empty($this->stack))
			{
				$this->cur = NULL;
			}
			else
			{
				$this->cur = $this->cur->GetSibling();
				$this->cur->SetAttribute('left', $this->visit_number++);
				$this->cur->SetAttribute('right', $this->visit_number);
			}
		}
		return $this->cur;
	}
	
}


//----------------------------------------------------------------------------------------
// Write tree
class TreeWriter
{
	var $tree;
	
	//------------------------------------------------------------------------------------
	function __construct($t)
	{
		$this->tree = $t;
	}

	function Write() {}

}

//----------------------------------------------------------------------------------------
// Write tree in Graphviz DOT format
class WriteDot extends TreeWriter
{
	//------------------------------------------------------------------------------------
	function Write()
	{
		$dot = "digraph{\n";
		$dot .= "rankdir=LR\n";
		
		$n = new NodeIterator ($this->tree->GetRoot());
		$q = $n->Begin();
		while ($q != NULL)
		{
			$dot .= "node [label=\"";			
			$label = $q->GetLabel();
			$dot .= addcslashes($label, '"') . "\"";						
			$dot .= "] n" . $q->GetId() . ";\n";
			$q = $n->Next();
		}
		
		// output edges
		$q = $n->Begin();
		while ($q != NULL)
		{
			if ($q->GetAncestor())
			{
				$dot .= "n" . $q->GetAncestor()->GetId() . " -> n" . $q->GetId() . ";\n";
			}
			$q = $n->Next();
		}
		$dot .= "}\n";
		return $dot;

	}

}

//----------------------------------------------------------------------------------------
// Read tree
class ReadTree
{
	var $tree;
	var $filename = '';
	
	//------------------------------------------------------------------------------------
	function __construct($filename)
	{
		$this->filename = $filename;
	}

	//------------------------------------------------------------------------------------
	function GetTree() { return $this->tree; }
	
	//------------------------------------------------------------------------------------
	function Read() {}

}

//----------------------------------------------------------------------------------------
// Read Trivial Graph Format
class ReadTreeTGF extends ReadTree
{
	
	//------------------------------------------------------------------------------------
	function Read() 
	{
		$file_handle = fopen($this->filename, "r");

		$this->tree = new Tree();

		$mode = 0;

		$ok = true;
		$done = false;

		while (!feof($file_handle) && !$done) 
		{
			$line = trim(fgets($file_handle));
	
			if ($line == "")
			{
				$done = true;
				break;
			}
	
			if (preg_match('/^#/', $line))
			{
				$mode = 1;
			}
			else
			{
				switch ($mode)
				{
					case 1:
						if (preg_match('/^\s*(?<source>\d+)\s+(?<target>\d+)(\s+?<label>[^\s].*)?$/', $line, $m))
						{
							$this->tree->AddEdge($m['source'], $m['target']);
						}
						else
						{
							$ok = false;
							$done = true;
						}			
						break;
			
					case 0:
					default:
						if (preg_match('/^\s*(?<id>\d+)\s+(?<label>[^\s].*)$/', $line, $m))
						{		
							$this->tree->AddNode($m['id'], $m['label']);
						}
						else
						{
							$ok = false;
						}
						break;
				}
			}
		}	

		return $ok;
	}


}

?>
