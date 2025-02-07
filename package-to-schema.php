<?php

// Read BOLD package and generate simple SQL schema

$file = 'BOLD_Public.06-Sep-2024/BOLD_Public.06-Sep-2024.datapackage.json';
$json = file_get_contents($file);
$obj = json_decode($json);

$database = 'sqlite';
$database = 'postgres';

$columns = array();

foreach ($obj->resources[0]->schema->fields as $field)
{
	// print_r($field);
	
	// fix bugs
	if ($field->name == 'province')
	{
		$field->name = 'province/state'; // package and TSV file differ in BOLD_Public.06-Sep-2024.datapackage.json
	}	
	
	if ($database == 'sqlite')
	{
		$values = array();
		$values[] = "`" . $field->name . "`";
		
		switch ($field->type)
		{
			case 'array':
				$values[] = 'TEXT';
				break;
	
			case 'char':
				$values[] = 'TEXT';
				break;
		
			case 'float':
				$values[] = 'REAL';
				break;
	
			case 'integer':
			case 'number':
				$values[] = 'INTEGER';
				break;
	
			case 'string':
				$values[] = 'TEXT';
				break;
	
			case 'string:date': // SQLite does not have a date type
				$values[] = 'TEXT';
				break;	
		
			default:
				$values[] = 'UNKNOWN';
				break;
		}
		
		if (count($columns) == 0)
		{
			$values[] = "NOT NULL PRIMARY KEY";
		}
			
		$columns[] = join(" ", $values);
	}
	
	if ($database == 'postgres')
	{
		$values = array();
		$values[] = '"' . $field->name . '"';
		
		switch ($field->type)
		{
			case 'array':
				switch ($field->name)
				{
					case 'coord':
						$values[] = 'real[]';
						break;
			
					default:
						$values[] = 'text[]';
						break;
				}
				break;
	
			case 'char':
				$values[] = 'char(1)';
				break;
		
			case 'float':
				$values[] = 'real';
				break;
	
			case 'integer':
			case 'number':
				$values[] = 'integer';
				break;
	
			case 'string':
				// https://maximorlov.com/char-varchar-text-postgresql/
				$values[] = 'text';
				break;
	
			case 'string:date': // SQLite does not have a date type
				$values[] = 'date';
				break;	
		
			default:
				$values[] = 'UNKNOWN';
				break;
		}
		
		if (count($columns) == 0)
		{
			$values[] = "NOT NULL PRIMARY KEY";
		}
			
		$columns[] = join(" ", $values);
	}	
	
}

echo "CREATE TABLE boldmeta (\n";
echo join("\n , ", $columns);
echo ");\n";

?>
