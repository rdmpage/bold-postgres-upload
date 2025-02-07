<?php

// Handle Latin29 numbers

// See https://github.com/CatalogueOfLife/backend/blob/master/api/src/main/java/life/catalogue/common/id/IdConverter.java// 

/*

Latin 29

23456789BCDFGHJKLMNPQRSTVWXYZ
          |         | 
          1111111111222222222
01234567890123456789012345678

*/

class BaseConverter 
{
	var $symbols;
	var $chars;
	var $radix;
	var $values;
	
	//------------------------------------------------------------------------------------
	// default is Latin29
	function __construct($symbols = "23456789BCDFGHJKLMNPQRSTVWXYZ")
	{
		// symbols we use
		$this->symbols = $symbols;
		
		// symbols as array
		$this->chars = str_split($this->symbols);
		
		// number of symbols
		$this->radix = count($this->chars);
		
		// map between character and correspondng value
		$this->values = array();
		for ($i = 0; $i < $this->radix; $i++)
		{
			$this->values[$this->chars[$i]] = $i;
		}		
	}

	//------------------------------------------------------------------------------------
	// Take number in base_$radix and convert to base_10
	function decode($code)
	{
		$number_as_chars = str_split($code);

		$num = 0;
		$length = count($number_as_chars) - 1;
		$power = pow($this->radix, $length);

		foreach ($number_as_chars as $c)
		{
			if (!isset($this->values[$c]))
			{
				echo "[$code] $c is not a valid symbol for this numbering system " . __FILE__ . " " . __LINE__ . "\n";
				exit();
			}
		
			$num += $this->values[strtoupper($c)] * $power;
			$power = $power / $this->radix;
		}

		return $num;
	}
	
	//------------------------------------------------------------------------------------
	// take a base_10 number and encode it
	function encode($number)
	{
		$bytes = array();

		if ($number == 0)
		{
			$bytes[0] = 0;
			$length = 1;
		}
		else
		{
			$length = ceil(log($number + 1, $this->radix));
	
			$index = $length;
			while ($number > 0)
			{
				$quotient = $number % $this->radix;
				$bytes[--$index] = $quotient;
				$number = (Integer)($number / $this->radix);
			}
		}
			
		$code = '';	
		for ($i = 0; $i < $length; $i++)
		{
			$code .= $this->chars[$bytes[$i]];
		}
	
		return $code;
	}
}

if (0)
{
	// "23456789BCDFGHJKLMNPQRSTVWXYZ";

	$b = new BaseConverter();

	$code = '8M8L';
	//$code = '3B';
	//$code = '10';
	
	$code = 'BNG7J';

	$number = $b->decode($code);
	
	$recode = $b->encode($number);

	echo "$code $number $recode\n";
}


if (0)
{
	// experiments with BOLD, numbers are huge, but SQLite and 64 bit PHP can handle them

	// Latin32
	$b = new BaseConverter("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ");

	$code = 'AAA7636';

	$number = $b->decode($code);
	
	$number = 9223372036854775807;
	
	$recode = $b->encode($number);

	echo "$code $number $recode\n";
	
	echo "PHP_INT_MAX=" . PHP_INT_MAX . "\n";
}

?>
