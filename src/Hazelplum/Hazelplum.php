<?php
/**
 * HazelplumDB
 * Delimited Text File Database System
 *
 * @package Hazelplum
 */

namespace Hazelplum;

use Hazelplum\Exception\DatabaseNotFoundException;

/*
Hazelplum DB v1.7 (2020-02-09))
Delimited Text File Database System

PUBLIC METHODS:
===============
- show_tables ()
- getTables ()
- select ($table, $columnlist='*', $criteria='', $order='', $headers=true)
- insert ($table, $columnlist='*', $inData)
- update ($table, $columnlist, $inUpdateData, $criteria='')
- delete ($table, $criteria)
- subSelect ($array, $index, $criteria, $order)

CHANGELOG:
==========
2006-06-18: Added delete() method.
2006-11-26: Added subSelect() method.
2006-11-26: Made the regex search case-insensitive (/expression/i)
            eg. if (preg_match($ccol_value."i", $aData[$r][$ccol_id])) {.
2007-01-05: Fixed a bug where the order by in the select would not work
            correctly when ordering by a field that contained an empty value.
2007-03-28: Added clearstatcache() in the _dtf_write_all() function.
            This allows multiple inserts within the same php script call.
2008-10-07: Updated comments, adjusted some formatting
2007-12-27: Updated insert() method to return the id of the new record.
2008-02-17: Modified so it returns an associative array as well as indices.
2008-03-26: Added option for sort descending (using 'colname DESC');
2008-11-19: Added options array, added option
            prepend_databasename_to_table_filename (defaults to false)
2008-11-20: Fixed warning that is produced when no results are returned
2009-02-11: Optimized performance of file retrieval methods;
            Added caching for dbd and dtf files.
2009-03-09: Updated _remove_headers to handle memory better
2009-09-20: Updated insert to not throw a notice error
            if not inserting all columns
2009-10-21: Updated select to default to not return headers
2009-10-21: Updated columnlist parsing to remove '`' character
2017-07-19: Update throw error to actually throw errors
            Correct bug with _dbd_parse_data not actually getting col names
 */

/**
 * Hazelplum
 *
 * @package Hazelplum
 * @version 1.6
 * @author Jansen Price <sumpygump@gmail.com>
 */
class Hazelplum
{
    const ERROR_DATABASE_NOT_FOUND = 1;

    /**
     * Path to where the datafiles live
     *
     * @var string
     */
    protected $datapath;

    /**
     * The name of the database
     *
     * @var string
     */
    protected $database_name;

    /**
     * The filename extension for the database definition files
     *
     * @var string
     */
    protected $db_fileextension;

    /**
     * The filename extension for the data table files
     *
     * @var string
     */
    protected $table_fileextension;

    /**
     * Column delimiter in the data files
     *
     * @var string
     */
    protected $col_delimiter;

    /**
     * Row delimiter in the data files
     *
     * @var mixed
     */
    protected $row_delimiter;

    /**
     * Storage for error messages
     *
     * @var array
     */
    protected $error;

    /**
     * Storage for list of tables
     *
     * @var array
     */
    protected $tables;

    /**
     * General options set on object
     *
     * @var array
     */
    private $_options;

    /**
     * Constructor
     *
     * @param string $in_datapath Path to database files
     * @param string $in_database_name Name of database
     * @param array $options Array of options to configure object
     */
    public function __construct($in_datapath, $in_database_name,
        $options=array())
    {
        $this->datapath = rtrim($in_datapath, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR;
        $this->database_name = $in_database_name;
        $this->tables = array();

        $this->db_fileextension = '.dbd';
        $this->table_fileextension = '.dtf';
        $this->col_delimiter = chr(31); // us
        $this->row_delimiter = chr(30); // rs

        $this->error = 0;
        $this->parseOptions($options);
        $this->_get_db_defs();
    }

    /**
     * _parse_options
     *
     * @param array $options The options to parse
     * @return void
     */
    private function parseOptions($options)
    {
        // set up defaults
        $this->_options = [
            'prepend_databasename_to_table_filename' => false,
            'use_cache' => true,
        ];

        // update with values from array passed in.
        foreach ($options as $key=>$option) {
            switch($key) {
            case 'prepend_databasename_to_table_filename':
                $this->_options[$key] = true;
                break;
            case 'no_cache':
                $this->_options['use_cache'] = false;
                break;
            case 'compat_delimiter_mode':
                // use the old standard delimiters
                $this->col_delimiter = chr(200);
                $this->row_delimiter = chr(201);
                break;
            }
        }
    }

    /**
     * Show tables
     *
     * Utility function to output a list of tables in the currently loaded
     * database
     *
     * @return void
     */
    public function show_tables()
    {
        echo "<div class=\"tabledef\" "
            . "style=\"font-family:courier new;font-size:10pt;\">\n";

        for ($i = 0; $i < count($this->tables); $i++) {
            echo "<b>" . $this->tables[$i]['name'] . "</b>"
                . " (".$this->tables[$i]['cols'].")\n"
                . "<div style=\"margin-left:1em;\">";

            for ($c = 0; $c < $this->tables[$i]['cols']; $c++) {
                echo $this->tables[$i][$c];
                if ($this->tables[$i][$c] == $this->tables[$i]['key']) {
                    echo " (PK)";
                }
                echo "<br />";
            }
            echo "</div><br />\n";
        }
        echo "</div>\n";
    }

    /**
     * Utility function to return a list of tables in the database.
     *
     * @return array
     */
    public function get_tables()
    {
        $tables = [];

        foreach($this->tables as $table) {
            $tables[] = $table['name'];
        }

        return $tables;
    }

    /**
     * Get the schema for a table
     *
     * @param mixed $table_name Name of table
     * @return array
     */
    public function get_table_schema($table_name)
    {
        if (!$table_name) {
            $this->_throw_error(2, 'Missing table name');
        }

        $tabid = $this->_get_tabid($table_name);

        if ($tabid === 'Not found') {
            $this->_throw_error(3, $table); // table doesn't exist
        }

        return $this->_get_cols_all($tabid);
    }

    /**
     * Get the primary key column name for a given table
     *
     * @param mixed $table_name The name of the table
     * @return string
     */
    public function get_primary_key($table_name)
    {
        if (!$table_name) {
            $this->_throw_error(2, 'Missing table name');
        }

        $tabid = $this->_get_tabid($table_name);

        if ($tabid === 'Not found') {
            $this->_throw_error(3, $table); // table doesn't exist
        }

        $key_id = $this->tables[$tabid]['key'];
        return $key_id;
    }

    /**
     * Return an array of data from database
     *
     * @param string $table The name of the table
     * @param string $columnlist Comma separated list of columns to select
     * @param string $criteria a simple statement to limit records (COL=VALUE)
     * @param string $order specify column name to for sort order of results
     * @param boolean $headers Return a row with the column names (1st row)
     * @return array An array of the data retrieved
     */
    public function select($table, $columnlist='*', $criteria='',
        $order='', $headers=false)
    {
        $aData = array();

        //verify table exists
        if (!empty($table)) {
            $tabid = $this->_get_tabid($table);
            if ($tabid === 'Not found') {
                $this->_throw_error(3, $table); //table does not exist.
            }
        } else {
            //table name is blank.
            $this->_throw_error(2, "Please enter table name");
        }

        //get the whole table
        $aColData = $this->_get_cols_all($tabid);
        $aData[0] = $aColData;

        $aTabData = $this->_get_table_data($table);
        $count    = count($aTabData);
        for ($r = 0; $r < $count; $r++) {
            $aData[] = $aTabData[$r];
        }

        //----------------------------------
        //parse criteria string (COL=VALUE)
        if ($criteria != '') {
            $criteria_def = explode("=", $criteria);
            $ccol_name    = trim($criteria_def[0]);
            $ccol_value   = trim($criteria_def[1]);
            if (substr($ccol_value, 0, 1) == "/"
                && substr($ccol_value, -1, 1) == "/"
            ) {
                $regex = true;
            } else {
                $regex = false;
            }

            $ccol_id = $this->_get_col_id($aColData, $ccol_name);
            if ($ccol_id === 'Not found') {
                //column doesn't exist. Ignore it.
            } else {
                $aRetData   = array();
                $aRetData[] = $aData[0];
                $count      = count($aData);
                for ($r=1;$r<$count;$r++) {
                    if ($regex) {
                        //use regular expression
                        if (preg_match($ccol_value."i", $aData[$r][$ccol_id])) {
                            //found a match, return this row.
                            $aRetData[] = $aData[$r];
                        }
                    } else {
                        //plain search.
                        if ($aData[$r][$ccol_id] == $ccol_value) {
                            //found a match, return this row.
                            $aRetData[] = $aData[$r];
                        }
                    }
                }
                $aData = $aRetData;
            }
        }

        //----------------------------------
        //parse order string (comma delimited??)
        if ($order != '') {
            //get the key column id
            $key_id = $this->_get_col_id(
                $aColData, $this->tables[$tabid]['key']
            );

            $order_parts = explode(" ", $order);
            $ocol_id     = $this->_get_col_id($aColData, $order_parts[0]);
            if ($ocol_id === 'Not found') {
                //column doesn't exist. Ignore it.
            } else {
                $order_values = array();
                $count        = count($aData);
                for ($r=1;$r<$count;$r++) {
                    $order_values[$r] = $aData[$r][$ocol_id];
                }
                //uasort($order_values, strcasecmp);
                natsort($order_values);

                if (isset($order_parts[1])
                    && strtolower($order_parts[1]) == 'desc'
                ) {
                    $order_values = array_reverse($order_values, true);
                }

                $aRetData    = array();
                $aRetData[0] = $aData[0];
                reset($aData);
                reset($order_values);
                while (list($key, $value) = each($order_values)) {
                    $aRetData[] = $aData[$key];
                    //$aRetData[] = $aData[key($order_values)];
                    //next($order_values);
                }
                $aData = $aRetData;
            }
        }

        if (!$headers) {
            $this->_remove_headers($aData);
        }

        //convert to assoc array
        $aRetData = array();
        $count    = count($aData);
        for ($r=0;$r<$count;$r++) {
            if ($aData[$r]) {
                foreach ($aData[$r] as $key => $value) {
                    // assoc
                    $aRetData[$r][$aColData[$key]] = $value;
                    // int
                    //$aRetData[$r][$key]            = $value;
                }
            }
        }
        $aData = $aRetData;

        //----------------------------------
        //limit data returned by $columnlist input param (comma delimited)
        if ($columnlist != '*') {
            // Remove ` character to be compatible with MySql format
            $columnlist = str_replace('`', '', $columnlist);

            $cols = explode(",", $columnlist);

            $aRetData = array();
            $count    = count($aData);
            for ($r=0;$r<$count;$r++) {
                foreach ($cols as $key=>$value) {
                    $value = trim($value);

                    $aRetData[$r][$key]   = $aData[$r][$value];
                    $aRetData[$r][$value] = $aData[$r][$value];
                }
            }

            $aData = $aRetData;
        }

        return $aData;
    }

    /**
     * Insert data into the desired table
     *
     * @param string $table The name of the table
     * @param string $columnlist Comma separated list of columns to insert into
     * @param array $inData An key-value array of the data to be inserted.
     * @return int|bool The inserted id or false
     */
    public function insert($table, $columnlist='*', $inData=array())
    {
        $make_new_table = false;

        if (!file_exists($this->_get_table_filename($table))) {
            $make_new_table = true;
        } else {
            $test_data = file_get_contents($this->_get_table_filename($table));
            if (empty($test_data) || preg_match("/^\s+$/", $test_data)) {
                $make_new_table = true;
            }
        }

        //verify table exists in db.
        if (!empty($table)) {
            $tabid = $this->_get_tabid($table);
            if ($tabid === 'Not found') {
                $this->_throw_error(3, $table); //table does not exist.
                $make_new_table = true;
            }
        } else {
            //table name is blank.
            $this->_throw_error(2, "Please enter table name");
        }

        //get the whole table
        $aColData = $this->_get_cols_all($tabid);
        $aTabData = $this->_dtf_parse_data($table);
        $count    = count($aTabData);
        for ($r=0;$r<$count;$r++) {
            $aData[] = $aTabData[$r];
        }

        //do the work
        if ($columnlist != '*') {
            // Remove ` character to be compatible with MySql format
            $columnlist = str_replace('`', '', $columnlist);

            $column_subset = explode(",", $columnlist);

            //cols is an array of the columnids that should be used
            $cols   = array();
            $count  = count($column_subset);
            $counta = count($aColData);
            for ($s=0;$s<$count;$s++) {
                for ($c=0;$c<$counta;$c++) {
                    if ($aColData[$c] == trim($column_subset[$s])) {
                        $cols[] = $c;
                    }
                }
            }

            $autokey = false;
            if (!$make_new_table) {
                $result = array_search(
                    $this->tables[$tabid]['key'], $column_subset
                );
                if ($result === false) {
                    //if user did not supply the key column as one of the
                    //columns, automatically make the next number (id).

                    //get the key column id
                    $key_id = $this->_get_col_id(
                        $aColData, $this->tables[$tabid]['key']
                    );

                    //get an array of all the key values
                    $key_values = array();
                    $count      = count($aData);
                    for ($r=0;$r<$count;$r++) {
                        $key_values[] = $aData[$r][$key_id];
                    }
                    $max_id  = max($key_values);
                    $next_id = $max_id + 1;

                    $autokey = true;
                } else {
                    //make sure that the key supplied is not duplicate.
                    $key_id = $this->_get_col_id(
                        $column_subset, $this->tables[$tabid]['key']
                    );

                    //get an array of all the key values
                    $key_values = array();
                    $count      = count($aData);
                    for ($r=1;$r<$count;$r++) {
                        $key_values[] = $aData[$r][$key_id];
                    }

                    if (array_search($inData[$key_id], $key_values)) {
                        //invalid key; not unique.
                        $this->_throw_error(4, $inData[$key_id]);
                        $this->error = 4;
                    }
                }
                end($aData);
                $arraykey = key($aData)+1;
            } else {
                //making a new table

                //get the key column id
                $key_id = $this->_get_col_id(
                    $aColData, $this->tables[$tabid]['key']
                );

                $result = array_search(
                    $this->tables[$tabid]['key'], $column_subset
                );
                if ($result === false) {
                    //if user did not supply the key column as one of the
                    //columns, automatically make the next number (id).
                    $next_id = 1;
                    $autokey = true;
                }
                $arraykey = 0;
            }

            if (!$this->error) {
                //add next keyid if it is to be automatic
                if ($autokey) {
                    $aData[$arraykey][$key_id] = $next_id;
                }

                //append the data to the table array
                $count = count($inData);
                for ($i=0;$i<$count;$i++) {
                    $aData[$arraykey][$cols[$i]] = $inData[$i];
                }
                $count = count($aColData);
                for ($i=0;$i<$count;$i++) {
                    if (!isset($aData[$arraykey][$i])
                        || $aData[$arraykey][$i] == ''
                    ) {
                        $aData[$arraykey][$i] = '';
                    }
                }

            }
            //$aData = $this->_remove_headers($aData);
            $this->_dtf_write_all($this->tables[$tabid]['name'], $aData);
            return $next_id;
        }
        return false;
    }

    /**
     * Update some rows in a table with data
     *
     * @param string $table The name of the table
     * @param string $columnlist Comma separated list of column names
     * @param array $inUpdateData Array of the data to be updated
     *                            (matches $columnlist)
     * @param string $criteria A simple clause to limit the records that
     *                         will be updated (COL=VALUE)
     * @return void
     */
    public function update($table, $columnlist, $inUpdateData, $criteria='')
    {
        //verify table exists in db.
        if (!empty($table)) {
            $tabid = $this->_get_tabid($table);
            if ($tabid === 'Not found') {
                $this->_throw_error(3, $table); //table does not exist.
                $make_new_table = true;
            }
        } else {
            // table name is blank.
            $this->_throw_error(2, "Please enter table name");
        }

        //get the whole table
        $aColData = $this->_get_cols_all($tabid);
        //$aData[0] = $aColData;
        $aTabData = $this->_dtf_parse_data($table);
        for ($r=0;$r<count($aTabData);$r++) {
            $aData[] = $aTabData[$r];
        }

        //----------------------------------
        //parse criteria string (COL=VALUE)
        if ($criteria != '') {
            $criteria_def = explode("=", $criteria);
            $ccol_name    = trim($criteria_def[0]);
            $ccol_value   = trim($criteria_def[1]);
            if (substr($ccol_value, 0, 1) == "/"
                && substr($ccol_value, -1, 1) == "/"
            ) {
                $regex = true;
            } else {
                $regex = false;
            }

            $ccol_id = $this->_get_col_id($aColData, $ccol_name);
            if ($ccol_id === 'Not found') {
                //column doesn't exist. Ignore it.
            } else {
                $rowSubset = array();
                for ($r=0;$r<count($aData);$r++) {
                    if ($regex) {
                        //use regular expression
                        if (preg_match($ccol_value."i", $aData[$r][$ccol_id])) {
                            //found a match, return this row.
                            $rowSubset[] = $r;
                        }
                    } else {
                        //plain search.
                        if ($aData[$r][$ccol_id] == $ccol_value) {
                            //found a match, return this row.
                            $rowSubset[] = $r;
                        }
                    }
                }
            }
        }

        //----------------------------------
        //parse column names (col1, col2, col3...)
        if ($columnlist != '') {
            // Remove ` character to be compatible with MySql format
            $columnlist = str_replace('`', '', $columnlist);

            $column_subset = explode(",", $columnlist);
            //cols is an array of the columnids that should be used
            $cols = array();
            for ($s=0;$s<count($column_subset);$s++) {
                $cols[] = $this->_get_col_id(
                    $aColData, trim($column_subset[$s])
                );
            }

            //loop through each record subset
            for ($r=0;$r<count($rowSubset);$r++) {
                for ($c=0;$c<count($cols);$c++) {
                    $aData[$rowSubset[$r]][$cols[$c]] = $inUpdateData[$c];
                }
            }

            //write the new table to the file.
            $this->_dtf_write_all($this->tables[$tabid]['name'], $aData);
        }
        return true;
    }

    /**
     * Update a specific field in a row.
     *
     * @param string $table The name of the table
     * @param string $record_id The record_id for which to update the data
     * @param string $col_id The id of the column to be updated
     * @param string $col_value The value to set the column to.
     * @return void
     */
    private function _update_field($table, $record_id, $col_id, $col_value='')
    {
        //get the whole table
        $tabid = $this->_get_tabid($table);
        $aData = $this->_dtf_parse_data($table);

        //update this field in this record
        $aData[$record_id][$col_id] = $col_value;

        //write the new table to the file.
        $this->_dtf_write_all($this->tables[$tabid]['name'], $aData);
    }

    /**
     * Delete record(s) from a table
     *
     * @param string $table The name of the table
     * @param string $criteria A Simple string to indicate a condition
     *                         for which to remove records (COL=VALUE)
     * @return void
     */
    public function delete($table, $criteria='')
    {
        //verify table exists in db.
        if (!empty($table)) {
            $tabid = $this->_get_tabid($table);
            if ($tabid === 'Not found') {
                $this->_throw_error(3, $table); //table does not exist.
                $make_new_table = true;
            }
        } else {
            //table name is blank.
            $this->_throw_error(2, "Please enter table name");
        }

        //get the whole table
        $aColData = $this->_get_cols_all($tabid);

        $aTabData = $this->_dtf_parse_data($table);
        for ($r=0;$r<count($aTabData);$r++) {
            $aData[] = $aTabData[$r];
        }

        //----------------------------------
        //parse criteria string (COL=VALUE)
        if ($criteria != '') {
            $criteria_def = explode("=", $criteria);
            $ccol_name    = trim($criteria_def[0]);
            $ccol_value   = trim($criteria_def[1]);
            if (substr($ccol_value, 0, 1) == "/"
                && substr($ccol_value, -1, 1) == "/"
            ) {
                $regex = true;
            } else {
                $regex = false;
            }

            $ccol_id = $this->_get_col_id($aColData, $ccol_name);
            if ($ccol_id === 'Not found') {
                //column doesn't exist. Ignore it.
            } else {
                $aData2  = array();
                $deleted = array();
                for ($r=0;$r<count($aData);$r++) {
                    if ($regex) {
                        //use regular expression
                        if (preg_match($ccol_value."i", $aData[$r][$ccol_id])) {
                            //found a (non)-match, return this row.
                            $deleted[] = $aData[$r];
                        } else {
                            $aData2[] = $aData[$r];
                        }
                    } else {
                        //plain search.
                        if ($aData[$r][$ccol_id] == $ccol_value) {
                            //found a (non)-match, return this row.
                            $deleted[] = $aData[$r];
                        } else {
                            $aData2[] = $aData[$r];
                        }
                    }
                }
            }
        }
        //write the new table to the file.
        $this->_dtf_write_all($this->tables[$tabid]['name'], $aData2);
    }

    /**
     * Allows you to further filter an array based on criteria for a
     * specific index of the array.
     * This assumes you do not include an array with headers.
     *
     * @param array $arrayData An array of data returned from a
     *                         previous select() statement.
     * @param string $index The index of the array data to select
     * @param string $criteria A simple string to specify a criteria
     *                         for limiting the records (COL=VALUE)
     * @param string $order Column name to sort order by.
     * @return array An array of the data retrieved from subselect
     */
    public function subSelect($arrayData, $index, $criteria, $order)
    {
        if (!is_array($arrayData)) {
            $this->_throw_error(3, $table);
        }

        //----------------------------------
        //parse criteria string (COL=VALUE)
        if ($criteria != '') {
            if (substr($criteria, 0, 1) == "/"
                && substr($criteria, -1, 1) == "/"
            ) {
                $regex = true;
            } else {
                $regex = false;
            }

            $ccol_id = $index;
            if ($ccol_id === 'Not found') {
                //column doesn't exist. Ignore it.
            } else {
                $aRetData = array();
                //$aRetData[] = $arrayData[0];
                for ($r=0;$r<count($arrayData);$r++) {
                    if ($regex) {
                        //use regular expression
                        $result = preg_match(
                            $criteria . "i", $arrayData[$r][$index]
                        );
                        if ($result) {
                            //found a match, return this row.
                            $aRetData[] = $arrayData[$r];
                        }
                    } else {
                        //plain search.
                        if ($arrayData[$r][$index] == $criteria) {
                            //found a match, return this row.
                            $aRetData[] = $arrayData[$r];
                        }
                    }
                }
                $aData = $aRetData;
            }
        }

        //----------------------------------
        //parse order string (comma delimited??)
        if ($order != '') {
            $order_values = array();
            for ($r=0;$r<count($aData);$r++) {
                $order_values[$r] = $aData[$r][$order];
            }
            //uasort($order_values, strcasecmp);
            natsort($order_values);

            $aRetData = array();
            //$aRetData[0] = $aData[0];
            reset($aData);
            while ($r = current($order_values)) {
                $aRetData[] = $aData[key($order_values)];
                next($order_values);
            }
            $aData = $aRetData;
        }

        return $aData;
    }

    /**
     * Removes the headers (columnnames) from the data array.
     *
     * @param array &$aData The array data from which the data column
     *                      should be stripped.
     * @return void
     */
    private function _remove_headers(&$aData)
    {
        //return the array without the first row.
        array_shift($aData);
    }

    /**
     * Get the database definitions and store in the tables array.
     *
     * @return bool Whether the data was loaded successfully.
     */
    private function _get_db_defs()
    {
        if ($this->_options['use_cache'] && $this->_get_dbd_cache()) {
            return true;
        } else {
            return $this->_dbd_parse_data();
        }
    }

    /**
     * Get the datbase definitions from the cache file.
     *
     * @return bool Whether the file was found and loaded.
     */
    private function _get_dbd_cache()
    {
        $dbd_cache_file = $this->_get_dbd_cache_filename();
        if (file_exists($dbd_cache_file)) {
            $dbd_data     = file_get_contents($dbd_cache_file);
            $this->tables = eval('return ' . $dbd_data . ';');
            return true;
        }
        return false;
    }

    /**
     * Write the dbd cache file.
     *
     * @return void
     */
    private function _write_dbd_cache()
    {
        $dbd_cache_file = $this->_get_dbd_cache_filename();
        file_put_contents($dbd_cache_file, var_export($this->tables, 1));
    }

    /**
     * Get the filename for the dbd cache file.
     *
     * @return void
     */
    private function _get_dbd_cache_filename()
    {
        return $this->datapath . "." . $this->database_name
            . $this->db_fileextension . ".php";
    }

    /**
     * Return the tabid for a given table. If not found returns "Not found"
     *
     * @param string $table The table name
     * @return mixed
     */
    private function _get_tabid($table)
    {
        for ($t=0;$t<count($this->tables);$t++) {
            if ($this->tables[$t]['name'] == $table) {
                return $t;
            }
        }
        return "Not found";
    }

    /**
     * Return an array of all the columns for a tabid
     *
     * @param string $tabid Table id
     * @return array
     */
    private function _get_cols_all($tabid)
    {
        $aCols = array();
        for ($c=0;$c<$this->tables[$tabid]['cols'];$c++) {
            $aCols[] = $this->tables[$tabid][$c];
        }
        return $aCols;
    }

    /**
     * Returns a col_id of a column name from a column list array. If not
     * found returns "Not found"
     *
     * @param array $colList Array of column ids
     * @param string $colName Name of column
     * @return mixed
     */
    private function _get_col_id($colList, $colName)
    {
        for ($i=0;$i<count($colList);$i++) {
            if ($colList[$i]==$colName) {
                return $i;
            }
        }
        return "Not found";
    }

    /**
     * Get the filename for a table
     *
     * @param string $table Name of table
     * @return string The filename
     */
    private function _get_table_filename($table)
    {
        $filename = $this->datapath;
        if ($this->_options['prepend_databasename_to_table_filename']) {
            $filename .= $this->database_name . ".";
        }
        $filename .= $table . $this->table_fileextension;
        return $filename;
    }

    /**
     * Parse the dbd file, put degs in array tables.
     *
     * @return boolean
     */
    private function _dbd_parse_data()
    {
        $dbd_path = $this->datapath . DIRECTORY_SEPARATOR . $this->database_name . DIRECTORY_SEPARATOR . $this->db_fileextension;
        if (!file_exists($dbd_path)) {
            $this->throw_error(self::ERROR_DATABASE_NOT_FOUND, $this->database_name . $this->db_fileextension);
            return false;
        }

        $dbd_data = file_get_contents(
            $this->datapath . $this->database_name . $this->db_fileextension
        );
        if ($dbd_data) {
            $dbd   = explode("\n", $dbd_data);
            $tabid = 0;
            $cols  = 0;

            $this->tables[$tabid]['name'] = '';
            $this->tables[$tabid]['cols'] = 0;
            $this->tables[$tabid]['key']  = '';

            for ($line=0;$line<count($dbd);$line++) {
                if (substr($dbd[$line], 0, 2) == "**") {
                    $tabid++;
                    $cols = 0;

                    $this->tables[$tabid]['name'] = '';
                    $this->tables[$tabid]['cols'] = 0;
                    $this->tables[$tabid]['key']  = '';
                }

                $key = trim(substr($dbd[$line], 0, 4));
                $value = trim(substr($dbd[$line], 3));
                switch ($key) {
                case 'TAB':
                    $this->tables[$tabid]['name'] = $value;
                    break;
                case 'KEY':
                    $this->tables[$tabid]['key'] = $value;
                    $this->tables[$tabid][]      = $value;
                    $cols++;
                    $this->tables[$tabid]['cols'] = $cols;
                    break;
                case 'COL':
                    $this->tables[$tabid][] = $value;
                    $cols++;
                    $this->tables[$tabid]['cols'] = $cols;
                    break;
                }
            }
            $this->_write_dbd_cache();
            return true;
        } else {
            $this->_throw_error(
                1, $this->database_name . $this->db_fileextension
            );
            return false;
        }
    }

    /**
     * Get the data for a table
     *
     * @param string $table_name The name of the table
     * @return mixed
     */
    private function _get_table_data($table_name)
    {
        if ($this->_options['use_cache']) {
            $data = $this->_get_dtf_cache($table_name);
            if ($data) {
                return $data;
            }
        }
        return $this->_dtf_parse_data($table_name);
    }

    /**
     * Get table data from cache file
     *
     * @param string $table The name of the table
     * @return bool Whether the file was found and loaded.
     */
    private function _get_dtf_cache($table)
    {
        $dtf_cache_file = $this->_get_dtf_cache_filename($table);
        if (file_exists($dtf_cache_file)) {
            $dtf_data = file_get_contents($dtf_cache_file);
            return eval('return ' . $dtf_data . ';');
        }
        return false;
    }

    /**
     * Write the dtf cache file.
     *
     * @param string $table The name of the table
     * @param array $table_data The table data to write
     * @return void
     */
    private function _write_dtf_cache($table, $table_data)
    {
        $dtf_cache_file = $this->_get_dtf_cache_filename($table);
        file_put_contents($dtf_cache_file, var_export($table_data, 1));
    }

    /**
     * Get the filename for the dtf cache file for a table.
     *
     * @param string $table The name of the table
     * @return string
     */
    private function _get_dtf_cache_filename($table)
    {
        return $this->datapath . "." . $this->database_name
            . "." . $table . $this->table_fileextension . ".php";
    }

    /**
     * Cleat the dtf cache for a given table
     *
     * @param string $table The name of the table
     * @return void
     */
    private function _clear_dtf_cache($table)
    {
        $dtf_cache_file = $this->_get_dtf_cache_filename($table);
        if (is_file($dtf_cache_file)) {
            unlink($dtf_cache_file);
        }
    }

    /**
     * Parse a dtf file and return 2d array.
     *
     * @param string $table The name of the table to parse
     * @return mixed Array data or false
     */
    private function _dtf_parse_data($table)
    {
        $filename = $this->_get_table_filename($table);
        if ($contents = $this->_read_file($filename)) {
            $rows  = explode($this->row_delimiter, $contents);
            $count = count($rows);
            for ($row=0;$row<$count;$row++) {
                $rows[$row] = explode($this->col_delimiter, $rows[$row]);
                // trim white space off the first column.
                $rows[$row][0] = trim($rows[$row][0]);
            }
            unset($rows[$count-1]); //take off the last row (it is blank)
            $this->_write_dtf_cache($table, $rows);
            return $rows;
        } else {
            return false;
        }
    }

    /**
     * Writes record(s) to datafile
     *
     * This function is deprecated
     *
     * @param string $filename The name of the file
     * @param array $aData The data to write
     * @return void
     */
    private function _dtf_write($filename, $aData)
    {
        global $datapath; global $col_delimiter; global $row_delimiter;

        //open the file to append
        $writefile = fopen($this->datapath.$filename, 'a');
        $rowline   = "";

        for ($col=0; $col<count($aData); $col++) {
            $rowline .= $aData[$col].$col_delimiter;
        }

        //got to take off the last $col_delimiter at the end of the row.
        $rowline = substr($rowline, 0, -1);
        fputs($writefile, stripslashes($rowline));
        fputs($writefile, $row_delimiter."\n");

        fclose($writefile);
    }

    /**
     * Writes records into entire file
     *
     * @param string $tablename The name of the table
     * @param array $aData The array of data to write
     * @return void
     */
    private function _dtf_write_all($tablename, $aData)
    {
        $filename = $this->_get_table_filename($tablename);

        //open the file to write to.
        $writefile = fopen($filename, 'w');
        for ($row=0; $row<count($aData); $row++) {
            $rowline = "";
            for ($col=0; $col<count($aData[$row]); $col++) {
                $rowline .= $aData[$row][$col] . $this->col_delimiter;
            }

            //got to take off the last $col_delimiter at the end of the row.
            $rowline = substr($rowline, 0, -1);
            fputs($writefile, stripslashes($rowline));
            fputs($writefile, $this->row_delimiter."\n");
        }
        fflush($writefile);
        fclose($writefile);
        clearstatcache();
        $this->_clear_dtf_cache($tablename);
    }

    /**
     * Simply reads a file and returns the contents.
     *
     * @param mixed $filename The name of the file to read
     * @return void
     */
    private function _read_file($filename)
    {
        if (!file_exists($filename)) {
            return;
        }

        if (filesize($filename) > 0) {
            return file_get_contents($filename);
        } else {
            return false;
        }
    }

    /**
     * Custom method to indicate an error
     *
     * @param int $err The error code
     * @param string $context Additional message to accompany error
     * @return void
     */
    private function throw_error($err, $context = '')
    {
        switch ($err) {
        case self::ERROR_DATABASE_NOT_FOUND:
            throw new DatabaseNotFoundException(
                sprintf("E%s: DBD file not found: '%s'.", self::ERROR_DATABASE_NOT_FOUND, $context)
            );
            break;
        case 2:
            $message = "No table parameter: $context.";
            break;
        case 3:
            $message = "Invalid table name: $context.";
            break;
        case 4:
            $message = "Invalid key (not unique): $context.";
            break;
        }

        throw new \Exception($message);
    }
}
