<?php

/**
 * Hazelplum database
 *
 * Delimited Text File Database System
 *
 * @package Hazelplum
 */

namespace Hazelplum;

/*
Hazelplum DB v2.2 (2023-02-09))
Delimited Text File Database System

PUBLIC METHODS:
===============
get_tables ()
select ($table, $columnlist='*', $criteria='', $order='')
insert ($table, $columnlist='*', $in_data)
update ($table, $columnlist, $in_update_data, $criteria='')
delete ($table, $criteria)

CHANGELOG:
==========
2006-06-18: Added delete() method.
2006-11-26: Added subSelect() method.
2006-11-26: Made the regex search case-insensitive (/expression/i)
            eg. if (preg_match($ccol_value."i", $aData[$r][$ccol_id])) {.
2007-01-05: Fixed a bug where the order by in the select would not work
            correctly when ordering by a field that contained an empty value.
2007-03-28: Added clearstatcache() in the dtf_write_all() function.
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
            Correct bug with dbd_parse_data not actually getting col names
2020-02-09: Change caching strategy to use serialize instead of var_export/eval
2020-02-11: Remove headers option from select()
            Much clean up and refactoring
2021-06-12: Updates to make compatible with PHP 8.0
2023-01-17: Additional refactoring
2023-02-09: Add a few bug fixes found through testing
 */

/**
 * Hazelplum
 *
 * @package Hazelplum
 * @author Jansen Price <jansen.price@gmail.com>
 * @license http://www.opensource.org/licenses/mit-license.php MIT
 * @version 2.2
 */
class Hazelplum
{
    public const COL_DELIMITER = 31; // Unit separator
    public const ROW_DELIMITER = 30; // Row separator

    public const TABLE_INDEX_NOT_FOUND = -1;
    public const COL_INDEX_NOT_FOUND = -1;

    public const ERR_NONE = 0;
    public const ERR_DBD_FILE_MISSING = 1;
    public const ERR_MISSING_TABLE_PARAM = 2;
    public const ERR_TABLE_NOT_FOUND = 3;
    public const ERR_KEY_NOT_UNIQUE = 4;
    public const ERR_DBD_FILE_EMPTY = 5;
    public const ERR_INVALID_COLUMN_NAME = 6;
    public const ERR_COLUMN_LIST_MISMATCH = 7;
    public const ERR_AUTOKEY_FAIL = 8;

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
     * @var string
     */
    protected $row_delimiter;

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
    private $options;

    /**
     * Constructor
     *
     * @param string $in_datapath      Path to database files
     * @param string $in_database_name Name of database
     * @param array  $options          Array of options to configure object
     */
    public function __construct($in_datapath, $in_database_name, $options = [])
    {
        $this->datapath = rtrim($in_datapath, DIRECTORY_SEPARATOR);
        $this->database_name = $in_database_name;
        $this->tables = [];

        $this->db_fileextension = '.dbd';
        $this->table_fileextension = '.dtf';
        $this->col_delimiter = chr(self::COL_DELIMITER);
        $this->row_delimiter = chr(self::ROW_DELIMITER);

        $this->parse_options($options);
        $this->get_db_defs();
    }

    /**
     * Parse options
     *
     * @param  array $options The options to parse
     * @return void
     */
    private function parse_options($options)
    {
        // Set up defaults
        $this->options = [
            'prepend_databasename_to_table_filename' => false,
            'use_cache' => true,
        ];

        // Update with values from array passed in.
        foreach ($options as $key => $option) {
            switch ($key) {
                case 'prepend_databasename_to_table_filename':
                    $this->options[$key] = (bool) $option;
                    break;
                case 'use_cache':
                    $this->options['use_cache'] = (bool) $option;
                    break;
                case 'no_cache':
                    // This one is here for compatibility
                    $this->options['use_cache'] = ! (bool) $option;
                    break;
                case 'compat_legacy_delimiters':
                    if ($option) {
                        // Use the legacy delimiters
                        $this->col_delimiter = chr(200);
                        $this->row_delimiter = chr(201);
                    }
                    break;
            }
        }
    }

    /**
     * Get the options that are set
     *
     * @return array
     */
    public function get_options()
    {
        return $this->options;
    }

    /**
     * Utility function to return a list of tables in the database.
     *
     * @return array
     */
    public function get_tables()
    {
        $tables = [];

        foreach ($this->tables as $table) {
            $tables[] = $table['name'];
        }

        return $tables;
    }

    /**
     * Get the schema for a table
     *
     * @param  mixed $table_name Name of table
     * @return array
     */
    public function get_table_schema($table_name)
    {
        $tabid = $this->get_valid_tabid($table_name);
        return $this->get_cols_all($tabid);
    }

    /**
     * Get the valid table id otherwise throw error
     *
     * @param  mixed $table_name
     * @return int
     */
    protected function get_valid_tabid($table_name)
    {
        if (empty($table_name) || !$table_name) {
            $this->throw_error(
                self::ERR_MISSING_TABLE_PARAM,
                'Missing table name'
            );
        }

        $tabid = $this->get_tabid($table_name);

        if ($tabid === self::TABLE_INDEX_NOT_FOUND) {
            // Table doesn't exist
            $this->throw_error(
                self::ERR_TABLE_NOT_FOUND,
                $table_name
            );
        }

        return $tabid;
    }

    /**
     * Get the primary key column name for a given table
     *
     * @param  mixed $table_name The name of the table
     * @return string
     */
    public function get_primary_key($table_name)
    {
        $tabid = $this->get_valid_tabid($table_name);
        $key_id = $this->tables[$tabid]['key'];
        return $key_id;
    }

    /**
     * Return an array of data from database
     *
     * @param  string $table      The name of the table
     * @param  string $columnlist Comma separated list of columns to select
     * @param  string $criteria   Simple statement to limit records (COL=VALUE)
     * @param  string $order      Specify column name to for sort order of results
     * @return array An array of the data retrieved
     */
    public function select($table, $columnlist = '*', $criteria = '', $order = '')
    {
        // Verify table exists
        $tabid = $this->get_valid_tabid($table);

        // Get the whole table
        $cols = $this->get_cols_all($tabid);
        $data = $this->get_table_data($table);

        //----------------------------------
        // Parse criteria string (COL=VALUE)
        if ($criteria != '') {
            $criteria = $this->parse_criteria($criteria, $tabid);

            $criteria->col_id = $this->get_col_id($cols, $criteria->col_name);
            if ($criteria->col_id !== false) {
                $data = $this->find_matching_rows($criteria, $data);
            }
        }

        //----------------------------------
        // Parse order string (TODO: comma delimited??)
        if ($order != '') {
            $order_parts = explode(" ", $order, 2);
            $ocol_id = $this->get_col_id($cols, $order_parts[0]);
            if ($ocol_id === self::COL_INDEX_NOT_FOUND) {
                // Column doesn't exist. Ignore it.
            } else {
                $order_values = [];
                foreach ($data as $row) {
                    $order_values[] = $row[$ocol_id];
                }

                // Sort but keep array keys
                natsort($order_values);

                if (
                    isset($order_parts[1])
                    && trim(strtolower($order_parts[1])) == 'desc'
                ) {
                    $order_values = array_reverse($order_values, true);
                }

                $result_data = [];
                foreach ($order_values as $key => $value) {
                    $result_data[] = $data[$key];
                }
                $data = $result_data;
            }
        }

        // Convert to assoc array
        $result_data = [];
        foreach ($data as $row) {
            if ($row) {
                $record = [];
                foreach ($row as $key => $value) {
                    // assoc
                    $record[$cols[$key]] = $value;
                }
                $result_data[] = $record;
            }
        }
        $data = $result_data;

        //----------------------------------
        // Limit data returned by $columnlist input param (comma delimited or array)
        $cols_requested = $this->parse_columnlist($columnlist);
        if ($cols_requested != []) {
            // Validate the column names requested
            foreach ($cols_requested as $col) {
                $index = $this->get_col_id($cols, $col);
                if ($index === self::COL_INDEX_NOT_FOUND) {
                    $this->throw_error(
                        self::ERR_INVALID_COLUMN_NAME,
                        "on table " . $table . ": " . $col,
                    );
                }
            }

            // Now replace each row with the requested columns only
            $result_data = [];
            foreach ($data as $row) {
                $record = [];
                foreach ($cols_requested as $col_name) {
                    $record[$col_name] = $row[$col_name];
                }
                $result_data[] = $record;
            }

            $data = $result_data;
        }

        return $data;
    }

    /**
     * Insert data into the desired table
     *
     * @param  string $table      The name of the table
     * @param  string $columnlist Comma separated list of columns to insert into
     * @param  array  $in_data    An key-value array of the data to be inserted.
     * @return int|bool The inserted id or false
     */
    public function insert($table, $columnlist = '*', $in_data = [])
    {
        // Verify table exists in db.
        $tabid = $this->get_valid_tabid($table);

        $make_new_table = false;
        $table_filename = $this->get_table_filename($table);

        if (!file_exists($table_filename)) {
            $make_new_table = true;
        } else {
            $test_data = file_get_contents($table_filename);
            if (empty($test_data) || preg_match("/^\s+$/", $test_data)) {
                $make_new_table = true;
            }
        }

        // Get the whole table
        $data = [];
        $cols = $this->get_cols_all($tabid);
        $data = $this->dtf_parse_data($table);

        // Finalize input column list
        $column_subset = $this->parse_columnlist($columnlist);
        if ($column_subset == []) {
            // Use all columns
            $column_subset = $cols;
            $col_ids = array_keys($cols);
        } else {
            // col_ids is an array of the columnids that should be used
            $col_ids = [];
            $invalid_cols = [];
            foreach ($column_subset as $col) {
                $col_id = $this->get_col_id($cols, $col);
                if ($col_id === self::COL_INDEX_NOT_FOUND) {
                    $invalid_cols[] = $col;
                } else {
                    $col_ids[] = $col_id;
                }
            }

            // Validate column list
            if (count($invalid_cols) > 0) {
                $this->throw_error(
                    self::ERR_INVALID_COLUMN_NAME,
                    "on table " . $table . ": " . implode(",", $invalid_cols),
                );
            }
        }

        if (count($column_subset) != count($in_data)) {
            $this->throw_error(
                self::ERR_COLUMN_LIST_MISMATCH,
                sprintf("got %s but expected %s", count($in_data), count($column_subset))
            );
        }

        // Get the key column id
        $key_id = $this->get_col_id($cols, $this->tables[$tabid]['key']);
        $autokey = false;

        // User supplied key column?
        $input_supplied_key_column = array_search(
            $this->tables[$tabid]['key'],
            $column_subset
        );
        if ($input_supplied_key_column === false) {
            // If user did not supply the key column as one of the
            // columns, automatically make the next number (id).
            $next_id = 1;
            $autokey = true;
        } else {
            $next_id = $in_data[$input_supplied_key_column];
        }

        if (!$make_new_table) {
            // Table already exists; not new

            // Get an array of all the key values
            $key_values = [];
            foreach ($data as $row) {
                $key_values[] = $row[$key_id];
            }

            if ($autokey) {
                if (max($key_values) < PHP_INT_MAX) {
                    $next_id = max($key_values) + 1;
                } else {
                    $this->throw_error(self::ERR_AUTOKEY_FAIL);
                }
            } else {
                // Make sure that the key supplied is not duplicate.
                $subset_key_id = $this->get_col_id(
                    $column_subset,
                    $this->tables[$tabid]['key']
                );

                $in_key_value = (string) $in_data[$subset_key_id];
                if (array_search($in_key_value, $key_values) !== false) {
                    // Invalid key; not unique.
                    $this->throw_error(
                        self::ERR_KEY_NOT_UNIQUE,
                        $in_data[$subset_key_id]
                    );
                }
            }
        }

        // Generate new blank record with all cols
        $new_record = array_fill(0, count($cols), '');

        // Add next keyid if it is to be automatic
        if ($autokey) {
            $new_record[$key_id] = $next_id;
        }

        // Add in input record data
        foreach ($in_data as $i => $col) {
            $new_record[$col_ids[$i]] = $col;
        }

        // Append the data to the table array
        $data[] = $new_record;

        $this->dtf_write_all($this->tables[$tabid]['name'], $data);
        return $next_id;
    }

    /**
     * Update some rows in a table with data
     *
     * @param  string $table          The name of the table
     * @param  string $columnlist     Comma separated list of column names
     * @param  array  $in_update_data Array of the data to be updated (matches
     *                                $columnlist)
     * @param  string $criteria       Simple clause to limit the records that
     *                                will be updated (COL=VALUE)
     * @return int Number of affected rows
     */
    public function update($table, $columnlist, $in_update_data, $criteria = '')
    {
        // Verify table exists in db
        $tabid = $this->get_valid_tabid($table);

        // Get the whole table
        $cols = $this->get_cols_all($tabid);
        $data = $this->get_table_data($table);

        // By default, will affect all records
        $subset_ids = array_keys($data);

        //----------------------------------
        // Parse criteria string (COL=VALUE)
        if ($criteria != '') {
            $criteria = $this->parse_criteria($criteria, $tabid);

            $criteria->col_id = $this->get_col_id($cols, $criteria->col_name);
            if ($criteria->col_id !== false) {
                $subset_ids = $this->find_matching_rows($criteria, $data, true);
            }
        } else {
            // Target all records
            $subset_ids = array_keys($data);
        }

        //----------------------------------
        // Parse column names (col1, col2, col3...)
        $column_subset = $this->parse_columnlist($columnlist);
        if ($column_subset != []) {
            // col_ids is an array of the columnids that should be used
            $col_ids = [];
            $invalid_cols = [];
            foreach ($column_subset as $col) {
                $col_id = $this->get_col_id($cols, $col);
                if ($col_id === self::COL_INDEX_NOT_FOUND) {
                    $invalid_cols[] = $col;
                } else {
                    $col_ids[] = $col_id;
                }
            }

            // Validate column list
            if (count($invalid_cols) > 0) {
                $this->throw_error(
                    self::ERR_INVALID_COLUMN_NAME,
                    "on table " . $table . ": " . implode(",", $invalid_cols),
                );
            }
        } else {
            // empty list means all cols
            $column_subset = $cols;
            $col_ids = array_keys($column_subset);
        }

        if (count($column_subset) != count($in_update_data)) {
            $this->throw_error(
                self::ERR_COLUMN_LIST_MISMATCH,
                sprintf("got %s but expected %s", count($in_update_data), count($column_subset))
            );
        }

        // Loop through each record subset and apply updates
        foreach ($subset_ids as $r) {
            foreach ($col_ids as $c => $col_id) {
                $data[$r][$col_id] = $in_update_data[$c];
            }
        }

        // Write the new table to the file.
        $this->dtf_write_all($this->tables[$tabid]['name'], $data);

        // Return number of rows affected
        return count($subset_ids);
    }

    /**
     * Delete record(s) from a table
     *
     * @param  string $table    The name of the table
     * @param  string $criteria Simple string to indicate a condition
     *                          for which to remove records (COL=VALUE)
     * @return int Number of affected rows
     */
    public function delete($table, $criteria = '')
    {
        // Verify table exists in db.
        $tabid = $this->get_valid_tabid($table);

        // Get the whole table
        $cols = $this->get_cols_all($tabid);
        $data = $this->get_table_data($table);

        //----------------------------------
        // Parse criteria string (COL=VALUE)
        if ($criteria != '') {
            $criteria = $this->parse_criteria($criteria, $tabid);

            $criteria->col_id = $this->get_col_id($cols, $criteria->col_name);
            $to_delete_ids = [];
            if ($criteria->col_id !== false) {
                $to_delete_ids = $this->find_matching_rows($criteria, $data, true);
            }
        } else {
            // Target all records!
            $to_delete_ids = array_keys($data);
        }

        if (count($to_delete_ids) > 0) {
            // Delete targeted rows
            foreach ($to_delete_ids as $id) {
                unset($data[$id]);
            }

            $new_data = array_values($data);

            // Write the new table to the file.
            $this->dtf_write_all($this->tables[$tabid]['name'], $new_data);
        }

        return count($to_delete_ids);
    }

    /**
     * Parse a columnlist input and return array
     *
     * @param  mixed $columnlist
     * @return array
     */
    private function parse_columnlist($columnlist)
    {
        if (!is_array($columnlist)) {
            $columnlist = trim($columnlist);

            if ($columnlist == "*" || $columnlist == "") {
                return [];
            }

            $columnlist = explode(",", $columnlist);
        }

        // Remove ` character to be compatible with mysql format
        $columnlist = array_map(
            function ($i) {
                return str_replace('`', '', $i);
            },
            $columnlist
        );

        $columnlist = array_map('trim', $columnlist);
        return $columnlist;
    }

    /**
     * Parse a criteria input
     *
     * @param  string $criteria
     * @return array
     */
    private function parse_criteria($criteria, $tabid)
    {
        // Generate a criteria object
        $obj = (object) ["is_regex" => false];

        if (strpos($criteria, '=') !== false) {
            $criteria_def = explode("=", $criteria);
            $obj->col_name = trim($criteria_def[0]);
            $obj->value = trim($criteria_def[1]);
        } else {
            // Assume key column is column to search
            $obj->col_name = $this->tables[$tabid]['key'];
            $obj->value = trim($criteria);
        }

        // Special handling for 'COL=true'
        if ($obj->value == 'true') {
            $obj->value = 1;
        }

        // Special handling for 'COL=false'
        if ($obj->value == 'false') {
            $obj->value = '';
        }

        // Detect if regex
        if (
            substr($obj->value, 0, 1) == "/"
            && substr($obj->value, -1, 1) == "/"
        ) {
            $obj->is_regex = true;
        }

        return $obj;
    }

    /**
     * Find a subset of matching rows from a criteria obj
     *
     * @param  object $criteria
     * @param  array  $records
     * @param  bool   $as_row_ids Whether to return a list of row ids instead
     * @return array
     */
    private function find_matching_rows($criteria, $records, $as_row_ids = false)
    {
        $rows = [];

        if ($criteria->col_id === self::COL_INDEX_NOT_FOUND) {
            // Column doesn't exist. Ignore it.
            // So, that means there are no results.
            return $rows;
        }

        foreach ($records as $row_id => $record) {
            if ($criteria->is_regex) {
                // Use regular expression
                if (preg_match($criteria->value . "i", $record[$criteria->col_id])) {
                    // Found a match, return this row.
                    $rows[] = $as_row_ids ? $row_id : $record;
                }
            } else {
                // Plain search
                if ($record[$criteria->col_id] == $criteria->value) {
                    // Found a match, return this row.
                    $rows[] = $as_row_ids ? $row_id : $record;
                }
            }
        }

        return $rows;
    }

    /**
     * Return the tabid for a given table. If not found returns -1
     *
     * @param  string $table_name The table name to find
     * @return mixed
     */
    private function get_tabid($table_name)
    {
        foreach ($this->tables as $t => $table) {
            if ($table['name'] == $table_name) {
                return $t;
            }
        }

        return self::TABLE_INDEX_NOT_FOUND;
    }

    /**
     * Return an array of all the columns for a tabid
     *
     * @param  string $tabid Table id
     * @return array
     */
    private function get_cols_all($tabid)
    {
        $cols = [];

        for ($c = 0; $c < $this->tables[$tabid]['cols']; $c++) {
            $cols[] = $this->tables[$tabid][$c];
        }

        return $cols;
    }

    /**
     * Returns a col_id of a column name from a column list array. If not
     * found returns -1
     *
     * @param  array  $cols Array of column names
     * @param  string $name Name of column
     * @return int
     */
    private function get_col_id($cols, $name)
    {
        foreach ($cols as $i => $col) {
            if ($col == $name) {
                return $i;
            }
        }

        return self::COL_INDEX_NOT_FOUND;
    }

    /**
     * Get the database definitions and store in the tables array.
     *
     * @return bool Whether the data was loaded successfully.
     */
    private function get_db_defs()
    {
        if ($this->options['use_cache'] && $this->get_dbd_cache()) {
            return true;
        }

        return $this->dbd_parse_data();
    }

    /**
     * Get the datbase definitions from the cache file.
     *
     * @return bool Whether the file was found and loaded.
     */
    private function get_dbd_cache()
    {
        $dbd_cache_file = $this->get_dbd_cache_filename();

        if (file_exists($dbd_cache_file)) {
            $dbd_data = file_get_contents($dbd_cache_file);

            $this->tables = unserialize($dbd_data);

            return true;
        }

        return false;
    }

    /**
     * Write the dbd cache file.
     *
     * @return void
     */
    private function write_dbd_cache()
    {
        $dbd_cache_file = $this->get_dbd_cache_filename();
        file_put_contents($dbd_cache_file, serialize($this->tables));
    }

    /**
     * Get the filename for the dbd cache file.
     *
     * @return string
     */
    private function get_dbd_cache_filename()
    {
        return $this->datapath . DIRECTORY_SEPARATOR
            . "." . $this->database_name . $this->db_fileextension . ".cache";
    }

    /**
     * Parse the dbd file, put defs in array tables.
     *
     * @return boolean
     */
    private function dbd_parse_data()
    {
        $dbd_filename = $this->datapath . DIRECTORY_SEPARATOR
            . $this->database_name . $this->db_fileextension;
        if (!file_exists($dbd_filename)) {
            $this->throw_error(self::ERR_DBD_FILE_MISSING, $dbd_filename);
        }

        $dbd_defs = file($dbd_filename);
        if (!$dbd_defs) {
            $this->throw_error(self::ERR_DBD_FILE_EMPTY, $dbd_filename);
        }

        $tabid = 0;
        $cols = 0;

        $this->tables[$tabid] = [];
        $this->tables[$tabid]['name'] = '';
        $this->tables[$tabid]['cols'] = 0;
        $this->tables[$tabid]['key'] = '';

        foreach ($dbd_defs as $line) {
            if (substr($line, 0, 2) == "**") {
                $tabid++;
                $cols = 0;

                $this->tables[$tabid] = [];
                $this->tables[$tabid]['name'] = '';
                $this->tables[$tabid]['cols'] = 0;
                $this->tables[$tabid]['key']  = '';
            }

            $key = trim(substr($line, 0, 4));
            $value = trim(substr($line, 3));
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

        $this->write_dbd_cache();

        return true;
    }

    /**
     * Get the data for a table
     *
     * @param  string $table_name The name of the table
     * @return mixed
     */
    private function get_table_data($table_name)
    {
        // Don't cache dtf files - it isn't performant
        return $this->dtf_parse_data($table_name);
    }

    /**
     * Get the filename for a table
     *
     * @param  string $table_name Name of table
     * @return string The filename
     */
    private function get_table_filename($table_name)
    {
        $prefix = "";
        if ($this->options['prepend_databasename_to_table_filename']) {
            $prefix = $this->database_name . ".";
        }

        return sprintf(
            "%s/%s%s%s",
            $this->datapath,
            $prefix,
            $table_name,
            $this->table_fileextension
        );
    }

    /**
     * Parse a dtf file and return 2d array.
     *
     * @param  string $table The name of the table to parse
     * @return mixed Array data or false
     */
    private function dtf_parse_data($table)
    {
        $filename = $this->get_table_filename($table);

        $contents = $this->read_file($filename);
        if (!$contents) {
            return [];
        }

        $rows = explode($this->row_delimiter, $contents);

        // Take off last row, it is blank
        array_pop($rows);

        foreach ($rows as &$row) {
            // Trim white space off at beginning
            $row = ltrim($row);
            $row = explode($this->col_delimiter, $row);
        }

        return $rows;
    }

    /**
     * Writes records into entire file
     *
     * @param  string $tablename The name of the table
     * @param  array  $data      The array of data to write
     * @return void
     */
    private function dtf_write_all($tablename, $data)
    {
        $filename = $this->get_table_filename($tablename);

        // Open the file to write to.
        $writefile = fopen($filename, 'w');

        foreach ($data as $row) {
            $rowline = implode($this->col_delimiter, $row);

            fputs($writefile, stripslashes($rowline));
            fputs($writefile, $this->row_delimiter . "\n");
        }

        fflush($writefile);
        fclose($writefile);
        clearstatcache();
    }

    /**
     * Simply reads a file and returns the contents.
     *
     * @parammixed $filename The name of the file to read
     * @return     false|string
     */
    private function read_file($filename)
    {
        if (!file_exists($filename)) {
            return false;
        }

        if (filesize($filename) > 0) {
            return file_get_contents($filename);
        }

        return false;
    }

    /**
     * Custom method to indicate an error
     *
     * @param  int    $err  The error code
     * @param  string $text Additional message to accompany error
     * @return void
     */
    private function throw_error($err, $text = '')
    {
        $exception_class = null;

        switch ($err) {
            case self::ERR_DBD_FILE_MISSING:
                $message = "DBD file missing or not readable: $text.";
                $exception_class = Exception\DatabaseNotFoundException::CLASS;
                break;
            case self::ERR_MISSING_TABLE_PARAM:
                $message = "No table parameter: $text.";
                $exception_class = Exception\TableNotFoundException::CLASS;
                break;
            case self::ERR_TABLE_NOT_FOUND:
                $message = "Table not found: $text.";
                $exception_class = Exception\TableNotFoundException::CLASS;
                break;
            case self::ERR_KEY_NOT_UNIQUE:
                $message = "Invalid key (not unique): $text.";
                $exception_class = Exception\DuplicateKeyException::CLASS;
                break;
            case self::ERR_DBD_FILE_EMPTY:
                $message = "DBD file empty: $text.";
                $exception_class = Exception\DatabaseNotFoundException::CLASS;
                break;
            case self::ERR_INVALID_COLUMN_NAME:
                $message = "Column name(s) do not exist $text.";
                $exception_class = Exception\ColumnNotFoundException::CLASS;
                break;
            case self::ERR_COLUMN_LIST_MISMATCH:
                $message = "Input column list not same length of input data; $text.";
                $exception_class = Exception\ColumnListMismatchException::CLASS;
                break;
            case self::ERR_AUTOKEY_FAIL:
                $message = "Cannot auto assign next key id, out of bounds; $text.";
                break;
            default:
                $message = "An error occurred.";
                break;
        }

        if ($exception_class) {
            throw new $exception_class($message);
        }

        throw new \Exception($message);
    }
}
