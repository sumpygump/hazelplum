# Hazelplum DB

This is a database adapter for a simple flat-file database that stores just strings. It is useful for small projects to store things in database-like tables where no proper database is available.

## Installation

Use composer to install

```
composer require sumpygump/hazelplum
```

## Usage

Hazelplum stores its data in a file on the filesystem. The database defintion file (.dbd file) provides detail on what tables and columns are available (the schema), and the actual data lives in separate files for each table (.dtf files).

### Creating a database definition file

To create a database, make a .dbd file in the desired data directory. The name of the dbd file is the name of the database. It should have the following format: (everywhere with double underscores should be thought of as a placeholder)

Each table and column definition should be on its own line in the file.

Each line starts with a three-letter code indicating what is being defined.

* `TAB`: Provide a new table name, the columns will be defined below.
* `KEY`: This is the primary key column of the table (the unique identifier for each row).
* `COL`: This is another column. You can have as many columns as you like.
* `**`: Two asterisks indicates end of table definition. Can start another table definition below.

Here is an example dbd file (work.dbd) defining two tables, `users` and `work_assignments`

```
TAB users
KEY id
COL username
COL first_name
COL last_name
COL date_created
COL is_active
**
TAB work_assignments
KEY id
COL user_id
COL assignment_name
COL date_created
COL status
```

### Using the library

Once we have a dbd file defined and in place, you can start using the Hazelplum library to create and access data.

First spin up a database connection object (Hazelplum class object), defining where the db files live and the name of the database to use.

```
use Hazelplum\Hazelplum;

$data_location = "data"; // Directory where the db files live
$db_name = "work"; // Name of database definition file (without dbd extension)
$db = new Hazelplum($data_location, $db_name);
```

Note there is no concept of a column type, everything is just stored as "text."

### Inserting records

You can insert some data into your table using the `insert` method

The `insert` method has three parameters:

 * table name: string representation of the name of the table to insert into
 * columns: comma separated list of columns to populate
 * values: array of values for each of the columns provided

The wonky nature of specifying the list of columns is for legacy reasons.

```
// Insert a new record into the users table
$data = ["gary123", "Gary", "Harris", time(), true];
$db->insert("users", "username,first_name,last_name,date_created,is_active", $data);
```

### Selecting records

You can select data from your database that has been previously inserted.

The `select` method has the following parameters:

 * table name: the table to select from 
 * column list: comma separated list of columns to select, can use "\*" to select all
 * criteria: simple statement to limit records, format "COL=VALUE"
 * sort order: column to sort by, can include "ASC" or "DESC" after

```
// Select some data
$data = $db->select("users", "*", "username=gary123", "date_created desc");

// Returns an array of associative arrays for each record
// [
//   [
//     'id' => 1,
//     'username' => 'gary123',
//     'first_name' => 'Gary',
//     'last_name' => 'Harris',
//     'date_created' => '1675963718',
//     'is_active' => '1',
//   ]
// ];
```

### Updating records

You can update records.

The `update` method has the following parameters:

 * table name: the table to update
 * column list: comma separated list of columns to update, can use "\*" to update all
 * values: array of values to update corresponding to the column list
 * criteria: simple statement to limit records, format "COL=VALUE"

It returns the number of affected rows.

```
// Update some data
$result = $db->update("users", "is_active", false, "id=1");

// Returns number of affected rows
// 1
```

### Deleting records

You can delete records.

The `delete` method has the following parameters:

 * table name: the table from which to delete records
 * criteria: simple statement to limit records affected, format "COL=VALUE"

It returns the number of affected rows.

```
// Delete a record
$result = $db->delete("users", "username=gary123");

// Returns number of affected rows
// 1
```

## Why did you make this?

I made this back in 2006 when I needed to store simple configuration data for a website. There are so many other options available, like json files, csv, ini, toml, yaml, sqlite and any other number of ways to store data in a flat file. My goal was to make it 'kinda like a SQL database' but really light. I thought it would be neat to explore a format that uses the ASCII record separator (`1E`) and unit separator (`1F`) characters as the row delimiter and column delimiter. This means you can store spaces, new lines, commas, semicolons, pipes etc in your data and it will just work.
