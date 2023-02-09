<?php

use PHPUnit\Framework\TestCase;

use Hazelplum\Hazelplum;

final class HazelplumTest extends TestCase
{
    public function tearDown(): void
    {
        @unlink('.foobard.dbd.cache');
        @unlink('foobard.dbd');
        @unlink('.foobar.dbd.cache');
        @unlink('foobar.dbd');
        @unlink('.foobar.elementary.dtf.cache');
        @unlink('foobar.elementary.dtf');
    }

    /**
     * testConstructorNoArgs
     *
     * @return void
     */
    public function testConstructorNoArgs()
    {
        $this->expectException("ArgumentCountError");
        $db = new Hazelplum();
    }

    public function testConstructMissingFile()
    {
        $this->expectException("Hazelplum\Exception\DatabaseNotFoundException");
        $db = new Hazelplum("data", "foobarx");
    }

    public function testConstructMissingFileInCurrentDir()
    {
        $this->expectException("Hazelplum\Exception\DatabaseNotFoundException");
        $db = new Hazelplum(".", "foobarx");
    }

    public function testConstructEmptyFile()
    {
        $this->expectException("Hazelplum\Exception\DatabaseNotFoundException");
        file_put_contents('foobard.dbd', '');
        $db = new Hazelplum(".", "foobard");
    }

    public function testConstructContentFile()
    {
        file_put_contents('foobare.dbd', 'hello');
        $db = new Hazelplum(".", "foobare");
        $this->assertInstanceOf('Hazelplum\\Hazelplum', $db);
        unlink('.foobare.dbd.cache');
        unlink('foobare.dbd');
    }

    public function testDbdParse()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        $db = new Hazelplum(".", "foobar");
        $tables = $db->getTables();

        $expected = ["elementary"];
        $this->assertEquals($expected, $tables);

        $expected = ["id", "name", "date"];
        $schema = $db->getTableSchema("elementary");
        $this->assertEquals($expected, $schema);

        $expected = "id";
        $pk = $db->getPrimaryKey("elementary");
        $this->assertEquals($expected, $pk);
    }

    public function testDbdParseMultitable()
    {
        file_put_contents("foobar.dbd", "TAB states\nKEY id\nCOL name\nCOL abbr\n**\nTAB colors\nKEY id\nCOL name\nCOL hex\n");
        $db = new Hazelplum(".", "foobar");
        $tables = $db->getTables();

        $expected = ["states", "colors"];
        $this->assertEquals($expected, $tables);

        $expected = ["id", "name", "abbr"];
        $schema = $db->getTableSchema("states");
        $this->assertEquals($expected, $schema);

        $expected = "id";
        $pk = $db->getPrimaryKey("states");
        $this->assertEquals($expected, $pk);

        $expected = ["id", "name", "hex"];
        $schema = $db->getTableSchema("colors");
        $this->assertEquals($expected, $schema);

        $expected = "id";
        $pk = $db->getPrimaryKey("colors");
        $this->assertEquals($expected, $pk);
    }

    public function testSelectNoData()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        $db = new Hazelplum(".", "foobar");

        $rows = $db->select("elementary");

        $this->assertEquals([], $rows);
    }

    public function testSelectData()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12sherlock1925-09-09\n");

        $db = new Hazelplum(".", "foobar");

        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "sherlock", "date" => "1925-09-09"],
        ];
        $this->assertEquals($expected, $rows);

        // With an empty value in column list should still operate as '*'
        $rows = $db->select("elementary", " ");
        $this->assertEquals($expected, $rows);
    }

    public function testSelectDataWithColumnlist()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12sherlock1925-09-09\n47watson1931-10-31\n");

        $db = new Hazelplum(".", "foobar");

        $rows = $db->select("elementary", "id,name");
        $expected = [
            ["id" => 12, "name" => "sherlock"],
            ["id" => 47, "name" => "watson"],
        ];
        $this->assertEquals($expected, $rows);

        // Should work the same with a space after the comma
        $rows = $db->select("elementary", "id, name");
        $this->assertEquals($expected, $rows);

        // Should work with backticks
        $rows = $db->select("elementary", "`id`, `name`");
        $this->assertEquals($expected, $rows);

        // Should work the same with an array of column names
        $rows = $db->select("elementary", ["id", "name"]);
        $this->assertEquals($expected, $rows);

        // Should work the same with an extra non-existent column name
        $rows = $db->select("elementary", ["id", "name", "pizza"]);
        $this->assertEquals($expected, $rows);

        // Should work the same with backticks
        $rows = $db->select("elementary", ["`id`", "`name`", "`pizza`"]);
        $this->assertEquals($expected, $rows);
    }

    public function testSelectWithCriteria()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12sherlock1925-09-09\n47watson1931-10-31\n");

        $db = new Hazelplum(".", "foobar");

        $rows = $db->select("elementary", "*", "id=12");
        $expected = [
            ["id" => 12, "name" => "sherlock", "date" => "1925-09-09"],
        ];
        $this->assertEquals($expected, $rows);

        // Should work the same with a space too
        $rows = $db->select("elementary", "*", "id = 12");
        $this->assertEquals($expected, $rows);

        // No results - no match
        $rows = $db->select("elementary", "*", "id = 144");
        $this->assertEquals([], $rows);
    }

    public function testSelectWithCriteriaRegex()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12sherlock1925-09-09\n47watson1931-10-31\n");

        $db = new Hazelplum(".", "foobar");

        $rows = $db->select("elementary", "*", "name=/s/");
        $expected = [
            ["id" => 12, "name" => "sherlock", "date" => "1925-09-09"],
            ["id" => 47, "name" => "watson", "date" => "1931-10-31"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testSelectWithCriteriaAndColumns()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12sherlock1925-09-09\n47watson1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");

        $rows = $db->select("elementary", "name", "name=/s/");
        $expected = [
            ["name" => "sherlock"],
            ["name" => "watson"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testSelectWithSortOrder()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");

        $rows = $db->select("elementary", "*", "", "name");
        $expected = [
            ["id" => 47, "name" => "alice", "date" => "1931-10-31"],
            ["id" => 23, "name" => "charlie", "date" => "2020-01-16"],
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
        ];
        $this->assertEquals($expected, $rows);

        // It should work with another modifier as "asc"
        $rows = $db->select("elementary", "*", "", "name asc");
        $this->assertEquals($expected, $rows);

        // It should work with desc modifier
        $rows = $db->select("elementary", "*", "", "name desc");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
            ["id" => 23, "name" => "charlie", "date" => "2020-01-16"],
            ["id" => 47, "name" => "alice", "date" => "1931-10-31"],
        ];
        $this->assertEquals($expected, $rows);

        // It should also work if all caps
        $rows = $db->select("elementary", "*", "", "name DESC");
        $this->assertEquals($expected, $rows);

        // It should also work with extra spaces
        $rows = $db->select("elementary", "*", "", "name   DESC");
        $this->assertEquals($expected, $rows);
    }

    public function testSelectWithSortOrderSameValues()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47james1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");

        $rows = $db->select("elementary", "*", "", "name");
        $expected = [
            ["id" => 23, "name" => "charlie", "date" => "2020-01-16"],
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
            ["id" => 47, "name" => "james", "date" => "1931-10-31"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testInsert()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");

        $result = $db->insert("elementary", "name, date", ["frank", "1986-08-28"]);
        $this->assertEquals(48, $result);
    }

    public function testInsertWithSuppliedKey()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");

        $result = $db->insert("elementary", "id, name, date", ["21", "frank", "1986-08-28"]);
        $this->assertEquals(21, $result);
    }

    public function testInsertWithDuplicateKey()
    {
        $this->expectException("Hazelplum\Exception\DuplicateKeyException");
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");

        $result = $db->insert("elementary", "id, name, date", ['12', "frank", "1986-08-28"]);
    }

    public function testInsertWithDuplicateKeyTypecast()
    {
        $this->expectException("Hazelplum\Exception\DuplicateKeyException");
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");

        $result = $db->insert("elementary", "id, name, date", [12, "frank", "1986-08-28"]);
    }

    public function testInsertWhenTableEmpty()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");

        $db = new Hazelplum(".", "foobar");

        $result = $db->insert("elementary", "name, date", ["frank", "1986-08-28"]);
        $this->assertEquals(1, $result);
    }

    public function testInsertWhenTableEmptyButPresent()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "");

        $db = new Hazelplum(".", "foobar");

        $result = $db->insert("elementary", "name, date", ["frank", "1986-08-28"]);
        $this->assertEquals(1, $result);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 1, "name" => "frank", "date" => "1986-08-28"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testInsertWhenNewTable()
    {
        $this->expectException("Hazelplum\Exception\TableNotFoundException");
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");

        $db = new Hazelplum(".", "foobar");

        $result = $db->insert("foods", "name, flavor", ["pizza", "A26"]);
    }

    public function testInsertWithAsterisk()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");

        $db = new Hazelplum(".", "foobar");

        $result = $db->insert("elementary", "*", ["88", "pizza", "A26"]);
        $this->assertEquals(1, $result);

        // Confirm side effects
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 88, "name" => "pizza", "date" => "A26"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testUpdate()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");

        $result = $db->update("elementary", "name, date", ["phoenix", "now"]);
        $this->assertTrue($result);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "phoenix", "date" => "now"],
            ["id" => 47, "name" => "phoenix", "date" => "now"],
            ["id" => 23, "name" => "phoenix", "date" => "now"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testUpdateWithCriteria()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");

        $result = $db->update("elementary", "name, date", ["phoenix", "now"], "id=12");
        $this->assertTrue($result);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "phoenix", "date" => "now"],
            ["id" => 47, "name" => "alice", "date" => "1931-10-31"],
            ["id" => 23, "name" => "charlie", "date" => "2020-01-16"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testUpdateWithAllColumnsAndCriteria()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");

        $result = $db->update("elementary", "*", [2800, "phoenix", "now"], "id=12");
        $this->assertTrue($result);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 2800, "name" => "phoenix", "date" => "now"],
            ["id" => 47, "name" => "alice", "date" => "1931-10-31"],
            ["id" => 23, "name" => "charlie", "date" => "2020-01-16"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testDelete()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");
        $result = $db->delete("elementary");
        $this->assertEquals(3, $result);

        // Confirm side effect
        $rows = $db->select("elementary");
        $this->assertEmpty($rows);
    }

    public function testDeleteWithCriteria()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");
        $result = $db->delete("elementary", "id=47");
        $this->assertEquals(1, $result);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
            ["id" => 23, "name" => "charlie", "date" => "2020-01-16"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testDeleteWithCriteriaNoMatch()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");
        $result = $db->delete("elementary", "id=28");
        $this->assertEquals(0, $result);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
            ["id" => 47, "name" => "alice", "date" => "1931-10-31"],
            ["id" => 23, "name" => "charlie", "date" => "2020-01-16"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testDeleteWithCriteriaInvalidColumnName()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");
        $result = $db->delete("elementary", "update_date=1");
        $this->assertEquals(0, $result);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
            ["id" => 47, "name" => "alice", "date" => "1931-10-31"],
            ["id" => 23, "name" => "charlie", "date" => "2020-01-16"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testDeleteWithCriteriaRegex()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n47alice1931-10-31\n23charlie2020-01-16\n");

        $db = new Hazelplum(".", "foobar");
        $result = $db->delete("elementary", "date=/19/");
        $this->assertEquals(2, $result);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 23, "name" => "charlie", "date" => "2020-01-16"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testParseOptionsPrependDbToFiles()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n");

        $options = [
            "prepend_databasename_to_table_filename" => true,
        ];
        $db = new Hazelplum(".", "foobar", $options);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testParseOptionsNoPrependDbToFiles()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("elementary.dtf", "12james1925-09-09\n");

        $options = [
            "prepend_databasename_to_table_filename" => false,
        ];
        $db = new Hazelplum(".", "foobar", $options);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testParseOptionsUseCache()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n");

        $options = [
            "use_cache" => true,
        ];
        $db = new Hazelplum(".", "foobar", $options);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
        ];
        $this->assertEquals($expected, $rows);

        // Make another one
        $db2 = new Hazelplum(".", "foobar", $options);
        $rows = $db2->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testParseOptionsUseCacheFalse()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n");

        $options = [
            "use_cache" => false,
        ];
        $db = new Hazelplum(".", "foobar", $options);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
        ];
        $this->assertEquals($expected, $rows);

        // Make another one
        $db2 = new Hazelplum(".", "foobar", $options);
        $rows = $db2->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testParseOptionsNoCache()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james1925-09-09\n");

        $options = [
            "no_cache" => 1,
        ];
        $db = new Hazelplum(".", "foobar", $options);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
        ];
        $this->assertEquals($expected, $rows);

        // Make another one
        $db2 = new Hazelplum(".", "foobar", $options);
        $rows = $db2->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testParseOptionsCompatDelimiterMode()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12ÈjamesÈ1925-09-09É\n");

        $options = [
            "compat_delimiter_mode" => 1,
        ];
        $db = new Hazelplum(".", "foobar", $options);

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "1925-09-09"],
        ];
        $this->assertEquals($expected, $rows);
    }

    public function testMultibyteData()
    {
        file_put_contents("foobar.dbd", "TAB elementary\nKEY id\nCOL name\nCOL date\n");
        file_put_contents("foobar.elementary.dtf", "12james日、に、本、ほん、語、ご\n");

        $db = new Hazelplum(".", "foobar");

        // Confirm side effect
        $rows = $db->select("elementary");
        $expected = [
            ["id" => 12, "name" => "james", "date" => "日、に、本、ほん、語、ご"],
        ];
        $this->assertEquals($expected, $rows);
    }
}
