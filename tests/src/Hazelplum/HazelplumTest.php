<?php

use PHPUnit\Framework\TestCase;

use Hazelplum\Hazelplum;

final class HazelplumTest extends TestCase
{
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
        $db = new Hazelplum("data", "foobar");
        $this->assertEquals(is_a(Hazelplum), $db);
    }
}
