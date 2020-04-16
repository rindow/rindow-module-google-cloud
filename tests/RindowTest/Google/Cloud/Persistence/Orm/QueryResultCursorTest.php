<?php
namespace RindowTest\Google\Cloud\Persistence\Orm\QueryResultCursorTest;

use PHPUnit\Framework\TestCase;

use Rindow\Module\Google\Cloud\Persistence\Orm\QueryResultCursor;
use Google\Cloud\Datastore\DatastoreClient;

class TestDataSource
{
    function __construct($connection)
    {
        $this->connection = $connection;
    }

    public function getConnection()
    {
        return $this->connection;
    }
}


class Test extends TestCase
{
	const TESTKIND = 'testkind';
    public static $skip = false;

	public function setUp()
	{
        if(self::$skip) {
            $this->markTestSkipped();
            return;
        }
        try {
			$client = $this->getClient();
			$query = $client->query()->kind(self::TESTKIND);
			$results = $client->runQuery($query);
			$keys = array();
			foreach ($results as $entity) {
				$keys[] = $entity->key();
			}
			$client->deleteBatch($keys);
		} catch(\Exception $e) {
            self::$skip = true;
            $this->markTestSkipped();
            return;
		}
	}

	public function getClient()
	{
		$client = new DatastoreClient();
		return $client;
	}

	public function testNormal()
	{
		$client = $this->getClient();
		$entities = array();
		$entities[] = $client->entity(self::TESTKIND,array('name'=>'test1','group'=>'a'));
		$entities[] = $client->entity(self::TESTKIND,array('name'=>'test2','group'=>'a'));
		$entities[] = $client->entity(self::TESTKIND,array('name'=>'test3','group'=>'b'));
		$client->insertBatch($entities);

		$queries = array();
		$queries[] = $client->query()->kind(self::TESTKIND)->filter('group','=','a')->order('name');
		$queries[] = $client->query()->kind(self::TESTKIND)->filter('group','=','b')->order('name');

        $dataSource = new TestDataSource($client);
		$queryResultCursor = new QueryResultCursor($dataSource,$queries,$limit=null,$offset=null);
		while($entity = $queryResultCursor->fetch()) {
			$values[] = $entity->get();
		}
		$this->assertEquals(
			array(
				array('name'=>'test1','group'=>'a'),
				array('name'=>'test2','group'=>'a'),
				array('name'=>'test3','group'=>'b'),
			),
			$values
		);
	}
}
