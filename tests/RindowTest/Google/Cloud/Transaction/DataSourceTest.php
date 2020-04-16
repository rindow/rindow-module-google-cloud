<?php
namespace RindowTest\Google\Cloud\Transaction\DataSourceTest;

use PHPUnit\Framework\TestCase;
use Google\Cloud\Core\ServiceBuilder;
use Rindow\Transaction\Local\TransactionManager;
use Rindow\Module\Google\Cloud\System\Environment;
use Rindow\Module\Google\Cloud\Cache\SimpleCache\Datastore;
use Rindow\Module\Google\Cloud\System\ServiceFactory;
use Rindow\Module\Google\Cloud\Transaction\DataSource;

class Test extends TestCase
{
    const WAIT = 1;
    const TEST_KIND = 'test';

    public static $skip = false;
    public static $runGroup = null;#'b';
    public static function setUpBeforeClass()
    {
    }
    public static function tearDownAfterClass()
    {
    }

    public function deleteAll($kind)
    {
        $datastore = $this->getClient();
        $query = $datastore->query()->kind($kind);
        $results = $datastore->runQuery($query);
        $entities = array();
        foreach ($results as $entity) {
            $entities[] = $entity;
        }
        foreach ($entities as $entity) {
            $datastore->delete($entity->key());
        }
    }

    public function findAll($kind)
    {
        $datastore = $this->getClient();
        $query = $datastore->query()->kind($kind);
        //$query->order('name');
        $results = $datastore->runQuery($query);
        $entities = array();
        foreach ($results as $entity) {
            $entities[] = $entity;
        }
        return $entities;
    }

    public function setUp()
    {
        if(self::$skip) {
            $this->markTestSkipped();
            return;
        }
        try {
            $this->deleteAll(self::TEST_KIND);
        } catch(\Exception $e) {
            self::$skip = true;
            $this->markTestSkipped();
            return;
        }

        // Must wait to reflect
        sleep(self::WAIT);
    }
    public function getClient()
    {
        $builder = new ServiceBuilder();
        return $builder->datastore();
    }

    public function getTransactionManager()
    {
        $tx = new TransactionManager();
        return $tx;
    }
    public function getDataSource($tx)
    {
        $serviceFactory = new ServiceFactory();
        $dataSource = new DataSource();
        $dataSource->setTransactionManager($tx);
        $dataSource->setServiceFactory($serviceFactory);
        return $dataSource;
    }

    public function testCommitAndAbort()
    {
        $tx = $this->getTransactionManager();
        $ds = $this->getDataSource($tx);
        $datastore = $ds->getDatastore();

        $connection = $ds->getConnection();
        $this->assertInstanceOf('Google\\Cloud\\Datastore\\DatastoreClient',$connection);

        // commit
        $tx->begin();
        $connection = $ds->getConnection();
        $this->assertInstanceOf('Google\\Cloud\\Datastore\\Transaction',$connection);
        $id1 = spl_object_hash($connection);
        $this->assertEquals($id1,spl_object_hash($ds->getConnection()));

        $entity = $datastore->entity(self::TEST_KIND,array('name'=>'test'));
        $connection->upsert($entity);
        $key = $entity->key();
        $tx->commit();
        $connection = $ds->getConnection();
        $this->assertInstanceOf('Google\\Cloud\\Datastore\\DatastoreClient',$connection);
        $this->assertEquals(array('name'=>'test'),$connection->lookup($key)->get());

        // abort
        $tx->begin();
        $connection = $ds->getConnection();
        $this->assertInstanceOf('Google\\Cloud\\Datastore\\Transaction',$connection);
        $entity = $datastore->entity(self::TEST_KIND,array('name'=>'test'));
        $connection->upsert($entity);
        $key = $entity->key();
        $tx->rollback();
        $connection = $ds->getConnection();
        $this->assertInstanceOf('Google\\Cloud\\Datastore\\DatastoreClient',$connection);
        $this->assertNull($connection->lookup($key));
    }
}