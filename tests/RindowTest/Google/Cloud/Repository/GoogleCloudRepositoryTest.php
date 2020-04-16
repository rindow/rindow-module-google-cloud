<?php
namespace RindowTest\Google\Cloud\Repository\GoogleCloudRepositoryTest;

use PHPUnit\Framework\TestCase;
use Interop\Lenient\Dao\Query\Expression;
use Interop\Lenient\Dao\Repository\DataMapper;
use Rindow\Container\ModuleManager;
use Rindow\Module\Google\Cloud\Repository\GoogleCloudRepository;
use Rindow\Database\Dao\Support\QueryBuilder;
use Rindow\Stdlib\Cache\SimpleCache\FileCache;
use Google\Cloud\Core\ServiceBuilder;
use Google\Cloud\Datastore\Query\Query as GoogleQuery;

class TestDataMapper implements DataMapper
{
    public function map($data)
    {
        return (object)$data;
    }

    public function demap($entity)
    {
        //var_dump($entity);
        $data = get_object_vars($entity);
        return $data;
    }

    public function fillId($entity,$id)
    {
        $entity->id = $id;
        return $entity;
    }
    public function getFetchClass()
    {
        return null;
    }
}

class TestBuildDatastoreFilter extends GoogleCloudRepository
{
    public function test(array $query=null)
    {
        return $this->buildDatastoreFilters($query);
    }
}

class TestGoogleCloudRepository extends GoogleCloudRepository
{
    public function map($data)
    {
        return (object)$data;
    }

    public function demap($entity)
    {
        //var_dump($entity);
        $data = get_object_vars($entity);
        return $data;
    }

    public function fillId($entity,$id)
    {
        $entity->id = $id;
        return $entity;
    }

    public function getFetchClass()
    {
        return null;
    }
}

class Test extends TestCase
{
    const WAIT = 1;
    const KIND_NAME = 'testrepo';
    public static $skip = false;
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
        sleep(self::WAIT);
        try {
            $this->deleteAll(self::KIND_NAME);
        } catch(\Exception $e) {
            self::$skip = true;
            $this->markTestSkipped();
            return;
        }
        $cache = new FileCache();
        $cache->clear();
        // Must wait to reflect
        sleep(self::WAIT);
    }
    public function getClient()
    {
        $builder = new ServiceBuilder();
        return $builder->datastore();
    }
    public function getConfig()
    {
        $config = array(
            'module_manager' => array(
                'version' => 1,
                'modules' => array(
                    'Rindow\Aop\Module' => true,
                    'Rindow\Transaction\Local\Module' => true,
                    'Rindow\Module\Google\Cloud\LocalTxModule' => true,
                ),
                'enableCache'=>false,
                'configCacheFactoryClass'=>'Rindow\\Module\\Google\\Cloud\\System\\ServiceFactory',
            ),
            'cache'=>array(
                'memCache'=>array(
                    'class'=>'Rindow\\Stdlib\\Cache\\SimpleCache\\ArrayCache',
                ),
                'fileCache'=>array(
                    'class'=>'Rindow\\Stdlib\\Cache\\SimpleCache\\ArrayCache',
                ),
            ),
            'container' => array(
            	'components' => array(
            		__NAMESPACE__.'\\TestRepository' => array(
	            		'parent'=>'Rindow\\Module\\Google\\Cloud\\Repository\\AbstractGoogleCloudRepository',
	            		'properties'=>array(
	            			'kindName'=>array('value'=>self::KIND_NAME),
                            //'unindexed'=>array('value'=>array(
                            //    'name'=>true,
                            //)),
                            'unique'=>array('value'=>array(
                                'ser'=>true,
                                'ser2'=>true,
                            )),
	            		),
            		),
                    __NAMESPACE__.'\\TestGoogleCloudRepository' => array(
                        'parent'=>'Rindow\\Module\\Google\\Cloud\\Repository\\AbstractGoogleCloudRepository',
                        'class' => __NAMESPACE__.'\\TestGoogleCloudRepository',
                        'properties'=>array(
                            'kindName'=>array('value'=>self::KIND_NAME),
                        ),
                    ),
            	),
            ),
        );
        return $config;
    }

    public function getRepository($component=null)
    {
        $mm = new ModuleManager($this->getConfig());
        if($component==null)
            $component = __NAMESPACE__.'\\TestRepository';
        $repository = $mm->getServiceLocator()->get($component);
        return $repository;
    }

    public function testBuildDatastoreFilter()
    {
        $qb = new QueryBuilder();
        $builder = new TestBuildDatastoreFilter();

        $dbFilters = $builder->test(array('name'=>'value'));
        $this->assertCount(1,$dbFilters);
        $this->assertInstanceof('stdClass',$dbFilters[0]);
        $this->assertEquals('name',$dbFilters[0]->propertyName);
        $this->assertEquals(GoogleQuery::OP_EQUALS,$dbFilters[0]->operator);
        $this->assertEquals('value',$dbFilters[0]->value);

        $dbFilters = $builder->test(array($qb->createExpression('name',$qb->gt(),'value')));
        $this->assertCount(1,$dbFilters);
        $this->assertInstanceof('stdClass',$dbFilters[0]);
        $this->assertEquals('name',$dbFilters[0]->propertyName);
        $this->assertEquals(GoogleQuery::OP_GREATER_THAN,$dbFilters[0]->operator);
        $this->assertEquals('value',$dbFilters[0]->value);

        //$dbFilters = $builder->test(array($qb->createExpression('name',$qb->in(),array('value1','value2'))));
        //$this->assertCount(1,$dbFilters[0]);
        //$this->assertInstanceof('stdClass',$dbFilters[0]);
        //$this->assertEquals('name',$dbFilters[0]->propertyName);
        //$this->assertEquals(GoogleQuery::OP_IN,$dbFilters[0]->operator);
        //$this->assertEquals(array('value1','value2'),$dbFilters[0]->value);

        //$dbFilters = $builder->test(array($qb->createExpression('name',$qb->in(),array('value1'))));
        //$this->assertInstanceof('stdClass',$dbFilters[0]);
        //$this->assertEquals('name',$dbFilters[0]->propertyName);
        //$this->assertEquals(GoogleQuery::OP_EQUALS,$dbFilters[0]->operator);
        //$this->assertEquals('value1',$dbFilters[0]->value);

        $dbFilters = $builder->test(array($qb->createExpression('name',$qb->gt(),$qb->createParameter('p1','value'))));
        $this->assertCount(1,$dbFilters);
        $this->assertInstanceof('stdClass',$dbFilters[0]);
        $this->assertEquals('name',$dbFilters[0]->propertyName);
        $this->assertEquals(GoogleQuery::OP_GREATER_THAN,$dbFilters[0]->operator);
        $this->assertEquals('value',$dbFilters[0]->value);

        $dbFilters = $builder->test(array('name1'=>'value1','name2'=>'value2'));
        $this->assertCount(2,$dbFilters);
        $this->assertEquals('name1',$dbFilters[0]->propertyName);
        $this->assertEquals(GoogleQuery::OP_EQUALS,$dbFilters[0]->operator);
        $this->assertEquals('value1',$dbFilters[0]->value);
        $this->assertEquals('name2',$dbFilters[1]->propertyName);
        $this->assertEquals(GoogleQuery::OP_EQUALS,$dbFilters[1]->operator);
        $this->assertEquals('value2',$dbFilters[1]->value);

        $daoFilter = array();
        $daoFilter[] = $qb->createExpression('name1',$qb->gt(),'value1');
        $daoFilter[] = $qb->createExpression('name2',$qb->gte(),'value2');
        $dbFilters = $builder->test($daoFilter);
        $this->assertCount(2,$dbFilters);
        $this->assertEquals('name1',$dbFilters[0]->propertyName);
        $this->assertEquals(GoogleQuery::OP_GREATER_THAN,$dbFilters[0]->operator);
        $this->assertEquals('value1',$dbFilters[0]->value);
        $this->assertEquals('name2',$dbFilters[1]->propertyName);
        $this->assertEquals(GoogleQuery::OP_GREATER_THAN_OR_EQUAL,$dbFilters[1]->operator);
        $this->assertEquals('value2',$dbFilters[1]->value);
    }


    public function testInsertNormal()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $this->assertNotEquals($id,$id2);

        $cursor = $this->findAll(self::KIND_NAME);
        $count = 0;
        foreach($cursor as $row) {
            $row_id = $row->key()->pathEndIdentifier();
            if($row_id==$id) {
                $this->assertEquals(array('name'=>'test','day'=>1,'ser'=>1),$row->get());
            } elseif($row_id==$id2) {
                $this->assertEquals(array('name'=>'test2','day'=>1,'ser'=>10),$row->get());
            } else {
                //var_dump(array($id,$id2,$row['_id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(2,$count);
    }

    public function testUpdateNormal()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $this->assertNotEquals($id,$id2);

        $repository->save(array('id'=>$id,'name'=>'update1'));

        $cursor = $this->findAll(self::KIND_NAME);
        $count = 0;
        foreach($cursor as $row) {
            $row_id = $row->key()->pathEndIdentifier();
            if($row_id==$id)
                $this->assertEquals(array('name'=>'update1'),$row->get());
            elseif($row_id==$id2)
                $this->assertEquals(array('name'=>'test2','day'=>1,'ser'=>10),$row->get());
            else {
                //var_dump(array($id,$id2,$row['_id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(2,$count);
    }

    public function testUpdateUpsert()
    {
        $connection = $this->getClient();
        $key = $connection->key(self::KIND_NAME);
        $key = $connection->allocateId($key);
        $id = $key->pathEndIdentifier();
        $repository = $this->getRepository();
        $row = $repository->save(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1));
        $this->assertEquals($id,$row['id']);
        $id = $row['id'];
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        sleep(self::WAIT);
        $cursor = $this->findAll(self::KIND_NAME);
        $count = 0;
        foreach($cursor as $row) {
            $row_id = $row->key()->pathEndIdentifier();
            if($row_id==$id)
                $this->assertEquals(array('name'=>'test','day'=>1,'ser'=>1),$row->get());
            else {
                //var_dump(array($id,$id2,$row['_id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);
    }

    public function testDeleteAndDeleteByIdNormal()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        sleep(self::WAIT);
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $this->assertNotEquals($id,$id2);
        sleep(self::WAIT);

        $repository->deleteById($id);

        sleep(self::WAIT);
        $cursor = $this->findAll(self::KIND_NAME);
        $count = 0;
        foreach($cursor as $row) {
            $row_id = $row->key()->pathEndIdentifier();
            if($row_id==$id2)
                $this->assertEquals(array('name'=>'test2','day'=>1,'ser'=>10),$row->get());
            else {
                //r_dump(array($id,$id2,$row['_id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $row = $repository->save(array('name'=>'test3','day'=>1,'ser'=>30));
        sleep(self::WAIT);
        $id3 = $row['id'];
        $repository->delete(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10));
        sleep(self::WAIT);
        $cursor = $this->findAll(self::KIND_NAME);
        $count = 0;
        foreach($cursor as $row) {
            $row_id = $row->key()->pathEndIdentifier();
            if($row_id==$id3)
                $this->assertEquals(array('name'=>'test3','day'=>1,'ser'=>30),$row->get());
            else {
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);
    }

    public function testDeleteAllNormal()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        sleep(self::WAIT);
        $id = $row['id'];
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        sleep(self::WAIT);
        $id2 = $row['id'];
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $this->assertNotEquals($id,$id2);
        sleep(self::WAIT);
        $repository->deleteAll(array('name'=>'test'));
        sleep(self::WAIT*2);

        $cursor = $this->findAll(self::KIND_NAME);
        $count = 0;
        foreach($cursor as $row) {
            $row_id = $row->key()->pathEndIdentifier();
            if($row_id==$id2)
                $this->assertEquals(array('name'=>'test2','day'=>1,'ser'=>10),$row->get());
            else {
                //r_dump(array($id,$id2,$row['_id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $row = $repository->save(array('name'=>'test3','day'=>1,'ser'=>30));
        $id3 = $row['id'];
        $repository->deleteAll();
        sleep(self::WAIT*2);
        $cursor = $this->findAll(self::KIND_NAME);
        $count = 0;
        foreach($cursor as $row) {
            $count++;
        }
        $this->assertEquals(0,$count);
    }

    public function testFindNormal()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $this->assertNotEquals($id,$id2);

        $cursor = $repository->findAll(array('name'=>'test2'));
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::GREATER_THAN,1);
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::GREATER_THAN_OR_EQUAL,10);
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::LESS_THAN,10);
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id)
                $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::LESS_THAN_OR_EQUAL,1);
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id)
                $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

/*
        ***********
        * Expression::NOT_EQUAL is not supported
        *

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::NOT_EQUAL,1);
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        ***********
        * Expression::IN is not supported
        *

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::IN,array(10));
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);
*/
        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('name',Expression::BEGIN_WITH,'test2');
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('name',Expression::BEGIN_WITH,'test');
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            $count++;
        }
        $this->assertEquals(2,$count);

        $row = $repository->save(array('name'=>'test3','day'=>1,'ser'=>100));
        $id3 = $row['id'];
        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::GREATER_THAN_OR_EQUAL,5);
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::LESS_THAN_OR_EQUAL,10);
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

    }

    /**
     * @expectedException        Rindow\Module\Google\Cloud\Repository\Exception\InvalidArgumentException
     * @expectedExceptionMessage Normally expression must not include array value.
     */
    public function testIllegalArrayValue1()
    {
        $repository = $this->getRepository();
        $filter['a'] = array('a1','b1');
        $results = $repository->findAll($filter);
    }

    /**
     * @expectedException        Rindow\Database\Dao\Exception\RuntimeException
     * @expectedExceptionMessage Normally expression must not include array value.
     */
    public function testIllegalArrayValue2()
    {
        $repository = $this->getRepository();
        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('a',Expression::EQUAL,array('a1'));
        $results = $repository->findAll($filter);
    }

    public function testGetNormal()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $this->assertNotEquals($id,$id2);

        $row = $repository->findById($id2);
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
    }

    public function testGetNoData()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $repository->deleteById($id);

        $row = $repository->findById($id);
        $this->assertNull($row);
    }

    public function testCount()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));

        $count = $repository->count();
        $this->assertEquals(2,$count);

        $count = $repository->count(array('name'=>'test2'));
        $this->assertEquals(1,$count);

        $count = $repository->count(array('name'=>'test3'));
        $this->assertEquals(0,$count);
    }

    public function testExistsById()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $row = $repository->save(array('name'=>'test3','day'=>1,'ser'=>11));
        $id3 = $row['id'];
        $repository->deleteById($id3);

        $this->assertTrue($repository->existsById($id));
        $this->assertTrue($repository->existsById($id2));
        $this->assertFalse($repository->existsById($id3));
    }

    public function testDataMapper()
    {
        $repository = $this->getRepository();
        $repository->setDataMapper(new TestDataMapper());

        $entity = new \stdClass();
        $entity->id = null;
        $entity->a = 'a1';
        $entity2 = $repository->save($entity);
        $this->assertEquals(array('a'=>'a1','id'=>$entity->id),get_object_vars($entity));
        $this->assertEquals($entity,$entity2);
        $id = $entity->id;

        $entity = new \stdClass();
        $entity->id = $id;
        $entity->field = 'boo';
        $entity2 = $repository->save($entity);
        $this->assertEquals(array('id'=>$id,'field'=>'boo'),get_object_vars($entity));
        $this->assertEquals($entity,$entity2);

        $results = $repository->findAll();
        $count=0;
        foreach ($results as $entity) {
            $r = new \stdClass();
            $r->id = $id;
            $r->field = 'boo';
            $this->assertEquals($r,$entity);
            $count++;
        }
        $this->assertEquals(1,$count);
    }

    public function testCustomizeForClassMapping()
    {
        $repository = $this->getRepository(__NAMESPACE__.'\\TestGoogleCloudRepository');
        $entity = new \stdClass();
        $entity->id = null;
        $entity->a = 'a1';
        $entity2 = $repository->save($entity);
        $this->assertEquals(array('a'=>'a1','id'=>$entity->id),get_object_vars($entity));
        $this->assertEquals($entity,$entity2);
        $id = $entity->id;

        $entity = new \stdClass();
        $entity->id = $id;
        $entity->field = 'boo';
        $entity2 = $repository->save($entity);
        $this->assertEquals(array('id'=>$id,'field'=>'boo'),get_object_vars($entity));
        $this->assertEquals($entity,$entity2);

        $results = $repository->findAll();
        $count=0;
        foreach ($results as $entity) {
            $r = new \stdClass();
            $r->id = $id;
            $r->field = 'boo';
            $this->assertEquals($r,$entity);
            $count++;
        }
        $this->assertEquals(1,$count);
    }

    /**
     * @expectedException        Interop\Lenient\Dao\Exception\DuplicateKeyException
     */
    public function testDuplicateInsert1()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>1));
    }

    /**
     * @expectedException        Interop\Lenient\Dao\Exception\DuplicateKeyException
     */
    public function testDuplicateInsert2()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1,'ser2'=>array(1,2)));
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>2,'ser2'=>1));
    }

    /**
     * @expectedException        Interop\Lenient\Dao\Exception\DuplicateKeyException
     */
    public function testDuplicateInsert3()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test', 'day'=>1,'ser'=>1,'ser2'=>1));
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>2,'ser2'=>array(1,2)));
    }

    public function testNoDuplicateInsert2()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test', 'day'=>1,'ser'=>1,'ser2'=>array(1,2)));
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>2,'ser2'=>3));
        $this->assertTrue(true);
    }

    public function testNoDuplicateInsert3()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test', 'day'=>1,'ser'=>1,'ser2'=>3));
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>2,'ser2'=>array(1,2)));
        $this->assertTrue(true);
    }

    public function testNoDuplicateUpdate1()
    {
        $repository = $this->getRepository();
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1,'ser2'=>1));
        $row['name'] = 'test2';
        $row2 = $repository->save($row);
        $row3 = $repository->findById($row['id']);
        $this->assertEquals('test2',$row3['name']);
    }

    /**
     * @expectedException        Interop\Lenient\Dao\Exception\DuplicateKeyException
     */
    public function testDuplicateUpdate1()
    {
        $repository = $this->getRepository();
        $tmp = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>2));
        $row['ser'] = 1;
        $row2 = $repository->save($row);
    }
}
