<?php
namespace RindowTest\Google\Cloud\Transaction\LocalTxModuleWithORMTest;

use PHPUnit\Framework\TestCase;
use Rindow\Container\ModuleManager;
use Rindow\Stdlib\Entity\AbstractEntity;
use Rindow\Persistence\OrmShell\DataMapper;

use Rindow\Transaction\Annotation\TransactionAttribute;
use Rindow\Transaction\Annotation\TransactionManagement;
use Rindow\Module\Google\Cloud\Persistence\Orm\AbstractMapper;
use Google\Cloud\Core\ServiceBuilder;

class TestLogger
{
    public $logdata = array();
    public function log($message)
    {
        $this->logdata[] = $message;
    }
}

class Category
{
    public $id;

    public $name;

    public function setId($id)
    {
        $this->id = $id;
    }

    public function getId()
    {
        return $this->id;
    }

    public function setName($name)
    {
        $this->name = $name;
    }

    public function getName()
    {
        return $this->name;
    }
}

class CategoryMapper extends AbstractMapper implements DataMapper
{
    const CLASS_NAME = 'RindowTest\Google\Cloud\Transaction\LocalTxModuleWithORMTest\Category';
    const TABLE_NAME = 'category';
    const PRIMARYKEY = 'id';

    public $errorInsert;
    protected $namedQuerys;

    public function className()
    {
        return self::CLASS_NAME;
    }

    public function tableName()
    {
        return self::TABLE_NAME;
    }

    public function primaryKey()
    {
        return self::PRIMARYKEY;
    }

    public function hash($entityManager,$entity)
    {
        $hash = md5(strval($this->getId($entity))) .
            md5(strval($this->getField($entity,'name')));
        return md5($hash);
    }

    public function supplementEntity($entityManager,$entity)
    {
        return $entity;
    }

    public function subsidiaryPersist($entityManager,$entity)
    {
    }

    public function subsidiaryRemove($entityManager,$entity)
    {
    }

    protected function insertStatement()
    {
        return self::INSERT;
    }

    protected function bulidInsertParameter($entity)
    {
        if($this->errorInsert)
            throw new TestException('Insert Error in CategoryMapper');

        return array(':name'=>$entity->name);
    }

    protected function updateByPrimaryKeyStatement()
    {
        return self::UPDATE_BY_PRIMARYKEY;
    }

    protected function bulidUpdateParameter($entity)
    {
        return array(':name'=>$entity->name,':id'=>$entity->id);
    }

    protected function deleteByPrimaryKeyStatement()
    {
        return self::DELETE_BY_PRIMARYKEY;
    }

    protected function selectByPrimaryKeyStatement()
    {
        return self::SELECT_BY_PRIMARYKEY;
    }

    protected function selectAllStatement()
    {
        return self::SELECT_ALL;
    }

    protected function countAllStatement()
    {
        return self::COUNT_ALL;
    }

    protected function unindexed()
    {
        return array();
    }

    protected function namedQueryBuilders()
    {
        if($this->namedQuerys)
            return $this->namedQuerys;
        $datastore = $this->getDatastore();
        $this->namedQuerys = array(
            'category.all' => function($params,$firstPosition,$maxResult) use($datastore) {
                $query = $datastore->query()->kind(CategoryMapper::TABLE_NAME);
                //$query->order('name');
                if($maxResult)
                    $query->limit($maxResult);
                if($firstPosition)
                    $query->offset($firstPosition);
                return $query;
            },
        );
        return $this->namedQuerys;
    }
}


/**
* @TransactionManagement()
*/
class TestDao
{
    protected $entityManager;
    protected $dataSource;
    protected $logger;

    public function setLogger($logger)
    {
        $this->logger = $logger;
    }

    public function setEntityManager($entityManager)
    {
        $this->entityManager = $entityManager;
    }

    public function setCriteriaBuilder($criteriaBuilder)
    {
        $this->criteriaBuilder = $criteriaBuilder;
    }
    public function FunctionName($value='')
    {
        // code...
    }

    public function setDataSource($dataSource)
    {
        $this->dataSource = $dataSource;
    }

    public function setMessagingTemplate($messagingTemplate)
    {
        $this->messagingTemplate = $messagingTemplate;
    }

    /**
    *  @TransactionAttribute('required')
    */
    public function testCommit($failure=false)
    {
        $this->logger->log('in testCommit');

        // ORM
        $category = new Category();
        $category->name = 'test';
        $this->entityManager->persist($category);

        // Messaging
        //$this->messagingTemplate->convertAndSend('/queue/testdest','testmessage');

        $this->logger->log('out testCommit');
    }

    /**
    *  @TransactionAttribute('required')
    */
    public function testRollback($failure=false)
    {
        $this->logger->log('in testRollback');

        // ORM
        $category = new Category();
        $category->name = 'test';
        $this->entityManager->persist($category);

        // Messaging
        //$this->messagingTemplate->convertAndSend('/queue/testdest','testmessage');

        $this->logger->log('out testRollback');
        throw new TestException("Error", 1);
    }

    /**
    *  @TransactionAttribute('required')
    */
    public function testFindAllOutOfTX()
    {
        $this->logger->log('in testCommit');

        // ORM
        //$queryCriteria = $this->criteriaBuilder->createQuery();
        //$root = $queryCriteria->from(__NAMESPACE__.'\\Category')->alias('t0');
        //$queryCriteria->select($root);
        //$query = $this->entityManager->createQuery($queryCriteria);
        //$results = $query->getResultList();


        $query = $this->entityManager->createNamedQuery('category.all');
        $results = $query->getResultList();
        // Messaging
        //$this->messagingTemplate->convertAndSend('/queue/testdest','testmessage');

        $this->logger->log('out testCommit');
        return $results;
    }
}
class TestException extends \Exception
{}

class Test extends TestCase
{
    const WAIT = 1;
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
        try {
            $this->deleteAll(CategoryMapper::TABLE_NAME);
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
/*
    public static function getQueueClientStatic()
    {
        $config = self::getStaticConfig();
        $config = $config['database']['connections']['default'];
        $connection = new Connection($config);
        $queue = new QueueDriver($connection,$config);
        return $queue;
    }
    public function getQueueClient()
    {
        return self::getQueueClientStatic();
    }
*/
    public function getConfig()
    {
        return self::getStaticConfig();
    }

    public static function getStaticConfig()
    {
        $config = array(
            'module_manager' => array(
                'modules' => array(
                    'Rindow\\Aop\\Module' => true,
                    'Rindow\\Persistence\\OrmShell\\Module' => true,
                    'Rindow\\Module\\Google\\Cloud\\LocalTxModule' => true,
                    //'Rindow\\Module\\Monolog\\Module' => true,
                ),
                'annotation_manager' => true,
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
            'aop' => array(
                //'debug' => true,
                'intercept_to' => array(
                    __NAMESPACE__.'\TestDao'=>true,
                ),
            ),
            'container' => array(
                'component_paths' => array(
                    __DIR__ => true,
                ),
                'aliases' => array(
                    'TestLogger' => __NAMESPACE__.'\TestLogger',
                    'EntityManager' => 'Rindow\\Persistence\\OrmShell\\Transaction\\DefaultPersistenceContext',
                    'CriteriaBuilder' => 'Rindow\\Persistence\\OrmShell\\DefaultCriteriaBuilder',
                    'DataSource' => 'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultDataSource',
                    'MessagingTemplate' => 'n/a',
                ),
                'components' => array(
                    __NAMESPACE__.'\TestDao' => array(
                        'properties' => array(
                            'entityManager' => array('ref'=>'EntityManager'),
                            'criteriaBuilder' => array('ref'=>'CriteriaBuilder'),
                            'dataSource' => array('ref'=>'DataSource'),
                            //'messagingTemplate' => array('ref'=>'MessagingTemplate'),
                            'logger' => array('ref'=>'TestLogger'),
                        ),
                    ),
                    'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultTransactionManager' => array(
                        'properties' => array(
                            //'logger' => array('ref'=>'Logger'),
                            //'debug' => array('value'=>true),
                        ),
                    ),
                    __NAMESPACE__.'\TestLogger'=>array(),
                    __NAMESPACE__.'\CategoryMapper'=>array(
                        'parent' => 'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultAbstractMapper',
                    ),
                ),
            ),
            'persistence' => array(
                'mappers' => array(
                    __NAMESPACE__.'\Category' => __NAMESPACE__.'\CategoryMapper',
                ),
            ),
            //'monolog' => array(
            //    'handlers' => array(
            //        'default' => array(
            //            'path'  => __DIR__.'/test.log',
            //        ),
            //    ),
            //),
        );
        return $config;
    }

    public function dumpTrace($e)
    {
        while($e) {
            echo "------------------\n";
            echo $e->getMessage()."\n";
            echo $e->getFile().'('.$e->getLine().')'."\n";
            echo $e->getTraceAsString();
            $e = $e->getPrevious();
        }
    }

    public function testRequiredCommitLevel1()
    {
        $config = $this->getConfig();
        $mm = new ModuleManager($config);
        $test = $mm->getServiceLocator()->get(__NAMESPACE__.'\TestDao');
        $test->testCommit($failure=false);

        $count = count($this->findAll(CategoryMapper::TABLE_NAME));
        $this->assertEquals(1,$count);
/*
        $queue = $this->getQueueClient();
        $count = 0;
        while($frame = $queue->receive('/queue/testdest')) {
            $msg = unserialize($frame->body);
            $this->assertEquals('testmessage',$msg['p']);
            $count++;
        }
        $this->assertEquals(1,$count);
*/
    }

    public function testRequiredRollbackLevel1()
    {
        $config = $this->getConfig();
        $mm = new ModuleManager($config);
        $test = $mm->getServiceLocator()->get(__NAMESPACE__.'\TestDao');

        try {
            $test->testRollback($failure=false);
        } catch(TestException $e) {
            ;
        }

        $count = count($this->findAll(CategoryMapper::TABLE_NAME));
        $this->assertEquals(0,$count);
/*
        $queue = $this->getQueueClient();
        $count = 0;
        while($frame = $queue->receive('/queue/testdest')) {
            $msg = unserialize($frame->body);
            $this->assertEquals('testmessage',$msg['p']);
            $count++;
        }
        $this->assertEquals(0,$count);
*/
    }

    public function testFindAllOutOfTX()
    {
        $config = $this->getConfig();
        $mm = new ModuleManager($config);
        $cb = $mm->getServiceLocator()->get('Rindow\\Persistence\\OrmShell\\DefaultCriteriaBuilder');
        $test = $mm->getServiceLocator()->get(__NAMESPACE__.'\TestDao');
        sleep(self::WAIT*2);
        $test->testCommit($failure=false);
        sleep(self::WAIT*2);

        $cursor = $test->testFindAllOutOfTX();
        $count = 0;
        foreach ($cursor as $value) {
            $count++;
        }
        $this->assertEquals(1,$count);
    }
}
