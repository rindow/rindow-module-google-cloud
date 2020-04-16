<?php
namespace RindowTest\Google\Cloud\Transaction\LocalTxModuleWithRepositoryTest;

use PHPUnit\Framework\TestCase;
use Rindow\Container\ModuleManager;
use Rindow\Stdlib\Entity\AbstractEntity;

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

/**
* @TransactionManagement()
*/
class TestDao
{
    protected $repository;
    protected $logger;

    public function setLogger($logger)
    {
        $this->logger = $logger;
    }

    public function setRepository($repository)
    {
        $this->repository = $repository;
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

        // repository
        $category = array('name' => 'test');
        $this->repository->save($category);

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

        // repository
        $category = array('name' => 'test');
        $this->repository->save($category);

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

        // repository
        $cursor = $this->repository->findAll();

        // Messaging
        //$this->messagingTemplate->convertAndSend('/queue/testdest','testmessage');

        $this->logger->log('out testCommit');
        return $cursor;
    }
}
class TestException extends \Exception
{}

class Test extends TestCase
{
    const WAIT = 1;
    const REPOSITORYNAME = 'category';
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
            sleep(self::WAIT);
            $this->deleteAll(self::REPOSITORYNAME);
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
                    'Repository' => __NAMESPACE__.'\\Repository',
                    'MessagingTemplate' => 'n/a',
                ),
                'components' => array(
                    __NAMESPACE__.'\TestDao' => array(
                        'properties' => array(
                            'repository' => array('ref'=>'Repository'),
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
                    __NAMESPACE__.'\\Repository'=>array(
                        'parent' => 'Rindow\\Module\\Google\\Cloud\\Repository\\AbstractGoogleCloudRepository',
                        'properties' => array(
                            'kindName' => array('value'=>self::REPOSITORYNAME),
                        ),
                    ),
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
        sleep(self::WAIT*2);
        $test->testCommit($failure=false);

        sleep(self::WAIT*2);
        $count = count($this->findAll(self::REPOSITORYNAME));
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

        sleep(self::WAIT*2);
        try {
            $test->testRollback($failure=false);
        } catch(TestException $e) {
            ;
        }
        sleep(self::WAIT*2);

        $count = count($this->findAll(self::REPOSITORYNAME));
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
        $test = $mm->getServiceLocator()->get(__NAMESPACE__.'\TestDao');

        sleep(self::WAIT*2);
        $test->testCommit($failure=false);
        sleep(self::WAIT*2);
        $cursor = $test->testFindAllOutOfTX($failure=false);
        $count=0;
        foreach ($cursor as $value) {
            $count++;
        }
        $this->assertEquals(1,$count);
    }
}
