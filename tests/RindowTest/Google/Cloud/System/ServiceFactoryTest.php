<?php
namespace RindowTest\Google\Cloud\System\ServiceFactoryTest;

use PHPUnit\Framework\TestCase;
use Google\Cloud\Core\ServiceBuilder;
use Rindow\Stdlib\Cache\SimpleCache\FileCache;
use Rindow\Module\Google\Cloud\Cache\SimpleCache\Datastore;
use Rindow\Module\Google\Cloud\System\Environment;
use Rindow\Module\Google\Cloud\System\ServiceFactory;
use Rindow\Container\ModuleManager;

class TestServiceFactoryWithoutApcu extends ServiceFactory
{
    protected function hasApcu($config=null)
    {
        return false;
    }
}

class TestServiceFactoryWithApcu extends ServiceFactory
{
    protected function hasApcu($config=null)
    {
        return true;
    }
}

class Test extends TestCase
{
    public function setUp()
    {
        $fileCache = new FileCache();
        $fileCache->clear();
        $fileCache = new FileCache(array('path'=>sys_get_temp_dir().'/memcache'));
        $fileCache->clear();
        $serviceBuilder = new ServiceBuilder();
        $datastoreCache = new Datastore(null,$serviceBuilder->datastore());
        $datastoreCache->clear();
    }

    public function testGetCacheWithForceFileCacheWithoutApcu()
    {
        $factory = new TestServiceFactoryWithoutApcu();
        $this->assertInstanceOf(
            'Google\\Cloud\\Core\\ServiceBuilder',
            $factory->getServiceBuilder()
        );
        $this->assertInstanceOf(
            'Google\\Cloud\\Datastore\\DatastoreClient',
            $factory->getDatastore()
        );
        $this->assertInstanceOf(
            'Rindow\\Stdlib\\Cache\\SimpleCache\\FileCache',
            $factory->getMemcache()
        );
        $this->assertEquals(
            sys_get_temp_dir().'/memcache',
            $factory->getMemcache()->getPath()
        );
        $this->assertInstanceOf(
            'Rindow\\Stdlib\\Cache\\SimpleCache\\FileCache',
            $factory->getFileCache()
        );
        $this->assertEquals(
            sys_get_temp_dir().'/cache',
            $factory->getFileCache()->getPath()
        );

        $cache = $factory->create('path',$forceFileCache=true);

        $this->assertInstanceOf(
            'Rindow\\Stdlib\\Cache\\SimpleCache\\ArrayCache',
            $cache->getPrimary()
        );

        $this->assertInstanceOf(
            'Rindow\\Stdlib\\Cache\\SimpleCache\\FileCache',
            $cache->getSecondary()
        );
        $this->assertEquals(
            sys_get_temp_dir().'/cache',
            $cache->getSecondary()->getPath()
        );
    }

    /**
     *  @requires extension apcu
     */
    public function testGetCacheWithForceFileCacheWithApcu()
    {
        $factory = new TestServiceFactoryWithApcu();
        $this->assertInstanceOf(
            'Google\\Cloud\\Core\\ServiceBuilder',
            $factory->getServiceBuilder()
        );
        $this->assertInstanceOf(
            'Google\\Cloud\\Datastore\\DatastoreClient',
            $factory->getDatastore()
        );
        $this->assertInstanceOf(
            'Rindow\\Stdlib\\Cache\\SimpleCache\\ApcCache',
            $factory->getMemcache()
        );
        $this->assertInstanceOf(
            'Rindow\\Stdlib\\Cache\\SimpleCache\\FileCache',
            $factory->getFileCache()
        );
        $this->assertEquals(
            sys_get_temp_dir().'/cache',
            $factory->getFileCache()->getPath()
        );

        $cache = $factory->create('path',$forceFileCache=true);

        $this->assertInstanceOf(
            'Rindow\\Stdlib\\Cache\\SimpleCache\\ApcCache',
            $cache->getPrimary()
        );

        // If php has apcu extention, then that system is flex mode.
        $this->assertInstanceOf(
            'Rindow\\Stdlib\\Cache\\SimpleCache\\FileCache',
            $cache->getSecondary()
        );
        $this->assertEquals(
            sys_get_temp_dir().'/cache',
            $cache->getSecondary()->getPath()
        );
    }

    public function testModuleManagerWithoutApcu()
    {
        $config=array(
            'module_manager'=>array(
                'modules' => array(),
                'configCacheFactoryClass'=>__NAMESPACE__.'\\TestServiceFactoryWithoutApcu',
            ),
            //'cache'=>array(
            //    'memCache'=>array(
            //        'class'=>'Rindow\\Stdlib\\Cache\\SimpleCache\\ArrayCache',
            //    ),
            //    'fileCache'=>array(
            //        'class'=>'Rindow\\Stdlib\\Cache\\SimpleCache\\ArrayCache',
            //    ),
            //),
        );
        $manager = new ModuleManager($config);
        $factory = $manager->getServiceLocator()->get('ConfigCacheFactory');
        $this->assertInstanceOf(__NAMESPACE__.'\\TestServiceFactoryWithoutApcu',$factory);
        $this->assertInstanceOf('Rindow\\Stdlib\\Cache\\SimpleCache\\FileCache',$manager->getServiceLocator()->get('SimpleCache'));
        $this->assertEquals(sys_get_temp_dir().'/memcache',$manager->getServiceLocator()->get('SimpleCache')->getPath());
        $cache = $factory->create('path',$forceFileCache=true);
        $memCache=$cache->getPrimary();
        $this->assertInstanceOf('Rindow\\Stdlib\\Cache\\SimpleCache\\ArrayCache',$memCache);
        $fileCache=$cache->getSecondary();
        $this->assertInstanceOf('Rindow\\Stdlib\\Cache\\SimpleCache\\FileCache',$fileCache);
        $this->assertEquals(sys_get_temp_dir().'/cache',$fileCache->getPath());

        $cache->set('abc','value');
        $this->assertEquals(array(true,'value'),$memCache->get('path/abc'));
        $this->assertEquals('value',$fileCache->get('path/abc'));
    }

    public function testDatastoreCacheWithoutApcu()
    {
        $config=array(
            'module_manager'=>array(
                'modules' => array(
                    'Rindow\\Module\\Google\\Cloud\\Module'=>true,
                ),
                'configCacheFactoryClass'=>__NAMESPACE__.'\\TestServiceFactoryWithoutApcu',
            ),
            'cache'=>array(
                'fileCache'=>array(
                    'class'=>'Rindow\\Module\\Google\\Cloud\\Cache\\SimpleCache\\Datastore',
                ),
                'configCache'=>array(
                    'enableMemCache'=>true,
                ),
            ),
        );
        $manager = new ModuleManager($config);
        $factory = $manager->getServiceLocator()->get('ConfigCacheFactory');
        $this->assertInstanceOf(__NAMESPACE__.'\\TestServiceFactoryWithoutApcu',$factory);
        $this->assertInstanceOf('Rindow\\Stdlib\\Cache\\SimpleCache\\FileCache',$manager->getServiceLocator()->get('SimpleCache'));
        $this->assertEquals(sys_get_temp_dir().'/cache',$manager->getServiceLocator()->get('SimpleCache')->getPath());

        $cache = $factory->create('path',$forceFileCache=true);
        $this->assertInstanceOf('Rindow\\Stdlib\\Cache\\SimpleCache\\FileCache',$cache->getPrimary());
        $this->assertEquals(sys_get_temp_dir().'/cache',$cache->getPrimary()->getPath());
        $this->assertInstanceOf('Rindow\\Module\\Google\\Cloud\\Cache\\SimpleCache\\Datastore',$cache->getSecondary());
    }
}
