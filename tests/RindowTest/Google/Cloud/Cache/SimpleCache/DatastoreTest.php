<?php
namespace RindowTest\Google\Cloud\Cache\SimpleCache\DatastoreTest;

use PHPUnit\Framework\TestCase;
use Rindow\Module\Google\Cloud\System\Environment;
use Rindow\Module\Google\Cloud\Cache\SimpleCache\Datastore;
use Google\Cloud\Datastore\DatastoreClient;

class Test extends TestCase
{
    public static $skip = false;
    public static function setUpBeforeClass()
    {
    }
    public function setUp()
    {
        if(self::$skip) {
            $this->markTestSkipped();
            return;
        }
        try {
            $datastore = new DatastoreClient();
            $query = $datastore->query()
                ->kind($this->getCacheKindName())
                ->keysOnly();
            //$ancestor = $datastore->key($this->getCacheKindName(), 'v');
            //$query->hasAncestor($ancestor);
            $result = $datastore->runQuery($query);
            $keys = array();
            foreach ($result as $entity) {
                $keys[] = $entity->key();
            }
            $datastore->deleteBatch($keys);
        } catch(\Exception $e) {
            self::$skip = true;
            $this->markTestSkipped();
            return;
        }
    }
    public function getData($collection,$filter=array())
    {
        $datastore = new DatastoreClient();
        $query = $datastore->query()
            ->kind($collection);
        $ancestor = $datastore->key($collection, 'v');
        $query->hasAncestor($ancestor);
        $cursor = $datastore->runQuery($query);
        $results = array();
        foreach ($cursor as $entity) {
            $values = array();
            $values['_id'] = strval($entity->key()->pathEndIdentifier());
            $blob = $entity['value'];
            $values['value'] = array(strtoupper(basename(str_replace('\\','/',get_class($blob)))), strval($blob->get()));
            $results[] = $values;
        }
        return $results;
    }

    public function getCacheKindName()
    {
    	return Datastore::CACHE_KIND;
    }

    public function testSetAndGetAndDelete()
    {
    	$cache = new Datastore();
    	$this->assertFalse($cache->has('abc'));
    	$this->assertEquals('defaultValue',$cache->get('abc','defaultValue'));

    	// new
    	$this->assertTrue($cache->set('abc','CacheValue'));
    	$this->assertEquals('CacheValue',$cache->get('abc','defaultValue'));
    	$this->assertTrue($cache->has('abc'));

    	$datas = $this->getData($this->getCacheKindName());
    	$this->assertCount(1,$datas);
    	$this->assertEquals('abc',$datas[0]['_id']);
    	$this->assertEquals(array('BLOB',serialize(array(NULL,'CacheValue'))),$datas[0]['value']);

    	// replace
    	$this->assertTrue($cache->set('abc','CacheAnotherValue'));
    	$this->assertEquals('CacheAnotherValue',$cache->get('abc','defaultValue'));

    	// delete
    	$this->assertTrue($cache->delete('abc'));
    	$this->assertFalse($cache->has('abc'));
    	$this->assertEquals('defaultValue',$cache->get('abc','defaultValue'));

    	$datas = $this->getData($this->getCacheKindName());
    	$this->assertCount(0,$datas);

    	// *** CAUTION ***
    	// The delete method returns always "True".
    	$this->assertTrue($cache->delete('abc'));
    }

    public function testMultipleSetAndGetAndDelete()
    {
    	$cache = new Datastore();
    	$this->assertFalse($cache->has('abc'));
    	$this->assertEquals(
    		array('abc'=>'defaultValue','def'=>'defaultValue','ghi'=>'defaultValue'),
    		$cache->getMultiple(array('abc','def','ghi'),'defaultValue'));

    	// new
    	$this->assertTrue($cache->setMultiple(
    		array('abc'=>'abcValue','ghi'=>'ghiValue')));
    	$this->assertEquals(
    		array('abc'=>'abcValue','def'=>'defaultValue','ghi'=>'ghiValue'),
    		$cache->getMultiple(array('abc','def','ghi'),'defaultValue'));

    	$datas = $this->getData($this->getCacheKindName());
    	$this->assertCount(2,$datas);
        foreach ($datas as $data) {
            if('abc'==$data['_id'])
                $this->assertEquals(array('BLOB',serialize(array(null,'abcValue'))),$data['value']);
            elseif('ghi'==$data['_id'])
                $this->assertEquals(array('BLOB',serialize(array(null,'ghiValue'))),$data['value']);
            else
                throw new \Exception("Invalid data: '".$data['_id']."'");
        }

    	// replace
    	$this->assertTrue($cache->setMultiple(
    		array('abc'=>'abcAnotherValue','ghi'=>'ghiAnotherValue')));
    	$this->assertEquals(
    		array('abc'=>'abcAnotherValue','def'=>'defaultValue','ghi'=>'ghiAnotherValue'),
    		$cache->getMultiple(array('abc','def','ghi'),'defaultValue'));

    	// delete
    	$this->assertTrue($cache->deleteMultiple(array('abc','ghi')));
    	$this->assertFalse($cache->has('abc'));
    	$this->assertFalse($cache->has('ghi'));

    	$datas = $this->getData($this->getCacheKindName());
    	$this->assertCount(0,$datas);

    	// *** CAUTION ***
    	// The delete method returns always "True".
    	$this->assertTrue($cache->deleteMultiple(array('abc','ghi')));
    }

    public function testSetAndClear()
    {
    	$cache = new Datastore();

    	$this->assertTrue($cache->setMultiple(
    		array('abc'=>'abcValue','def'=>'defValue')));
    	$this->assertEquals(
    		array('abc'=>'abcValue','def'=>'defValue'),
    		$cache->getMultiple(array('abc','def'),'defaultValue'));

    	$datas = $this->getData($this->getCacheKindName());
    	$this->assertCount(2,$datas);

    	$this->assertTrue($cache->clear());

    	$this->assertEquals(
    		array('abc'=>'defaultValue','def'=>'defaultValue'),
    		$cache->getMultiple(array('abc','def'),'defaultValue'));

    	$datas = $this->getData($this->getCacheKindName());
    	$this->assertCount(0,$datas);
    }
}
