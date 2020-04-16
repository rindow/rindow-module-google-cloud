<?php
namespace Rindow\Module\Google\Cloud\Cache\SimpleCache;

use Google\Cloud\Datastore\DatastoreClient;
use Google\Cloud\Core\Blob;
use Rindow\Module\Google\Cloud\System\Environment;
use Rindow\Stdlib\Cache\Cache;
use Rindow\Stdlib\Cache\Exception\InvalidArgumentException;
use Rindow\Stdlib\Cache\Exception\CacheException;
use Traversable;

class Datastore implements Cache
{
    const CACHE_KIND = 'rindow_cache';
    const NOT_FOUND = '$$$$$$$$$$$$$NOTFOUND$$$$$$$$$$$$';

    //public static $readCount = 0;
    //public static $writeCount = 0;
    //public static $deleteCount = 0;

    protected $config=array();
    protected $kind;
    protected $version;
    protected $serviceBuilder;
    protected $datastore;
    protected $lastKey;
    protected $hasLastValue = false;
    protected $lastGetTime;
    protected $lastTimeout;
    protected $lastValue;
    protected $fastGetTtl = 0;

    public function __construct($config=null,$datastoreClient=null)
    {
        if($config)
            $this->setConfig($config);
        if($datastoreClient)
            $this->setDatastoreClient($datastoreClient);
    }

    public function setConfig($config)
    {
        if($config)
            $this->config = $config;
    }

    public function setDatastoreClient($datastoreClient)
    {
        $this->datastore = $datastoreClient;
    }

    public function setKind($kind)
    {
        $this->kind = $kind;
    }

    public function getKind()
    {
        return $this->kind;
    }

    public function setVersion($version)
    {
        $this->version = $version;
    }

    public function getVersion()
    {
        return $this->version;
    }

    protected function getDatastore()
    {
        if($this->datastore==null) {
            $this->datastore = new DatastoreClient();
        }
        return $this->datastore;
    }

    public function generateKindName($version=null)
    {
        if($this->kind==null)
            $kind = self::CACHE_KIND;
        else
            $kind = $this->kind;
        $kindName = $kind;
        return $kindName;
    }

    protected function generateAncestorKey()
    {
        if($this->version==null)
            $version = Environment::currentVersionId();
        else
            $version = $this->version;
        $datastore = $this->getDatastore();
        $ancestor = $datastore->key($this->generateKindName(), 'v'.$version);
        return $ancestor;
    }

    protected function generateKey($name)
    {
        $datastore = $this->getDatastore();
        $storeKey = $datastore->key($this->generateKindName(),$name);
        $ancestor = $this->generateAncestorKey();
        $storeKey->ancestorKey($ancestor);
        return $storeKey;
    }

    protected function toArrayWithIndex($traversable)
    {
        if(is_array($traversable))
            return $traversable;
        $values = array();
        foreach ($traversable as $value) {
            $values[] = $value;
        }
        return $values;
    }

    public function transFromOffsetToPath($offset)
    {
        #return str_replace(
        #    array('\\',':',  '*',  '?',  '"',  '<',  '>',  '|',  '.'),
        #    array('/', '%3A','%2A','%3F','%22','%3C','%3E','%7C','%46'),
        #    $offset);
        return str_replace(
            array('\\',':',  '*',  '?',  '"',  '<',  '>',  '|',  '.'),
            array('/', '%3A','%2A','%3F','%22','%3C','%3E','%7C','%46'),
            $offset);
    }

    public function setFastGetTtl($fastGetTtl)
    {
        $this->fastGetTtl = $fastGetTtl;
    }

    protected function clearLastGetItem()
    {
        $this->lastKey = null;
        $this->hasLastValue = false;
    }

    public function get($key,$default=null)
    {
        if(!is_string($key) && !is_numeric($key))
            throw new InvalidArgumentException('Key must be string.');
        $now = time();
        if($this->lastKey===$key && $this->hasLastValue) {
            if($this->lastGetTime && $now<=$this->lastGetTime+$this->fastGetTtl) {
                if(!$this->lastTimeout || $now<$this->lastTimeout) {
                    return $this->lastValue;
                }
            }
        }
        $this->clearLastGetItem();

        $name = $this->transFromOffsetToPath($key);
//$out = fopen('php://stdout', 'w');
//fwrite($out,'name:('.$name.")\n");
//fclose($out);

#echo '<pre>';
#echo 'get:'.$this->generateKindName().':'.$name."\n";
#echo '</pre>';
        $storeKey = $this->generateKey($name);
        $datastore = $this->getDatastore();
        $entity = $datastore->lookup($storeKey);
//self::$readCount++;
        if($entity==null)
           return $default;
        $blob = $entity['value'];
        if(!($blob instanceof Blob))
            throw new CacheException('Invalid type of internal data in:'.$key);
        $item = unserialize(strval($blob->get()));
        if(!is_array($item))
            throw new CacheException('Invalid cache item format.');
        list($timeout,$value) = $item;
        $this->lastTimeout = $timeout;
        if($timeout && $now>=$timeout) {
            return $default;
        }
        $this->lastValue = $value;
        $this->lastTimeout = $timeout;
        $this->lastKey = $key;
        $this->hasLastValue = true;
        $this->lastGetTime = $now;
        return $value;
    }

    public function set($key,$value,$ttl=null)
    {
        if(!is_string($key) && !is_numeric($key))
            throw new InvalidArgumentException('Key must be string.');
        $this->clearLastGetItem();

        if($ttl)
            $timeout = time() + $ttl;
        else
            $timeout = null;
        $name = $this->transFromOffsetToPath($key);
        $storeKey = $this->generateKey($name);
        $datastore = $this->getDatastore();
        $blob = $datastore->blob(serialize(array($timeout,$value)));
        $entity = $datastore->entity($storeKey,array('value'=>$blob));
        $entity->setExcludeFromIndexes(array('value'));
#echo '<pre>';
#echo 'offset:'.$offset."\n";
#echo 'put:'.$this->generateKindName().':'.$name."\n";
#echo '</pre>';
        $datastore->upsert($entity);
//self::$writeCount++;
#echo '<pre>';
#echo 'done:put:'.$this->generateKindName().':'.$name."\n";
#echo '</pre>';
#if($offset=='Acme\MyApp\Persistence\EntityManager') {
#    echo '<pre>';
#    var_dump($value);
#    echo '</pre>';
#}
        return true;
    }

    public function delete($key)
    {
        if(!is_string($key) && !is_numeric($key))
            throw new InvalidArgumentException('Key must be string.');
        $this->clearLastGetItem();

        $name = $this->transFromOffsetToPath($key);
        $storeKey = $this->generateKey($name);
#echo '<pre>';
#echo 'delete:'.$filename."\n";
#echo '</pre>';
        $datastore = $this->getDatastore();
        $datastore->delete($storeKey);
//self::$deleteCount++;
#echo '<pre>';
#echo 'unlink:'.$filename."\n";
#echo '</pre>';
        return true;
    }

    public function clear()
    {
        $this->clearLastGetItem();
        $datastore = $this->getDatastore();
        $query = $datastore->query()
            ->kind($this->generateKindName())
            ->keysOnly();
        $query->hasAncestor($this->generateAncestorKey());
        $result = $datastore->runQuery($query);
        $keys = array();
        foreach ($result as $entity) {
            $keys[] = $entity->key();
        }
        $datastore->deleteBatch($keys);
//self::$deleteCount+=count($keys);
        return true;
    }

    public function getMultiple($keys, $default = null)
    {
        if(!is_array($keys) && !($keys instanceof Traversable))
            throw new InvalidArgumentException('Keys must be array or Traversable.');

        foreach ($keys as $key) {
            $values[$key] = $this->get($key,$default);
        }
        return $values;
    }

    public function setMultiple($values, $ttl = null)
    {
        if(!is_array($values) && !($values instanceof Traversable))
            throw new InvalidArgumentException('Values must be array or Traversable.');
        $this->clearLastGetItem();

        if($ttl)
            $timeout = time() + $ttl;
        else
            $timeout = null;
        $datastore = $this->getDatastore();
        $entities = array();
        foreach($values as $key => $value) {
            if(!is_string($key) && !is_numeric($key))
                throw new InvalidArgumentException('Key must be string.');
            $data = $datastore->blob(serialize(array($timeout,$value)));
            $name = $this->transFromOffsetToPath($key);
            $storeKey = $this->generateKey($name);
            $entity = $datastore->entity($storeKey,array('value'=>$data));
            $entity->setExcludeFromIndexes(array('value'));
            $entities[] = $entity;
        }
        $datastore->upsertBatch($entities);
//self::$writeCount+=count($entities);
        return true;
    }

    public function deleteMultiple($keys)
    {
        if(!is_array($keys) && !($keys instanceof Traversable))
            throw new InvalidArgumentException('Keys must be array or Traversable.');
        $this->clearLastGetItem();

        $datastore = $this->getDatastore();
        $storeKeys = array();
        foreach($keys as $key) {
            if(!is_string($key) && !is_numeric($key))
                throw new InvalidArgumentException('Key must be string.');
            $name = $this->transFromOffsetToPath($key);
            $storeKey = $this->generateKey($name);
            $storeKeys[] = $storeKey;
        }
        $datastore->deleteBatch($storeKeys);
//self::$writeCount+=count($storeKeys);
        return true;
    }

    public function has($key)
    {
        $value = $this->get($key,self::NOT_FOUND);
        if($value===self::NOT_FOUND)
            return false;
        return true;
    }

    public function isNonvolatile()
    {
        return true;
    }

    public function isReady()
    {
        return true;
    }
}
