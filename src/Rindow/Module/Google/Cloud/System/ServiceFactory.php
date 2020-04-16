<?php
namespace Rindow\Module\Google\Cloud\System;

use Google\Cloud\Core\ServiceBuilder;
use Rindow\Stdlib\Cache\Exception;
use Rindow\Stdlib\Cache\SimpleCache\ArrayCache;
use Rindow\Stdlib\Cache\ConfigCache\ConfigCacheFactory as StdlibConfigCacheFactory;

class ServiceFactory extends StdlibConfigCacheFactory
{
    const GAE_FILECACHE      = 'Rindow\\Stdlib\\Cache\\SimpleCache\\FileCache';
    const GAE_DATASTORECACHE = 'Rindow\\Module\\Google\\Cloud\\Cache\\SimpleCache\\Datastore';

    protected $serviceBuilder;
    protected $datastore;

    public function __construct(array $config=null)
    {
        if(!$this->hasApcu($config)) {
            $this->enableMemCache = false;
            $this->memCacheClassName = self::GAE_FILECACHE;
            $this->fileCacheClassName = self::GAE_FILECACHE;
            $this->config['memCache']['path'] = sys_get_temp_dir().'/memcache';
        }
        if(isset($_SERVER['GOOGLE_CLOUD_PROJECT'])) {
            $config['filePath'] = sys_get_temp_dir().'/cache';
        }
        parent::__construct($config);
    }

    protected function hasApcu($config=null)
    {
        if(isset($config['configCache']['apcu'])) {
            return ($config['configCache']['apcu'])?true:false;
        }
        return extension_loaded('apc')||extension_loaded('apcu');
    }

    protected function createFileCache($className)
    {
        $fileCache = parent::createFileCache($className);
        if(is_object($fileCache) && method_exists($fileCache, 'setDatastore'))
            $fileCache->setDatastore($this->getDatastore());
        return $fileCache;
    }

    public function getServiceBuilder()
    {
        if($this->serviceBuilder==null) {
            $config = array();
            if(isset($this->config['serviceBuilder']))
                $config = $this->config['serviceBuilder'];
            $this->serviceBuilder = new ServiceBuilder($config);
        }
        return $this->serviceBuilder;
    }

    public function getDatastore()
    {
        if($this->datastore==null) {
            $config = array();
            if(isset($this->config['datastore']))
                $config = $this->config['datastore'];
            $this->datastore = $this->getServiceBuilder()->datastore($config);
        }
        return $this->datastore;
    }
}
