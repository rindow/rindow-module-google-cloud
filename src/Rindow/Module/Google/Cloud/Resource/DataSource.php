<?php
namespace Rindow\Module\Google\Cloud\Resource;

use Interop\Lenient\Dao\Resource\DataSource as DataSourceInterface;

use Google\Cloud\Core\ServiceBuilder;

class DataSource implements DataSourceInterface
{
    protected $config;
    protected $serviceFactory;
    protected $driver;
    protected $connection;
    protected $logger;
    protected $debug;

    public function __construct($config=null,$serviceFactory=null)
    {
        if($config)
            $this->setConfig($config);
        if($serviceFactory)
            $this->setServiceFactory($serviceFactory);
    }

    public function setConfig($config)
    {
        $this->config = $config;
    }

    public function setServiceFactory($serviceFactory)
    {
        $this->serviceFactory = $serviceFactory;
    }

    public function setDriver($driver)
    {
        $this->driver = $driver;
    }

    public function setLogger($logger)
    {
        $this->logger = $logger;
    }

    public function setDebug($debug=true)
    {
        $this->debug = $debug;
    }

    public function getDatastore()
    {
        return $this->serviceFactory->getDatastore();
    }

    public function getConnection($username=null, $password=null)
    {
        $this->logDebug('get connection from the datasource.');
        if($this->connection) {
            return $this->connection;
        }
        $connection =  $this->serviceFactory->getDatastore();
        if($this->logger)
            $connection->setLogger($this->logger);
        if($this->debug)
            $connection->setDebug($this->debug);
        $this->connection = $connection;
        return $connection;
    }

    protected function logDebug($message, array $context = array())
    {
        if($this->debug && $this->logger!=null)
            $this->logger->debug($message,$context);
    }

    protected function logError($message, array $context = array())
    {
        if($this->logger!=null)
            $this->logger->error($message,$context);
    }
}
