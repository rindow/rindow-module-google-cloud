<?php
namespace Rindow\Module\Google\Cloud\Transaction;

use Interop\Lenient\Dao\Resource\DataSource as DataSourceInterface;

use Google\Cloud\Core\ServiceBuilder;

class DataSource implements DataSourceInterface
{
    protected $config;
    protected $serviceFactory;
    protected $driver;
    protected $resourceManagers = array();
    protected $transactionManager;
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

    public function setTransactionManager($transactionManager)
    {
        $this->transactionManager = $transactionManager;
    }

    public function getTransactionManager()
    {
        return $this->transactionManager;
    }

    public function getDatastore()
    {
        return $this->serviceFactory->getDatastore();
    }

    public function getConnection($username=null, $password=null)
    {
        $this->logDebug('DataSource::getConnection: get connection from the datasource.');
        $this->garbageCorrection();
        $txConnection = $this->enlistToTransaction($this->serviceFactory->getDatastore());
        return $txConnection;
    }

    protected function enlistToTransaction($datastore)
    {
        if($this->transactionManager) {
            $transaction = $this->transactionManager->getTransaction();
            if($transaction) {
                $txId = spl_object_hash($transaction);
                if(isset($this->resourceManagers[$txId])) {
                    $resourceManager = $this->resourceManagers[$txId];
                } else {
                    $resourceManager = $this->getResource($datastore->transaction());
                    $this->resourceManagers[$txId] = $resourceManager;
                }
                $transaction->enlistResource($resourceManager);
                return $resourceManager->getConnection();
            }
        }
        return $datastore;
    }

    protected function getResource($transaction)
    {
        $this->logDebug('DataSource::New ResourceManager');
        $resourceManager = new ResourceManager($transaction,$this->config,$this->logger,$this->debug);
        return $resourceManager;
    }

    protected function garbageCorrection()
    {
        $garbages = array();
        foreach($this->resourceManagers as $txId => $resourceManager) {
            if(!$resourceManager->getConnection())
                $garbages[] = $txId;
        }
        foreach ($garbages as $txId) {
            unset($this->resourceManagers[$txId]);
        }
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
