<?php
namespace Rindow\Module\Google\Cloud\Transaction;

use Interop\Lenient\Transaction\ResourceManager as ResourceManagerInterface;
use Rindow\Database\Dao\Exception;

class ResourceManager implements ResourceManagerInterface
{
    //protected $listener;
    protected $transaction;
    protected $name;
    protected $logger;
    protected $debug;

    public function __construct($transaction,array $config=null,$logger=null,$debug=null)
    {
        $this->transaction = $transaction;
        if(isset($config['name']))
            $this->name = $config['name'];
        $this->logger = $logger;
        $this->debug = $debug;
    }

    public function getConnection()
    {
        return $this->transaction;
    }

    public function setTimeout($seconds)
    {}

    public function isNestedTransactionAllowed()
    {
        return false;
    }

    public function getName()
    {
        return $this->name;
    }

    protected function logDebug($message, array $context = array())
    {
        if($this->debug && $this->logger!=null)
            $this->logger->debug($message,$context);
    }

    public function beginTransaction($definition=null)
    {
        $this->logDebug('ResourceManager::beginTransaction');
        if(!$this->transaction)
            throw new Exception\IllegalStateException('transaction is gone.');
    }

    public function commit()
    {
        $this->logDebug('ResourceManager::commit');
        if(!$this->transaction)
            throw new Exception\IllegalStateException('the connection is not in transaction.');
        $this->transaction->commit();
        $this->transaction = null;
    }

    public function rollback()
    {
        $this->logDebug('ResourceManager::rollback');
        if(!$this->transaction)
            throw new Exception\IllegalStateException('the connection is not in transaction.');
        $this->transaction->rollback();
        $this->transaction = null;
    }

    public function suspend()
    {
        throw new Exception\NotSupportedException('suspend operation is not supported.');
    }

    public function resume($txObject)
    {
        throw new Exception\NotSupportedException('resume operation is not supported.');
    }
}