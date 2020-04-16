<?php
namespace Rindow\Module\Google\Cloud\Persistence\Orm;

use Iterator;
use Interop\Lenient\Dao\Query\Cursor;

class QueryResultCursor implements Cursor
{
    protected $dataSource;
    protected $connection;
    protected $queryList;
    protected $entityResultList;
    protected $limit = 0;
    protected $fetchCount = 0;

    public function __construct($dataSource,array $queryList,$limit=null)
    {
        $this->dataSource = $dataSource;
        $this->queryList = $queryList;
        if($limit)
            $this->limit = $limit;
    }

    public function getConnection()
    {
        if($this->connection==null) {
            $this->connection = $this->dataSource->getConnection();
        }
        return $this->connection;
    }

    public function fetch()
    {
        if($this->limit) {
            if($this->fetchCount >= $this->limit)
                return false;
        }

        if($this->entityResultList==null) {
            $entityResult=false;
        } elseif($this->entityResultList->valid()) {
            $entityResult = $this->entityResultList->current();
        } else {
            $entityResult=false;
        }
        if($entityResult===false) {
            $this->nextQueryResult();
            if($this->entityResultList->valid()) {
                $entityResult = $this->entityResultList->current();
            } else {
                $entityResult=false;
            }
        }

        if($entityResult===false) {
            return false;
        }
        $this->entityResultList->next();

        //$this->cursorString = $entityResult->cursor();
        $this->fetchCount += 1;
        return $entityResult;
    }

    protected function nextQueryResult()
    {
        $connection = $this->getConnection();
        while(true) {
            $query = current($this->queryList);
            if($query===false) {
                return false;
            }
            $entityResultList = $connection->runQuery($query);

            next($this->queryList);
            if(!$entityResultList) {
                //$this->currentQueryCursor = null;
                continue;
            }
            $this->entityResultList = $entityResultList;
            // MUST rewind array pointer. the pointer is gived API points a end of array.
            $this->entityResultList->rewind();
            return;
        }
    }

    public function close()
    {
    }
/*
    protected function runQuery($query)
    {
        if($this->currentQueryCursor)
            $query->setStartCursor($this->currentQueryCursor);

        if($msgBatch->hasEndCursor())
            $this->currentQueryCursor = $msgBatch->getEndCursor();
        else
            $this->currentQueryCursor = null;

    }
*/
}
