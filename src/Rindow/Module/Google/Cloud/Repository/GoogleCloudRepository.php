<?php
namespace Rindow\Module\Google\Cloud\Repository;

use Interop\Lenient\Dao\Repository\CrudRepository;
use Interop\Lenient\Dao\Repository\DataMapper;
use Interop\Lenient\Dao\Query\Expression as ExpressionInterface;
use Interop\Lenient\Dao\Query\Parameter;

use Rindow\Database\Dao\Support\ResultList;
use Rindow\Module\Google\Cloud\Repository\Exception;

use Google\Cloud\Datastore\Key as GoogleKey;
use Google\Cloud\Datastore\Query\Query as GoogleQuery;

class GoogleCloudRepository implements CrudRepository,DataMapper
{
    static protected $operators = array(
        ExpressionInterface::EQUAL => GoogleQuery::OP_EQUALS,
        ExpressionInterface::GREATER_THAN => GoogleQuery::OP_GREATER_THAN,
        ExpressionInterface::GREATER_THAN_OR_EQUAL => GoogleQuery::OP_GREATER_THAN_OR_EQUAL,
        ExpressionInterface::LESS_THAN => GoogleQuery::OP_LESS_THAN,
        ExpressionInterface::LESS_THAN_OR_EQUAL => GoogleQuery::OP_LESS_THAN_OR_EQUAL,
        //ExpressionInterface::NOT_EQUAL => '!=',
        //ExpressionInterface::IN => 'IN',
    );
    protected $keyName = 'id';
    protected $kindName;
    protected $defaultLimit = 20;
    protected $dataSource;
    protected $queryBuilder;
    protected $dataMapper;
    protected $unindexed = array();

    public function __construct($dataSource=null,$kindName=null,$queryBuilder=null,array $unindexed=null)
    {
        if($dataSource)
            $this->setDataSource($dataSource);
        if($kindName)
            $this->setKindName($kindName);
        if($queryBuilder)
            $this->setQueryBuilder($queryBuilder);
        if($unindexed)
            $this->setUnindexed($unindexed);
    }

    public function setDataSource($dataSource)
    {
        $this->dataSource = $dataSource;
    }

    public function setDataMapper($dataMapper)
    {
        $this->dataMapper = $dataMapper;
    }

    public function setKindName($kindName)
    {
        $this->kindName = $kindName;
    }

    public function getKindName()
    {
        return $this->kindName;
    }

    public function setQueryBuilder($queryBuilder)
    {
        $this->queryBuilder = $queryBuilder;
    }

    public function getQueryBuilder()
    {
        return $this->queryBuilder;
    }

    public function setUnindexed(array $unindexed=null)
    {
        if($unindexed==null)
            return;
        $this->unindexed = $unindexed;
    }

    public function setUnique(array $unique=null)
    {
        if($unique==null)
            return;
        $this->unique = $unique;
    }

    public function assertKindName()
    {
        if($this->kindName==null)
            throw new Exception\InvalidArgumentException('kindName is not specified.');
    }

    public function getConnection()
    {
        return $this->dataSource->getConnection();
    }

    protected function getDatastore()
    {
        return $this->dataSource->getDatastore();
    }

    protected function makeDocument($entity)
    {
        if($this->dataMapper) {
            $entity = $this->dataMapper->demap($entity);
            if(!is_array($entity)) {
                throw new Exception\InvalidArgumentException('mapped document must be array.');
            }
        }
        $entity = $this->demap($entity);
        if(!is_array($entity)) {
            throw new Exception\InvalidArgumentException('the entity must be array.');
        }
        return $entity;
    }

    protected function extractId($values)
    {
        if(!isset($values[$this->keyName]))
            return null;
        return $values[$this->keyName];
    }

    protected function extractValue($value)
    {
        if($value instanceof Parameter) {
            $value = $value->getValue();
        }
        return $value;
    }

    protected function extractDatastoreExpression($propertyName,$value)
    {
        if($value instanceof ExpressionInterface) {
            if($value->getPropertyName())
                $propertyName = $value->getPropertyName();
            $operator = $value->getOperator();
            $value = $value->getValue();
        } else {
            $operator = ExpressionInterface::EQUAL;
        }
        $value = $this->extractValue($value);
        if(is_array($value) && $operator!=ExpressionInterface::IN)
            throw new Exception\InvalidArgumentException('Normally expression must not include array value.');
        $filters = array();
        switch($operator) {
            case ExpressionInterface::EQUAL:
            case ExpressionInterface::GREATER_THAN:
            case ExpressionInterface::GREATER_THAN_OR_EQUAL:
            case ExpressionInterface::LESS_THAN:
            case ExpressionInterface::LESS_THAN_OR_EQUAL:
                $filters[] = (object)array('propertyName'=>$propertyName,'operator'=>self::$operators[$operator],'value'=>$value);
                break;
            case ExpressionInterface::BEGIN_WITH:
                $filters[] = (object)array('propertyName'=>$propertyName,'operator'=>GoogleQuery::OP_GREATER_THAN_OR_EQUAL,'value'=>$value);
                $filters[] = (object)array('propertyName'=>$propertyName,'operator'=>GoogleQuery::OP_LESS_THAN,'value'=>$value.chr(0x7F));
                break;
            case ExpressionInterface::NOT_EQUAL:
            case ExpressionInterface::IN:
                //if(!is_array($value) || count($value)==0) {
                //    throw new Exception\InvalidArgumentException('IN operator requires one or more value.');
                //}
                //if(count($value)==1) {
                //    $v = array_shift($value);
                //    $expression = new FilterPredicate($propertyName,FilterOperator::EQUAL,$v);
                //    break;
                //}
                //$expression = new FilterPredicate($propertyName,FilterOperator::IN,$value);
                //break;
                throw new Exception\InvalidArgumentException('Operator is not supported.: '.$operator);
            default:
                throw new Exception\InvalidArgumentException('Unkown operator code in a filter.: '.$operator);
        }
        return $filters;
    }

    protected function buildDatastoreFilters(array $query=null)
    {
        if($query==null)
            return array();
        if(count($query)==0)
            return array();
        $filters = array();
        foreach ($query as $key => $value) {
            $filters = array_merge($filters,$this->extractDatastoreExpression($key,$value));
        }
        return $filters;
    }

    public function save($entity)
    {
        $this->assertKindName();
        $values = $this->makeDocument($entity);
        if($this->extractId($values)!==null) {
            $this->update($values);
            return $entity;
        } else {
            list($id,$values) = $this->create($values);
            if($this->dataMapper) {
                $entity = $this->dataMapper->fillId($entity,$id);
            } else {
                $entity = $this->fillId($entity,$id);
            }
        }
        return $entity;
    }

    protected function checkEntity($connection,$dbEntity,$ancestor,$values,$updateId=null)
    {
        $checkValues = array();
        $unindexed = array();
        foreach($values as $propertyName => $value) {
            if(isset($this->unique[$propertyName]) && $this->unique[$propertyName]) {
                $checkValues[$propertyName] = $value;
            }
            if(isset($this->unindexed[$propertyName]) && $this->unindexed[$propertyName]) {
                $unindexed[] = $propertyName;
            }
        }
        //
        // Set unindexed property
        //
        if($unindexed)
            $dbEntity->setExcludeFromIndexes($unindexed);

        if(count($checkValues)==0)
            return;
        //
        // Emulating the unique key
        //
        $datastore = $this->getDatastore();
        foreach ($checkValues as $propertyName => $value) {
            if(!is_array($value)) {
                $value = array($value);
            }
            foreach ($value as $v) {
                $dbQuery = $datastore->query()->kind($this->getKindName());
                $dbQuery->hasAncestor($ancestor);
                $dbQuery->keysOnly();
                $dbQuery->filter($propertyName,GoogleQuery::OP_EQUALS,$v);
                $dbQuery->limit(1);
                $results = $connection->runQuery($dbQuery);
                $count = 0;
                foreach ($results as $dmy) {
                    if($updateId===null || $dmy->key()->pathEndIdentifier()!=$updateId)
                        $count++;
                }
                if($count)
                    throw new Exception\DuplicateKeyException('Duplicate error in '.$this->getKindName());
            }
        }
    }

    protected function create($values)
    {
        $datastore = $this->getDatastore();
        $ancestor = $datastore->key($this->getKindName(),$this->getKindName());
        $dbEntity = $datastore->entity($this->getKindName(),$values);
        $dbEntity->key()->ancestorKey($ancestor);
        $connection = $this->getConnection();
        $this->checkEntity($connection,$dbEntity,$ancestor,$values);
        $connection->insert($dbEntity);
        return array($dbEntity->key()->pathEndIdentifier(),$values);
    }

    protected function update($values)
    {
        $datastore = $this->getDatastore();
        $id = $values[$this->keyName];
        unset($values[$this->keyName]);
        $ancestor = $datastore->key($this->getKindName(),$this->getKindName());
        $key = $datastore->key($this->getKindName(),$id,array('identifierType' => GoogleKey::TYPE_ID));
        $key->ancestorKey($ancestor);
        $dbEntity = $datastore->entity($key,$values);
        $connection = $this->getConnection();
        $this->checkEntity($connection,$dbEntity,$ancestor,$values,$updateId=$id);
        $connection->upsert($dbEntity);
    }

    public function delete($entity)
    {
        $this->assertKindName();
        $values = $this->makeDocument($entity);
        $id = $this->extractId($values);
        if($id===null)
            throw new Exception\InvalidArgumentException('the KeyName "'.$this->keyName.'" is not found in entity');
        $this->deleteById($id);
    }

    public function deleteById($id)
    {
        $this->assertKindName();
        $datastore = $this->getDatastore();
        $ancestor = $datastore->key($this->getKindName(),$this->getKindName());
        $key = $datastore->key($this->getKindName(),$id,array('identifierType' => GoogleKey::TYPE_ID));
        $key->ancestorKey($ancestor);
        $this->getConnection()->delete($key);
    }

    public function deleteAll(array $filter=null)
    {
        $this->assertKindName();
        $datastore = $this->getDatastore();
        $filters = $this->buildDatastoreFilters($filter);
        $ancestor = $datastore->key($this->getKindName(),$this->getKindName());
        $dbQuery = $datastore->query()->kind($this->getKindName());
        $dbQuery->hasAncestor($ancestor);
        foreach ($filters as $dbFilter) {
            $dbQuery->filter($dbFilter->propertyName,$dbFilter->operator,$dbFilter->value);
        }
        $dbQuery->keysOnly();
        $dbQuery->limit($this->defaultLimit);
        $connection = $this->getConnection();
        $entities = $connection->runQuery($dbQuery);
        $keys = array();
        foreach($entities as $entity) {
            $keys[] = $entity->key();
        }
        if(empty($keys))
            return;
        $connection->deleteBatch($keys);
    }

    public function findById($id)
    {
        $this->assertKindName();
        $datastore = $this->getDatastore();
        $ancestor = $datastore->key($this->getKindName(),$this->getKindName());
        $key = $datastore->key($this->getKindName(),$id,array('identifierType' => GoogleKey::TYPE_ID));
        $key->ancestorKey($ancestor);
        $dbEntity = $this->getConnection()->lookup($key);
        $values = $this->_translateFromDbEntity($dbEntity);
        if(!$values)
            return null;
        $entity = $this->map($values);
        if($this->dataMapper) {
            $entity = $this->dataMapper->map($entity);
        }
        return $entity;
    }

    public function findAll(array $filter=null,array $sort=null,$limit=null,$offset=null)
    {
        $this->assertKindName();

        $datastore = $this->getDatastore();
        $filters = $this->buildDatastoreFilters($filter);
        $ancestor = $datastore->key($this->getKindName(),$this->getKindName());
        $dbQuery = $datastore->query()->kind($this->getKindName());
        $dbQuery->hasAncestor($ancestor);
        foreach ($filters as $dbFilter) {
            $dbQuery->filter($dbFilter->propertyName,$dbFilter->operator,$dbFilter->value);
        }
        if($sort) {
            foreach ($sort as $key => $direction) {
                $dbDirection = ($direction>0) ? GoogleQuery::ORDER_ASCENDING : GoogleQuery::ORDER_DESCENDING;
                $dbQuery->order($key, $dbDirection);
            }
        }
        if($limit) {
            $dbQuery->limit($limit);
            //if($limit==1) {
            //    $dbQuery->setDistinct(true);
            //}
        }
        if($offset) {
            $dbQuery->offset($offset);
        }
        //$connection = $this->getConnection();
        $cursor = new GoogleCloudCursor($this->dataSource,array($dbQuery),$limit);

        $resultList = new ResultList(array($cursor,'fetch'),array($cursor,'close'));
        $resultList->addFilter(array($this,'_translateFromDbEntity'));
        $resultList->addFilter(array($this,'map'));
        if($this->dataMapper) {
            $resultList->addFilter(array($this->dataMapper,'map'));
        }
        return $resultList;
    }

    public function _translateFromDbEntity($dbEntity)
    {
        if(!$dbEntity)
            return null;
        $values = $dbEntity->get();
        $values[$this->keyName] = $dbEntity->key()->pathEndIdentifier();
        return $values;
    }

    public function findOne(array $filter=null,array $sort=null,$offset=null)
    {
        $limit = 1;
        $results = $this->findAll($filter,$sort,$limit,$offset);
        $entity = null;
        foreach ($results as $result) {
            $entity = $result;
        }
        return $entity;
    }

    public function count(array $filter=null)
    {
        $this->assertKindName();
        $datastore = $this->getDatastore();
        $filters = $this->buildDatastoreFilters($filter);
        $ancestor = $datastore->key($this->getKindName(),$this->getKindName());
        $dbQuery = $datastore->query()->kind($this->getKindName());
        $dbQuery->hasAncestor($ancestor);
        foreach ($filters as $dbFilter) {
            $dbQuery->filter($dbFilter->propertyName,$dbFilter->operator,$dbFilter->value);
        }
        $dbQuery->keysOnly();
        $dbQuery->limit($this->defaultLimit);
        $connection = $this->getConnection();
        $results = $connection->runQuery($dbQuery);
        $count = 0;
        foreach ($results as $value) {
            $count++;
        }
        return $count;
    }

    public function existsById($id)
    {
        $this->assertKindName();
        $datastore = $this->getDatastore();
        $ancestor = $datastore->key($this->getKindName(),$this->getKindName());
        $key = $datastore->key($this->getKindName(),$id,array('identifierType' => GoogleKey::TYPE_ID));
        $key->ancestorKey($ancestor);
        $dbEntity = $this->getConnection()->lookup($key);
        return $dbEntity ? true : false;
    }

    public function demap($data)
    {
        return $data;
    }

    public function map($entity)
    {
        return $entity;
    }

    public function fillId($values,$id)
    {
        $values[$this->keyName] = $id;
        return $values;
    }

    public function getFetchClass()
    {
        return null;
    }
}
