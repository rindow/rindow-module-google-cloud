<?php
namespace Rindow\Module\Google\Cloud\Persistence\Orm;

use Rindow\Stdlib\Entity\PropertyAccessPolicy;
use Rindow\Persistence\OrmShell\DataMapper;

use Rindow\Module\Google\Cloud\Persistence\Exception;
//use Rindow\Module\Google\Cloud\Persistence\Paginator\DatastoreAdapter;
use Google\Cloud\Datastore\EntityInterface;
use Google\Cloud\Datastore\Key;

abstract class AbstractMapper implements DataMapper
{
    const ANCESTOR = '__ancestor';
    const NAME_KEY = '__name';

    abstract public function className();
    abstract public function supplementEntity($entityManager,$entity);
    abstract public function subsidiaryPersist($entityManager,$entity);
    abstract public function subsidiaryRemove($entityManager,$entity);

    abstract public function tableName();
    abstract public function primaryKey();
    abstract protected function unindexed();
    abstract protected function bulidInsertParameter($entity);
    abstract protected function bulidUpdateParameter($entity);

    //protected $resource;
    protected $database;
    protected $dataSource;
    protected $hydrator;
    //protected $entityManager;

    protected $cache;

    //public function setResource($resource)
    //{
    //    $this->resource = $resource;
    //    $this->setHydrator($resource->getHydrator());
    //}

    /**
     * No Transaction Manager Mode
     */
    public function setDatabase($database)
    {
        $this->database = $database;
    }

    public function setDataSource($dataSource)
    {
        $this->dataSource = $dataSource;
    }

    // function setEntityManager($entityManager)
    //{
    //    $this->entityManager = $entityManager;
    //}

    public function getConnection()
    {
        if($this->dataSource==null)
            throw new Exception\DomainException('DataSource is not specifed.');
        return $this->dataSource->getConnection();
    }

    public function getDatastore()
    {
        if($this->dataSource==null)
            throw new Exception\DomainException('DataSource is not specifed.');
        return $this->dataSource->getDatastore();
    }
/*
    public function getMappedEntityClass($field)
    {
        if(!array_key_exists($field, $this->mappingClasses))
            return null;
        return $this->mappingClasses[$field];
    }
*/
    public function setHydrator($hydrator)
    {
        $this->hydrator = $hydrator;
    }

    protected function setField($entity,$name,$value)
    {
        if($entity instanceof PropertyAccessPolicy) {
            $entity->$name = $value;
        } else {
            $setter = 'set'.ucfirst($name);
            $entity->$setter($value);
        }
        return $entity;
    }

    protected function getField($entity,$name)
    {
        if(!is_object($entity)) {
            throw new Exception\DomainException('entity is not object.:'.gettype($entity));
        }

        if($entity instanceof PropertyAccessPolicy) {
            return $entity->$name;
        } else {
            $getter = 'get'.ucfirst($name);
            if(!is_callable(array($entity, $getter)))
                throw new Exception\DomainException('getter "'.$getter.'" not found in "'.get_class($entity).'".');
            return $entity->$getter();
        }
    }

    public function getId($entity)
    {
        return $this->getField($entity,$this->primaryKey());
    }

    public function _createEntity(EntityInterface $dbEntity)
    {
        $data = $dbEntity->get();
        $data[$this->primaryKey()] = $dbEntity->key();
        $className = $this->className();
        $entity = new $className();
        $this->hydrator->hydrate($data,$entity);
        return $entity;
    }

    public function create($entity)
    {
        $data = $this->bulidInsertParameter($entity);
        $ancestor = $name = null;
        if(array_key_exists(self::ANCESTOR, $data)) {
            $ancestor = $data[self::ANCESTOR];
            unset($data[self::ANCESTOR]);
        } elseif(array_key_exists(self::NAME_KEY, $data)) {
            $name = $data[self::NAME_KEY];
            unset($data[self::NAME_KEY]);
        }
        $datastore = $this->getDatastore();
        $key = $datastore->key($this->tableName(),$name);
        if($ancestor) {
            $key->ancestorKey($ancestor);
        }
        $dbEntity = $datastore->entity($key);
        $dbEntity->set($data);
        $dbEntity->setExcludeFromIndexes($this->unindexed());
        if($key->state()==Key::STATE_INCOMPLETE) {
            $datastore->allocateIds(array($key));
        }
        $this->getConnection()->insert($dbEntity);
        $this->setField($entity,$this->primaryKey(),$key);
        return $entity;
    }

    public function save($entity)
    {
        $key = $this->getField($entity,$this->primaryKey());
        $datastore = $this->getDatastore();
        $dbEntity = $datastore->entity($key);
        $properties = $this->bulidUpdateParameter($entity);
        $dbEntity->set($properties);
        $dbEntity->setExcludeFromIndexes($this->unindexed());
        $this->getConnection()->upsert($dbEntity);
    }

    public function remove($entity)
    {
        $key = $this->getField($entity,$this->primaryKey());
        $this->getConnection()->delete($key);
    }

    public function find($key,$entity=null,$lockMode=null,array $properties=null)
    {
        $datastore = $this->getDatastore();
        if(is_numeric($key)) {
            $key = $datastore->key($this->tableName(),$key,array('identifierType' => Key::TYPE_NAME));
        } elseif(is_string($key)) {
            $key = $datastore->key($this->tableName(),$key,array('identifierType' => Key::TYPE_ID));
        } elseif(!($key instanceof Key)) {
            throw new Exception\InvalidArgumentException('Invalid of Key.:'.get_class($key));
        }
        $dbEntity = $this->getConnection()->lookup($key);
        if(!$dbEntity)
            return null;
        $entity = $this->_createEntity($dbEntity);
        return $entity;
    }

/*
    public function findAll($resultListFactory,$pagination=false)
    {
        $dbQuery = new Query($this->tableName());
        if($pagination) {
            $countQuery = new Query($this->tableName());
            $keysOnlyProjection = new Projection(Entity::KEY_RESERVED_PROPERTY);
            $countQuery->addProjection($keysOnlyProjection);
            $adapter = new DatastoreAdapter($this->getDatabase());
            $adapter->setQuery($dbQuery)
                    ->setCountQuery($countQuery)
                    ->setLoader(array($this,'load'));
            return $adapter;
        } else {
            $result = $this->executeQuery($resultListFactory,$dbQuery);
            $result->setLoader(array($this,'load'));
            $result->setMapper($this);
            return $result;
        }
    }
*/
    public function findBy(
        $resultListFactory,
        $query,
        $params=null,
        $firstPosition=null,
        $maxResult=null,
        $lockMode=null)
    {
        $unmapped = false;
        $compoundSelection = false;
        $countQuery = false;
        $datastore = $this->getDatastore();
        if(is_array($query)) {
            $dbQueries = array();
            $dbQuery = $datastore->query()->kind($this->tableName());
            if(isset($query[self::ANCESTOR])) {
                $dbQuery->hasAncestor($query[self::ANCESTOR]);
                unset($query[self::ANCESTOR]);
            }
            foreach ($query as $fieldName => $value) {
                $dbQuery->filter($fieldName,'=',$value);
            }
            $dbQueries[] = $dbQuery;
        } elseif($query instanceof PreparedCriteria && $query->getQueries()!==null) {
            $dbQueries = array();
            $queries = $query->getQueries();
            if(count($queries)==0)
                $queries = array(array());
            foreach($queries as $filters) {
                $dbQuery = $datastore->query()->kind($query->getKind());
                if(!is_array($filters)) {
                    if($filters->getOperator()!='AND') {
                        throw new Exception\DomainException('Invalid Status');
                    }
                    $filters = $filters->getExpressions();
                }
                foreach ($filters as $filter) {
                    $expressions = $filter->getExpressions();
                    $operator = $filter->getOperator();
                    if($filter->getExpressionType()!='OPERATOR' ||
                        $operator=='OR' || $operator=='AND') {
                        throw new Exception\DomainException('Invalid Operator');
                    }
                    $fieldName = $expressions[0]->getNodeName();
                    $type = $expressions[1]->getExpressionType();
                    if($type=='CONSTANT') {
                        $value = $expressions[1]->getValue();
                    } elseif($type=='PARAMETER') {
                        $parameterName = $expressions[1]->getName();
                        if(is_array($params) && array_key_exists($parameterName, $params)) {
                            $value = $params[$parameterName];
                        } else {
                            throw new Exception\DomainException('query parameter "'.$name.'" does match.');
                        }
                    } else {
                        throw new Exception\DomainException('Invalid expression type: "'.$type.'".');
                    }
                    if($fieldName==self::ANCESTOR)
                        $dbQuery->hasAncestor($value);
                    else
                        $dbQuery->filter($fieldName,$operator,$value);
                }
                foreach ($query->getDistincts() as $fieldName => $switch) {
                    if($switch)
                        $dbQuery->distinctOn($fieldName);
                }
                foreach ($query->getOrders() as $fieldName => $direction) {
                    $dbQuery->order($fieldName,$direction);
                }
                if($query->isKeysOnly())
                    $dbQuery->keysOnly();
                $projection = $query->getProjection();
                if(!empty($projection)) {
                    $dbQuery->projection($projection);
                }
                //********* CAUTION *********
                //* When divided into multiple queries,
                //* maxResult and firstPosition do not work correctly.
                if($maxResult)
                    $dbQuery->limit($maxResult);
                if($firstPosition&&count($dbQueries)==0)
                    $dbQuery->offset($firstPosition);

                $dbQueries[] = $dbQuery;
            }
            if($query->isCountQuery()) {
                $unmapped = true;
                $countQuery = true;
            } else {
                if($query->getProjectionType()!='ROOT') {
                    $unmapped = true;
                    $compoundSelection = $query->isCompoundSelection() ? 'MULTI':'SINGLE';
                }
            }
        } elseif($query instanceof PreparedCriteria && $query->getDbQuery()) {
            $dbQuery = call_user_func($query->getDbQuery(),$params,$firstPosition,$maxResult);
            $dbQueries = array($dbQuery);
        } elseif(is_string($query)) {
            $gql = $query;
        } else {
            var_dump($query);
            throw new Exception\InvalidArgumentException('Invalid Type of Query for "'.$this->className().'".');
        }

        if($countQuery) {
            $result = $this->executeCountQuery($resultListFactory,$dbQueries,$maxResult);
        } else {
            $result = $this->executeQuery($resultListFactory,$dbQueries,$maxResult);
        }
        if($unmapped) {
            $result->setMapped(false);
            if($compoundSelection=='MULTI')
                $result->addFilter(array($this,'_multiValue'));
            elseif($compoundSelection=='SINGLE')
                $result->addFilter(array($this,'_singleValue'));
        } else {
            $result->addFilter(array($this,'_createEntity'));
        }
        return $result;
    }

    public function _singleValue($dbEntity)
    {
        $properties = $dbEntity->get();
        return array_pop($properties);
    }

    public function _multiValue($dbEntity)
    {
        return $dbEntity->get();
    }

    public function getNamedQuery($name,$resultClass=null)
    {
        $namedQueries = $this->namedQueryBuilders();
        if(!isset($namedQueries[$name]))
            return null;
        $prepared = new PreparedCriteria($creiteria=null,$query=null,$parameters=null,$gql=null,$this->className(),$resultClass);
        $prepared->setDbQuery($namedQueries[$name]);
        return $prepared;
    }
/*
    protected function query($dbQuery,$countQueryForPagination=null)
    {
        if($countQueryForPagination) {
            $keysOnlyProjection = new Projection(Entity::KEY_RESERVED_PROPERTY);
            $countQueryForPagination->addProjection($keysOnlyProjection);
            $sqlAdapter = new DatastoreAdapter($this->getDatabase());
            $sqlAdapter->setQuery($dbQuery)
                    ->setCountQuery($countQueryForPagination)
                    ->setLoader(array($this,'_createEntity'));
            return $sqlAdapter;
        } else {
            $result = $this->executeQuery($dbQuery);
            $result->setLoader(array($this,'_createEntity'));
            return $result;
        }
    }
*/
    protected function executeUpdate($dbEntity)
    {
        return $this->getConnection()->upsert($dbEntity);
    }

    protected function executeQuery($resultListFactory,$dbQueries,$limit)
    {
        //$connection = $this->getConnection();
        $dataSource = $this->dataSource;
        $executor = function () use ($dataSource,$dbQueries,$limit) {
            return new QueryResultCursor($dataSource,$dbQueries,$limit);
        };
        $resultList = call_user_func($resultListFactory,$executor);
        return $resultList;
    }

    protected function executeCountQuery($resultListFactory,$dbQueries,$limit=null)
    {
        //$connection = $this->getConnection();
        $dataSource = $this->dataSource;
        $executor = function () use ($dataSource,$dbQueries,$limit) {
            $cursor = new QueryResultCursor($dataSource,$dbQueries,$limit);
            $count = 0;
            while($cursor->fetch()) {
                $count++;
            }
            return new ArrayCursor(array($count));
        };
        $resultList = call_user_func($resultListFactory,$executor);
        return $resultList;
    }

    public function createSchema()
    {
    }

    public function dropSchema()
    {
    }

    public function close()
    {
    }
}
