<?php
namespace Rindow\Module\Google\Cloud\Persistence\Orm;

use Rindow\Persistence\Orm\Criteria\PreparedCriteria as PreparedCriteriaInterface;

class PreparedCriteria implements PreparedCriteriaInterface
{
    protected $criteria;
    protected $queries;
    protected $parameters = array();
    protected $orders = array();
    protected $kind;
    protected $distincts = array();
    protected $countQuery = false;
    protected $compoundSelection = false;
    protected $projectionType;
    protected $projection = array();
    protected $mapping;
    protected $gql;
    protected $dbQuery;
    protected $entityClass;
    protected $resultClass;
    protected $keysOnly = false;
    
    public function __construct(
        $criteria=null,
        $queries=null,
        $parameters=null,
        $gql=null,
        $entityClass=null,
        $resultClass=null)
    {
        $this->criteria = $criteria;
        if($queries)
            $this->queries = $queries;
        if($parameters)
            $this->parameters = $parameters;
        $this->gql = $gql;
        $this->entityClass = $entityClass;
        $this->resultClass = $resultClass;
    }

    public function getCriteria()
    {
        return $this->criteria;
    }

    public function setCriteria($criteria)
    {
        $this->criteria = $criteria;
    }

    public function getQueries()
    {
        return $this->queries;
    }

    public function setQueries(array $queries)
    {
        $this->queries = $queries;
    }

    public function getKind()
    {
        return $this->kind;
    }

    public function setKind($kind)
    {
        $this->kind = $kind;
    }

    public function getParameters()
    {
        return $this->parameters;
    }

    public function setParameters(array $parameters)
    {
        $this->parameters = $parameters;
    }

    public function getOrders()
    {
        return $this->orders;
    }

    public function setOrder($nodeName,$direction)
    {
        $this->orders[$nodeName] = $direction;
    }

    public function distinctOn($nodeName)
    {
        $this->distincts[$nodeName] = true;
    }

    public function getDistincts()
    {
        return $this->distincts;
    }

    public function setCountQuery()
    {
        $this->countQuery = true;
    }

    public function isCountQuery()
    {
        return $this->countQuery;
    }

    public function setCompoundSelection($compoundSelection)
    {
        $this->compoundSelection = $compoundSelection;
    }

    public function isCompoundSelection()
    {
        return $this->compoundSelection;
    }

    public function setProjectionType($projectionType)
    {
        $this->projectionType = $projectionType;
    }

    public function getProjectionType()
    {
        return $this->projectionType;
    }

    public function addProjection($propertyName)
    {
        $this->projection[$propertyName] = true;
    }

    public function getProjection()
    {
        return array_keys($this->projection);
    }

    public function setKeysOnly()
    {
        $this->keysOnly = true;
    }

    public function isKeysOnly()
    {
        return $this->keysOnly ? true:false;
    }

    public function getGql()
    {
        return $this->gql;
    }

    public function setGql($gql)
    {
        return $this->gql = $gql;
    }

    public function getDbQuery()
    {
        return $this->dbQuery;
    }

    public function setDbQuery($dbQuery)
    {
        return $this->dbQuery = $dbQuery;
    }

    public function getResultClass()
    {
        return $this->resultClass;
    }

    public function setResultClass($resultClass)
    {
        $this->resultClass = $resultClass;
    }

    public function getEntityClass()
    {
        return $this->entityClass;
    }

    public function setEntityClass($entityClass)
    {
        $this->entityClass = $entityClass;
    }
}