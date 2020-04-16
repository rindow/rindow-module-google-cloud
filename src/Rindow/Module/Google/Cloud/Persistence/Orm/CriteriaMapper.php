<?php
namespace Rindow\Module\Google\Cloud\Persistence\Orm;

use Rindow\Persistence\Orm\Criteria\CriteriaMapper as CriteriaMapperInterface;
use Rindow\Module\Google\Cloud\Persistence\Exception;
use Google\Cloud\Datastore\Query\Query;

class CriteriaMapper implements CriteriaMapperInterface
{
    const EQUAL        = 'EQUAL';
    const GREATER_THAN = 'GREATER_THAN';
    const GREATER_THAN_OR_EQUAL = 'GREATER_THAN_OR_EQUAL';
    const LESS_THAN    = 'LESS_THAN';
    const LESS_THAN_OR_EQUAL = 'LESS_THAN_OR_EQUAL';

    protected $entityManager;

    public function setContext($context)
    {
        $this->entityManager = $context;
    }

    public function getCriteriaBuilder()
    {
        return $this->entityManager->getCriteriaBuilder();
    }

    public function prepare(/* Criteria */$criteria,$resultClass=null)
    {
        $gql = null;
        $prepared = new PreparedCriteria();
        if(is_object($criteria) && method_exists($criteria, 'getRoots')) {
            $parameters = array();
            $prepared->setCriteria($criteria);
            $this->buildRoot($prepared,$criteria);
            $this->buildProjection($prepared,$criteria);
            $this->buildFilter($prepared,$criteria,$parameters);
            $this->buildSort($prepared,$criteria);
            //$this->buildGrouping($query,$criteria);
            //$this->buildJoinPart($criteriaBuilder,$criteria);
            $prepared->setEntityClass($criteria->getRoots()->getNodeName());
            $prepared->setResultClass($resultClass);
        } elseif(is_string($criteria)) {
            $prepared->setGql($criteria);
            $prepared->setResultClass($resultClass);
        } else {
            throw new Exception\InvalidArgumentException('Query must be a Criteria or a string of DQL.');
        }
        return $prepared;
    }

    protected function generateKind($root)
    {
        return $this->entityManager->getRepository($root->getNodeName())->getMapper()->tableName();
    }

    protected function buildRoot($prepared,$criteria)
    {
        $roots = $criteria->getRoots();
        if($roots->getJoins())
            throw new Exception\DomainException('"join" is not supported.');
        return $prepared->setKind($this->generateKind($roots));
    }

    protected function assertKind($kind, $selection)
    {
        if($selection->getExpressionType()=='FUNCTION')
            throw new Exception\DomainException('"FUNCTION" expression is not supported on Google Cloud Datastore.');
        if($selection->getExpressionType()!='PATH')
            throw new Exception\DomainException('selection must be property.');
        $root = $selection->getParentPath();
        if($root->getExpressionType()!='ROOT')
            throw new Exception\DomainException('selection must be property.');
        $selectionKind = $this->generateKind($root);
        if($kind != $selectionKind)
            throw new Exception\DomainException('selection must belong to same "kind".');
    }

    protected function buildProjection($prepared,$criteria)
    {
        $selection = $criteria->getSelection();
        $prepared->setProjectionType($selection->getExpressionType());
        if($selection->isCompoundSelection()) {
            $prepared->setCompoundSelection(true);
            $selections = $selection->getCompoundSelectionItems();
        } else {
            if($selection->getExpressionType()=='ROOT') {
                $kind = $this->generateKind($selection);
                if($kind!=$prepared->getKind())
                    throw new Exception\DomainException('Selection must be comprised a root kind.');
                return;
            } elseif ($selection->getExpressionType()=='FUNCTION') {
                $this->buildFunctionProjection($prepared,$selection);
                return;
            }
            $selections = array($selection);
        }
        $rootKind = $prepared->getKind();
        if($criteria->getGroupRestriction())
            throw new Exception\DomainException('"HAVING" expression is not supported on Google Cloud Datastore.');

        foreach ($criteria->getGroupList() as $group) {
            $this->assertKind($rootKind, $group);
            $groups[$group->getNodeName()] = true;
        }
        foreach ($selections as $selection) {
            $this->assertKind($rootKind, $selection);
            $propertyName = $selection->getNodeName();
            $prepared->addProjection($propertyName);
            if(array_key_exists($propertyName, $groups) && $groups[$propertyName]) {
                $prepared->distinctOn($propertyName);
            }
        }
        //if($criteria->isDistinct())
        //    $query->setDistinct(true);
    }

    protected function buildFunctionProjection($prepared,$function)
    {
        if($function->getOperator()!='COUNT')
            throw new Exception\DomainException('"'.$function->getOperator().'" function is not supported on Google Cloud Datastore.');

        $expressions = $function->getExpressions();
        if(count($expressions)!=1)
            throw new Exception\DomainException('COUNT must provide a property.');
        $expression = $expressions[0];
        $kind = $this->generateKind($expression);
        if($expression->getExpressionType()=='ROOT') {
            if($kind!=$prepared->getKind())
                throw new Exception\DomainException('Selection must be comprised a root kind.');
        } else {
            $this->assertKind($kind,$expression);
        }
        $prepared->setKeysOnly();
        $prepared->setCountQuery();
    }

    protected function buildFilter($prepared,$criteria,&$parameters)
    {
        $restriction = $criteria->getRestriction();
        if($restriction === null) {
            $prepared->setQueries(array());
            return;
        }
        if($restriction->getExpressionType() != 'OPERATOR')
            throw new Exception\DomainException('restriction must be "OPERATOR".');
        $kind = $prepared->getKind();
        $restriction = $this->extractDisjunction($kind,$restriction,$parameters);
        $restriction = $this->putTogetherAndRestriction($restriction);
        if($restriction->getOperator()=='OR') {
            $prepared->setQueries($restriction->getExpressions());
        } elseif($restriction->getOperator()=='AND') {
            $prepared->setQueries(array($restriction));
        } else {
            $prepared->setQueries(array(array($restriction)));
        }
        $prepared->setParameters($parameters);
    }
/*
    protected function buildFilter($prepared,$query,$criteria,&$parameters)
    {
        $restriction = $criteria->getRestriction();
        if($restriction === null)
            return;
        if($restriction->getExpressionType() != 'OPERATOR')
            throw new Exception\DomainException('restriction must be "OPERATOR".');
        $kind = $prepared->getKind();
        list($ancestor,$restriction) = $this->getAncestorRestriction($kind,$restriction,$parameters);
        if($ancestor) {
            $query->hasAncestor($ancestor);
        }
        if($restriction) {
            $restriction = $this->extractDisjunction($kind,$restriction,&$parameters);
            $restriction = $this->putTogetherAndRestriction($restriction);

            $filter = $this->getFilter($kind,$restriction,$parameters);
            $query->setFilter($filter);
        }
    }
*/
    protected function putTogetherAndRestriction($restriction)
    {
        if($restriction->getOperator()=='OR') {
            $newExpressions = array();
            foreach ($restriction->getExpressions() as $expression) {
                $newExpressions[] = $this->putTogetherAndRestriction($expression);
            }
            return call_user_func_array(array($this->getCriteriaBuilder(),'or_'),$newExpressions);
        } elseif($restriction->getOperator()=='AND') {
            $newExpressions = array();
            foreach ($restriction->getExpressions() as $andExpression) {
                $andExpression = $this->putTogetherAndRestriction($andExpression);
                if($andExpression->getOperator()=='AND') {
                    foreach($andExpression->getExpressions() as $tmpExpression) {
                        $newExpressions[] = $tmpExpression;
                    }
                } else {
                    $newExpressions[] = $andExpression;
                }
            }
            return call_user_func_array(array($this->getCriteriaBuilder(),'and_'),$newExpressions);
        } else {
            return $restriction;
        }
    }

    protected function extractDisjunction($kind,$restriction,&$parameters)
    {
        $operator = $restriction->getOperator();
        if($operator=='OR'||$operator=='AND') {
            if($restriction->isNegated()) {
                $newExpressions = array();
                foreach ($restriction->getExpressions() as $expression) {
                    $newExpressions[] = $expression->not();
                }
                if($operator=='OR')
                    $restriction = $this->getCriteriaBuilder()->and_($newExpressions);
                else
                    $restriction = $this->getCriteriaBuilder()->or_($newExpressions);
            }
            $operator = $restriction->getOperator();
        } else {
            $restriction = $this->extractPredicateFilter($kind,$restriction,$parameters);
        }
        if($operator=='OR') {
            $restriction = $this->extractOrRestriction($kind,$restriction,$parameters);
        } elseif($operator=='AND') {
            $restriction = $this->extractAndRestriction($kind,$restriction,$parameters);
        }
        return $restriction;
    }

    protected function extractAndRestriction($kind,$restriction,&$parameters)
    {
        $newOrExpressions = array();
        foreach ($restriction->getExpressions() as $expression) {
            if($expression->getExpressionType() != 'OPERATOR')
                throw new Exception\DomainException('restriction must be "OPERATOR".');
            $expression = $this->extractDisjunction($kind,$expression,$parameters);
            if($expression->getOperator()=='OR') {
                if(count($newOrExpressions)==0) {
                    $newOrExpressions[] = array();
                }
                $tmpNewOrExpressions = array();
                foreach ($newOrExpressions as $newAndExpressions) {
                    foreach ($expression->getExpressions() as $subExpression) {
                        $tmpNewAndExpressions = $newAndExpressions;
                        $tmpNewAndExpressions[] = $subExpression;
                        $tmpNewOrExpressions[] = $tmpNewAndExpressions;
                    }
                }
                $newOrExpressions = $tmpNewOrExpressions;
            } else {
                if(count($newOrExpressions)==0)
                    $newOrExpressions[] = array();
                $tmpNewOrExpressions = array();
                foreach($newOrExpressions as $newAndExpressions) {
                    $newAndExpressions[] = $expression;
                    $tmpNewOrExpressions[] = $newAndExpressions;
                }
                $newOrExpressions = $tmpNewOrExpressions;
            }
        }
        if(count($newOrExpressions)==0) {
            throw new Exception\DomainException('Invalid State');
        } elseif(count($newOrExpressions)==1) {
            $newAndExpressions = $newOrExpressions[0];
            if(count($newAndExpressions)==0)
                throw new Exception\DomainException('Invalid State');
            if(count($newAndExpressions)==1)
                return $newAndExpressions[0];
            return call_user_func_array(array($this->getCriteriaBuilder(),'and_'),$newAndExpressions);
        } else {
            $resultOrExpressions = array();
            foreach ($newOrExpressions as $newAndExpressions) {
                if(count($newAndExpressions)==0) {
                    throw new Exception\DomainException('Invalid State');
                } elseif(count($newAndExpressions)==1) {
                    $resultOrExpressions[] = $newAndExpressions[0];
                } else {
                    $resultOrExpressions[] = call_user_func_array(array($this->getCriteriaBuilder(),'and_'), $newAndExpressions);
                }
            }
            return call_user_func_array(array($this->getCriteriaBuilder(),'or_'),$resultOrExpressions);
        }
    }

    protected function extractOrRestriction($kind,$restriction,&$parameters)
    {
        $newOrExpressions = array();
        foreach ($restriction->getExpressions() as $expression) {
            if($expression->getExpressionType() != 'OPERATOR')
                throw new Exception\DomainException('restriction must be "OPERATOR".');
            $expression = $this->extractDisjunction($kind,$expression,$parameters);
            if($expression->getOperator()=='OR') {
                foreach($expression->getExpressions() as $tmpExpression) {
                    $tmpExpression = $this->extractDisjunction($kind,$tmpExpression,$parameters);
                    $newOrExpressions[] = $tmpExpression;
                }
            } else {
                $newOrExpressions[] = $expression;
            }
        }
        return call_user_func_array(array($this->getCriteriaBuilder(),'or_'),$newOrExpressions);
    }
/*
    protected function getAncestorRestriction($kind,$restriction,&$parameters)
    {
        $ancestor = null;
        if($restriction->getOperator()=='EQUAL') {
            list($property, $value, $swap) = $this->getFilterParameter($kind,$restriction);
            if($property->getNodeName()=='ancestor') {
                $ancestor = $value;
                $restriction = null;
            }
        } elseif($restriction->getOperator()=='AND') {
            $expressions = $restriction->getExpressions();
            $newExpressions = array();
            foreach ($expressions as $expression) {
                if($expression->getOperator()=='EQUAL') {
                    list($property, $value, $swap) = $this->getFilterParameter($kind,$expression);
                    if($property->getNodeName()=='ancestor') {
                        if($ancestor)
                            throw new Exception\DomainException('"ancestor" restriction is duplicated.');
                        $ancestor = $value;
                    } else {
                        $newExpressions[] = $expression;
                    }
                } else {
                    $newExpressions[] = $expression;
                }
            }
            if($ancestor) {
                if(count($newExpressions)==1)
                    $restriction = $newExpressions[0];
                else
                    $restriction = $this->entityManager->getCriteriaBuilder()->and_($newExpressions);
            }
        }
        if($ancestor==null) {
            return array(null,$restriction);
        } elseif($ancestor->getExpressionType()=='CONSTANT') {
            return array($ancestor->getValue(),$restriction);
        } elseif($ancestor->getExpressionType()=='PARAMETER') {
            $filter = new FilterPredicate('__ancestor',FilterOperator::EQUAL,null);
            $parameters[$ancestor->getName()][] = $filter;
            return array(null,$restriction);
        } else {
            throw new Exception\DomainException('Invalid ancestor restriction.');
        }
    }
*/

    protected function extractPredicateFilter($kind,$restriction,&$parameters)
    {
        list($property, $value, $swap) = $this->getFilterParameter($kind,$restriction);
        switch ($restriction->getOperator()) {
            case self::EQUAL:
                $operator = self::EQUAL;
                break;
            case self::GREATER_THAN:
                if($swap)
                    $operator = self::LESS_THAN;
                else
                    $operator = self::GREATER_THAN;
                break;
            case self::GREATER_THAN_OR_EQUAL:
                if($swap)
                    $operator = self::LESS_THAN_OR_EQUAL;
                else
                    $operator = self::GREATER_THAN_OR_EQUAL;
                break;
            case self::LESS_THAN:
                if($swap)
                    $operator = self::GREATER_THAN;
                else
                    $operator = self::LESS_THAN;
                break;
            case self::LESS_THAN_OR_EQUAL:
                if($swap)
                    $operator = self::GREATER_THAN_OR_EQUAL;
                else
                    $operator = self::LESS_THAN_OR_EQUAL;
                break;
            default:
                throw new Exception\DomainException('unknown operator "'.$restriction->getOperator().'".');
        }
        $isNegated = $restriction->isNegated();
        $builder = $this->getCriteriaBuilder();
        switch ($operator) {
            case self::EQUAL:
                if($isNegated) {
                    $restriction = $builder->or_(
                        $builder->lt($property,$value),
                        $builder->gt($property,$value)
                    );
                } else {
                    $restriction = $builder->equal($property,$value);
                }
                break;
            case self::GREATER_THAN:
                if($isNegated)
                    $restriction = $builder->le($property,$value);
                else
                    $restriction = $builder->gt($property,$value);
                break;
            case self::GREATER_THAN_OR_EQUAL:
                if($isNegated)
                    $restriction = $builder->lt($property,$value);
                else
                    $restriction = $builder->ge($property,$value);
                break;
            case self::LESS_THAN:
                if($isNegated)
                    $restriction = $builder->ge($property,$value);
                else
                    $restriction = $builder->lt($property,$value);
                break;
            case self::LESS_THAN_OR_EQUAL:
                if($isNegated)
                    $restriction = $builder->gt($property,$value);
                else
                    $restriction = $builder->le($property,$value);
                break;
            default:
                throw new Exception\DomainException('unknown operator "'.$restriction->getOperator().'".');
        }
        if($value->getExpressionType()=='PARAMETER')
            $parameters[$value->getName()][] = $value;
        return $restriction;
    }

//        if($value->getExpressionType()=='CONSTANT') {
//            return new FilterPredicate($property->getNodeName(),$operator,$value->getValue());
//        }
//        $filter = new FilterPredicate($property->getNodeName(),$operator,null);

    protected function getFilterParameter($kind,$restriction)
    {
        $expressions = $restriction->getExpressions();
        if(count($expressions)!=2)
            throw new Exception\DomainException('a filter must have two expressions.');
        $x = $expressions[0];
        $y = $expressions[1];
        $swap = false;
        if($y->getExpressionType() == 'PATH') {
            list($x,$y) = array($y,$x);
            $swap = true;
        }
        if($x->getExpressionType() != 'PATH' ||
            ($y->getExpressionType()!='CONSTANT' && $y->getExpressionType()!='PARAMETER'))
            throw new Exception\DomainException('a filter must set of selection and constant value.');
        $this->assertKind($kind, $x);
        return array($x,$y,$swap);
    }
/*
    protected function getCompositeFilter($kind,$restriction,&$parameters)
    {
        switch ($restriction->getOperator()) {
            case 'AND':
                $operator = CompositeFilterOperator::AND_;
                break;
            case 'OR':
                $operator = CompositeFilterOperator::OR_;
                break;
            default:
                throw new Exception\DomainException('unknown operator "'.$restriction->getOperator().'".');
        }
        $subFilters = array();
        foreach ($restriction->getExpressions() as $expression) {
            if($expression->getExpressionType() != 'OPERATOR')
                throw new Exception\DomainException('restriction must be "OPERATOR".');
            $subFilters[] = $this->getFilter($kind,$expression,$parameters);
        }
        return new CompositeFilter($operator,$subFilters);
    }
*/
    protected function buildSort($prepared,$criteria)
    {
        $kind = $prepared->getKind();
        foreach ($criteria->getOrderList() as $order) {
            $expression = $order->getExpression();
            $this->assertKind($kind, $expression);
            if($order->isAscending())
                $direction = Query::ORDER_ASCENDING;
            else
                $direction = Query::ORDER_DESCENDING;
            $prepared->setOrder($expression->getNodeName(),$direction);
        }
    }
}
