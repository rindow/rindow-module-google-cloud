<?php
namespace RindowTest\Google\Cloud\Persistence\Orm\CriteriaMapperTest;

use PHPUnit\Framework\TestCase;
use Google\Cloud\Core\ServiceBuilder;

use Rindow\Module\Google\Cloud\System\Environment;
use Rindow\Module\Google\Cloud\Persistence\Orm\CriteriaMapper;

use Rindow\Stdlib\Entity\AbstractEntity;
use Rindow\Stdlib\Entity\PropertyAccessPolicy;

use Rindow\Container\ModuleManager;
use Rindow\Container\Container;

use Rindow\Persistence\Orm\Criteria\CriteriaBuilder;
use Rindow\Persistence\OrmShell\EntityManager;
use Rindow\Persistence\OrmShell\DataMapper;
use Rindow\Database\Dao\Support\Parameter;


use Rindow\Module\Google\Cloud\Persistence\Orm\AbstractMapper;


class TestCriteriaMapper extends CriteriaMapper
{
    protected function generateKind($root)
    {
        return $root->getNodeName();
    }

    public function TestGetAncestorRestriction($kind,$restriction,&$parameters)
    {
        return parent::getAncestorRestriction($kind,$restriction,$parameters);
    }
    public function TestExtractDisjunction($kind,$restriction,&$parameters)
    {
        return parent::extractDisjunction($kind,$restriction,$parameters);
    }
    public function TestPutTogetherAndRestriction($result)
    {
        return parent::putTogetherAndRestriction($result);
    }
}
class TestEntityManager
{
    protected $criteriaBuilder;
    public function getCriteriaBuilder()
    {
        if($this->criteriaBuilder==null)
            $this->criteriaBuilder = new CriteriaBuilder();
        return $this->criteriaBuilder;
    }
}

class TestMapper
{
    public function tablename()
    {
        return 'testtable';
    }

    public function setEntityManager()
    {
    }

    public function setResource()
    {
    }
}

class Color implements PropertyAccessPolicy
{
    public $id;

    public $product;

    public $color;
}

class Category
{
    public $id;

    public $name;

    public function setId($id)
    {
        $this->id = $id;
    }

    public function getId()
    {
        return $this->id;
    }

    public function setName($name)
    {
        $this->name = $name;
    }

    public function getName()
    {
        return $this->name;
    }
}

class Product extends AbstractEntity
{
    static public $colorNames = array(1=>"Red",2=>"Green",3=>"Blue");

    public function getColorNames()
    {
        return self::$colorNames;
    }

    public $id;

    public $category;

    public $name;

    public $colors;

    public function addColor($colorId)
    {
        $color = new Color();
        $color->color = $colorId;
        $color->product = $this;
        $this->colors[] = $color;
    }
}

class CategoryMapper extends AbstractMapper implements DataMapper
{
    const CLASS_NAME = 'RindowTest\Google\Cloud\Persistence\Orm\CriteriaMapperTest\Category';
    const TABLE_NAME = 'category';
    const PRIMARYKEY = 'id';

    public function className()
    {
        return self::CLASS_NAME;
    }

    public function tableName()
    {
        return self::TABLE_NAME;
    }

    public function primaryKey()
    {
        return self::PRIMARYKEY;
    }

    public function hash($entityManager,$entity)
    {
        $hash = md5(strval($this->getId($entity))) .
            md5(strval($this->getField($entity,'name')));
        return md5($hash);
    }

    public function supplementEntity($entityManager,$entity)
    {
        return $entity;
    }

    public function subsidiaryPersist($entityManager,$entity)
    {
    }

    public function subsidiaryRemove($entityManager,$entity)
    {
    }

    protected function bulidInsertParameter($entity)
    {
        return array('name'=>$entity->name);
    }

    protected function bulidUpdateParameter($entity)
    {
        return array('name'=>$entity->name);
    }

    protected function unindexed()
    {
        return array();
    }
}

class ColorMapper extends AbstractMapper implements DataMapper
{
    const CLASS_NAME = 'RindowTest\Google\Cloud\Persistence\Orm\CriteriaMapperTest\Color';
    const TABLE_NAME = 'color';
    const PRIMARYKEY = 'id';

    const CLASS_PRODUCT = 'RindowTest\Google\Cloud\Persistence\Orm\CriteriaMapperTest\Product';

    protected $productRepository;
    protected $mappingClasses = array(
        'product' => self::CLASS_PRODUCT,
    );

    protected function getProductRepository($entityManager)
    {
        //if($this->productRepository)
        //    return $this->productRepository;
        return $entityManager->getRepository(self::CLASS_PRODUCT);
    }

    public function className()
    {
        return self::CLASS_NAME;
    }

    public function tableName()
    {
        return self::TABLE_NAME;
    }

    public function primaryKey()
    {
        return self::PRIMARYKEY;
    }

    public function hash($entityManager,$entity)
    {
        $hash = md5(strval($this->getId($entity))) .
            md5(strval($this->getField($entity,'product')->id)) .
            md5(strval($this->getField($entity,'color')));
        return md5($hash);
    }

    public function supplementEntity($entityManager,$entity)
    {
        $entity->product = $entityManager->find(self::CLASS_PRODUCT, $entity->product);
        return $entity;
    }

    public function subsidiaryPersist($entityManager,$entity)
    {
    }

    public function subsidiaryRemove($entityManager,$entity)
    {
    }

    protected function bulidInsertParameter($entity)
    {
        return array('product'=>$entity->product->id, 'color'=>$entity->color);
    }

    protected function bulidUpdateParameter($entity)
    {
        return array('product'=>$entity->product->id, 'color'=>$entity->color);
    }

    protected function unindexed()
    {
        return array();
    }
}

class ProductMapper extends AbstractMapper implements DataMapper
{
    const CLASS_NAME = 'RindowTest\Google\Cloud\Persistence\Orm\CriteriaMapperTest\Product';
    const TABLE_NAME = 'product';
    const PRIMARYKEY = 'id';

    const CLASS_CATEGORY = 'RindowTest\Google\Cloud\Persistence\Orm\CriteriaMapperTest\Category';
    const CLASS_COLOR    = 'RindowTest\Google\Cloud\Persistence\Orm\CriteriaMapperTest\Color';

    protected $categoryRepository;
    protected $colorRepository;
    protected $mappingClasses = array(
        'category' => self::CLASS_CATEGORY,
    );

    protected function getCategoryRepository($entityManager)
    {
        //if($this->categoryRepository)
        //    return $this->categoryRepository;
        return $entityManager->getRepository(self::CLASS_CATEGORY);
    }

    protected function getColorRepository($entityManager)
    {
        //if($this->colorRepository)
        //    return $this->colorRepository;
        return $entityManager->getRepository(self::CLASS_COLOR);
    }

    public function className()
    {
        return self::CLASS_NAME;
    }

    public function tableName()
    {
        return self::TABLE_NAME;
    }

    public function primaryKey()
    {
        return self::PRIMARYKEY;
    }

    public function hash($entityManager,$entity)
    {
        $categoryMapper = $this->getCategoryRepository($entityManager)->getMapper();
        $hash = md5(strval($this->getId($entity))) .
            md5(strval($categoryMapper->getId($this->getField($entity,'category')))) .
            md5(strval($this->getField($entity,'name')));
        return md5($hash);
    }

    public function supplementEntity($entityManager,$entity)
    {
        $entity->category = $this->getCategoryRepository($entityManager)->find($entity->category);
        $entity->colors = $this->getColorRepository($entityManager)->findBy(array('product'=>$entity->id));
        $entity->colors->setCascade(array('persist','remove'));
        return $entity;
    }

    public function subsidiaryPersist($entityManager,$entity)
    {
        if($entity->colors===null)
            return;
        $colorRepository = $this->getColorRepository($entityManager);
        foreach ($entity->colors as $color) {
            $colorRepository->persist($color);
        }
    }

    public function subsidiaryRemove($entityManager,$entity)
    {
        if($entity->colors===null)
            return;
        $colorRepository = $this->getColorRepository($entityManager);
        foreach ($entity->colors as $color) {
            $colorRepository->remove($color);
        }
    }

    protected function bulidInsertParameter($entity)
    {
        return array('category'=>$entity->category->id,'name'=>$entity->name);
    }

    protected function bulidUpdateParameter($entity)
    {
        return array('category'=>$entity->category->id,'name'=>$entity->name);
    }

    protected function unindexed()
    {
        return array();
    }
}

class Test extends TestCase
{
    const WAIT = 1;

    public static $skip = false;
    public static $runGroup = null;#'b';

    public function deleteAll($kind)
    {
        $datastore = $this->getClient();
        $query = $datastore->query()->kind($kind);
        $results = $datastore->runQuery($query);
        $entities = array();
        foreach ($results as $entity) {
            $entities[] = $entity;
        }
        foreach ($entities as $entity) {
            $datastore->delete($entity->key());
        }
    }

    public function findAll($kind)
    {
        $datastore = $this->getClient();
        $query = $datastore->query()->kind($kind);
        if($kind==ColorMapper::TABLE_NAME)
            $query->order('color');
        else
            $query->order('name');
        $results = $datastore->runQuery($query);
        $entities = array();
        foreach ($results as $entity) {
            $entities[] = $entity;
        }
        return $entities;
    }

    public function setUp()
    {
        if(self::$skip) {
            $this->markTestSkipped();
            return;
        }
        try {
            $this->deleteAll(ColorMapper::TABLE_NAME);
            $this->deleteAll(CategoryMapper::TABLE_NAME);
            $this->deleteAll(ProductMapper::TABLE_NAME);
        } catch(\Exception $e) {
            self::$skip = true;
            $this->markTestSkipped();
            return;
        }

        // Must wait to reflect
        sleep(self::WAIT);
    }

    public function getClient()
    {
        $builder = new ServiceBuilder();
        return $builder->datastore();
    }

    public function getConfig()
    {
        $config = array(
            'module_manager' => array(
                'version' => 1,
                'modules' => array(
                    'Rindow\Persistence\OrmShell\Module' => true,
                    'Rindow\Module\Google\Cloud\Module' => true,
                ),
                'configCacheFactoryClass'=>'Rindow\\Module\\Google\\Cloud\\System\\ServiceFactory',
            ),
            'cache'=>array(
                'memCache'=>array(
                    'class'=>'Rindow\\Stdlib\\Cache\\SimpleCache\\ArrayCache',
                ),
                'fileCache'=>array(
                    'class'=>'Rindow\\Stdlib\\Cache\\SimpleCache\\ArrayCache',
                ),
            ),
            'container' => array(
                'aliases' => array(
                    // Google Datastore
                    'EntityManager' => 'Rindow\Persistence\OrmShell\DefaultEntityManager',
                    'CriteriaBuilder' => 'Rindow\Persistence\OrmShell\DefaultCriteriaBuilder',
                    'PaginatorFactory' => 'Rindow\Persistence\OrmShell\Paginator\DefaultPaginatorFactory',
                ),
                'components' => array(
                    //'Rindow\Module\Google\Cloud\Persistence\Orm\DefaultResource' => array(
                    //    'properties' => array(
                    //        'hydrator' => array('ref'=>'Rindow\Stdlib\Entity\PropertyHydrator'),
                    //    ),
                    //),
                    __NAMESPACE__.'\\DefaultAbstractMapper'=>array(
                        'parent' => 'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultAbstractMapper',
                        'properties' => array(
                            'hydrator' => array('ref'=>'Rindow\Stdlib\Entity\PropertyHydrator'),
                        ),
                    ),
                    'Rindow\Stdlib\Entity\PropertyHydrator'=>array(),
                    __NAMESPACE__.'\ProductMapper'=>array(
                        'parent' => __NAMESPACE__.'\\DefaultAbstractMapper',
                    ),
                    __NAMESPACE__.'\CategoryMapper'=>array(
                        'parent' => __NAMESPACE__.'\\DefaultAbstractMapper',
                    ),
                    __NAMESPACE__.'\ColorMapper'=>array(
                        'parent' => __NAMESPACE__.'\\DefaultAbstractMapper',
                    ),
                ),
            ),
            'persistence' => array(
                'mappers' => array(
                    // O/R Mapping for PDO
                    __NAMESPACE__.'\Product'  => __NAMESPACE__.'\ProductMapper',
                    __NAMESPACE__.'\Category' => __NAMESPACE__.'\CategoryMapper',
                    __NAMESPACE__.'\Color'    => __NAMESPACE__.'\ColorMapper',
        
                    // O/D Mapping for MongoDB
                    //'Acme\MyApp\Entity\Product'  => 'Acme\MyApp\Persistence\ODM\ProductMapper',
                    //'Acme\MyApp\Entity\Category' => 'Acme\MyApp\Persistence\ODM\CategoryMapper',
                ),
            ),
        );
        return $config;
    }

    public function getCriteriaMapper()
    {
        $entityManager = new TestEntityManager();
        $criteriaMapper = new TestCriteriaMapper();
        $criteriaMapper->setContext($entityManager);
        return $criteriaMapper;
    }

    public function testExtractDisjunctionFactorization1()
    {
        $criteriaMapper = $this->getCriteriaMapper();
        $cb = $criteriaMapper->getCriteriaBuilder();
        $q = $cb->createQuery();
        $r = $q->from('FooEntity')->alias('p');
        $p1 = $cb->parameter('integer','p1');
        $q->select($r)
            //->distinct(true)
            ->where($cb->and_(
                $cb->or_(
                    $cb->equal($r->get('field1'),$p1),
                    $cb->lt($r->get('field1_2'),$p1)
                ),
                $cb->gt($r->get('field2_2'),$p1)
            ));

        $restriction = $q->getRestriction();
        $parameters = array();
        $result = $criteriaMapper->TestExtractDisjunction('FooEntity',$restriction,$parameters);
        $result = $criteriaMapper->TestPutTogetherAndRestriction($result);
        $this->assertEquals('OR',$result->getOperator());
        $expressions = $result->getExpressions();
        $this->assertCount(2,$expressions);
        $this->assertEquals('AND',$expressions[0]->getOperator());
        $subExpressions = $expressions[0]->getExpressions();
        $this->assertCount(2,$subExpressions);
        $this->assertEquals('EQUAL',$subExpressions[0]->getOperator());
        $this->assertEquals('GREATER_THAN',$subExpressions[1]->getOperator());

        $this->assertEquals('AND',$expressions[1]->getOperator());
        $subExpressions = $expressions[1]->getExpressions();
        $this->assertCount(2,$subExpressions);
        $this->assertEquals('LESS_THAN',$subExpressions[0]->getOperator());
        $this->assertEquals('GREATER_THAN',$subExpressions[1]->getOperator());
    }

    public function testExtractDisjunctionSwapParameter2()
    {
        $criteriaMapper = $this->getCriteriaMapper();
        $cb = $criteriaMapper->getCriteriaBuilder();
        $q = $cb->createQuery();
        $r = $q->from('FooEntity')->alias('p');
        $p1 = $cb->parameter('integer','p1');
        $q->select($r)
            //->distinct(true)
            ->where($cb->or_(
                $cb->and_(
                    $cb->equal($r->get('field1'),$p1),
                    $cb->lt($r->get('field1_2'),$p1)
                ),
                $cb->gt($r->get('field2_2'),$p1)
            ));

        $restriction = $q->getRestriction();
        $parameters = array();
        $result = $criteriaMapper->TestExtractDisjunction('FooEntity',$restriction,$parameters);
        $result = $criteriaMapper->TestPutTogetherAndRestriction($result);
        $this->assertEquals('OR',$result->getOperator());
        $expressions = $result->getExpressions();
        $this->assertCount(2,$expressions);
        $this->assertEquals('AND',$expressions[0]->getOperator());
        $subExpressions = $expressions[0]->getExpressions();
        $this->assertCount(2,$subExpressions);
        $this->assertEquals('EQUAL',$subExpressions[0]->getOperator());
        $this->assertEquals('LESS_THAN',$subExpressions[1]->getOperator());

        $this->assertEquals('GREATER_THAN',$expressions[1]->getOperator());
    }

    public function testExtractDisjunctionSwapParameter3()
    {
        $criteriaMapper = $this->getCriteriaMapper();
        $cb = $criteriaMapper->getCriteriaBuilder();
        $q = $cb->createQuery();
        $r = $q->from('FooEntity')->alias('p');
        $p1 = $cb->parameter('integer','p1');
        $q->select($r)
            //->distinct(true)
            ->where($cb->or_(
                $cb->or_(
                    $cb->equal($r->get('field1'),$p1),
                    $cb->or_(
                        $cb->lt($r->get('field2_1'),$p1),
                        $cb->gt($r->get('field2_2'),$p1)
                    )
                ),
                $cb->ge($r->get('field3_1'),$p1)
            ));

        $restriction = $q->getRestriction();
        $parameters = array();
        $result = $criteriaMapper->TestExtractDisjunction('FooEntity',$restriction,$parameters);
        $result = $criteriaMapper->TestPutTogetherAndRestriction($result);
        $this->assertEquals('OR',$result->getOperator());
        $expressions = $result->getExpressions();
        $this->assertCount(4,$expressions);

        $this->assertEquals('EQUAL',$expressions[0]->getOperator());
        $this->assertEquals('LESS_THAN',$expressions[1]->getOperator());
        $this->assertEquals('GREATER_THAN',$expressions[2]->getOperator());
        $this->assertEquals('GREATER_THAN_OR_EQUAL',$expressions[3]->getOperator());
    }

    public function testExtractDisjunctionSwapParameter4()
    {
        $criteriaMapper = $this->getCriteriaMapper();
        $cb = $criteriaMapper->getCriteriaBuilder();
        $q = $cb->createQuery();
        $r = $q->from('FooEntity')->alias('p');
        $p1 = $cb->parameter('integer','p1');
        $q->select($r)
            //->distinct(true)
            ->where($cb->and_(
                $cb->and_(
                    $cb->equal($r->get('field1'),$p1),
                    $cb->and_(
                        $cb->lt($r->get('field2_1'),$p1),
                        $cb->gt($r->get('field2_2'),$p1)
                    )
                ),
                $cb->ge($r->get('field3_1'),$p1)
            ));

        $restriction = $q->getRestriction();
        $parameters = array();
        $result = $criteriaMapper->TestExtractDisjunction('FooEntity',$restriction,$parameters);
        $result = $criteriaMapper->TestPutTogetherAndRestriction($result);


        $this->assertEquals('AND',$result->getOperator());
        $expressions = $result->getExpressions();
        $this->assertCount(4,$expressions);

        $this->assertEquals('EQUAL',$expressions[0]->getOperator());
        $this->assertEquals('LESS_THAN',$expressions[1]->getOperator());
        $this->assertEquals('GREATER_THAN',$expressions[2]->getOperator());
        $this->assertEquals('GREATER_THAN_OR_EQUAL',$expressions[3]->getOperator());
    }

    public function testExtractDisjunctionFactorization5()
    {
        $criteriaMapper = $this->getCriteriaMapper();
        $cb = $criteriaMapper->getCriteriaBuilder();
        $q = $cb->createQuery();
        $r = $q->from('FooEntity')->alias('p');
        $p1 = $cb->parameter('integer','p1');
        $q->select($r)
            //->distinct(true)
            ->where($cb->and_(
                $cb->and_(
                    $cb->or_(
                        $cb->equal($r->get('field1'),$p1),
                        $cb->lt($r->get('field1_2'),$p1)
                    ),
                    $cb->ge($r->get('field2'),$p1)
                ),
                $cb->gt($r->get('field3'),$p1)
            ));

        $restriction = $q->getRestriction();
        $parameters = array();
        $result = $criteriaMapper->TestExtractDisjunction('FooEntity',$restriction,$parameters);
        $result = $criteriaMapper->TestPutTogetherAndRestriction($result);
        $this->assertEquals('OR',$result->getOperator());
        $expressions = $result->getExpressions();
        $this->assertCount(2,$expressions);
        $this->assertEquals('AND',$expressions[0]->getOperator());

        $subExpressions = $expressions[0]->getExpressions();
        $this->assertCount(3,$subExpressions);
        $this->assertEquals('EQUAL',$subExpressions[0]->getOperator());
        $this->assertEquals('GREATER_THAN_OR_EQUAL',$subExpressions[1]->getOperator());
        $this->assertEquals('GREATER_THAN',$subExpressions[2]->getOperator());

        $subExpressions = $expressions[1]->getExpressions();
        $this->assertCount(3,$subExpressions);
        $this->assertEquals('LESS_THAN',$subExpressions[0]->getOperator());
        $this->assertEquals('GREATER_THAN_OR_EQUAL',$subExpressions[1]->getOperator());
        $this->assertEquals('GREATER_THAN',$subExpressions[2]->getOperator());
    }

    public function testSwapParamenters()
    {
        $criteriaMapper = $this->getCriteriaMapper();
        $cb = $criteriaMapper->getCriteriaBuilder();
        $q = $cb->createQuery();
        $r = $q->from('FooEntity')->alias('p');
        $p1 = $cb->parameter('integer','p1');
        $q->select($r)
            //->distinct(true)
            ->where($cb->or_(
                $cb->equal($p1,$r->get('field1')),
                $cb->lt($p1,$r->get('field2')),
                $cb->le($p1,$r->get('field3')),
                $cb->gt($p1,$r->get('field4')),
                $cb->ge($p1,$r->get('field5'))
            ));

        $restriction = $q->getRestriction();
        $parameters = array();
        $result = $criteriaMapper->TestExtractDisjunction('FooEntity',$restriction,$parameters);
        $result = $criteriaMapper->TestPutTogetherAndRestriction($result);

        $this->assertEquals('OR',$result->getOperator());
        $expressions = $result->getExpressions();
        $this->assertCount(5,$expressions);
        $this->assertEquals('EQUAL',$expressions[0]->getOperator());
        $this->assertEquals('GREATER_THAN',$expressions[1]->getOperator());
        $this->assertEquals('GREATER_THAN_OR_EQUAL',$expressions[2]->getOperator());
        $this->assertEquals('LESS_THAN',$expressions[3]->getOperator());
        $this->assertEquals('LESS_THAN_OR_EQUAL',$expressions[4]->getOperator());
    }

    public function testNegatedOperator()
    {
        $criteriaMapper = $this->getCriteriaMapper();
        $cb = $criteriaMapper->getCriteriaBuilder();
        $q = $cb->createQuery();
        $r = $q->from('FooEntity')->alias('p');
        $p1 = $cb->parameter('integer','p1');
        $q->select($r)
            //->distinct(true)
            ->where($cb->or_(
                $cb->not($cb->equal($r->get('field1'),$p1)),
                $cb->not($cb->lt($r->get('field2'),$p1)),
                $cb->not($cb->le($r->get('field3'),$p1)),
                $cb->not($cb->gt($r->get('field4'),$p1)),
                $cb->not($cb->ge($r->get('field5'),$p1))
            ));

        $restriction = $q->getRestriction();
        $parameters = array();
        $result = $criteriaMapper->TestExtractDisjunction('FooEntity',$restriction,$parameters);
        $result = $criteriaMapper->TestPutTogetherAndRestriction($result);

        $this->assertEquals('OR',$result->getOperator());
        $expressions = $result->getExpressions();
        $this->assertCount(6,$expressions);
        $this->assertEquals('LESS_THAN',$expressions[0]->getOperator());
        $this->assertEquals('GREATER_THAN',$expressions[1]->getOperator());
        $this->assertEquals('GREATER_THAN_OR_EQUAL',$expressions[2]->getOperator());
        $this->assertEquals('GREATER_THAN',$expressions[3]->getOperator());
        $this->assertEquals('LESS_THAN_OR_EQUAL',$expressions[4]->getOperator());
        $this->assertEquals('LESS_THAN',$expressions[5]->getOperator());
    }

    public function testPrepare()
    {
        $criteriaMapper = $this->getCriteriaMapper();
        $cb = $criteriaMapper->getCriteriaBuilder();
        $q = $cb->createQuery();
        $r = $q->from('FooEntity')->alias('p');
        $p1 = $cb->parameter('integer','p1');
        $q->select($r)
            ->where(
                $cb->equal($r->get('field1'),$p1));

        $prepared = $criteriaMapper->prepare($q);
        //$this->assertEquals(array(),$prepared->getQueries());
        $queries = $prepared->getQueries();
        $this->assertCount(1,$queries);
        $this->assertCount(1,$queries[0]);
        $this->assertInstanceOf('Rindow\Persistence\Orm\Criteria\ComparisonOperator',$queries[0][0]);
        $this->assertEquals('EQUAL',$queries[0][0]->getOperator());
        $expressions = $queries[0][0]->getExpressions();
        $this->assertInstanceOf('Rindow\Persistence\Orm\Criteria\Path',$expressions[0]);
        $this->assertEquals('field1',$expressions[0]->getNodeName());
        $this->assertInstanceOf('Rindow\Persistence\Orm\Criteria\Parameter',$expressions[1]);
        $this->assertEquals('p1',$expressions[1]->getName());
        $this->assertEquals('integer',$expressions[1]->getParameterType());

        $parameters = $prepared->getParameters();
        $this->assertCount(1,$parameters);
        $this->assertInstanceOf('Rindow\Persistence\Orm\Criteria\Parameter',$parameters['p1'][0]);
        $this->assertEquals('p1',$parameters['p1'][0]->getName());
    }

    public function testFindAll()
    {
        $criteriaMapper = $this->getCriteriaMapper();
        $cb = $criteriaMapper->getCriteriaBuilder();
        $q = $cb->createQuery();
        $r = $q->from('FooEntity')->alias('p');
        $q->select($r);

        $prepared = $criteriaMapper->prepare($q);
        $this->assertEquals(array(),$prepared->getQueries());
    }

    public function testBuilder1()
    {
        if(self::$runGroup != null && self::$runGroup!='a') {
            $this->markTestSkipped();
            return;
        }
        $cb = new CriteriaBuilder();

        $q = $cb->createQuery('FooResult');
        $this->assertInstanceOf('Rindow\Persistence\Orm\Criteria\CriteriaQuery',$q);

        $r = $q->from('FooEntity')->alias('p');
        $p1 = $cb->parameter('integer','p1');
        $q->select($r)
            //->distinct(true)
            ->where($cb->and_(
                $cb->gt($r->get('field1'),$p1),
                $cb->le($p1,$r->get('field1_2'))))
            ->orderBy(
                $cb->desc($r->get('field2')),
                $r->get('field2_2'));
            //->groupBy(
            //    $r->get('field3'),
            //    $r->get('field3_2'));
            //->having($cb->gt($r->get('field4'),100));

        $className = 'FooEntity';
        $mapperName = __NAMESPACE__.'\TestMapper';
        $sm = new Container();
        $sm->setInstance($mapperName,new TestMapper());
        $em = new EntityManager();
        $em->setServiceLocator($sm);
        $em->registerMapper($className, $mapperName);
        $criteriaMapper = new CriteriaMapper();
        $criteriaMapper->setContext($em);

        $preparedCriteria = $criteriaMapper->prepare($q);


        $this->assertInstanceof('Rindow\Module\Google\Cloud\Persistence\Orm\PreparedCriteria',
                                $preparedCriteria);
        //$query = $preparedCriteria->getQuery();
        //$this->assertInstanceof('Rindow\Module\Google\Cloud\Datastore\Query',$query);

        $this->assertEquals('testtable',$preparedCriteria->getKind());
        $this->assertEquals(array(),$preparedCriteria->getDistincts());

        $parameters = $preparedCriteria->getParameters();
        $queries = $preparedCriteria->getQueries();
        $this->assertCount(1,$queries);
        $this->assertInstanceof('Rindow\Persistence\Orm\Criteria\ComparisonOperator',$queries[0]);
        $this->assertEquals('AND',$queries[0]->getOperator());
        $filters = $queries[0]->getExpressions();
        $this->assertCount(2,$filters);
        $filter = $filters[0];
        $this->assertInstanceof('Rindow\Persistence\Orm\Criteria\ComparisonOperator',$filter);
        $this->assertEquals('GREATER_THAN',$filter->getOperator());
        $expressions = $filter->getExpressions();
        $this->assertEquals('field1',$expressions[0]->getNodeName());
        $this->assertEquals('p1',$expressions[1]->getName());
        $this->assertEquals(spl_object_hash($expressions[1]),spl_object_hash($parameters['p1'][0]));

        $filter = $filters[1];
        $this->assertInstanceof('Rindow\Persistence\Orm\Criteria\ComparisonOperator',$filter);
        $this->assertEquals('GREATER_THAN_OR_EQUAL',$filter->getOperator());
        $expressions = $filter->getExpressions();
        $this->assertEquals('field1_2',$expressions[0]->getNodeName());
        $this->assertEquals('p1',$expressions[1]->getName());
        $this->assertEquals(spl_object_hash($expressions[1]),spl_object_hash($parameters['p1'][1]));

        $sortList = $preparedCriteria->getOrders();
        $this->assertCount(2,$sortList);

        $this->assertEquals(array('field2','field2_2'),array_keys($sortList));
        $this->assertEquals('DESCENDING',$sortList['field2']);
        $this->assertEquals('ASCENDING',$sortList['field2_2']);

        $this->assertEquals(array(),$preparedCriteria->getProjection());
    }

    public function testBuilder2()
    {
        if(self::$runGroup != null && self::$runGroup!='a') {
            $this->markTestSkipped();
            return;
        }
        $cb = new CriteriaBuilder();

        $q = $cb->createQuery('FooResult');
        $this->assertInstanceOf('Rindow\Persistence\Orm\Criteria\CriteriaQuery',$q);

        $r = $q->from('FooEntity')->alias('p');
        $p1 = $cb->parameter('integer','p1');
        $q->distinct(true)
            ->multiselect(
                $r->get('field3'),
                $r->get('field3_2')
            )
            //->where($cb->and_(
            //    $cb->gt($r->get('field1'),$p1),
            //    $cb->le($p1,$r->get('field1_2'))))
            //->orderBy(
            //    $cb->desc($r->get('field2')),
            //    $r->get('field2_2'));
            ->groupBy(
                $r->get('field3')
            );
            //->having($cb->gt($r->get('field4'),100));

        $className = 'FooEntity';
        $mapperName = __NAMESPACE__.'\TestMapper';
        $sm = new Container();
        $sm->setInstance($mapperName,new TestMapper());
        $em = new EntityManager();
        $em->setServiceLocator($sm);
        $em->registerMapper($className, $mapperName);
        $criteriaMapper = new CriteriaMapper();
        $criteriaMapper->setContext($em);
        $preparedCriteria = $criteriaMapper->prepare($q);
        $this->assertInstanceof('Rindow\Module\Google\Cloud\Persistence\Orm\PreparedCriteria',
                                $preparedCriteria);
        //$query = $preparedCriteria->getQuery();
        //$this->assertInstanceof('Rindow\Module\Google\Cloud\Datastore\Query',$query);

        $this->assertEquals('testtable',$preparedCriteria->getKind());
        $this->assertEquals(array('field3'=>true),$preparedCriteria->getDistincts());

        $parameters = $preparedCriteria->getParameters();
        $this->assertEquals(array(),$parameters);
        $queries = $preparedCriteria->getQueries();
        $this->assertEquals(array(),$queries);

        $sortList = $preparedCriteria->getOrders();
        $this->assertEquals(array(),$sortList);

        $projections = $preparedCriteria->getProjection();
        $this->assertEquals(array('field3','field3_2'),$projections);
    }

    public function testQueryCount()
    {
        if(self::$runGroup != null && self::$runGroup!='a') {
            $this->markTestSkipped();
            return;
        }
        $datastore = $this->getClient();
        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat1');
        $datastore->upsert($entity);
        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat2');
        $datastore->upsert($entity);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $cb = $mm->getServiceLocator()->get('CriteriaBuilder');

        $q = $cb->createQuery();
        $root = $q->from(__NAMESPACE__.'\Category');
        $q->select($cb->count($root));

        $query = $em->createQuery($q);
        $result = $query->getSingleResult();
        $this->assertEquals(2,$result);

        $em->close();
    }

    public function testCriteriaQueryWhere()
    {
        if(self::$runGroup != null && self::$runGroup!='a') {
            $this->markTestSkipped();
            return;
        }
        $datastore = $this->getClient();
        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat1');
        $datastore->upsert($entity);
        $catKey = $entity->key();

        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat2');
        $datastore->upsert($entity);
        $catKey2 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod1');
        $datastore->upsert($entity);
        $prodKey = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey2);
        $entity->setProperty('name','prod2');
        $datastore->upsert($entity);
        $prodKey2 = $entity->key();

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey2);
        $entity->setProperty('color',3);
        $datastore->upsert($entity);

        unset($datastore);
        // Must wait to reflect
        sleep(self::WAIT);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $cb = $mm->getServiceLocator()->get('CriteriaBuilder');

        $q = $cb->createQuery();
        $root = $q->from(__NAMESPACE__.'\Product');
        $p = $cb->parameter(null,'p');
        $q->select($root)
            ->where($cb->equal($root->get('category'),$p));
        $query = $em->createQuery($q);
        $query->setParameter($p,$catKey2);
        $results = $query->getResultList();
        $count = 0;
        foreach ($results as $product) {
            $this->assertEquals($prodKey2,$product->id);
            $this->assertEquals('prod2',$product->name);
            $this->assertEquals($catKey2,$product->category->id);
            $this->assertEquals('cat2',$product->category->name);
            $colorCount=0;
            foreach ($product->colors as $color) {
                $this->assertEquals(3,$color->color);
                $colorCount++;
            }
            $this->assertEquals(1,$colorCount);
            $count++;
        }
        $this->assertEquals(1,$count);

        $em->close();
    }

    public function testCriteriaQueryAncestor1()
    {
        if(self::$runGroup != null && self::$runGroup!='b') {
            $this->markTestSkipped();
            return;
        }
        $datastore = $this->getClient();
        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat1');
        $datastore->upsert($entity);
        $catKey = $entity->key();

        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat2');
        $datastore->upsert($entity);
        $catKey2 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->key()->ancestorKey($catKey);

        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod1');
        $datastore->upsert($entity);
        $prodKey = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->key()->ancestorKey($catKey2);
        $entity->setProperty('category',$catKey2);
        $entity->setProperty('name','prod2');
        $datastore->upsert($entity);
        $prodKey2 = $entity->key();

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey2);
        $entity->setProperty('color',3);
        $datastore->upsert($entity);

        unset($datastore);
        // Must wait to reflect
        sleep(self::WAIT);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $cb = $mm->getServiceLocator()->get('CriteriaBuilder');

        $q = $cb->createQuery();
        $root = $q->from(__NAMESPACE__.'\Product');
        $p = $cb->parameter(null,'p');
        $q->select($root)
            ->where($cb->equal($root->get('__ancestor'),$p));
        $query = $em->createQuery($q);
        $query->setParameter($p,$catKey2);
        $results = $query->getResultList();
        $count = 0;
        foreach ($results as $product) {
            $this->assertEquals($prodKey2,$product->id);
            $this->assertEquals('prod2',$product->name);
            $this->assertEquals($catKey2,$product->category->id);
            $this->assertEquals('cat2',$product->category->name);
            $count++;
        }
        $this->assertEquals(1,$count);

        $em->close();
    }

    public function testCriteriaQueryAncestor2()
    {
        if(self::$runGroup != null && self::$runGroup!='b') {
            $this->markTestSkipped();
            return;
        }
        $datastore = $this->getClient();
        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat1');
        $datastore->upsert($entity);
        $catKey = $entity->key();

        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat2');
        $datastore->upsert($entity);
        $catKey2 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->key()->ancestorKey($catKey);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod1');
        $datastore->upsert($entity);
        $prodKey = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->key()->ancestorKey($catKey2);
        $entity->setProperty('category',$catKey2);
        $entity->setProperty('name','prod2');
        $datastore->upsert($entity);
        $prodKey2 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->key()->ancestorKey($catKey2);
        $entity->setProperty('category',$catKey2);
        $entity->setProperty('name','prod3');
        $datastore->upsert($entity);
        $prodKey3 = $entity->key();

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey2);
        $entity->setProperty('color',3);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey3);
        $entity->setProperty('color',4);
        $datastore->upsert($entity);

        unset($datastore);
        // Must wait to reflect
        sleep(self::WAIT);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $cb = $mm->getServiceLocator()->get('CriteriaBuilder');

        $q = $cb->createQuery();
        $root = $q->from(__NAMESPACE__.'\Product');
        $p = $cb->parameter(null,'p');
        $p2 = $cb->parameter(null,'p2');
        $q->select($root)
            ->where($cb->and_(
                $cb->equal($root->get('__ancestor'),$p),
                $cb->equal($root->get('name'),$p2)
            ));
        $query = $em->createQuery($q);
        $query->setParameter($p,$catKey2);
        $query->setParameter($p2,'prod3');
        $results = $query->getResultList();
        $count = 0;
        foreach ($results as $product) {
            $this->assertEquals($prodKey3,$product->id);
            $this->assertEquals('prod3',$product->name);
            $this->assertEquals($catKey2,$product->category->id);
            $this->assertEquals('cat2',$product->category->name);
            $count++;
        }
        $this->assertEquals(1,$count);

        $em->close();
    }

    public function testCriteriaQueryAncestor3()
    {
        if(self::$runGroup != null && self::$runGroup!='b') {
            $this->markTestSkipped();
            return;
        }
        $datastore = $this->getClient();
        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat1');
        $datastore->upsert($entity);
        $catKey = $entity->key();

        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat2');
        $datastore->upsert($entity);
        $catKey2 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->key()->ancestorKey($catKey);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod1');
        $datastore->upsert($entity);
        $prodKey = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->key()->ancestorKey($catKey2);
        $entity->setProperty('category',$catKey2);
        $entity->setProperty('name','prod2');
        $datastore->upsert($entity);
        $prodKey2 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->key()->ancestorKey($catKey2);
        $entity->setProperty('category',$catKey2);
        $entity->setProperty('name','prod3');
        $datastore->upsert($entity);
        $prodKey3 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->key()->ancestorKey($catKey2);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod3');
        $datastore->upsert($entity);
        $prodKey4 = $entity->key();

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey2);
        $entity->setProperty('color',3);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey3);
        $entity->setProperty('color',4);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey4);
        $entity->setProperty('color',5);
        $datastore->upsert($entity);

        unset($datastore);
        // Must wait to reflect
        sleep(self::WAIT);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $cb = $mm->getServiceLocator()->get('CriteriaBuilder');

        $q = $cb->createQuery();
        $root = $q->from(__NAMESPACE__.'\Product');
        $p = $cb->parameter(null,'p');
        $p2 = $cb->parameter(null,'p2');
        $p3 = $cb->parameter(null,'p3');
        $q->select($root)
            ->where($cb->and_(
                $cb->equal($root->get('__ancestor'),$p),
                $cb->equal($root->get('name'),$p2),
                $cb->equal($root->get('category'),$p3)
            ));
        $query = $em->createQuery($q);
        $query->setParameter($p,$catKey2);
        $query->setParameter($p2,'prod3');
        $query->setParameter($p3,$catKey2);
        $results = $query->getResultList();
        $count = 0;
        foreach ($results as $product) {
            $this->assertEquals($prodKey3,$product->id);
            $this->assertEquals('prod3',$product->name);
            $this->assertEquals($catKey2,$product->category->id);
            $this->assertEquals('cat2',$product->category->name);
            $count++;
        }
        $this->assertEquals(1,$count);

        $em->close();
    }

    public function testCriteriaQueryGroupBy()
    {
        if(self::$runGroup != null && self::$runGroup!='c') {
            $this->markTestSkipped();
            return;
        }
        //if(!isset($_SERVER['APPLICATION_ID'])) {
        //    $this->markTestSkipped('Teststore does not support groupBy');
        //}
        /**
        *    "having" is not supported on Google Cloud Datastore.
        */
        $datastore = $this->getClient();

        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat1');
        $datastore->upsert($entity);
        $catKey = $entity->key();

        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat2');
        $datastore->upsert($entity);
        $catKey2 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod1');
        $datastore->upsert($entity);
        $prodKey = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey2);
        $entity->setProperty('name','prod2');
        $datastore->upsert($entity);
        $prodKey2 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey2);
        $entity->setProperty('name','prod3');
        $datastore->upsert($entity);
        $prodKey3 = $entity->key();

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey2);
        $entity->setProperty('color',3);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey3);
        $entity->setProperty('color',4);
        $datastore->upsert($entity);

        unset($datastore);
        // Must wait to reflect
        sleep(self::WAIT);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $cb = $mm->getServiceLocator()->get('CriteriaBuilder');

        $q = $cb->createQuery();
        $root = $q->from(__NAMESPACE__.'\Product');
        $p = $cb->parameter(null,'p');
        $q->multiselect(
                $root->get('category')->alias('category'),
                $root->get('name')->alias('name')
            )
            ->groupBy($root->get('category'));

        $query = $em->createQuery($q);
        $results = $query->getResultList();
        $countCat1 = $countCat2 = 0;
        foreach ($results as $row) {
            if($row['category'] == $catKey) {
                $this->assertEquals('prod1',$row['name']);
                $countCat1++;
            } elseif ($row['category'] == $catKey2) {
                if($row['name']=='prod2') {
                    $this->assertEquals('prod2',$row['name']);
                    $countCat2++;
                } elseif ($row['name']=='prod3') {
                    $this->assertEquals('prod3',$row['name']);
                    $countCat2++;
                }
            }
        }
        $this->assertEquals(1,$countCat1);
        $this->assertEquals(1,$countCat2);

        $em->close();
    }

    public function testCriteriaQueryJoin()
    {
        $this->markTestSkipped('"join" is not supported on Google Cloud Datastore.');
        if(self::$runGroup != null && self::$runGroup!='c') {
            $this->markTestSkipped();
            return;
        }
        /**
        *    "join" is not supported on Google Cloud Datastore.
        */

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $cb = $mm->getServiceLocator()->get('CriteriaBuilder');

        $q = $cb->createQuery();
        $product = $q->from(__NAMESPACE__.'\Product');
        $p = $cb->parameter(null,'p');
        $category = $product->join('category');
        $category->on($cb->ge($category->get('name'),'cat2'));
        $q->select($product);

        $query = $em->createQuery($q);
        $results = $query->getResultList();
        $count = 0;
        foreach ($results as $prod) {
            if($count==0)
                $this->assertEquals('prod2',$prod->name);
            else
                $this->assertEquals('prod3',$prod->name);
            $count++;
        }
        $this->assertEquals(2,$count);

        $em->close();
    }

    public function testPaginator()
    {
        if(self::$runGroup != null && self::$runGroup!='c') {
            $this->markTestSkipped();
            return;
        }
        $datastore = $this->getClient();

        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat1');
        $datastore->upsert($entity);
        $catKey = $entity->key();

        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat2');
        $datastore->upsert($entity);
        $catKey2 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey2);
        $entity->setProperty('name','prod1');
        $datastore->upsert($entity);
        $prodKey = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod2');
        $datastore->upsert($entity);
        $prodKey2 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod3');
        $datastore->upsert($entity);
        $prodKey3 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod4');
        $datastore->upsert($entity);
        $prodKey4 = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod5');
        $datastore->upsert($entity);
        $prodKey5 = $entity->key();

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey2);
        $entity->setProperty('color',3);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey3);
        $entity->setProperty('color',1);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey4);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey5);
        $entity->setProperty('color',3);
        $datastore->upsert($entity);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $cb = $mm->getServiceLocator()->get('CriteriaBuilder');
        $paginatorFactory = $mm->getServiceLocator()->get('PaginatorFactory');

        /* page 1  */

        $q = $cb->createQuery();
        $root = $q->from(__NAMESPACE__.'\Product');
        $p = $cb->parameter();
        $q->select($root)
            ->where($cb->equal($root->get('category'),$p));
        $query = $em->createQuery($q);
        $query->setParameter($p,$catKey);

        $paginator = $paginatorFactory->createPaginator($query);
        $paginator->setItemMaxPerPage(3);
        $paginator->setPage(1);
        $count = $count2 = $count3 = $count4 = $count5 = 0;
        foreach ($paginator as $product) {
            if($prodKey2 == $product->id) {
                $count2++;
            } elseif($prodKey3 == $product->id) {
                $count3++;
            } elseif($prodKey4 == $product->id) {
                $count4++;
            } elseif($prodKey5 == $product->id) {
                $count5++;
            } else {
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(3,$count);
        $this->assertLessThanOrEqual(1,$count2);
        $this->assertLessThanOrEqual(1,$count3);
        $this->assertLessThanOrEqual(1,$count4);
        $this->assertLessThanOrEqual(1,$count5);

        /* page 2  */

        $q = $cb->createQuery();
        $root = $q->from(__NAMESPACE__.'\Product');
        $p = $cb->parameter();
        $q->select($root)
            ->where($cb->equal($root->get('category'),$p));
        $query = $em->createQuery($q);
        $query->setParameter($p,$catKey);

        $paginator = $paginatorFactory->createPaginator($query);
        $paginator->setItemMaxPerPage(3);
        $paginator->setPage(2);
        $count = $count2 = $count3 = $count4 = $count5 = 0;
        foreach ($paginator as $product) {
            if($prodKey2 == $product->id) {
                $count2++;
            } elseif($prodKey3 == $product->id) {
                $count3++;
            } elseif($prodKey4 == $product->id) {
                $count4++;
            } elseif($prodKey5 == $product->id) {
                $count5++;
            } else {
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);
        $this->assertLessThanOrEqual(1,$count2);
        $this->assertLessThanOrEqual(1,$count3);
        $this->assertLessThanOrEqual(1,$count4);
        $this->assertLessThanOrEqual(1,$count5);

        $em->close();

    }
}