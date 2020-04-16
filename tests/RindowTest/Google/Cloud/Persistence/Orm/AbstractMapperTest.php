<?php
namespace RindowTest\Google\Cloud\Persistence\Orm\AbstractMapperTest;

use PHPUnit\Framework\TestCase;
use Rindow\Stdlib\Entity\AbstractEntity;
use Rindow\Stdlib\Entity\PropertyAccessPolicy;
use Rindow\Container\ModuleManager;
use Rindow\Persistence\OrmShell\EntityManager;
use Rindow\Persistence\OrmShell\DataMapper;
use Rindow\Database\Dao\Support\Parameter;

use Rindow\Module\Google\Cloud\Persistence\Orm\AbstractMapper;
use Google\Cloud\Core\ServiceBuilder;

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
    const CLASS_NAME = 'RindowTest\Google\Cloud\Persistence\Orm\AbstractMapperTest\Category';
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
    const CLASS_NAME = 'RindowTest\Google\Cloud\Persistence\Orm\AbstractMapperTest\Color';
    const TABLE_NAME = 'color';
    const PRIMARYKEY = 'id';

    const CLASS_PRODUCT = 'RindowTest\Google\Cloud\Persistence\Orm\AbstractMapperTest\Product';

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
    const CLASS_NAME = 'RindowTest\Google\Cloud\Persistence\Orm\AbstractMapperTest\Product';
    const TABLE_NAME = 'product';
    const PRIMARYKEY = 'id';

    const CLASS_CATEGORY = 'RindowTest\Google\Cloud\Persistence\Orm\AbstractMapperTest\Category';
    const CLASS_COLOR    = 'RindowTest\Google\Cloud\Persistence\Orm\AbstractMapperTest\Color';

    protected $categoryRepository;
    protected $colorRepository;
    protected $mappingClasses = array(
        'category' => self::CLASS_CATEGORY,
    );
    protected $criteriaBuilder;
    protected $namedQuerys;

    public function setCriteriaBuilder($criteriaBuilder)
    {
        $this->criteriaBuilder = $criteriaBuilder;
    }

    protected function namedQueryBuilders()
    {
        if($this->namedQuerys)
            return $this->namedQuerys;
        $datastore = $this->getDatastore();
        $this->namedQuerys = array(
            'product.by.category' => function($params,$firstPosition,$maxResult) use($datastore) {
                $query = $datastore->query()->kind(ProductMapper::TABLE_NAME);
                $p1 = new Parameter('category');
                $query->filter('category','=',$params['category']);
                $query->order('name');
                if($maxResult)
                    $query->limit($maxResult);
                if($firstPosition)
                    $query->offset($firstPosition);
                return $query;
            },
        );
        return $this->namedQuerys;
    }

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
    public static function setUpBeforeClass()
    {
    }
    public static function tearDownAfterClass()
    {
    }

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
                        //'parent' => 'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultAbstractMapper',
                        'properties' => array(
                            'criteriaBuilder' => array('ref'=>'Rindow\\Persistence\\OrmShell\\DefaultCriteriaBuilder'),
                        ),
                    ),
                    __NAMESPACE__.'\CategoryMapper'=>array(
                        'parent' => __NAMESPACE__.'\\DefaultAbstractMapper',
                        //'parent' => 'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultAbstractMapper',
                    ),
                    __NAMESPACE__.'\ColorMapper'=>array(
                        'parent' => __NAMESPACE__.'\\DefaultAbstractMapper',
                        //'parent' => 'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultAbstractMapper',
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

    public function testSimpleCreateReadUpdateDelete()
    {
        if(self::$runGroup != null && self::$runGroup!='a') {
            $this->markTestSkipped();
            return;
        }
        /* Create */
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');

        $category = new Category();
        $category->name = 'test';
        $em->persist($category);
        $em->flush();
        $em->close();
        $key = $category->id;
        $this->assertNotNull($category->id);
        // Must wait to reflect
        sleep(self::WAIT);

        $smt = $this->findAll(CategoryMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $this->assertEquals('test',$row->getProperty('name'));
            $this->assertEquals(strval($key),strval($row->key()));
            $count++;
        }
        $this->assertEquals(1,$count);
        unset($smt);

        /* Read */
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $category = $em->find(__NAMESPACE__.'\Category', $key);
        $this->assertEquals(strval($key),strval($category->id));
        $this->assertEquals('test',$category->name);

        /* Update */
        $category->name ='updated';
        $em->flush();
        // Must wait to reflect
        sleep(self::WAIT);

        $smt = $this->findAll(CategoryMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $this->assertEquals('updated',$row->getProperty('name'));
            $count++;
        }
        $this->assertEquals(1,$count);
        unset($smt);

        /* Delete */
        $em->remove($category);
        $em->flush();
        // Must wait to reflect
        sleep(self::WAIT);

        $smt = $this->findAll(CategoryMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(0,$count);
        unset($smt);
        $em->close();
    }


    public function testSubsidiaryPersistOnCreate1()
    {
        if(self::$runGroup != null && self::$runGroup!='a') {
            $this->markTestSkipped();
            return;
        }
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $datastore = $mm->getServiceLocator()->get('ConfigCacheFactory')->getDatastore();

        $category = new Category();
        $catkey = $category->id = $datastore->key(CategoryMapper::TABLE_NAME,'testcat1');
        $category->name = 'cat1';

        $product = new Product();
        $product->name = 'prod1';
        $product->category = $category;
        $product->addColor(2);
        $product->addColor(3);
        $em->persist($product);
        $em->flush();

        $protkey = $product->id;
        $this->assertNotNull($product->id);
        // Must wait to reflect
        sleep(self::WAIT);

        $smt = $this->findAll(ProductMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $this->assertEquals('prod1',$row->getProperty('name'));
            $this->assertEquals(strval($catkey),strval($row->getProperty('category')));
            $this->assertEquals(strval($protkey),strval($row->key()));
            $count++;
        }
        $this->assertEquals(1,$count);

        $smt = $this->findAll(CategoryMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(0,$count);
        $smt = $this->findAll(ColorMapper::TABLE_NAME);
        $count2 = $count3 = 0;
        foreach($smt as $row) {
            if(2 == $row->getProperty('color')) {
                $this->assertEquals(strval($protkey),strval($row->getProperty('product')));
                $count2++;
            } elseif(3 == $row->getProperty('color')) {
                $this->assertEquals(strval($protkey),strval($row->getProperty('product')));
                $count3++;
            } else {
                $this->assertTrue(false);
            }
        }
        $this->assertEquals(1,$count2);
        $this->assertEquals(1,$count3);
        unset($smt);

        $em->close();
    }

    public function testSupplementEntity1()
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

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod1');
        $datastore->upsert($entity);
        $prodKey = $entity->key();

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',3);
        $datastore->upsert($entity);

        unset($datastore);
        // Must wait to reflect
        sleep(self::WAIT);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $product = $em->find(__NAMESPACE__.'\Product', $prodKey);
        $this->assertEquals(strval($prodKey),strval($product->id));
        $this->assertEquals('prod1',$product->name);
        $this->assertEquals('cat1',$product->category->name);
        $colors = array();
        foreach ($product->colors as $color) {
            $colors[] = $color;
        }
        $this->assertCount(2,$colors);
        if(2 == $colors[0]->color)
            $color2idx = 0;
        if(2 == $colors[1]->color)
            $color2idx = 1;
        if(3 == $colors[0]->color)
            $color3idx = 0;
        if(3 == $colors[1]->color)
            $color3idx = 1;
        //$this->assertEquals(1,$colors[0]->id);
        $this->assertEquals(2,$colors[$color2idx]->color);
        $this->assertEquals(strval($prodKey),strval($colors[$color2idx]->product->id));
        //$this->assertEquals(2,$colors[1]->id);
        $this->assertEquals(3,$colors[$color3idx]->color);
        $this->assertEquals(strval($prodKey),strval($colors[$color3idx]->product->id));

        $em->close();
    }

    public function testSubsidiaryRemove1()
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

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod1');
        $datastore->upsert($entity);
        $prodKey = $entity->key();

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',3);
        $datastore->upsert($entity);

        unset($datastore);
        // Must wait to reflect
        sleep(self::WAIT);

        $smt = $this->findAll(CategoryMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(1,$count);
        $smt = $this->findAll(ProductMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(1,$count);
        $smt = $this->findAll(ColorMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(2,$count);

        // ========= Remove Product and cascated Colors ===========
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $product = $em->find(__NAMESPACE__.'\Product', $prodKey);
        $em->remove($product);
        $em->flush();
        // ========================================================

        // Must wait to reflect
        sleep(self::WAIT*2);

        $smt = $this->findAll(CategoryMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $this->assertEquals('cat1',$row->getProperty('name'));
            $count++;
        }
        $this->assertEquals(1,$count);
        $smt = $this->findAll(ColorMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(0,$count);
        $smt = $this->findAll(ProductMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(0,$count);

        $em->close();
    }

    public function testSubsidiaryUpdateAndCascadeRemove1()
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

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod1');
        $datastore->upsert($entity);
        $prodKey = $entity->key();

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',3);
        $datastore->upsert($entity);

        unset($datastore);
        // Must wait to reflect
        sleep(self::WAIT);

        $smt = $this->findAll(CategoryMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(1,$count);
        $smt = $this->findAll(ProductMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(1,$count);
        $smt = $this->findAll(ColorMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(2,$count);

        // ======= Update category name and Remove ColorId =========
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $product = $em->find(__NAMESPACE__.'\Product', $prodKey);
        $product->category->name = 'Updated';
        foreach ($product->colors as $idx => $color) {
            if($color->color == 3)
                unset($product->colors[$idx]);
        }
        $em->flush();
        // =========================================================
        // Must wait to reflect
        sleep(self::WAIT);

        $smt = $this->findAll(CategoryMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $this->assertEquals('Updated',$row->getProperty('name'));
            $count++;
        }
        $this->assertEquals(1,$count);
        $smt = $this->findAll(ColorMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $this->assertEquals(2,$row->getProperty('color'));
            $count++;
        }
        $this->assertEquals(1,$count);
        unset($smt);

        $em->close();
    }

    public function testSubsidiaryUpdateAndCascadePersist1()
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
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod1');
        $datastore->upsert($entity);
        $prodKey = $entity->key();

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',3);
        $datastore->upsert($entity);

        unset($datastore);
        // Must wait to reflect
        sleep(self::WAIT);

        $smt = $this->findAll(CategoryMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(2,$count);
        $smt = $this->findAll(ProductMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(1,$count);
        $smt = $this->findAll(ColorMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(2,$count);

        // ============== Change category and Add ColorId ==========
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $product = $em->find(__NAMESPACE__.'\Product', $prodKey);
        $category2 = $em->find(__NAMESPACE__.'\Category', $catKey2);
        $product->category = $category2;
        $product->addColor(4);
        $em->flush();
        // =========================================================
        // Must wait to reflect
        sleep(self::WAIT);

        $smt = $this->findAll(CategoryMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $count++;
        }
        $this->assertEquals(2,$count);
        $smt = $this->findAll(ProductMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            $this->assertEquals(strval($catKey2),$row->getProperty('category'));
            $count++;
        }
        $this->assertEquals(1,$count);
        $smt = $this->findAll(ColorMapper::TABLE_NAME);
        $count = 0;
        foreach($smt as $row) {
            if(2 == $row->getProperty('color')) {
                $count++;
            } elseif(3 == $row->getProperty('color')) {
                $count++;
            } elseif(4 == $row->getProperty('color')) {
                $count++;
                $this->assertEquals(strval($prodKey),strval($row->getProperty('product')));
            }
        }
        $this->assertEquals(3,$count);
        unset($smt);

        $em->close();
    }

    public function testNamedQuery()
    {
        $datastore = $this->getClient();
        $entity = $datastore->entity(CategoryMapper::TABLE_NAME);
        $entity->setProperty('name','cat1');
        $datastore->upsert($entity);
        $catKey = $entity->key();

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod1');
        $datastore->upsert($entity);
        $prodKey = $entity->key();

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prodKey);
        $entity->setProperty('color',3);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ProductMapper::TABLE_NAME);
        $entity->setProperty('category',$catKey);
        $entity->setProperty('name','prod2');
        $datastore->upsert($entity);
        $prod2Key = $entity->key();

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prod2Key);
        $entity->setProperty('color',1);
        $datastore->upsert($entity);

        $entity = $datastore->entity(ColorMapper::TABLE_NAME);
        $entity->setProperty('product',$prod2Key);
        $entity->setProperty('color',2);
        $datastore->upsert($entity);

        unset($datastore);
        // Must wait to reflect
        sleep(self::WAIT);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $dummy = new Product();
        $productClassName = get_class($dummy);
        $query = $em->createNamedQuery("product.by.category");
        //$this->assertEquals('SELECT * FROM product WHERE category = :category',$query->getPreparedCriteria()->getSql());
        $query->setParameter('category',$catKey);
        $results = $query->getResultList();
        $productCount = 0;
        foreach ($results as $product) {
            $this->assertInstanceOf(__NAMESPACE__.'\Product',  $product);
            $this->assertInstanceOf(__NAMESPACE__.'\Category', $product->category);
            $colorCount = 0;
            foreach ($product->colors as $color) {
                $this->assertInstanceOf(__NAMESPACE__.'\Color',$color);
                $colorCount++;
            }
            $this->assertEquals(2,$colorCount);
            $productCount++;
        }
        $this->assertEquals(2,$productCount);
        $em->close();
    }
}
