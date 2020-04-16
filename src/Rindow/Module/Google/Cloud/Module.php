<?php
namespace Rindow\Module\Google\Cloud;

class Module
{
    public function getConfig()
    {
        return array(
            // *** MUST INJECTION FOLLOWING MODULE_MANAGER SETTING
            //'module_manager'=>array(
            //    'configCacheFactoryClass'=>'Rindow\\Module\\Google\\Cloud\\System\\ServiceFactory',
            //),
            'aop' => array(
                // When you use GAE standard mode for php72, 
                // it must enable cache path to /sys_get_temp_dir/cache directory.
                // If you want to use GAE flex mode, No special settings needed.
                'codeLoadingMode' => 'file',
                'cacheFilePath' => sys_get_temp_dir().'/cache',
            ),
            'container' => array(
                'aliases' => array(
                    'Rindow\\Persistence\\OrmShell\\DefaultCriteriaMapper' => 'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultCriteriaMapper',
                    //'Rindow\\Persistence\\OrmShell\\DefaultResource'       => 'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultResource',
                    'Rindow\\Persistence\\OrmShell\\Transaction\\DefaultTransactionSynchronizationRegistry' => 'dummy',
                    'Rindow\\Module\\Google\\Cloud\\Datastore\\DefaultServiceFactory' => 'ConfigCacheFactory',
                ),
                'components' => array(
                    // Adjust UrlGenerator
                    'Rindow\\Web\\Mvc\\Util\\DefaultUrlGenerator' => array(
                        'properties' => array(
                            'scriptNames' => array('config' => 'googleAppEngine::scriptNames'),
                        ),
                    ),
                    // Datastore
                    'Rindow\\Module\\Google\\Cloud\\Datastore\\DefaultDataSource' => array(
                        'class' => 'Rindow\\Module\\Google\\Cloud\\Resource\\DataSource',
                        'properties' => array(
                            'serviceFactory' => array('ref'=>'Rindow\\Module\\Google\\Cloud\\Datastore\\DefaultServiceFactory'),
                        ),
                    ),

                    // Datastore ORM
                    'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultAbstractMapper' => array(
                        'properties' => array(
                            'dataSource' => array('ref'=>'Rindow\\Module\\Google\\Cloud\\Datastore\\DefaultDataSource'),
                            'hydrator' => array('ref'=>'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultSetterHydrator'),
                        ),
                    ),
                    'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultCriteriaMapper' => array(
                        'class' => 'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\CriteriaMapper',
                    ),
                    //'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultResource' => array(
                    //    'class' => 'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\Resource',
                    //    'properties' => array(
                    //        'datastore' => array('ref'=>'Rindow\\Module\\Google\\Cloud\\Api\\Datastore\\DefaultDatastore'),
                    //        'hydrator' => array('ref'=>'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultSetterHydrator'),
                    //    ),
                    //),
                    'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultSetterHydrator' => array(
                        'class'=>'Rindow\\Stdlib\\Entity\\SetterHydrator',
                    ),
                ),
            ),
        );
    }
}
