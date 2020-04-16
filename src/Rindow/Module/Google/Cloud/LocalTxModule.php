<?php
namespace Rindow\Module\Google\Cloud;

class LocalTxModule
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
                'plugins' => array(
                    'Rindow\\Transaction\\Support\\AnnotationHandler'=>'Rindow\\Transaction\\Support\\AnnotationHandler',
                ),
                'transaction' => array(
                    'defaultTransactionManager' => 'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultTransactionManager',
                    'managers' => array(
                        'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultTransactionManager' => array(
                            'transactionManager' => 'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultTransactionManager',
                            'advisorClass' => 'Rindow\\Transaction\\Support\\TransactionAdvisor',
                        ),
                    ),
                ),
                'intercept_to' => array(
                    'Rindow\\Module\\Google\\Cloud\\Repository\\GoogleCloudRepository' => true,
                ),
                'pointcuts' => array(
                    'Rindow\\Module\\Google\\Cloud\\Repository\\GoogleCloudRepository'=>
                        'execution(Rindow\\Module\\Google\\Cloud\\Repository\\GoogleCloudRepository::'.
                            '(save|findById|findAll|findOne|delete|deleteById|existsById|count)())',
                    'Rindow\\Module\\Google\\Cloud\\Repository\\GoogleCloudRepositoryInTransaction'=>
                        'execution(Rindow\\Module\\Google\\Cloud\\Repository\\GoogleCloudRepository::'.
                            '(save|delete|deleteById)())',
                ),
                'aspectOptions' => array(
                    'Rindow\\Transaction\\DefaultTransactionAdvisor' => array(
                        'advices' => array(
                            'required' => array(
                                'pointcut_ref' => array(
                                    'Rindow\\Module\\Google\\Cloud\\Repository\\GoogleCloudRepositoryInTransaction' => true,
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            'container' => array(
                'aliases' => array(
                    'Rindow\\Persistence\\OrmShell\\DefaultCriteriaMapper' => 'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultCriteriaMapper',
                    //'Rindow\\Persistence\\OrmShell\\DefaultResource'       => 'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultResource',
                    'Rindow\\Persistence\\OrmShell\\Transaction\\DefaultTransactionSynchronizationRegistry' => 'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultTransactionSynchronizationRegistry',
                    'Rindow\\Module\\Google\\Cloud\\Datastore\\DefaultServiceFactory' => 'ConfigCacheFactory',
                ),
                'components' => array(
                    //'Rindow\\Module\\Google\\Cloud\\Datastore\\DefaultDataSource' => array(
                    //    'class' => 'Rindow\\Module\\Google\\Cloud\\Resource\\DataSource',
                    //    'properties' => array(
                    //        'serviceFactory' => array('ref'=>'Rindow\\Module\\Google\\Cloud\\Datastore\\DefaultServiceFactory'),
                    //    ),
                    //),

                    // Adjust UrlGenerator
                    'Rindow\\Web\\Mvc\\Util\\DefaultUrlGenerator' => array(
                        'properties' => array(
                            'scriptNames' => array('config' => 'googleAppEngine::scriptNames'),
                        ),
                    ),
                    // Datastore Local Transaction
                    'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultDataSource' => array(
                        'class'=>'Rindow\\Module\\Google\\Cloud\\Transaction\\DataSource',
                        'properties' => array(
                            'transactionManager' => array('ref'=>'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultTransactionManager'),
                            'serviceFactory' => array('ref'=>'Rindow\\Module\\Google\\Cloud\\Datastore\\DefaultServiceFactory'),
                        ),
                        'proxy' => 'disable',
                    ),
                    'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultTransactionManager' => array(
                        'class' => 'Rindow\\Transaction\\Local\\TransactionManager',
                        'proxy' => 'disable',
                    ),
                    'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultTransactionSynchronizationRegistry' => array(
                        'class'=>'Rindow\\Transaction\\Support\\TransactionSynchronizationRegistry',
                        'properties' => array(
                            'transactionManager' => array('ref'=>'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultTransactionManager'),
                        ),
                        'proxy' => 'disable',
                    ),

                    // Datastore ORM
                    'Rindow\\Module\\Google\\Cloud\\Persistence\\Orm\\DefaultAbstractMapper' => array(
                        'properties' => array(
                            'dataSource' => array('ref'=>'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultDataSource'),
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

                    /*************
                     *  Repository
                     */
                    'Rindow\\Module\\Google\\Cloud\\Repository\\AbstractGoogleCloudRepository' => array(
                        'class' => 'Rindow\\Module\\Google\\Cloud\\Repository\\GoogleCloudRepository',
                        'properties' => array(
                            'dataSource' => array('ref'=>'Rindow\\Module\\Google\\Cloud\\Transaction\\DefaultDataSource'),
                            'queryBuilder' => array('ref' => 'Rindow\\Module\\Google\\Cloud\\DefaultQueryBuilder'),
                            // inject properties
                            // 'kindName' => array('value'=>'kind name of data store'),
                            // 'unindexed' => array('value' => array(
                            //     'field name' => true,
                            // )),
                            // 'unique'=>array('value'=>array(
                            //     'field1'=>true,
                            //     'field2'=>true,
                            // )),
                            // 'dataMapper' => array('ref' => 'Data Mapper component'),
                        ),
                    ),
                    'Rindow\\Module\\Google\\Cloud\\DefaultQueryBuilder' => array(
                        'class' => 'Rindow\\Database\\Dao\\Support\\QueryBuilder',
                    ),
                ),
            ),
        );
    }
}
