<?php
date_default_timezone_set('UTC');
$loader = require 'init_autoloader.php';
$loader->add('Acme\\', __DIR__.'/src');
define('RINDOWTEST_LOG_DIR',__DIR__.'/log');
//putenv('APPLICATION_ID=dev~dummy_app_id');
//putenv('APPLICATION_ID=dev~phpdatastore');
