<?php
namespace Rindow\Module\Google\Cloud\System;

class Environment
{
    public static function applicationId()
    {
        $id = getenv('GAE_APPLICATION');
        if($id)
            return $id;
        return getenv('APPLICATION_ID');
    }

    public static function currentModuleId()
    {
        return getenv('CURRENT_MODULE_ID');
    }

    public static function currentVersionId()
    {
        $id = getenv('GAE_VERSION');
        if($id)
            return $id;
        $id = getenv('CURRENT_VERSION_ID');
        return substr($id, 0, strpos($id, '.'));
    }

    public static function currentFullVersionId()
    {
        $id = getenv('GAE_VERSION');
        if($id)
            return $id;
        return getenv('CURRENT_VERSION_ID');
    }

    public static function defaultVersionHostname()
    {
        return getenv('DEFAULT_VERSION_HOSTNAME');
    }
}
