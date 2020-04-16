<?php
namespace Rindow\Module\Google\Cloud\Persistence\Orm;

use Iterator;
use Interop\Lenient\Dao\Query\Cursor;

class ArrayCursor implements Cursor
{
    protected $data;

    public function __construct(array $data)
    {
        $this->data = $data;
    }

    public function fetch()
    {
        $value = current($this->data);
        next($this->data);
        return $value;
    }

    public function close()
    {
        $this->data = null;
    }
}