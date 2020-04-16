<?php
namespace Rindow\Module\Google\Cloud\Transaction\Exception;

use Interop\Lenient\Transaction\Exception\NotSupportedException as NotSupportedExceptionInterface;
use Exception;

class NotSupportedException extends Exception implements NotSupportedExceptionInterface
{}
