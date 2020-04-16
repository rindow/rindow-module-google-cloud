<?php
namespace Rindow\Module\Google\Cloud\Transaction\Exception;

use Interop\Lenient\Transaction\Exception\IllegalStateException as IllegalStateExceptionInterface;
use Exception;

class IllegalStateException extends Exception implements IllegalStateExceptionInterface
{}
