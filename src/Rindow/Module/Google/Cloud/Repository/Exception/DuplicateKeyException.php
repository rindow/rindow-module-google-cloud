<?php
namespace Rindow\Module\Google\Cloud\Repository\Exception;

use Interop\Lenient\Dao\Exception\DuplicateKeyException as DuplicateKeyExceptionInterface;

class DuplicateKeyException
extends \DomainException
implements DuplicateKeyExceptionInterface
{}
