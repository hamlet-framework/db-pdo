<?php

declare(strict_types=1);

namespace Hamlet\Database\PDO;

use Hamlet\Database\Database;
use Hamlet\Database\DatabaseException;
use Hamlet\Database\Session;
use Hamlet\Database\SimpleConnectionPool;
use PDO;

/**
 * @extends Database<PDO>
 */
class PDODatabase extends Database
{
    public function __construct(string $dsn, string $user = null, string $password = null)
    {
        parent::__construct(new SimpleConnectionPool(
            function () use ($dsn, $user, $password): PDO {
                return new PDO($dsn, $user, $password, [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
                ]);
            }
        ));
    }

    public static function exception(PDO $connection): DatabaseException
    {
        $info = $connection->errorInfo();
        return new DatabaseException((string) ($info[2] ?? 'Unknown error'), (int) ($info[1] ?? -1));
    }

    /**
     * @param mixed $handle
     * @psalm-param PDO $handle
     * @return Session
     * @psalm-return Session<PDO>
     */
    protected function createSession($handle): Session
    {
        $session = new PDOSession($handle);
        $session->setLogger($this->logger);
        return $session;
    }
}
