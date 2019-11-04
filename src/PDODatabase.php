<?php

declare(strict_types=1);

namespace Hamlet\Database\PDO;

use Hamlet\Database\Database;
use Hamlet\Database\DatabaseException;
use Hamlet\Database\Procedure;
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

    public function prepare(string $query): Procedure
    {
        $procedure = new PDOProcedure($this->executor(), $query);
        $procedure->setLogger($this->logger);
        return $procedure;
    }

    /**
     * @param PDO $connection
     * @return void
     */
    protected function startTransaction($connection)
    {
        $this->logger->debug('Starting transaction');
        $success = $connection->beginTransaction();
        if (!$success) {
            throw self::exception($connection);
        }
    }

    /**
     * @param PDO $connection
     * @return void
     */
    protected function commit($connection)
    {
        $this->logger->debug('Committing transaction');
        $success = $connection->commit();
        if (!$success) {
            throw self::exception($connection);
        }
    }

    /**
     * @param PDO $connection
     * @return void
     */
    protected function rollback($connection)
    {
        $this->logger->debug('Rolling back transaction');
        $success = $connection->rollback();
        if (!$success) {
            throw self::exception($connection);
        }
    }

    public static function exception(PDO $connection): DatabaseException
    {
        $info = $connection->errorInfo();
        return new DatabaseException((string) ($info[2] ?? 'Unknown error'), (int) ($info[1] ?? -1));
    }
}
