<?php

declare(strict_types=1);

namespace Hamlet\Database\PDO;

use Hamlet\Database\Procedure;
use Hamlet\Database\Session;
use PDO;

/**
 * @extends Session<PDO>
 */
class PDOSession extends Session
{
    /**
     * @param PDO $handle
     */
    public function __construct($handle)
    {
        parent::__construct($handle);
    }

    /**
     * @param string $query
     * @return Procedure
     */
    public function prepare(string $query): Procedure
    {
        $procedure = new PDOProcedure($this->handle, $query);
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
            throw PDODatabase::exception($connection);
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
            throw PDODatabase::exception($connection);
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
            throw PDODatabase::exception($connection);
        }
    }
}
