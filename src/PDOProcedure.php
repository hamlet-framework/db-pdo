<?php

declare(strict_types=1);

namespace Hamlet\Database\PDO;

use Generator;
use Hamlet\Database\DatabaseException;
use Hamlet\Database\Procedure;
use Hamlet\Database\Traits\QueryExpanderTrait;
use PDO;
use PDOStatement;

class PDOProcedure extends Procedure
{
    use QueryExpanderTrait;

    /** @var callable */
    private $executor;

    /** @var string */
    private $query;

    /** @var int */
    private $affectedRows = 0;

    public function __construct(callable $executor, string $query)
    {
        $this->executor = $executor;
        $this->query = $query;
    }

    /**
     * @return int
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress MixedReturnStatement
     */
    public function insert(): int
    {
        $procedure = function (PDO $connection): int {
            $this->bindParameters($connection)->execute();
            return (int) $connection->lastInsertId();
        };
        return ($this->executor)($procedure);
    }

    /**
     * @return void
     */
    public function execute()
    {
        $procedure =
            /**
             * @param PDO $connection
             * @return void
             */
            function (PDO $connection) {
                $this->bindParameters($connection)->execute();
            };

        ($this->executor)($procedure);
    }

    /**
     * @return int
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress MixedReturnStatement
     */
    public function affectedRows(): int
    {
        return $this->affectedRows;
    }

    /**
     * @return Generator
     * @psalm-return Generator<int,array<string,int|string|float|null>,mixed,void>
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress MixedReturnStatement
     */
    protected function fetch(): Generator
    {
        $procedure =
            /**
             * @param PDO $connection
             * @return Generator
             * @psalm-return Generator<int,array<string,int|string|float|null>,mixed,void>
             * @psalm-suppress MixedReturnTypeCoercion
             */
            function (PDO $connection) {
                $statement = $this->bindParameters($connection);
                $statement->execute();
                $this->affectedRows = $statement->rowCount();
                $index = 0;
                /** @psalm-suppress MixedAssignment */
                while (($row = $statement->fetch(PDO::FETCH_ASSOC)) !== false) {
                    yield $index++ => $row;
                }
                $statement = null;
            };

        return ($this->executor)($procedure);
    }

    private function bindParameters(PDO $connection): PDOStatement
    {
        list($query, $parameters) = $this->unwrapQueryAndParameters($this->query, $this->parameters);
        $this->parameters = [];

        $statement = $connection->prepare($query);
        if ($statement === false) {
            throw new DatabaseException('Cannot prepare statement ' . $query);
        }

        $position = 1;
        foreach ($parameters as list($type, $value)) {
            $statement->bindValue($position++, $value, $this->resolveType($type, $value));
        }
        return $statement;
    }

    /**
     * @param string $type
     * @param string|int|float|null $value
     * @return int
     */
    private function resolveType(string $type, $value): int
    {
        if ($value === null) {
            return PDO::PARAM_NULL;
        }
        switch ($type) {
            case 'b':
                return PDO::PARAM_LOB;
            case 'i':
                return PDO::PARAM_INT;
            case 'd':
            case 's':
                return PDO::PARAM_STR;
            default:
                throw new DatabaseException('Cannot resolve type "' . $type . '"');
        }
    }
}
