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

    /**
     * @var PDO
     */
    private $handle;

    /**
     * @var string
     */
    private $query;

    /**
     * @var int
     */
    private $affectedRows = 0;

    /**
     * @var PDOStatement
     * @psalm-var array<string,PDOStatement>
     */
    private $cache = [];

    public function __construct(PDO $handle, string $query)
    {
        $this->handle = $handle;
        $this->query = $query;
    }

    /**
     * @return int
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress MixedReturnStatement
     */
    public function insert(): int
    {
        $this->bindParameters($this->handle)->execute();
        return (int) $this->handle->lastInsertId();
    }

    /**
     * @return void
     */
    public function execute()
    {
        $statement = $this->bindParameters($this->handle);
        $statement->execute();
        $this->affectedRows = $statement->rowCount();
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
     * @psalm-return Generator<int,array<string,null|scalar>,mixed,void>
     * @psalm-suppress ImplementedReturnTypeMismatch
     * @psalm-suppress MixedReturnTypeCoercion
     * @psalm-suppress MixedAssignment
     */
    protected function fetch(): Generator
    {
        $statement = $this->bindParameters($this->handle);
        $statement->execute();
        $this->affectedRows = $statement->rowCount();
        $index = 0;
        while (($row = $statement->fetch(PDO::FETCH_ASSOC)) !== false) {
            yield $index++ => $row;
        }
    }

    private function bindParameters(PDO $connection): PDOStatement
    {
        list($query, $parameters) = $this->unwrapQueryAndParameters($this->query, $this->parameters);
        $this->parameters = [];

        $statement = $this->cache[$query] = ($this->cache[$query] ?? $connection->prepare($query));
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
