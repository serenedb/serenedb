<?php
declare(strict_types=1);

namespace Serenedb\Drivers\Tests;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\TransactionIsolationLevel;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use PHPUnit\Framework\TestCase;
use Serenedb\Drivers\Spec;
use Serenedb\Drivers\Target;
use Serenedb\Drivers\TargetAssertions;

final class DoctrineDbalTest extends TestCase
{
    use TargetAssertions;

    private const TABLES = ['authors', 'books', 'notes', 'tags'];

    private static string $database;
    private static Connection $conn;

    public static function setUpBeforeClass(): void
    {
        self::$database = Target::databaseName('doctrine');
        Target::recreateDatabase(self::$database);
        self::$conn = self::connect();
        self::$conn->createSchemaManager()->createSchemaObjects(self::dbalSchema());
        foreach ([
            'CREATE TABLE authors (id serial PRIMARY KEY, name varchar(100) NOT NULL UNIQUE, born date)',
            'CREATE TABLE books (id serial PRIMARY KEY, author_id int NOT NULL REFERENCES authors (id), '
                . 'title varchar(200) NOT NULL, pages int NOT NULL DEFAULT 0 CHECK (pages >= 0))',
            'CREATE INDEX books_author_idx ON books (author_id)',
            'CREATE VIEW author_names AS SELECT id, name FROM authors',
        ] as $sql) {
            self::$conn->executeStatement($sql);
        }
    }

    public static function tearDownAfterClass(): void
    {
        self::$conn->close();
        Target::dropDatabase(self::$database);
    }

    private static function connect(): Connection
    {
        $a = Spec::connArgs();
        return DriverManager::getConnection([
            'driver' => 'pdo_pgsql',
            'host' => $a['host'],
            'port' => $a['port'],
            'user' => $a['user'],
            'dbname' => self::$database,
        ]);
    }

    private static function dbalSchema(): Schema
    {
        $schema = new Schema();
        $tags = $schema->createTable('tags');
        $tags->addColumn('id', Types::INTEGER, ['notnull' => true]);
        $tags->addColumn('label', Types::STRING, ['length' => 50, 'notnull' => true]);
        $tags->addColumn('weight', Types::FLOAT, ['notnull' => false]);
        $tags->addColumn('created', Types::DATE_IMMUTABLE, ['notnull' => false]);
        $tags->setPrimaryKey(['id']);
        $tags->addUniqueIndex(['label'], 'tags_label_uniq');
        $tags->setComment('labels for books');

        $notes = $schema->createTable('notes');
        $notes->addColumn('id', Types::BIGINT, ['notnull' => true]);
        $notes->addColumn('body', Types::TEXT, ['notnull' => false, 'comment' => 'free text']);
        $notes->addColumn('flag', Types::BOOLEAN, ['notnull' => true, 'default' => false]);
        $notes->addColumn('price', Types::DECIMAL, ['precision' => 10, 'scale' => 2, 'notnull' => false]);
        $notes->addColumn('uid', Types::GUID, ['notnull' => false]);
        $notes->addColumn('data', Types::JSON, ['notnull' => false]);
        $notes->addColumn('at', Types::DATETIME_IMMUTABLE, ['notnull' => false]);
        $notes->addColumn('attz', Types::DATETIMETZ_IMMUTABLE, ['notnull' => false]);
        $notes->addColumn('bin', Types::BLOB, ['notnull' => false]);
        $notes->addColumn('ratio', Types::SMALLFLOAT, ['notnull' => false]);
        $notes->addColumn('code', Types::STRING, ['length' => 3, 'fixed' => true, 'notnull' => false]);
        $notes->addColumn('labels', Types::SIMPLE_ARRAY, ['notnull' => false]);
        $notes->setPrimaryKey(['id']);
        $notes->addIndex(['at'], 'notes_at_idx');
        $notes->addIndex(['code'], 'notes_code_partial', [], ['where' => 'code IS NOT NULL']);
        return $schema;
    }

    private static function describeColumns(array $columns): array
    {
        $out = [];
        foreach ($columns as $c) {
            $out[] = [
                $c->getName(), Type::getTypeRegistry()->lookupName($c->getType()), $c->getLength(),
                $c->getPrecision(), $c->getScale(), $c->getFixed(), $c->getNotnull(),
                $c->getDefault(), $c->getAutoincrement(), $c->getComment(), $c->getPlatformOptions(),
            ];
        }
        return $out;
    }

    public function testServerVersionSelectsPostgresqlPlatform(): void
    {
        $this->assertStringStartsWith('18.', self::$conn->getServerVersion());
        $this->assertSame('Doctrine\DBAL\Platforms\PostgreSQL120Platform', self::$conn->getDatabasePlatform()::class);
    }

    public function testIntrospectionMatchesPostgresql(): void
    {
        $sm = self::$conn->createSchemaManager();
        $result = [
            'tables' => array_values(array_intersect($sm->listTableNames(), self::TABLES)),
            'views' => Target::outcome(fn () => array_values(array_filter(
                array_map(fn ($v) => $v->getName(), $sm->listViews()),
                fn ($name) => str_starts_with($name, 'public.'),
            ))),
            'sequences' => Target::outcome(fn () => array_map(
                fn ($s) => [$s->getName(), $s->getAllocationSize(), $s->getInitialValue()],
                $sm->listSequences(),
            )),
            'schemas' => Target::outcome(fn () => $sm->listSchemaNames()),
            'database_listed' => Target::outcome(fn () => in_array(self::$database, $sm->listDatabases(), true)),
        ];
        foreach (self::TABLES as $table) {
            $result[$table] = [
                'comment' => Target::outcome(fn () => $sm->introspectTable($table)->getComment()),
                'columns' => Target::outcome(fn () => self::describeColumns($sm->listTableColumns($table))),
                'indexes' => Target::outcome(fn () => Target::sortRows(array_map(
                    fn ($i) => [$i->getName(), $i->getColumns(), $i->isUnique(), $i->isPrimary(), $i->getFlags(),
                        $i->getOptions()],
                    array_values($sm->listTableIndexes($table)),
                ))),
                'foreign_keys' => Target::outcome(fn () => Target::sortRows(array_map(
                    fn ($f) => [$f->getName(), $f->getLocalColumns(), $f->getForeignTableName(),
                        $f->getForeignColumns(), $f->onDelete(), $f->onUpdate(), $f->getOptions()],
                    $sm->listTableForeignKeys($table),
                ))),
            ];
        }
        $this->assertGolden(Target::fixture('doctrine', 'introspection.json'), Target::json($result));
    }

    public function testComparatorFindsNoChangesAfterCreate(): void
    {
        $sm = self::$conn->createSchemaManager();
        $introspected = new Schema(array_map(fn ($t) => $sm->introspectTable($t), ['notes', 'tags']));
        $diff = $sm->createComparator()->compareSchemas($introspected, self::dbalSchema());
        $sql = self::$conn->getDatabasePlatform()->getAlterSchemaSQL($diff);
        $this->assertGolden(Target::fixture('doctrine', 'schema_diff.json'), Target::json($sql));
    }

    public function testPlatformAlterTableDdl(): void
    {
        $sm = self::$conn->createSchemaManager();
        $platform = self::$conn->getDatabasePlatform();
        $table = new Table('scratch');
        $table->addColumn('id', Types::INTEGER, ['notnull' => true]);
        $table->addColumn('name', Types::STRING, ['length' => 20, 'notnull' => false]);
        $table->addColumn('size', Types::INTEGER, ['notnull' => false]);
        $table->setPrimaryKey(['id']);
        $sm->createTable($table);
        $steps = [
            'add column' => fn (Table $t) => $t->addColumn('extra', Types::STRING, ['length' => 20, 'notnull' => false]),
            'widen integer' => fn (Table $t) => $t->modifyColumn('size', ['type' => Type::getType(Types::BIGINT)]),
            'set default' => fn (Table $t) => $t->modifyColumn('size', ['default' => 7]),
            'set not null' => fn (Table $t) => $t->modifyColumn('size', ['notnull' => true]),
            'set comment' => fn (Table $t) => $t->modifyColumn('size', ['comment' => 'how big']),
            'rename column' => fn (Table $t) => $t->renameColumn('extra', 'note'),
            'widen varchar' => fn (Table $t) => $t->modifyColumn('name', ['length' => 80]),
            'add index' => fn (Table $t) => $t->addIndex(['name'], 'scratch_name_idx'),
            'rename index' => fn (Table $t) => $t->renameIndex('scratch_name_idx', 'scratch_name_idx2'),
            'add unique index' => fn (Table $t) => $t->addUniqueIndex(['note'], 'scratch_note_uniq'),
            'drop index' => fn (Table $t) => $t->dropIndex('scratch_name_idx2'),
            'drop column' => fn (Table $t) => $t->dropColumn('size'),
        ];
        $log = [];
        foreach ($steps as $label => $change) {
            $before = $sm->introspectTable('scratch');
            $after = $sm->introspectTable('scratch');
            $change($after);
            $diff = $sm->createComparator()->compareTables($before, $after);
            foreach ($platform->getAlterTableSQL($diff) as $sql) {
                $log[] = [$label, $sql, Target::outcome(function () use ($sql) {
                    self::$conn->executeStatement($sql);
                    return 'ok';
                })];
            }
        }
        $sm->dropTable('scratch');
        $this->assertGolden(Target::fixture('doctrine', 'alter_table_ddl.json'), Target::json($log));
    }

    public function testStockSchemaWithIdentityAndForeignKeys(): void
    {
        $schema = new Schema();
        $owners = $schema->createTable('stock_owners');
        $owners->addColumn('id', Types::INTEGER, ['autoincrement' => true]);
        $owners->setPrimaryKey(['id']);
        $pets = $schema->createTable('stock_pets');
        $pets->addColumn('id', Types::INTEGER, ['autoincrement' => true]);
        $pets->addColumn('owner_id', Types::INTEGER);
        $pets->setPrimaryKey(['id']);
        $pets->addForeignKeyConstraint('stock_owners', ['owner_id'], ['id']);
        $sm = self::$conn->createSchemaManager();
        try {
            $this->xfailOnSereneDB('SereneDB has no identity columns (#1426), DBAL creates autoincrement ' .
                'columns as GENERATED BY DEFAULT AS IDENTITY and adds foreign keys with ALTER TABLE (#1427)',
                function () use ($sm, $schema) {
                    $sm->createSchemaObjects($schema);
                    self::$conn->executeStatement('INSERT INTO stock_owners DEFAULT VALUES');
                    self::$conn->insert('stock_pets', ['owner_id' => (int) self::$conn->lastInsertId()]);
                    $this->assertSame(1, (int) self::$conn->fetchOne('SELECT count(*) FROM stock_pets'));
                });
        } finally {
            self::$conn->executeStatement('DROP TABLE IF EXISTS stock_pets');
            self::$conn->executeStatement('DROP TABLE IF EXISTS stock_owners');
        }
    }

    public function testTimeColumn(): void
    {
        $table = new Table('alarms');
        $table->addColumn('id', Types::INTEGER, ['notnull' => true]);
        $table->addColumn('rings', Types::TIME_IMMUTABLE, ['notnull' => false]);
        $table->setPrimaryKey(['id']);
        try {
            $this->xfailOnSereneDB('SereneDB rejects TIME(0), DBAL declares time columns with a precision (#1434)',
                function () use ($table) {
                    self::$conn->createSchemaManager()->createTable($table);
                    self::$conn->insert('alarms', ['id' => 1, 'rings' => new \DateTimeImmutable('13:14:15')],
                        ['rings' => Types::TIME_IMMUTABLE]);
                    $this->assertSame('13:14:15', self::$conn->fetchOne('SELECT rings FROM alarms'));
                });
        } finally {
            self::$conn->executeStatement('DROP TABLE IF EXISTS alarms');
        }
    }

    public function testQueryBuilderCrud(): void
    {
        $qb = self::$conn->createQueryBuilder();
        $qb->insert('authors')->values(['name' => ':name', 'born' => ':born'])
            ->setParameter('name', 'Pudge')->setParameter('born', '2004-02-01')->executeStatement();
        $authorId = (int) self::$conn->fetchOne("SELECT id FROM authors WHERE name = 'Pudge'");
        $this->assertGreaterThan(0, $authorId);
        for ($i = 0; $i < 5; $i++) {
            self::$conn->insert('books', ['author_id' => $authorId, 'title' => "Hook $i", 'pages' => $i * 10]);
        }
        $rows = self::$conn->createQueryBuilder()->select('b.title', 'b.pages')->from('books', 'b')
            ->innerJoin('b', 'authors', 'a', 'a.id = b.author_id')
            ->where('a.name = :name')->andWhere('b.pages >= :min')
            ->orderBy('b.pages', 'DESC')->setMaxResults(2)->setFirstResult(1)
            ->setParameter('name', 'Pudge')->setParameter('min', 10)
            ->executeQuery()->fetchAllAssociative();
        $this->assertSame([['title' => 'Hook 3', 'pages' => 30], ['title' => 'Hook 2', 'pages' => 20]], $rows);
        $this->assertSame(3, (int) self::$conn->fetchOne(
            'SELECT count(*) FROM books WHERE pages IN (?)', [[0, 10, 20]], [ArrayParameterType::INTEGER]));
        $this->assertSame(1, self::$conn->update('books', ['pages' => 99], ['title' => 'Hook 0']));
        $this->assertSame(99, self::$conn->fetchOne("SELECT pages FROM books WHERE title = 'Hook 0'"));
        $titles = [];
        foreach (self::$conn->iterateAssociative('SELECT title FROM books ORDER BY title') as $row) {
            $titles[] = $row['title'];
        }
        $this->assertCount(5, $titles);
        $this->assertSame(5, self::$conn->delete('books', ['author_id' => $authorId]));
        $this->assertSame(['Pudge'], self::$conn->fetchFirstColumn('SELECT name FROM author_names'));
    }

    public function testTypeConversions(): void
    {
        $platform = self::$conn->getDatabasePlatform();
        $at = new \DateTimeImmutable('2024-05-06 07:08:09');
        self::$conn->insert('notes', [
            'id' => 1, 'body' => 'hook', 'flag' => true, 'price' => '12.50',
            'uid' => '6f1c3b2a-1d2e-4c3f-8a9b-0c1d2e3f4a5b', 'data' => ['hero' => 'pudge', 'items' => [1, 2]],
            'at' => $at, 'bin' => "\x00\x01binary", 'ratio' => 0.5, 'code' => 'abc', 'labels' => ['a', 'b'],
        ], [
            'flag' => Types::BOOLEAN, 'data' => Types::JSON, 'at' => Types::DATETIME_IMMUTABLE,
            'bin' => ParameterType::LARGE_OBJECT, 'labels' => Types::SIMPLE_ARRAY,
        ]);
        $row = self::$conn->fetchAssociative('SELECT * FROM notes WHERE id = 1');
        $value = fn (string $col, string $type) => Type::getType($type)->convertToPHPValue($row[$col], $platform);
        $this->assertTrue($value('flag', Types::BOOLEAN));
        $this->assertSame('12.50', $value('price', Types::DECIMAL));
        $this->assertSame('6f1c3b2a-1d2e-4c3f-8a9b-0c1d2e3f4a5b', $value('uid', Types::GUID));
        $this->assertSame(['hero' => 'pudge', 'items' => [1, 2]], $value('data', Types::JSON));
        $this->assertEquals($at, $value('at', Types::DATETIME_IMMUTABLE));
        $this->assertSame("\x00\x01binary", stream_get_contents($value('bin', Types::BLOB)));
        $this->assertSame(0.5, $value('ratio', Types::SMALLFLOAT));
        $this->assertSame('abc', $value('code', Types::STRING));
        $this->assertSame(['a', 'b'], $value('labels', Types::SIMPLE_ARRAY));
        self::$conn->delete('notes', ['id' => 1]);
    }

    public function testTimestampWithTimeZoneColumn(): void
    {
        $attz = new \DateTimeImmutable('2024-05-06 07:08:09+02:00');
        self::$conn->insert('notes', ['id' => 2, 'attz' => $attz], ['attz' => Types::DATETIMETZ_IMMUTABLE]);
        try {
            $this->xfailOnSereneDB('SereneDB creates TIMESTAMP(0) WITH TIME ZONE as timestamp without time zone ' .
                'and drops the offset of stored values', function () use ($attz) {
                    $stored = Type::getType(Types::DATETIMETZ_IMMUTABLE)->convertToPHPValue(
                        self::$conn->fetchOne('SELECT attz FROM notes WHERE id = 2'),
                        self::$conn->getDatabasePlatform(),
                    );
                    $this->assertSame($attz->getTimestamp(), $stored->getTimestamp());
                });
        } finally {
            self::$conn->delete('notes', ['id' => 2]);
        }
    }

    public function testLastInsertId(): void
    {
        $this->xfailOnSereneDB('SereneDB has no lastval(), PDO and DBAL read the last serial value with it',
            function () {
                self::$conn->insert('authors', ['name' => 'Tinker']);
                $this->assertSame(
                    (int) self::$conn->fetchOne("SELECT id FROM authors WHERE name = 'Tinker'"),
                    (int) self::$conn->lastInsertId(),
                );
            });
        self::$conn->delete('authors', ['name' => 'Tinker']);
    }

    public function testTransactions(): void
    {
        self::$conn->transactional(fn (Connection $c) => $c->insert('tags', ['id' => 1, 'label' => 'kept']));
        try {
            self::$conn->transactional(function (Connection $c) {
                $c->insert('tags', ['id' => 2, 'label' => 'dropped']);
                throw new \RuntimeException('roll back');
            });
        } catch (\RuntimeException) {
        }
        $this->assertSame(['kept'], self::$conn->fetchFirstColumn('SELECT label FROM tags ORDER BY id'));
        self::$conn->delete('tags', ['id' => 1]);
    }

    public function testNestedTransactionUsesSavepoint(): void
    {
        $this->xfailOnSereneDB('SereneDB has no SAVEPOINT, DBAL nests transactions with savepoints (#722)',
            function () {
                self::$conn->beginTransaction();
                try {
                    self::$conn->insert('tags', ['id' => 11, 'label' => 'outer']);
                    self::$conn->beginTransaction();
                    self::$conn->insert('tags', ['id' => 12, 'label' => 'inner']);
                    self::$conn->rollBack();
                    self::$conn->commit();
                } catch (\Throwable $e) {
                    while (self::$conn->isTransactionActive()) {
                        self::$conn->rollBack();
                    }
                    throw $e;
                }
                $this->assertSame(['outer'], self::$conn->fetchFirstColumn('SELECT label FROM tags WHERE id > 10'));
            });
        self::$conn->executeStatement('DELETE FROM tags WHERE id > 10');
    }

    public function testTransactionIsolationLevel(): void
    {
        $conn = self::connect();
        $conn->setTransactionIsolation(TransactionIsolationLevel::REPEATABLE_READ);
        $this->assertSame(TransactionIsolationLevel::REPEATABLE_READ, $conn->getTransactionIsolation());
        $this->assertSame('repeatable read', $conn->fetchOne('SHOW transaction_isolation'));
        $conn->close();
    }

    public function testSerializableIsolationLevel(): void
    {
        $this->xfailOnSereneDB('SereneDB rejects transaction isolation level serializable', function () {
            $conn = self::connect();
            try {
                $conn->setTransactionIsolation(TransactionIsolationLevel::SERIALIZABLE);
                $conn->transactional(fn (Connection $c) => $c->fetchOne('SELECT 1'));
                $this->assertSame('serializable', $conn->fetchOne('SHOW transaction_isolation'));
            } finally {
                $conn->close();
            }
        });
    }

    public function testSelectForUpdate(): void
    {
        $this->xfailOnSereneDB('SereneDB has no SELECT ... FOR UPDATE (#1431)', function () {
            $rows = self::$conn->transactional(fn (Connection $c) => $c->createQueryBuilder()->select('id')
                ->from('authors')->forUpdate()->executeQuery()->fetchAllAssociative());
            $this->assertIsArray($rows);
        });
    }
}
