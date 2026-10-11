<?php
declare(strict_types=1);

namespace Serenedb\Drivers;

use PDO;

final class Target
{
    private static ?bool $serenedb = null;

    public static function databaseName(string $suite): string
    {
        return 'php_' . $suite . '_' . (getenv('SDB_DRV_RUN_ID') ?: '0');
    }

    public static function pdo(string $database): PDO
    {
        $a = Spec::connArgs();
        $dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s', $a['host'], $a['port'], $database);
        return new PDO($dsn, $a['user'], null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    }

    public static function recreateDatabase(string $name): void
    {
        $admin = self::pdo(Spec::connArgs()['database']);
        $admin->exec(sprintf('DROP DATABASE IF EXISTS %s', $name));
        $admin->exec(sprintf('CREATE DATABASE %s', $name));
    }

    public static function dropDatabase(string $name): void
    {
        self::pdo(Spec::connArgs()['database'])->exec(sprintf('DROP DATABASE IF EXISTS %s', $name));
    }

    public static function isSereneDB(): bool
    {
        if (self::$serenedb === null) {
            $version = self::pdo(Spec::connArgs()['database'])->query('SELECT version()')->fetchColumn();
            self::$serenedb = str_contains((string) $version, 'SereneDB');
        }
        return self::$serenedb;
    }

    public static function fixture(string $suite, string $name): string
    {
        return __DIR__ . '/../fixtures/' . $suite . '/' . $name;
    }

    public static function outcome(callable $body): mixed
    {
        try {
            return $body();
        } catch (\Throwable $e) {
            return ['error' => $e::class . ': ' . strtok($e->getMessage(), "\n")];
        }
    }

    public static function sortRows(array $rows): array
    {
        usort($rows, fn (array $a, array $b) => strcmp((string) $a[0], (string) $b[0]));
        return $rows;
    }

    public static function json(mixed $value): string
    {
        return json_encode($value, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) . "\n";
    }
}
