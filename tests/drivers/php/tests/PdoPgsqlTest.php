<?php
declare(strict_types=1);

namespace Serenedb\Drivers\Tests;

use PDO;
use PHPUnit\Framework\TestCase;
use Serenedb\Drivers\Spec;

final class PdoPgsqlTest extends TestCase
{
    private const DRIVER_KEY = 'php_pdo';
    private static ?PDO $pdo = null;
    private static string $schema = '';

    public static function setUpBeforeClass(): void
    {
        $a = Spec::connArgs();
        $dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s', $a['host'], $a['port'], $a['database']);
        self::$pdo = new PDO($dsn, $a['user'], null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        self::$schema = Spec::schemaName(self::DRIVER_KEY);
        self::$pdo->exec(sprintf('CREATE SCHEMA IF NOT EXISTS "%s"', self::$schema));
        self::$pdo->exec(sprintf(
            'SET search_path TO "%s", public, pg_catalog', self::$schema));
    }

    public static function tearDownAfterClass(): void
    {
        if (self::$pdo) {
            try {
                self::$pdo->exec(sprintf(
                    'DROP SCHEMA IF EXISTS "%s" CASCADE', self::$schema));
            } catch (\Throwable) {}
            self::$pdo = null;
        }
    }

    public function testSmokeSelectOne(): void
    {
        $r = self::$pdo->query('SELECT 1 AS v')->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $r['v']);
    }

    public function testSmokeServerVersion(): void
    {
        $r = self::$pdo->query('SHOW server_version')->fetch(PDO::FETCH_ASSOC);
        $this->assertNotEmpty(array_values($r)[0]);
    }

    public static function casesProvider(): \Generator
    {
        foreach (Spec::casesFor(self::DRIVER_KEY) as $c) {
            $name = sprintf('%d-%s-%s-%d',
                $c['type']['oid'],
                $c['type']['name'] ?? $c['type']['pg_typname'],
                $c['proto'],
                $c['idx']);
            yield $name => [$c];
        }
    }

    /** @dataProvider casesProvider */
    public function testRoundtrip(array $c): void
    {
        $pgname = $c['type']['pg_typname'];
        $sample = $c['sample'];

        $expected = $this->serverText($pgname, $sample);
        $proto = $c['proto'];

        if ($proto === 'simple' || $proto === 'extended-noparam') {
            if ($sample === null) {
                $sql = sprintf('SELECT NULL::%s::text AS v', $pgname);
            } else {
                $lit = str_replace("'", "''", (string) $sample);
                $sql = sprintf("SELECT '%s'::%s::text AS v", $lit, $pgname);
            }
            $row = self::$pdo->query($sql)->fetch(PDO::FETCH_ASSOC);
            $this->assertSame($expected, $row['v']);
            return;
        }

        // Bound modes use placeholders. PDO doesn't expose binary-bind so
        // extended-binary degrades to text-bind on the wire.
        $cast = $c['type']['cast_sql'] ?? sprintf('$1::%s', $pgname);
        $cast = str_replace('$1', '?', $cast);
        $sql = sprintf('SELECT (%s)::text AS v', $cast);
        $stmt = self::$pdo->prepare($sql);
        $stmt->execute([$sample]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame($expected, $row['v']);
    }

    private function serverText(string $pgType, mixed $sample): ?string
    {
        if ($sample === null) return null;
        $lit = str_replace("'", "''", (string) $sample);
        $sql = sprintf("SELECT ('%s'::%s)::text", $lit, $pgType);
        $r = self::$pdo->query($sql)->fetch(PDO::FETCH_ASSOC);
        return array_values($r)[0];
    }
}
