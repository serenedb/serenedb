<?php
declare(strict_types=1);

namespace Serenedb\Drivers;

use Symfony\Component\Yaml\Yaml;

final class Spec
{
    public const PROTOCOLS = [
        'simple', 'extended-noparam', 'extended-text', 'extended-binary',
    ];

    public static function specDir(): string
    {
        return getenv('SDB_DRV_SPEC') ?: __DIR__ . '/../../spec';
    }

    /** @return array<int,array<string,mixed>> */
    public static function loadTypes(): array
    {
        $path = self::specDir() . '/types.yaml';
        return Yaml::parseFile($path) ?? [];
    }

    /**
     * @return iterable<array{type:array<string,mixed>,proto:string,idx:int,sample:mixed}>
     */
    public static function casesFor(string $driverKey): iterable
    {
        $pat = getenv('SDB_DRV_TYPES') ?: '.*';
        $protos = explode(',', getenv('SDB_DRV_PROTOCOLS') ?: implode(',', self::PROTOCOLS));
        foreach (self::loadTypes() as $t) {
            $name = $t['name'] ?? ($t['pg_typname'] ?? '');
            if (!preg_match("#$pat#", (string) $name)) continue;
            $text = !empty($t['text']);
            $bin = !empty($t['binary']);
            if (!$text && !$bin) continue;
            foreach ($protos as $p) {
                if (!in_array($p, self::PROTOCOLS, true)) continue;
                $supports = $p === 'extended-binary' ? $bin : $text;
                if (!$supports) continue;
                $skip = $t['skip'][$driverKey] ?? [];
                if (in_array('all', $skip, true) || in_array($p, $skip, true)) continue;
                foreach ($t['samples'] ?? [] as $idx => $sample) {
                    yield [
                        'type' => $t,
                        'proto' => $p,
                        'idx' => $idx,
                        'sample' => $sample,
                    ];
                }
            }
        }
    }

    public static function connArgs(): array
    {
        return [
            'host' => getenv('SDB_DRV_HOST') ?: 'localhost',
            'port' => (int) (getenv('SDB_DRV_PORT') ?: '5432'),
            'database' => getenv('SDB_DRV_DATABASE') ?: 'postgres',
            'user' => getenv('SDB_DRV_USER') ?: 'postgres',
        ];
    }

    public static function schemaName(string $driverKey): string
    {
        return 'drv_' . $driverKey . '_' . (getenv('SDB_DRV_RUN_ID') ?: '0');
    }
}
