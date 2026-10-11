<?php
declare(strict_types=1);

namespace Serenedb\Drivers;

trait TargetAssertions
{
    protected function xfailOnSereneDB(string $reason, callable $body): void
    {
        if (!Target::isSereneDB()) {
            $body();
            return;
        }
        try {
            $body();
        } catch (\Throwable $e) {
            $this->markTestSkipped(sprintf('xfail: %s (%s: %s)', $reason, $e::class, strtok($e->getMessage(), "\n")));
        }
        $this->fail('XPASS(strict): ' . $reason);
    }

    protected function assertGolden(string $path, string $text): void
    {
        $info = pathinfo($path);
        $twin = sprintf('%s/%s.serenedb.%s', $info['dirname'], $info['filename'], $info['extension']);
        if (getenv('SDB_DRV_UPDATE_GOLDENS') === '1') {
            if (!Target::isSereneDB()) {
                @mkdir($info['dirname'], 0777, true);
                file_put_contents($path, $text);
            } elseif ($text === file_get_contents($path)) {
                @unlink($twin);
            } else {
                file_put_contents($twin, $text);
            }
            $this->addToAssertionCount(1);
            return;
        }
        $expected = Target::isSereneDB() && is_file($twin) ? $twin : $path;
        $this->assertSame(file_get_contents($expected), $text);
    }
}
