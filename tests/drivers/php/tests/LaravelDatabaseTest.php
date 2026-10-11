<?php
declare(strict_types=1);

namespace Serenedb\Drivers\Tests;

use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\BelongsToMany;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Database\Eloquent\SoftDeletes;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Schema\Builder;
use Illuminate\Database\UniqueConstraintViolationException;
use PHPUnit\Framework\TestCase;
use Serenedb\Drivers\Spec;
use Serenedb\Drivers\Target;
use Serenedb\Drivers\TargetAssertions;

final class LaravelAuthor extends Model
{
    use SoftDeletes;

    protected $table = 'authors';
    protected $guarded = [];
    protected $casts = ['royalty' => 'decimal:2', 'active' => 'boolean', 'meta' => 'array'];

    public function books(): HasMany
    {
        return $this->hasMany(LaravelBook::class, 'author_id');
    }
}

final class LaravelDatedAuthor extends Model
{
    protected $table = 'authors';
    protected $guarded = [];
    protected $casts = ['born' => 'date'];
}

final class LaravelBook extends Model
{
    protected $table = 'books';
    protected $guarded = [];
    protected $casts = ['published_at' => 'datetime', 'rating' => 'float', 'keywords' => 'array'];

    public function author(): BelongsTo
    {
        return $this->belongsTo(LaravelAuthor::class, 'author_id');
    }

    public function tags(): BelongsToMany
    {
        return $this->belongsToMany(LaravelTag::class, 'book_tag', 'book_id', 'tag_id');
    }
}

final class LaravelTag extends Model
{
    protected $table = 'tags';
    protected $guarded = [];
    public $timestamps = false;
}

final class LaravelDatabaseTest extends TestCase
{
    use TargetAssertions;

    private const TABLES = ['authors', 'book_tag', 'books', 'memos', 'tags'];

    private static string $database;
    private static Capsule $capsule;

    public static function setUpBeforeClass(): void
    {
        self::$database = Target::databaseName('laravel');
        Target::recreateDatabase(self::$database);
        $a = Spec::connArgs();
        self::$capsule = new Capsule();
        self::$capsule->addConnection([
            'driver' => 'pgsql',
            'host' => $a['host'],
            'port' => $a['port'],
            'database' => self::$database,
            'username' => $a['user'],
            'password' => '',
            'charset' => 'utf8',
            'prefix' => '',
            'search_path' => 'public',
            'sslmode' => 'prefer',
            'timezone' => 'UTC',
        ]);
        self::$capsule->setAsGlobal();
        self::$capsule->bootEloquent();
        self::createSchema(self::schema());
    }

    public static function tearDownAfterClass(): void
    {
        self::$capsule->getDatabaseManager()->disconnect();
        Target::dropDatabase(self::$database);
    }

    private static function schema(): Builder
    {
        return self::$capsule->getConnection()->getSchemaBuilder();
    }

    private static function createSchema(Builder $schema): void
    {
        $schema->create('authors', function (Blueprint $t) {
            $t->id();
            $t->string('name', 100)->unique();
            $t->string('email')->nullable();
            $t->text('bio')->nullable()->comment('free text about the author');
            $t->date('born')->nullable();
            $t->smallInteger('rank')->nullable();
            $t->decimal('royalty', 8, 2)->default(0);
            $t->boolean('active')->default(true);
            $t->enum('mood', ['calm', 'angry'])->default('calm');
            $t->json('meta')->nullable();
            $t->timestamps();
            $t->softDeletes();
            $t->comment('people who write books');
        });
        self::$capsule->getConnection()->statement(
            'CREATE TABLE books (id bigserial PRIMARY KEY, author_id bigint NOT NULL REFERENCES authors (id), '
            . 'title varchar(200) NOT NULL, pages integer NOT NULL DEFAULT 0 CHECK (pages >= 0), '
            . 'rating double precision, published_at timestamp(0) without time zone, cover bytea, '
            . 'keywords json, created_at timestamp(0) without time zone, updated_at timestamp(0) without time zone)'
        );
        $schema->table('books', function (Blueprint $t) {
            $t->index(['author_id', 'published_at']);
            $t->unique(['author_id', 'title']);
        });
        $schema->create('tags', function (Blueprint $t) {
            $t->id();
            $t->string('label', 50)->unique();
            $t->uuid('uid')->nullable();
        });
        self::$capsule->getConnection()->statement(
            'CREATE TABLE book_tag (book_id bigint NOT NULL REFERENCES books (id), '
            . 'tag_id bigint NOT NULL REFERENCES tags (id), PRIMARY KEY (book_id, tag_id))'
        );
        $schema->create('memos', function (Blueprint $t) {
            $t->id();
            $t->string('body')->nullable();
        });
        $schema->table('memos', fn (Blueprint $t) => $t->string('nickname')->nullable());
        $schema->table('memos', fn (Blueprint $t) => $t->renameColumn('nickname', 'handle'));
        $schema->table('memos', fn (Blueprint $t) => $t->index('handle', 'memos_handle_idx'));
        $schema->table('memos', fn (Blueprint $t) => $t->renameIndex('memos_handle_idx', 'memos_handle_index'));
        $schema->table('memos', fn (Blueprint $t) => $t->dropIndex('memos_handle_index'));
        $schema->table('memos', fn (Blueprint $t) => $t->dropColumn('handle'));
        self::$capsule->getConnection()->statement('CREATE VIEW author_names AS SELECT id, name FROM authors');
    }

    private function writer(string $name): LaravelAuthor
    {
        return LaravelAuthor::create(['name' => $name, 'email' => strtolower(str_replace(' ', '_', $name)) . '@dota',
            'born' => '2004-02-01']);
    }

    public function testIntrospectionMatchesPostgresql(): void
    {
        $schema = self::schema();
        $result = [
            'tables' => Target::outcome(fn () => array_values(array_map(
                fn ($t) => [$t['name'], $t['schema'], $t['comment']],
                array_filter($schema->getTables(), fn ($t) => in_array($t['name'], self::TABLES, true)),
            ))),
            'views' => Target::outcome(fn () => array_map(
                fn ($v) => [$v['name'], $v['schema']],
                array_values(array_filter($schema->getViews(), fn ($v) => $v['schema'] === 'public')),
            )),
            'types' => Target::outcome(fn () => array_map(
                fn ($t) => [$t['name'], $t['type'], $t['category']],
                array_values(array_filter($schema->getTypes(), fn ($t) => $t['schema'] === 'public')),
            )),
        ];
        foreach (self::TABLES as $table) {
            $result[$table] = [
                'columns' => Target::outcome(fn () => array_map(
                    fn ($c) => [$c['name'], $c['type_name'], $c['type'], $c['collation'], $c['nullable'],
                        $c['default'], $c['auto_increment'], $c['comment'], $c['generation']],
                    $schema->getColumns($table),
                )),
                'indexes' => Target::outcome(fn () => Target::sortRows(array_map(
                    fn ($i) => [$i['name'], $i['columns'], $i['type'], $i['unique'], $i['primary']],
                    $schema->getIndexes($table),
                ))),
                'foreign_keys' => Target::outcome(fn () => Target::sortRows(array_map(
                    fn ($f) => [$f['name'], $f['columns'], $f['foreign_table'], $f['foreign_columns'],
                        $f['on_update'], $f['on_delete']],
                    $schema->getForeignKeys($table),
                ))),
            ];
        }
        $this->assertGolden(Target::fixture('laravel', 'introspection.json'), Target::json($result));
    }

    public function testSchemaPredicates(): void
    {
        $schema = self::schema();
        $this->assertTrue($schema->hasTable('authors'));
        $this->assertFalse($schema->hasTable('nope'));
        $this->assertTrue($schema->hasColumn('books', 'keywords'));
        $this->assertTrue($schema->hasColumns('authors', ['name', 'meta']));
        $this->assertSame(['id', 'body'], $schema->getColumnListing('memos'));
        $this->assertTrue($schema->hasIndex('books', ['author_id', 'title'], 'unique'));
        $this->assertTrue($schema->hasView('author_names'));
    }

    public function testEloquentCrud(): void
    {
        $author = $this->writer('Pudge');
        $author->update(['royalty' => '12.34', 'rank' => 7, 'meta' => ['hook' => true]]);
        $fresh = LaravelAuthor::findOrFail($author->id);
        $this->assertSame('12.34', $fresh->royalty);
        $this->assertSame(['hook' => true], $fresh->meta);
        $this->assertSame('calm', $fresh->mood);
        $this->assertTrue($fresh->active);
        $book = $fresh->books()->create(['title' => 'Fresh Meat', 'pages' => 321, 'rating' => 4.5,
            'published_at' => '2024-05-06 07:08:09', 'keywords' => ['hook', 'rot']]);
        $this->assertSame('Pudge', LaravelBook::with('author')->find($book->id)->author->name);
        $tag = LaravelTag::create(['label' => 'meat', 'uid' => '6f1c3b2a-1d2e-4c3f-8a9b-0c1d2e3f4a5b']);
        $book->tags()->attach($tag->id);
        $this->assertSame(['meat'], LaravelBook::find($book->id)->tags->pluck('label')->all());
        $book->tags()->detach();
        $this->assertSame(['hook', 'rot'], LaravelBook::find($book->id)->keywords);
        $fresh->delete();
        $this->assertNull(LaravelAuthor::find($author->id));
        $this->assertNotNull(LaravelAuthor::withTrashed()->find($author->id));
    }

    public function testDateCast(): void
    {
        $this->xfailOnSereneDB("SereneDB rejects '2004-02-01 00:00:00' bound to a date parameter, Laravel's date " .
            "cast binds dates as 'Y-m-d H:i:s'", function () {
                $author = LaravelDatedAuthor::create(['name' => 'Oracle', 'born' => '2004-02-01']);
                $this->assertSame('2004-02-01', LaravelDatedAuthor::find($author->id)->born->format('Y-m-d'));
            });
    }

    public function testQueries(): void
    {
        $author = $this->writer('Crystal Maiden');
        for ($i = 0; $i < 5; $i++) {
            $author->books()->create(['title' => "Frostbite $i", 'pages' => $i * 100,
                'published_at' => sprintf('2020-%02d-15 00:00:00', $i + 1)]);
        }
        $books = LaravelBook::where('author_id', $author->id);
        $this->assertSame(1, (clone $books)->whereRaw('title ILIKE ?', ['FROSTBITE 1'])->count());
        $this->assertSame(3, (clone $books)->whereBetween('pages', [100, 300])->count());
        $this->assertSame(2, (clone $books)->whereIn('pages', [0, 400, 999])->count());
        $this->assertSame([400, 300], (clone $books)->orderByDesc('pages')->limit(2)->pluck('pages')->all());
        $this->assertSame(1000, (int) (clone $books)->sum('pages'));
        $this->assertSame(200, (int) (clone $books)->avg('pages'));
        $this->assertSame(1, LaravelAuthor::whereHas('books', fn ($q) => $q->where('pages', 400))
            ->where('id', $author->id)->count());
        $this->assertSame(5, LaravelAuthor::withCount('books')->find($author->id)->books_count);
        $this->assertSame([0, 100], LaravelBook::query()->fromSub(
            (clone $books)->where('pages', '<', 200), 'books')->orderBy('pages')->pluck('pages')->all());
        $seen = [];
        (clone $books)->orderBy('id')->chunk(2, function ($chunk) use (&$seen) {
            foreach ($chunk as $b) {
                $seen[] = $b->pages;
            }
        });
        $this->assertSame([0, 100, 200, 300, 400], $seen);
        $this->assertSame(5, (clone $books)->lazyById(2)->count());
        $this->assertSame(5, iterator_count((clone $books)->cursor()));
        $this->assertSame(1, (clone $books)->whereYear('published_at', 2020)->whereMonth('published_at', 3)->count());
    }

    public function testQueryBuilderWrites(): void
    {
        $db = self::$capsule->getConnection();
        $id = $db->table('tags')->insertGetId(['label' => 'insert-get-id']);
        $this->assertSame('insert-get-id', $db->table('tags')->where('id', $id)->value('label'));
        $this->assertSame(2, $db->table('tags')->upsert(
            [['label' => 'insert-get-id'], ['label' => 'upserted']], ['label'], ['label']));
        $this->assertSame(1, $db->table('tags')->insertOrIgnore([['label' => 'upserted'], ['label' => 'ignored']]));
        $tag = LaravelTag::firstOrCreate(['label' => 'blink']);
        $this->assertSame($tag->id, LaravelTag::firstOrCreate(['label' => 'blink'])->id);
        LaravelTag::updateOrCreate(['label' => 'blink'], ['uid' => '6f1c3b2a-1d2e-4c3f-8a9b-0c1d2e3f4a5c']);
        $this->assertSame('6f1c3b2a-1d2e-4c3f-8a9b-0c1d2e3f4a5c', LaravelTag::where('label', 'blink')->value('uid'));
        $this->assertSame(4, $db->table('tags')->whereIn('label', ['insert-get-id', 'upserted', 'ignored', 'blink'])
            ->delete());
        $author = $this->writer('Tiny');
        $book = $author->books()->create(['title' => 'Avalanche']);
        $this->assertSame(1, LaravelBook::whereKey($book->id)->increment('pages', 5));
        $this->assertSame(5, LaravelBook::find($book->id)->pages);
    }

    public function testTransactionRollback(): void
    {
        try {
            self::$capsule->getConnection()->transaction(function () {
                $this->writer('Rolled Back');
                throw new \RuntimeException('roll back');
            });
        } catch (\RuntimeException) {
        }
        $this->assertFalse(LaravelAuthor::where('name', 'Rolled Back')->exists());
        $this->writer('Unique');
        $this->expectException(UniqueConstraintViolationException::class);
        $this->writer('Unique');
    }

    public function testTransactionCommit(): void
    {
        $this->xfailOnSereneDB('SQL DEALLOCATE cannot see statements prepared through the protocol, pdo_pgsql ' .
            'deallocates a freed statement inside the transaction, SereneDB aborts it and COMMIT rolls back',
            function () {
                self::$capsule->getConnection()->transaction(fn () => $this->writer('Earthshaker'));
                $this->assertTrue(LaravelAuthor::where('name', 'Earthshaker')->exists());
            });
    }

    public function testNestedTransactionUsesSavepoint(): void
    {
        $db = self::$capsule->getConnection();
        $this->xfailOnSereneDB('SereneDB has no SAVEPOINT, Laravel nests transactions with savepoints (#722)',
            function () use ($db) {
                $db->transaction(function () use ($db) {
                    $this->writer('Kunkka');
                    try {
                        $db->transaction(function () {
                            $this->writer('Tidehunter');
                            throw new \RuntimeException('inner');
                        });
                    } catch (\RuntimeException) {
                    }
                });
                $this->assertTrue(LaravelAuthor::where('name', 'Kunkka')->exists());
                $this->assertFalse(LaravelAuthor::where('name', 'Tidehunter')->exists());
            });
    }

    public function testPessimisticLocks(): void
    {
        $author = $this->writer('Sven');
        $this->xfailOnSereneDB('SereneDB has no SELECT ... FOR UPDATE / FOR SHARE (#1431)', function () use ($author) {
            self::$capsule->getConnection()->transaction(function () use ($author) {
                $this->assertSame('Sven', LaravelAuthor::lockForUpdate()->find($author->id)->name);
                $this->assertSame('Sven', LaravelAuthor::sharedLock()->find($author->id)->name);
            });
        });
    }

    public function testForeignIdConstrained(): void
    {
        $schema = self::schema();
        try {
            $this->xfailOnSereneDB('SereneDB cannot add a FOREIGN KEY with ALTER TABLE, foreignId()->constrained() ' .
                'emits one (#1427)', function () use ($schema) {
                    $schema->create('kennels', fn (Blueprint $t) => $t->id());
                    $schema->create('hounds', function (Blueprint $t) {
                        $t->id();
                        $t->foreignId('kennel_id')->constrained();
                    });
                    $this->assertCount(1, $schema->getForeignKeys('hounds'));
                });
        } finally {
            $schema->dropIfExists('hounds');
            $schema->dropIfExists('kennels');
        }
    }

    public function testChangeColumn(): void
    {
        $schema = self::schema();
        $schema->create('knobs', function (Blueprint $t) {
            $t->id();
            $t->string('state', 20)->nullable();
        });
        try {
            $this->xfailOnSereneDB('SereneDB allows one subcommand per ALTER TABLE, ->change() emits several (#1430)',
                function () use ($schema) {
                    $schema->table('knobs', fn (Blueprint $t) => $t->string('state', 80)->default('off')->change());
                    $this->assertSame("'off'::character varying",
                        collect($schema->getColumns('knobs'))->firstWhere('name', 'state')['default']);
                });
        } finally {
            $schema->dropIfExists('knobs');
        }
    }

    public function testTimeColumn(): void
    {
        $schema = self::schema();
        try {
            $this->xfailOnSereneDB('SereneDB rejects time(0), Laravel declares time columns with a precision (#1434)',
                function () use ($schema) {
                    $schema->create('alarms', function (Blueprint $t) {
                        $t->id();
                        $t->time('rings');
                    });
                    self::$capsule->getConnection()->table('alarms')->insert(['rings' => '13:14:15']);
                    $this->assertSame('13:14:15', self::$capsule->getConnection()->table('alarms')->value('rings'));
                });
        } finally {
            $schema->dropIfExists('alarms');
        }
    }

    public function testTimestampTzColumns(): void
    {
        $schema = self::schema();
        $schema->create('events', function (Blueprint $t) {
            $t->id();
            $t->timestampTz('happened_at');
        });
        try {
            $this->xfailOnSereneDB('SereneDB creates timestamp(0) with time zone as timestamp without time zone ' .
                'and drops the offset of stored values', function () {
                    $db = self::$capsule->getConnection();
                    $db->table('events')->insert(['happened_at' => '2024-05-06 07:08:09+02']);
                    $this->assertSame('2024-05-06 05:08:09+00', $db->table('events')->value('happened_at'));
                });
        } finally {
            $schema->dropIfExists('events');
        }
    }

    public function testJsonbColumn(): void
    {
        $schema = self::schema();
        try {
            $this->xfailOnSereneDB('SereneDB has no jsonb column type (#613)', function () use ($schema) {
                $schema->create('documents', function (Blueprint $t) {
                    $t->id();
                    $t->jsonb('data');
                });
                $db = self::$capsule->getConnection();
                $db->table('documents')->insert(['data' => json_encode(['hero' => 'pudge', 'tags' => ['hook']])]);
                $this->assertSame(1, $db->table('documents')->whereJsonContains('data->tags', 'hook')->count());
            });
        } finally {
            $schema->dropIfExists('documents');
        }
    }

    public function testFullTextIndex(): void
    {
        $schema = self::schema();
        $schema->create('articles', function (Blueprint $t) {
            $t->id();
            $t->text('body');
        });
        try {
            $this->xfailOnSereneDB('SereneDB has no to_tsvector, fullText() indexes to_tsvector(\'english\', ...) (#1221)',
                function () use ($schema) {
                    $schema->table('articles', fn (Blueprint $t) => $t->fullText('body'));
                    $db = self::$capsule->getConnection();
                    $db->table('articles')->insert(['body' => 'fresh meat for the hook']);
                    $this->assertSame(1, $db->table('articles')->whereFullText('body', 'hook')->count());
                });
        } finally {
            $schema->dropIfExists('articles');
        }
    }

    public function testRenameReferencedTable(): void
    {
        $this->xfailOnSereneDB('SereneDB cannot rename a table that a foreign key references', function () {
            self::schema()->rename('tags', 'labels');
            self::schema()->rename('labels', 'tags');
            $this->assertTrue(self::schema()->hasTable('tags'));
        });
    }

    public function testDeleteParentAfterChildren(): void
    {
        $author = $this->writer('Undying');
        $author->books()->create(['title' => 'Tombstone']);
        $this->xfailOnSereneDB('SereneDB rejects deleting a row whose referencing rows were deleted earlier in the ' .
            'same transaction (#1433)', function () use ($author) {
                self::$capsule->getConnection()->transaction(function () use ($author) {
                    $author->books()->delete();
                    $author->forceDelete();
                });
                $this->assertNull(LaravelAuthor::withTrashed()->find($author->id));
            });
    }
}
