require_relative 'support'
require 'sequel'
require 'minitest/autorun'

Drivers.report_junit('sequel')
Sequel.extension :migration

module SequelSuite
  DATABASE = Drivers.database_name('sequel')
  FIXTURES = Drivers::FIXTURES.join('sequel')
  TABLES = %i[authors books books_tags memos tags].freeze

  module_function

  def connect
    args = Drivers.conn_args
    db = Sequel.connect(adapter: 'postgres', host: args[:host], port: args[:port], user: args[:user],
                        database: DATABASE, max_connections: 4)
    db.extension :pg_array, :pg_enum
    db
  end

  def migrate(db, target)
    Sequel::Migrator.run(db, FIXTURES.join('migrate').to_s, target: target)
  end
end

Drivers.recreate_database(SequelSuite::DATABASE)
DB = SequelSuite.connect
SequelSuite.migrate(DB, nil)
Minitest.after_run do
  DB.disconnect
  Drivers.drop_database(SequelSuite::DATABASE)
end

class Author < Sequel::Model(DB[:authors])
  one_to_many :books
end

class Book < Sequel::Model(DB[:books])
  many_to_one :author
  many_to_many :tags, join_table: :books_tags
end

class Tag < Sequel::Model(DB[:tags])
  many_to_many :books, join_table: :books_tags
end

class SequelTest < Minitest::Test
  include DriverAssertions

  def writer(name)
    Author.create(name: name, email: "#{name.downcase.tr(' ', '_')}@dota", born: Date.new(2004, 2, 1))
  end

  def drop(*tables)
    tables.each { |t| DB.drop_table?(t) }
  end

  def test_migration_state
    assert_equal 4, DB[:schema_info].get(:version)
    assert Sequel::Migrator.is_current?(DB, SequelSuite::FIXTURES.join('migrate').to_s)
  end

  def test_rollback_and_redo
    SequelSuite.migrate(DB, 3)
    refute DB.views.include?(:author_names)
    SequelSuite.migrate(DB, nil)
    assert DB.views.include?(:author_names)
  end

  def test_introspection_matches_postgresql
    result = {
      'server_version' => DB.server_version / 10_000,
      'tables' => DB.tables.sort,
      'views' => DB.views.sort,
    }
    SequelSuite::TABLES.each do |table|
      result[table.to_s] = {
        'schema' => Drivers.outcome do
          DB.schema(table, reload: true).map do |name, s|
            [name, s[:type], s[:db_type], s[:default], s[:allow_null], s[:primary_key], s[:auto_increment],
             s[:max_length], DB.literal(s[:ruby_default]), s[:comment]]
          end
        end,
        'indexes' => Drivers.outcome { DB.indexes(table).sort.to_h },
        'foreign_keys' => Drivers.outcome { DB.foreign_key_list(table).sort_by { |fk| fk[:name].to_s } },
        'check_constraints' => Drivers.outcome { DB.check_constraints(table).sort.to_h },
        'primary_key_sequence' => Drivers.outcome { DB.primary_key_sequence(table) },
      }
    end
    assert_golden(SequelSuite::FIXTURES.join('introspection.json'), "#{JSON.pretty_generate(result)}\n")
  end

  def test_primary_key
    xfail_on_serenedb('pg_index.indkey is 1-based on SereneDB, Sequel reads indkey[0] like PostgreSQL, ' \
                      'so Dataset#insert sends RETURNING NULL and returns nil') do
      assert_equal 'id', DB.primary_key(:authors)
      id = DB[:tags].insert(label: 'returning')
      assert_equal 'returning', DB[:tags].where(id: id).get(:label)
      assert_equal id, DB[:tags].insert_conflict(target: :label, update: { label: Sequel[:excluded][:label] })
                                .insert(label: 'returning')
    end
  ensure
    DB[:tags].where(label: 'returning').delete
  end

  def test_model_crud
    author = writer('Pudge')
    published = Time.utc(2024, 5, 6, 7, 8, 9, 123_456)
    book = Book.create(author_id: author.id, title: 'Fresh Meat', pages: 321, rating: 0.1,
                       published_at: published, cover: Sequel.blob("\x00\x01binary".b),
                       keywords: Sequel.pg_array(%w[hook rot], :varchar))
    fresh = Book[book.id]
    assert_match(/\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/, fresh.uid)
    assert_equal 0.1, fresh.rating
    assert_equal published, fresh.published_at.utc
    assert_equal "\x00\x01binary".b, fresh.cover
    assert_equal %w[hook rot], fresh.keywords.to_a
    assert_equal 'Pudge', fresh.author.name
    author.update(royalty: BigDecimal('12.34'), rank: 7)
    assert_equal BigDecimal('12.34'), author.refresh.royalty
    tag = Tag.create(label: 'meat')
    book.add_tag(tag)
    assert_equal ['meat'], Book[book.id].tags.map(&:label)
    assert_equal [book.id], Tag.where(label: 'meat').eager(:books).all.first.books.map(&:id)
    assert_equal 1, Author.where(Sequel[:authors][:id] => author.id).eager_graph(:books).all.first.books.size
    book.remove_all_tags
    book.destroy
    refute Book[book.id]
  end

  def test_dataset_queries
    author = writer('Crystal Maiden')
    books = DB[:books]
    books.import(%i[author_id title pages published_at],
                 Array.new(5) { |i| [author.id, "Frostbite #{i}", i * 100, Time.utc(2020, i + 1, 15)] })
    mine = books.where(author_id: author.id)
    assert_equal 1, mine.where(Sequel.ilike(:title, 'FROSTBITE 1')).count
    assert_equal 3, mine.where(pages: 100..300).count
    assert_equal 4, mine.exclude(pages: 0).count
    assert_equal 2, mine.where(Sequel.|({ pages: 0 }, { pages: 400 })).count
    assert_equal [400, 300], mine.reverse(:pages).limit(2).select_map(:pages)
    assert_equal 1000, mine.sum(:pages)
    assert_equal 200, mine.avg(:pages).to_i
    assert_equal [0, 400], [mine.min(:pages), mine.max(:pages)]
    assert_equal({ author.id => 5 }, mine.group_and_count(:author_id).to_hash(:author_id, :count))
    assert_equal 5, DB[:authors].join(:books, author_id: :id).where(Sequel[:authors][:id] => author.id).count
    assert_equal 1, DB[:authors].where(id: books.where(pages: 400).select(:author_id)).count
    ranked = mine.select(:pages, Sequel.function(:row_number).over(order: Sequel.desc(:pages)).as(:rn))
                 .order(:pages).map(%i[pages rn])
    assert_equal [[0, 5], [100, 4], [200, 3], [300, 2], [400, 1]], ranked
    thin = DB[:thin].with(:thin, mine.where { pages < 200 }).order(:pages).select_map(:pages)
    assert_equal [0, 100], thin
    assert_equal 2, mine.where(pages: 0).union(mine.where(pages: 400)).count
    assert_equal 1, mine.distinct(:author_id).select(:author_id).order(:author_id).count
    assert_equal 5, mine.where(Sequel.lit("published_at >= '2020-01-01'")).count
    seen = []
    mine.order(:id).extension(:pagination).each_page(2) { |page| seen.concat(page.select_map(:pages)) }
    assert_equal [0, 100, 200, 300, 400], seen
  end

  def test_writes
    ids = DB[:tags].returning(:id).multi_insert(Array.new(10) { |i| { label: "bulk-#{i}" } })
    assert_equal 10, ids.size
    upserted = DB[:tags].returning(:id).insert_conflict(target: :label, update: { label: Sequel[:excluded][:label] })
                        .insert(label: 'bulk-0')
    assert_equal [{ id: ids.first }], upserted
    assert_empty DB[:tags].returning(:id).insert_conflict.insert(label: 'bulk-1')
    assert_equal 10, DB[:tags].where(Sequel.like(:label, 'bulk-%')).update(label: Sequel.function(:upper, :label))
    assert_equal [{ id: ids.first, label: 'renamed' }],
                 DB[:tags].where(id: ids.first).returning(:id, :label).update(label: 'renamed')
    assert_equal 9, DB[:tags].where(Sequel.like(:label, 'BULK-%')).delete
    assert_equal 1, DB[:tags].where(label: 'renamed').delete
  end

  def test_prepared_statements_and_bound_variables
    writer('Lich')
    by_name = DB[:authors].where(name: :$name).prepare(:select, :author_by_name)
    assert_equal 'Lich', by_name.call(name: 'Lich').first[:name]
    assert_equal 'Lich', DB.call(:author_by_name, name: 'Lich').first[:name]
    assert_equal 5, DB['SELECT ?::int + ?::int AS total', 2, 3].get(:total)
    assert_equal 'Lich', DB[:authors].where(name: :$n).call(:first, n: 'Lich')[:name]
  end

  def test_transactions
    DB.transaction(rollback: :always) { writer('Earthshaker') }
    refute Author.where(name: 'Earthshaker').any?
    DB.transaction do
      writer('Tiny')
      raise Sequel::Rollback
    end
    refute Author.where(name: 'Tiny').any?
    committed = []
    DB.transaction do
      writer('Mars')
      DB.after_commit { committed << true }
    end
    assert_equal [true], committed
    assert_raises(Sequel::UniqueConstraintViolation) { DB.transaction { 2.times { writer('Clone') } } }
    refute Author.where(name: 'Clone').any?
  end

  def test_isolation_levels
    DB.transaction(isolation: :repeatable) { writer('Oracle') }
    assert_equal 'Oracle', DB.transaction(isolation: :committed) { Author.where(name: 'Oracle').get(:name) }
  end

  def test_serializable_isolation
    xfail_on_serenedb('SereneDB rejects SET TRANSACTION ISOLATION LEVEL SERIALIZABLE') do
      DB.transaction(isolation: :serializable) { writer('Visage') }
      assert Author.where(name: 'Visage').any?
    end
  end

  def test_read_only_transaction
    assert_raises(Sequel::DatabaseError) { DB.transaction(read_only: true) { writer('Silencer') } }
    refute Author.where(name: 'Silencer').any?
  end

  def test_copy
    DB.copy_into(:tags, columns: [:label], data: "copied-a\ncopied-b\n")
    assert_equal 2, DB[:tags].where(Sequel.like(:label, 'copied-%')).count
    csv = DB.copy_table(DB[:tags].where(Sequel.like(:label, 'copied-%')).order(:label).select(:label), format: :csv)
    assert_equal "copied-a\ncopied-b\n", csv
  end

  def test_enum_type
    DB.create_enum(:mood, %w[calm angry])
    DB.create_table(:moods) do
      primary_key :id, serial: true
      mood :mood, null: false, default: 'calm'
    end
    DB[:moods].insert(mood: 'angry')
    assert_equal 'angry', DB[:moods].get(:mood)
    assert_equal %w[calm angry], DB['SELECT unnest(enum_range(NULL::mood))::text AS v'].select_map(:v)
  ensure
    drop(:moods)
    DB.drop_enum(:mood, if_exists: true)
  end

  def test_group_by_primary_key
    author = writer('Shadow Fiend')
    DB[:books].insert(author_id: author.id, title: 'Requiem')
    xfail_on_serenedb('SereneDB requires every selected column in GROUP BY, PostgreSQL accepts ' \
                      'columns functionally dependent on the primary key (#1432)') do
      row = DB[:authors].join(:books, author_id: :id).where(Sequel[:authors][:id] => author.id)
                        .group(Sequel[:authors][:id]).select_all(:authors)
                        .select_append { count(Sequel[:books][:id]).as(:total) }.first
      assert_equal 1, row[:total]
    end
  end

  def test_order_desc_puts_nulls_first
    author = writer('Bane')
    DB[:books].insert(author_id: author.id, title: 'Nightmare')
    DB[:books].insert(author_id: author.id, title: 'Brain Sap', rating: 3.0)
    xfail_on_serenedb('SereneDB sorts NULLs last in DESC order, PostgreSQL sorts them first') do
      assert_equal [nil, 3.0], DB[:books].where(author_id: author.id).reverse(:rating).select_map(:rating)
    end
  end

  def test_delete_parent_after_children
    author = writer('Undying')
    DB[:books].insert(author_id: author.id, title: 'Tombstone')
    xfail_on_serenedb('SereneDB rejects deleting a row whose referencing rows were deleted ' \
                      'earlier in the same transaction (#1433)') do
      DB.transaction do
        DB[:books].where(author_id: author.id).delete
        DB[:authors].where(id: author.id).delete
      end
      refute Author[author.id]
    end
  end

  def test_savepoint
    xfail_on_serenedb('SereneDB has no SAVEPOINT (#722)') do
      DB.transaction do
        writer('Kunkka')
        DB.transaction(savepoint: true) do
          writer('Tidehunter')
          raise Sequel::Rollback
        end
      end
      assert Author.where(name: 'Kunkka').any?
      refute Author.where(name: 'Tidehunter').any?
    end
  end

  def test_row_locks
    author = writer('Sven')
    xfail_on_serenedb('SereneDB has no SELECT ... FOR UPDATE / FOR SHARE (#1431)') do
      DB.transaction do
        assert_equal 'Sven', DB[:authors].where(id: author.id).for_update.get(:name)
        assert_equal 'Sven', DB[:authors].where(id: author.id).for_share.get(:name)
        assert_equal 'Sven', DB[:authors].where(id: author.id).for_update.skip_locked.get(:name)
      end
    end
  end

  def test_cursor
    3.times { |i| DB[:tags].insert(label: "cursor-#{i}") }
    xfail_on_serenedb('SereneDB has no DECLARE CURSOR (#720)') do
      labels = []
      DB[:tags].where(Sequel.like(:label, 'cursor-%')).order(:label).use_cursor(rows_per_fetch: 2)
               .each { |r| labels << r[:label] }
      assert_equal %w[cursor-0 cursor-1 cursor-2], labels
    end
  end

  def test_listen_notify
    xfail_on_serenedb('SereneDB has no LISTEN/NOTIFY (#723)') do
      received = nil
      listener = Thread.new { DB.listen('drv_channel', timeout: 5) { |_, _, payload| received = payload } }
      sleep 0.2
      DB.notify('drv_channel', payload: 'hook')
      listener.join
      assert_equal 'hook', received
    end
  end

  def test_identity_primary_key
    xfail_on_serenedb('SereneDB has no identity columns, Sequel creates primary keys as ' \
                      'GENERATED BY DEFAULT AS IDENTITY on PostgreSQL 10.2+ (#1426)') do
      DB.create_table(:counters) do
        primary_key :id
        Integer :hits
      end
      assert_equal 1, DB[:counters].insert(hits: 1)
    end
  ensure
    drop(:counters)
  end

  def test_create_table_with_index_in_migration
    xfail_on_serenedb('SereneDB has no SAVEPOINT, Sequel creates the indexes of create_table in one ' \
                      'when a migration transaction is open (#722)') do
      DB.transaction do
        DB.create_table(:shelves) do
          primary_key :id, serial: true
          String :label
          index :label
        end
      end
      assert_equal [:shelves_label_index], DB.indexes(:shelves).keys
    end
  ensure
    drop(:shelves)
  end

  def test_combined_alter_table
    DB.create_table(:knobs) { primary_key :id, serial: true }
    xfail_on_serenedb('SereneDB allows one subcommand per ALTER TABLE, Sequel combines operations (#1430)') do
      DB.alter_table(:knobs) do
        add_column :state, String
        add_column :level, Integer
      end
      assert_equal %i[id state level], DB.schema(:knobs, reload: true).map(&:first)
    end
  ensure
    drop(:knobs)
  end

  def test_add_foreign_key
    DB.create_table(:kennels) { primary_key :id, serial: true }
    DB.create_table(:hounds) do
      primary_key :id, serial: true
      Integer :kennel_id
    end
    xfail_on_serenedb('SereneDB cannot add a FOREIGN KEY with ALTER TABLE (#1427)') do
      DB.alter_table(:hounds) { add_foreign_key [:kennel_id], :kennels, name: :hounds_kennel_fk }
    end
  ensure
    drop(:hounds, :kennels)
  end

  def test_deferrable_foreign_key
    DB.create_table(:stables) { primary_key :id, serial: true }
    xfail_on_serenedb('SereneDB rejects DEFERRABLE on constraints (#1381)') do
      DB.create_table(:ponies) do
        primary_key :id, serial: true
        foreign_key :stable_id, :stables, deferrable: true
      end
      DB.transaction do
        DB.run('SET CONSTRAINTS ALL DEFERRED')
        DB[:ponies].insert(stable_id: 1)
        DB[:stables].insert(id: 1)
      end
    end
  ensure
    drop(:ponies, :stables)
  end

  def test_cascading_foreign_key
    DB.create_table(:barns) { primary_key :id, serial: true }
    xfail_on_serenedb('SereneDB rejects ON DELETE CASCADE / SET NULL / SET DEFAULT and ON UPDATE CASCADE') do
      DB.create_table(:cows) do
        primary_key :id, serial: true
        foreign_key :barn_id, :barns, on_delete: :cascade
      end
      barn = DB[:barns].insert
      DB[:cows].insert(barn_id: barn)
      DB[:barns].where(id: barn).delete
      assert_equal 0, DB[:cows].count
    end
  ensure
    drop(:cows, :barns)
  end

  def test_change_type_of_unique_column
    DB.create_table(:handles) do
      primary_key :id, serial: true
      String :name, size: 30, unique: true
    end
    xfail_on_serenedb('SereneDB cannot change the type of a UNIQUE column (#1429)') do
      DB.alter_table(:handles) { set_column_type :name, String, size: 150 }
    end
  ensure
    drop(:handles)
  end

  def test_drop_column_before_indexed_column
    DB.create_table(:labels) do
      primary_key :id, serial: true
      String :legacy
      String :app
      String :model
      unique %i[app model]
    end
    xfail_on_serenedb('SereneDB cannot drop a column that precedes an indexed column (#1428)') do
      DB.alter_table(:labels) { drop_column :legacy }
    end
  ensure
    drop(:labels)
  end

  def test_rename_column_of_referenced_table
    DB.create_table(:owners) do
      primary_key :id, serial: true
      String :nick
    end
    DB.create_table(:pets) do
      primary_key :id, serial: true
      foreign_key :owner_id, :owners
    end
    xfail_on_serenedb('SereneDB cannot rename a column of a table that a foreign key references') do
      DB.alter_table(:owners) { rename_column :nick, :handle }
    end
  ensure
    drop(:pets, :owners)
  end

  def test_jsonb_column
    xfail_on_serenedb('SereneDB has no jsonb column type (#613)') do
      DB.create_table(:documents) do
        primary_key :id, serial: true
        jsonb :data, null: false
      end
      DB[:documents].insert(data: Sequel.lit(%q('{"hero": "pudge", "level": 25}'::jsonb)))
      assert_equal 1, DB[:documents].where(Sequel.lit(%q(data @> '{"level": 25}'))).count
    end
  ensure
    drop(:documents)
  end
end
