require_relative 'support'
require 'active_record'
require 'stringio'
require 'minitest/autorun'

Drivers.report_junit('activerecord')
ActiveRecord::Migration.verbose = false
ActiveRecord.default_timezone = :utc

module ArSuite
  DATABASE = Drivers.database_name('ar')
  FIXTURES = Drivers::FIXTURES.join('activerecord')
  TABLES = %w[authors books books_tags reviews shelves tags].freeze

  module_function

  def config(advisory_locks)
    args = Drivers.conn_args
    { adapter: 'postgresql', host: args[:host], port: args[:port], username: args[:user],
      database: DATABASE, advisory_locks: advisory_locks }
  end

  def connection
    ActiveRecord::Base.lease_connection
  end

  def migration_context
    ActiveRecord::MigrationContext.new(FIXTURES.join('migrate').to_s)
  end

  def migrate(statements)
    Class.new(ActiveRecord::Migration[8.1]) { define_method(:change) { instance_eval(&statements) } }
         .new.migrate(:up)
  end
end

Drivers.recreate_database(ArSuite::DATABASE)
ActiveRecord::Base.establish_connection(ArSuite.config(false))
ArSuite.migration_context.migrate
Minitest.after_run do
  ActiveRecord::Base.connection_handler.clear_all_connections!(:all)
  Drivers.drop_database(ArSuite::DATABASE)
end

class Author < ActiveRecord::Base
  has_many :books, dependent: :destroy
  enum :mood, { calm: 'calm', angry: 'angry' }
end

class CaselessAuthor < ActiveRecord::Base
  self.table_name = 'authors'
  validates :name, uniqueness: { case_sensitive: false }
end

class Book < ActiveRecord::Base
  belongs_to :author
  has_one :review
  has_and_belongs_to_many :tags
end

class Tag < ActiveRecord::Base
  has_and_belongs_to_many :books
end

class Review < ActiveRecord::Base
  belongs_to :book
end

class CountedReview < ActiveRecord::Base
  self.table_name = 'reviews'
  belongs_to :book, counter_cache: :reviews_count
end

class AuthorName < ActiveRecord::Base
  self.primary_key = 'id'
end

class ActiveRecordTest < Minitest::Test
  include DriverAssertions

  def conn
    ArSuite.connection
  end

  def writer(name)
    Author.create!(name: name, email: "#{name.downcase.tr(' ', '_')}@dota", born: Date.new(2004, 2, 1))
  end

  def test_migration_state
    assert_empty ArSuite.migration_context.open.pending_migrations
    assert_equal 20260101000006, ArSuite.migration_context.current_version
    assert ArSuite.connection.table_exists?('ar_internal_metadata')
  end

  def test_migrate_with_advisory_locks
    xfail_on_serenedb('SereneDB has no pg_try_advisory_lock, ActiveRecord migrations take one') do
      ActiveRecord::Base.establish_connection(ArSuite.config(true))
      ArSuite.migration_context.migrate
    ensure
      ActiveRecord::Base.establish_connection(ArSuite.config(false))
    end
  end

  def test_rollback_and_redo
    ArSuite.migration_context.rollback(1)
    refute conn.table_exists?('shelves')
    refute conn.view_exists?('author_names')
    ArSuite.migration_context.migrate
    assert conn.table_exists?('shelves')
  end

  def test_schema_dump_matches_postgresql
    xfail_on_serenedb('generate_subscripts(...) AS idx does not name its column idx, ' \
                      "ActiveRecord's foreign_keys query reads c.conkey[idx]") do
      io = StringIO.new
      ActiveRecord::SchemaDumper.dump(ActiveRecord::Base.connection_pool, io)
      assert_golden(ArSuite::FIXTURES.join('schema.rb'), io.string)
    end
  end

  def test_introspection_matches_postgresql
    result = {
      'tables' => conn.tables.sort & (ArSuite::TABLES + %w[ar_internal_metadata schema_migrations]),
      'views' => conn.views.sort,
      'enum_types' => Drivers.outcome { conn.enum_types },
      'schema_names' => Drivers.outcome { conn.schema_names },
      'extensions' => Drivers.outcome { conn.extensions },
    }
    ArSuite::TABLES.each do |table|
      result[table] = {
        'comment' => Drivers.outcome { conn.table_comment(table) },
        'primary_keys' => Drivers.outcome { conn.primary_keys(table) },
        'pk_and_sequence' => Drivers.outcome { conn.pk_and_sequence_for(table)&.map(&:to_s) },
        'columns' => Drivers.outcome do
          conn.columns(table).map do |c|
            [c.name, c.sql_type, c.type.to_s, c.null, c.default, c.default_function, c.limit,
             c.precision, c.scale, c.comment, c.collation, c.serial?, c.identity?, c.array?]
          end
        end,
        'indexes' => Drivers.outcome do
          conn.indexes(table).map do |i|
            [i.name, i.columns, i.unique, i.using.to_s, i.where, i.orders, i.opclasses, i.nulls_not_distinct]
          end
        end,
        'foreign_keys' => Drivers.outcome do
          conn.foreign_keys(table).map do |fk|
            [fk.name, fk.to_table, fk.column, fk.primary_key, fk.on_delete.to_s, fk.on_update.to_s,
             fk.deferrable.to_s]
          end
        end,
        'check_constraints' => Drivers.outcome do
          conn.check_constraints(table).map { |cc| [cc.name, cc.expression, cc.validated?] }
        end,
        'unique_constraints' => Drivers.outcome { conn.unique_constraints(table).map(&:name) },
      }
    end
    assert_golden(ArSuite::FIXTURES.join('introspection.json'), "#{JSON.pretty_generate(result)}\n")
  end

  def test_schema_predicates
    assert conn.data_source_exists?('authors')
    assert conn.view_exists?('author_names')
    assert conn.column_exists?(:books, :keywords)
    assert conn.index_name_exists?(:authors, 'authors_email_partial')
    assert_equal 'books_id_seq', conn.default_sequence_name('books', 'id').split('.').last.delete('"')
    assert_equal 'public.books_id_seq', conn.serial_sequence('books', 'id')
    assert_equal 'id', conn.primary_key('books')
    assert conn.check_constraint_exists?(:books, name: 'books_pages_nonneg')
  end

  def test_index_predicates
    xfail_on_serenedb("generate_subscripts(...) AS k does not name its column k, ActiveRecord's " \
                      'indexes query reads pg_get_indexdef(d.indexrelid, k + 1, true)') do
      assert conn.index_exists?(:books, %i[author_id title], unique: true)
      assert conn.index_exists?(:authors, :email, where: 'email IS NOT NULL')
    end
  end

  def test_rename_column
    ArSuite.migrate(proc { add_column :shelves, :tint, :string })
    xfail_on_serenedb("generate_subscripts(...) AS k does not name its column k, rename_column reads " \
                      "ActiveRecord's indexes query to rename the column's indexes") do
      ArSuite.migrate(proc { rename_column :shelves, :tint, :colour })
      assert conn.column_exists?(:shelves, :colour)
    end
  ensure
    conn.remove_column(:shelves, :tint, if_exists: true)
    conn.remove_column(:shelves, :colour, if_exists: true)
  end

  def test_remove_index_by_column
    ArSuite.migrate(proc { add_index :shelves, :label })
    xfail_on_serenedb("generate_subscripts(...) AS k does not name its column k, ActiveRecord's " \
                      'indexes query reads pg_get_indexdef(d.indexrelid, k + 1, true)') do
      ArSuite.migrate(proc { remove_index :shelves, :label })
      refute conn.index_exists?(:shelves, :label)
    end
  ensure
    conn.execute('DROP INDEX IF EXISTS index_shelves_on_label')
  end

  def test_foreign_key_predicates
    xfail_on_serenedb("generate_subscripts(...) AS idx does not name its column idx, " \
                      "ActiveRecord's foreign_keys query reads c.conkey[idx]") do
      assert conn.foreign_key_exists?(:books, :authors)
      assert conn.foreign_key_exists?(:reviews, :books)
    end
  end

  def test_crud_round_trip_of_every_column
    author = writer('Pudge')
    published = Time.utc(2024, 5, 6, 7, 8, 9, 123_456)
    book = author.books.create!(title: 'Fresh Meat', pages: 321, rating: 4.5, published_at: published,
                                cover: "\x00\x01binary".b, keywords: %w[hook rot])
    fresh = Book.find(book.id)
    assert_match(/\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/, fresh.uid)
    assert_equal 4.5, fresh.rating
    assert_equal published, fresh.published_at
    assert_equal 6, fresh.published_at.usec.to_s.size
    assert_equal "\x00\x01binary".b, fresh.cover
    assert_equal %w[hook rot], fresh.keywords
    assert_equal 'calm', fresh.author.mood
    fresh.author.angry!
    assert Author.angry.exists?(author.id)
    assert_equal BigDecimal('0.0'), fresh.author.royalty
    author.update!(royalty: BigDecimal('12.34'), rank: 7, bio: 'hook')
    assert_equal BigDecimal('12.34'), author.reload.royalty
    assert_equal 7, author.rank
  end

  def test_column_defaults
    xfail_on_serenedb("pg_get_expr renders defaults without PostgreSQL's casts ('calm' for 'calm'::mood, " \
                      "nextval('s') for nextval('s'::regclass)), ActiveRecord drops uncast literals") do
      assert_equal 'calm', Author.new.mood
      assert_equal [], Book.new.keywords
      assert Author.columns_hash['id'].serial?
    end
  end

  def test_float_is_double_precision
    book = writer('Arc Warden').books.create!(title: 'Tempest Double', rating: 16_777_217.0)
    xfail_on_serenedb('SereneDB creates float columns as 4-byte real, PostgreSQL as double precision') do
      assert_equal 16_777_217.0, Book.find(book.id).rating
    end
  end

  def test_queries
    author = writer('Crystal Maiden')
    5.times do |i|
      author.books.create!(title: "Frostbite #{i}", pages: i * 100,
                           published_at: Time.utc(2020, i + 1, 15), keywords: ["k#{i}", 'cold'])
    end
    books = author.books
    assert_equal 1, books.where('title ILIKE ?', 'FROSTBITE 1').count
    assert_equal 3, books.where(pages: 100..300).count
    assert_equal 2, books.where(pages: [0, 400, 999]).count
    assert_equal 4, books.where.not(pages: 0).count
    assert_equal 2, books.where(pages: 0).or(books.where(pages: 400)).count
    assert_equal [400, 300], books.order(pages: :desc).limit(2).pluck(:pages)
    assert_equal 1, books.where("'k3' = ANY(keywords)").count
    assert_equal 5, books.where('keywords @> ARRAY[?]::varchar[]', 'cold').count
    assert_equal 1000, books.sum(:pages)
    assert_equal 200, books.average(:pages).to_i
    assert_equal({ author.id => 5 }, books.group(:author_id).count)
    assert_equal 400, books.maximum(:pages)
    assert_equal 'Frostbite 0', books.order(:published_at).pick(:title)
    assert_equal 5, Author.joins(:books).where(id: author.id).count
    assert_equal 1, Author.where(id: Book.where(pages: 400).select(:author_id)).count
    assert_equal 5, Author.includes(:books).find(author.id).books.size
    assert_equal 5, Author.eager_load(:books).find(author.id).books.size
    assert_equal 5, Author.preload(:books).find(author.id).books.size
    assert_equal 1, Author.left_outer_joins(:books).where(books: { pages: 300 }).count
    assert_equal [0, 100], Book.with(thin: books.where('pages < 200')).from('thin AS books').order(:pages).pluck(:pages)
    assert_equal 2, books.where(published_at: Time.utc(2020, 2, 1)..Time.utc(2020, 3, 31)).count
    assert_equal 5, books.distinct.count(:author_id) * 5
    seen = []
    books.find_each(batch_size: 2) { |b| seen << b.pages }
    assert_equal [0, 100, 200, 300, 400], seen.sort
    assert_equal 3, books.in_batches(of: 2).count
    assert books.where(pages: 1..).explain.inspect.include?('books')
  end

  def test_group_by_primary_key
    xfail_on_serenedb('SereneDB requires every selected column in GROUP BY, PostgreSQL accepts ' \
                      'columns functionally dependent on the primary key (#1432)') do
      author = writer('Shadow Fiend')
      author.books.create!(title: 'Requiem', pages: 10)
      row = Author.joins(:books).where(id: author.id).group(:id).select('authors.*, COUNT(books.id) AS total').first
      assert_equal 1, row.total
    end
  end

  def test_order_desc_puts_nulls_first
    xfail_on_serenedb('SereneDB sorts NULLs last in DESC order, PostgreSQL sorts them first') do
      author = writer('Bane')
      author.books.create!(title: 'Nightmare', rating: nil)
      author.books.create!(title: 'Brain Sap', rating: 3.0)
      assert_equal [nil, 3.0], author.books.order(rating: :desc).pluck(:rating)
    end
  end

  def test_bulk_writes
    xfail_on_serenedb("generate_subscripts(...) AS k does not name its column k, insert_all and " \
                      "upsert_all read ActiveRecord's indexes query to find the conflict target") do
      rows = Array.new(20) { |i| { label: "bulk-#{i}" } }
      inserted = Tag.insert_all!(rows, returning: %i[id label])
      assert_equal 20, inserted.rows.size
      upserted = Tag.upsert_all([{ label: 'bulk-0' }, { label: 'bulk-new' }], unique_by: :label,
                                                                             returning: :label)
      assert_equal %w[bulk-new], upserted.rows.flatten
      assert_equal 21, Tag.where('label LIKE ?', 'bulk-%').count
      Author.upsert_all([{ name: 'Viper', email: 'v1' }], unique_by: :name)
      updated = Author.upsert_all([{ name: 'Viper', email: 'v2' }], unique_by: :name, returning: %i[name email])
      assert_equal [%w[Viper v2]], updated.rows
      assert_equal 'v2', Author.find_by!(name: 'Viper').email
    ensure
      Tag.where('label LIKE ?', 'bulk-%').delete_all
    end
  end

  def test_set_based_writes
    20.times { |i| Tag.create!(label: "set-#{i}") }
    assert_equal 20, Tag.where('label LIKE ?', 'set-%').update_all('label = upper(label)')
    assert_equal 20, Tag.where('label LIKE ?', 'SET-%').delete_all
    tag = Tag.find_or_create_by!(label: 'blink')
    assert_equal tag, Tag.find_or_create_by!(label: 'blink')
    assert_equal tag.id, Tag.create_or_find_by!(label: 'blink').id
    assert_equal 1, Tag.where(label: 'blink').destroy_all.size
  end

  def test_counter_cache
    book = writer('Ogre').books.create!(title: 'Multicast')
    xfail_on_serenedb('UPDATE ... SET c = COALESCE(c, 0) + $1 with an untyped parameter answers ' \
                      "'current transaction is aborted' inside a transaction and UPDATE 0 outside one") do
      CountedReview.create!(book: book, stars: 4)
      assert_equal 1, book.reload.reviews_count
      Book.update_counters(book.id, reviews_count: 2)
      assert_equal 3, book.reload.reviews_count
    end
  end

  def test_associations
    author = writer('Lion')
    book = author.books.create!(title: 'Finger of Death')
    book.tags << Tag.create!(label: 'nuke') << Tag.create!(label: 'mana')
    assert_equal %w[mana nuke], Book.find(book.id).tags.order(:label).pluck(:label)
    assert_equal [book.id], Tag.find_by!(label: 'nuke').books.pluck(:id)
    Review.create!(book: book, stars: 5, opened_at: '13:14:15', likes: 2**40)
    review = book.review
    assert_equal 2**40, review.likes
    assert_equal '13:14:15', review.opened_at.strftime('%H:%M:%S')
    book.tags.delete(Tag.find_by!(label: 'mana'))
    assert_equal 1, book.tags.count
  end

  def test_optimistic_locking
    book = writer('Tinker').books.create!(title: 'Rearm')
    stale = Book.find(book.id)
    book.update!(pages: 2)
    assert_raises(ActiveRecord::StaleObjectError) { stale.update!(pages: 3) }
  end

  def test_pessimistic_locking
    book = writer('Sven').books.create!(title: 'Storm Hammer')
    xfail_on_serenedb('SereneDB has no SELECT ... FOR UPDATE (#1431)') do
      book.with_lock { book.update!(pages: 10) }
      assert_equal 10, Book.lock.find(book.id).pages
    end
  end

  def test_transactions
    Author.transaction do
      writer('Earthshaker')
      raise ActiveRecord::Rollback
    end
    refute Author.exists?(name: 'Earthshaker')
    assert_raises(ActiveRecord::RecordNotUnique) do
      Author.transaction do
        writer('Tiny')
        Author.create!(name: 'tiny-dup', email: 'x').update_columns(name: 'Tiny')
      end
    end
    refute Author.exists?(name: 'Tiny')
    committed = []
    Author.transaction do
      writer('Mars')
      ActiveRecord.after_all_transactions_commit { committed << true }
    end
    assert_equal [true], committed
  end

  def test_nested_transaction_savepoint
    xfail_on_serenedb('SereneDB has no SAVEPOINT, requires_new opens one (#722)') do
      Author.transaction do
        writer('Kunkka')
        Author.transaction(requires_new: true) do
          writer('Tidehunter')
          raise ActiveRecord::Rollback
        end
      end
      assert Author.exists?(name: 'Kunkka')
      refute Author.exists?(name: 'Tidehunter')
    end
  end

  def test_destroy_with_dependent_children
    author = writer('Undying')
    author.books.create!(title: 'Tombstone')
    xfail_on_serenedb('SereneDB rejects deleting a row whose referencing rows were deleted ' \
                      'earlier in the same transaction (#1433)') do
      author.destroy!
      refute Author.exists?(author.id)
    end
  end

  def test_case_insensitive_uniqueness_validation
    writer('Lina')
    xfail_on_serenedb('ARRAY[casttarget]::oidvector stays oid[], joining it to pg_proc.proargtypes ' \
                      'trips the hash join type assertion') do
      refute CaselessAuthor.new(name: 'LINA').valid?
    end
  end

  def test_raw_sql_and_binds
    writer('Lich')
    assert_equal 'Lich', conn.select_value(Author.sanitize_sql(['SELECT name FROM authors WHERE name = ?', 'Lich']))
    result = conn.exec_query('SELECT $1::int + $2::int AS total', 'SQL',
                             [ActiveRecord::Relation::QueryAttribute.new('a', 2, ActiveRecord::Type::Integer.new),
                              ActiveRecord::Relation::QueryAttribute.new('b', 3, ActiveRecord::Type::Integer.new)])
    assert_equal [{ 'total' => 5 }], result.to_a
    assert_equal %w[calm angry], conn.select_values("SELECT unnest(enum_range(NULL::mood))::text")
  end

  def test_view_model
    author = writer('Razor')
    assert_equal 'Razor', AuthorName.find(author.id).name
  end

  def test_reset_pk_sequence
    Tag.create!(id: 900_000, label: 'manual-id')
    conn.reset_pk_sequence!('tags')
    assert_operator Tag.create!(label: 'after-reset').id, :>, 900_000
  end

  def test_interval_column
    ArSuite.migrate(proc { create_table(:spans) { |t| t.interval :span } })
    span = Class.new(ActiveRecord::Base) { self.table_name = 'spans' }
    row = span.create!(span: 3.hours + 7.seconds)
    xfail_on_serenedb('SereneDB ignores IntervalStyle, ActiveRecord sets iso_8601 and parses ISO durations') do
      assert_equal 3.hours + 7.seconds, span.find(row.id).span
    end
  ensure
    conn.drop_table(:spans, if_exists: true)
  end

  def test_add_foreign_key
    ArSuite.migrate(proc do
      create_table(:kennels)
      create_table(:hounds) { |t| t.bigint :kennel_id }
    end)
    xfail_on_serenedb('SereneDB cannot add a FOREIGN KEY with ALTER TABLE (#1427)') do
      ArSuite.migrate(proc { add_foreign_key :hounds, :kennels })
    end
  ensure
    conn.drop_table(:hounds, if_exists: true)
    conn.drop_table(:kennels, if_exists: true)
  end

  def test_deferrable_foreign_key
    ArSuite.migrate(proc { create_table(:stables) })
    xfail_on_serenedb('SereneDB rejects DEFERRABLE on constraints (#1381)') do
      ArSuite.migrate(proc do
        create_table(:ponies) { |t| t.references :stable, foreign_key: { deferrable: :deferred } }
      end)
    end
  ensure
    conn.drop_table(:ponies, if_exists: true)
    conn.drop_table(:stables, if_exists: true)
  end

  def test_cascading_foreign_key
    ArSuite.migrate(proc { create_table(:barns) })
    xfail_on_serenedb('SereneDB rejects ON DELETE CASCADE / SET NULL / SET DEFAULT and ON UPDATE CASCADE') do
      ArSuite.migrate(proc do
        create_table(:cows) { |t| t.references :barn, foreign_key: { on_delete: :cascade, on_update: :cascade } }
      end)
      barn = Class.new(ActiveRecord::Base) { self.table_name = 'barns' }.create!
      cows = Class.new(ActiveRecord::Base) { self.table_name = 'cows' }
      cows.create!(barn_id: barn.id)
      barn.delete
      assert_equal 0, cows.count
    end
  ensure
    conn.drop_table(:cows, if_exists: true)
    conn.drop_table(:barns, if_exists: true)
  end

  def test_change_column_type_of_unique_column
    ArSuite.migrate(proc { create_table(:handles) { |t| t.string :name, limit: 30, index: { unique: true } } })
    xfail_on_serenedb('SereneDB cannot change the type of a UNIQUE column (#1429)') do
      ArSuite.migrate(proc { change_column :handles, :name, :string, limit: 150 })
    end
  ensure
    conn.drop_table(:handles, if_exists: true)
  end

  def test_change_column_with_default_and_null
    ArSuite.migrate(proc { create_table(:knobs) { |t| t.string :state } })
    xfail_on_serenedb('SereneDB allows one subcommand per ALTER TABLE, change_column with ' \
                      ':default and :null emits three (#1430)') do
      ArSuite.migrate(proc { change_column :knobs, :state, :text, default: 'off', null: false })
      assert_equal 'off', conn.columns(:knobs).find { |c| c.name == 'state' }.default
    end
  ensure
    conn.drop_table(:knobs, if_exists: true)
  end

  def test_remove_column_before_indexed_column
    ArSuite.migrate(proc do
      create_table(:labels) do |t|
        t.string :legacy
        t.string :app
        t.string :model
        t.index %i[app model], unique: true
      end
    end)
    xfail_on_serenedb('SereneDB cannot drop a column that precedes an indexed column (#1428)') do
      ArSuite.migrate(proc { remove_column :labels, :legacy, :string })
    end
  ensure
    conn.drop_table(:labels, if_exists: true)
  end

  def with_referenced_table
    ArSuite.migrate(proc do
      create_table(:owners) do |t|
        t.string :name
        t.string :nick
      end
      create_table(:pets) { |t| t.references :owner, foreign_key: true }
    end)
    yield
  ensure
    conn.drop_table(:pets, if_exists: true)
    conn.drop_table(:owners, if_exists: true)
  end

  def test_rename_column_of_referenced_table
    with_referenced_table do
      xfail_on_serenedb('SereneDB cannot rename a column of a table that a foreign key references') do
        ArSuite.migrate(proc { rename_column :owners, :nick, :handle })
      end
    end
  end

  def test_change_column_of_referenced_table
    with_referenced_table do
      xfail_on_serenedb('SereneDB cannot change a column type of a table that a foreign key references') do
        ArSuite.migrate(proc { change_column :owners, :nick, :text })
      end
    end
  end

  def test_remove_column_of_referenced_table
    with_referenced_table do
      xfail_on_serenedb('SereneDB cannot drop a column of a table that a foreign key references') do
        ArSuite.migrate(proc { remove_column :owners, :nick, :string })
      end
    end
  end

  def test_rename_referenced_table
    with_referenced_table do
      xfail_on_serenedb('SereneDB cannot rename a table that a foreign key references') do
        ArSuite.migrate(proc { rename_table :owners, :keepers })
        ArSuite.migrate(proc { rename_table :keepers, :owners })
      end
    end
  end

  def test_rename_foreign_key_column
    with_referenced_table do
      xfail_on_serenedb('SereneDB cannot rename a column that a foreign key constraint uses') do
        ArSuite.migrate(proc { rename_column :pets, :owner_id, :keeper_id })
      end
    end
  end

  def test_drop_renamed_referencing_table
    ArSuite.migrate(proc do
      create_table(:hives)
      create_table(:bees) { |t| t.references :hive, foreign_key: true }
    end)
    conn.execute('ALTER TABLE bees RENAME TO drones')
    xfail_on_serenedb('after the referencing table is renamed and dropped, SereneDB still refuses to ' \
                      'drop the referenced table under the old referencing name') do
      conn.drop_table(:drones)
      conn.drop_table(:hives)
    end
  end

  def test_rename_table_renames_sequence
    ArSuite.migrate(proc { create_table(:drafts) { |t| t.string :body } })
    xfail_on_serenedb('SereneDB lists <table>_pkey in pg_class but ALTER INDEX cannot address it, and ' \
                      'cannot rename an owned sequence, rename_table renames both') do
      ArSuite.migrate(proc { rename_table :drafts, :notes })
      assert_equal 'public.notes_id_seq', conn.serial_sequence('notes', 'id')
    end
  ensure
    conn.drop_table(:notes, if_exists: true)
    conn.drop_table(:drafts, if_exists: true)
  end

  def test_jsonb_column
    xfail_on_serenedb('SereneDB has no jsonb column type (#613)') do
      ArSuite.migrate(proc { create_table(:documents) { |t| t.jsonb :data, null: false, default: {} } })
      doc = Class.new(ActiveRecord::Base) { self.table_name = 'documents' }
      doc.create!(data: { 'hero' => 'pudge', 'level' => 25 })
      assert_equal 1, doc.where("data->>'hero' = ?", 'pudge').count
      assert_equal 1, doc.where('data @> ?', { level: 25 }.to_json).count
    end
  ensure
    conn.drop_table(:documents, if_exists: true)
  end

  def test_create_enum
    xfail_on_serenedb('SereneDB has no DO blocks, create_enum wraps CREATE TYPE in one') do
      ArSuite.migrate(proc { create_enum :color, %w[red green] })
      assert_includes conn.enum_types.map(&:first), 'color'
    end
  ensure
    conn.execute('DROP TYPE IF EXISTS color')
  end

  def test_fixtures_disable_referential_integrity
    xfail_on_serenedb('SereneDB has no ALTER TABLE ... DISABLE TRIGGER ALL, test fixtures load through it') do
      conn.disable_referential_integrity do
        conn.execute('INSERT INTO books (id, author_id, title, created_at, updated_at) ' \
                     "VALUES (990001, 990001, 'Orphan', now(), now())")
        conn.execute("INSERT INTO authors (id, name, created_at, updated_at) VALUES (990001, 'Late', now(), now())")
      end
      assert_equal 'Late', Book.find(990_001).author.name
    ensure
      conn.execute('DELETE FROM books WHERE id = 990001')
      conn.execute('DELETE FROM authors WHERE id = 990001')
    end
  end
end
