class CreateBooks < ActiveRecord::Migration[8.1]
  def change
    create_table :books do |t|
      t.references :author, null: false, foreign_key: true
      t.string :title, null: false, limit: 200
      t.integer :pages, null: false, default: 0
      t.float :rating
      t.datetime :published_at
      t.binary :cover
      t.uuid :uid, null: false, default: -> { 'gen_random_uuid()' }
      t.string :keywords, array: true, default: []
      t.integer :lock_version, null: false, default: 0
      t.integer :reviews_count, null: false, default: 0
      t.check_constraint 'pages >= 0', name: 'books_pages_nonneg'
      t.timestamps
    end
    add_index :books, %i[author_id title], unique: true
    add_index :books, :published_at, order: { published_at: :desc }
  end
end
