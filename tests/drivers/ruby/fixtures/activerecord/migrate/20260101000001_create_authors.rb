class CreateAuthors < ActiveRecord::Migration[8.1]
  def change
    reversible do |dir|
      dir.up { execute "CREATE TYPE mood AS ENUM ('calm', 'angry')" }
      dir.down { execute 'DROP TYPE mood' }
    end
    create_table :authors, comment: 'people who write books' do |t|
      t.string :name, null: false, limit: 100
      t.string :email
      t.text :bio, comment: 'free text about the author'
      t.date :born
      t.integer :rank, limit: 2
      t.decimal :royalty, precision: 8, scale: 2, default: '0.0'
      t.boolean :active, null: false, default: true
      t.enum :mood, enum_type: :mood, null: false, default: 'calm'
      t.timestamps
    end
    add_index :authors, :name, unique: true
    add_index :authors, :born
    add_index :authors, :email, where: 'email IS NOT NULL', name: 'authors_email_partial'
  end
end
