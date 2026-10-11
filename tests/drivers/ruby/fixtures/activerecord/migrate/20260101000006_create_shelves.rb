class CreateShelves < ActiveRecord::Migration[8.1]
  def up
    create_table :shelves do |t|
      t.string :label
      t.timestamps
    end
    execute 'CREATE VIEW author_names AS SELECT id, name FROM authors'
  end

  def down
    execute 'DROP VIEW author_names'
    drop_table :shelves
  end
end
