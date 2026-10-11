class AlterSchema < ActiveRecord::Migration[8.1]
  def up
    change_table_comment :authors, 'writers'
    change_column_comment :authors, :bio, 'about'
    add_check_constraint :authors, 'rank >= 0', name: 'authors_rank_nonneg'
    rename_index :authors, 'index_authors_on_born', 'authors_born_idx'
    add_column :tags, :nickname, :string
    execute 'ALTER TABLE tags RENAME COLUMN nickname TO handle'
    change_column_default :tags, :handle, 'anon'
    change_column_null :tags, :handle, false, 'anon'
    add_index :tags, :handle
    remove_index :tags, name: 'index_tags_on_handle'
    remove_column :tags, :handle
  end

  def down
    rename_index :authors, 'authors_born_idx', 'index_authors_on_born'
    remove_check_constraint :authors, name: 'authors_rank_nonneg'
    change_column_comment :authors, :bio, 'free text about the author'
    change_table_comment :authors, 'people who write books'
  end
end
