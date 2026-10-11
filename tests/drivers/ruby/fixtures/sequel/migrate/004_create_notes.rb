Sequel.migration do
  up do
    create_table(:notes) do
      primary_key :id, serial: true
      String :body
    end
    alter_table(:notes) { add_column :nickname, String }
    alter_table(:notes) { rename_column :nickname, :handle }
    alter_table(:notes) { set_column_default :handle, 'anon' }
    from(:notes).where(handle: nil).update(handle: 'anon')
    alter_table(:notes) { set_column_not_null :handle }
    alter_table(:notes) { add_index :handle, name: :notes_handle_idx }
    alter_table(:notes) { drop_index :handle, name: :notes_handle_idx }
    alter_table(:notes) { drop_column :handle }
    alter_table(:notes) { set_column_type :body, :text }
    alter_table(:notes) { add_constraint(:notes_body_len) { char_length(body) < 1000 } }
    alter_table(:notes) { drop_constraint :notes_body_len }
    alter_table(:notes) { add_unique_constraint :body, name: :notes_body_key }
    rename_table :notes, :memos
    run 'CREATE VIEW author_names AS SELECT id, name FROM authors'
  end

  down do
    run 'DROP VIEW author_names'
    drop_table :memos
  end
end
