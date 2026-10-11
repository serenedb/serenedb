Sequel.migration do
  change do
    create_table(:tags) do
      primary_key :id, serial: true
      String :label, null: false, unique: true
    end
    create_join_table({ book_id: :books, tag_id: :tags }, no_index: true)
    add_index :books_tags, %i[tag_id book_id]
  end
end
