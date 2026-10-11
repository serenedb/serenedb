Sequel.migration do
  change do
    create_table(:books) do
      primary_key :id, serial: true
      foreign_key :author_id, :authors, null: false
      String :title, size: 200, null: false
      Integer :pages, null: false, default: 0
      Float :rating
      DateTime :published_at
      File :cover
      uuid :uid, null: false, default: Sequel.function(:gen_random_uuid)
      column :keywords, 'varchar[]', null: false, default: Sequel.lit("'{}'")
      unique %i[author_id title], name: :books_author_title_key
      constraint(:books_pages_nonneg) { pages >= 0 }
    end
    add_index :books, :published_at
  end
end
