class CreateTags < ActiveRecord::Migration[8.1]
  def change
    create_table :tags do |t|
      t.string :label, null: false, index: { unique: true }
    end
    create_join_table :books, :tags do |t|
      t.index %i[book_id tag_id], unique: true
    end
  end
end
