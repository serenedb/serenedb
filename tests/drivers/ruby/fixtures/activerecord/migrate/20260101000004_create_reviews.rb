class CreateReviews < ActiveRecord::Migration[8.1]
  def change
    create_table :reviews do |t|
      t.references :book, null: false, foreign_key: { on_delete: :restrict }, index: { unique: true }
      t.integer :stars, limit: 2, null: false
      t.time :opened_at
      t.bigint :likes, null: false, default: 0
      t.timestamps
    end
  end
end
