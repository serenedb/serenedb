# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `bin/rails
# db:schema:load`. When creating a new database, `bin/rails db:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema[8.1].define(version: 2026_01_01_000006) do
  # These are extensions that must be enabled in order to support this database
  enable_extension "pg_catalog.plpgsql"

  # Custom types defined in this database.
  # Note that some types may not work with other database engines. Be careful if changing database.
  create_enum "mood", ["calm", "angry"]

  create_table "authors", comment: "writers", force: :cascade do |t|
    t.string "name", limit: 100, null: false
    t.string "email"
    t.text "bio", comment: "about"
    t.date "born"
    t.integer "rank", limit: 2
    t.decimal "royalty", precision: 8, scale: 2, default: "0.0"
    t.boolean "active", default: true, null: false
    t.enum "mood", default: "calm", null: false, enum_type: "mood"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["born"], name: "authors_born_idx"
    t.index ["email"], name: "authors_email_partial", where: "(email IS NOT NULL)"
    t.index ["name"], name: "index_authors_on_name", unique: true
    t.check_constraint "rank >= 0", name: "authors_rank_nonneg"
  end

  create_table "books", force: :cascade do |t|
    t.bigint "author_id", null: false
    t.string "title", limit: 200, null: false
    t.integer "pages", default: 0, null: false
    t.float "rating"
    t.datetime "published_at"
    t.binary "cover"
    t.uuid "uid", default: -> { "gen_random_uuid()" }, null: false
    t.string "keywords", default: [], array: true
    t.integer "lock_version", default: 0, null: false
    t.integer "reviews_count", default: 0, null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["author_id", "title"], name: "index_books_on_author_id_and_title", unique: true
    t.index ["author_id"], name: "index_books_on_author_id"
    t.index ["published_at"], name: "index_books_on_published_at", order: :desc
    t.check_constraint "pages >= 0", name: "books_pages_nonneg"
  end

  create_table "books_tags", id: false, force: :cascade do |t|
    t.bigint "book_id", null: false
    t.bigint "tag_id", null: false
    t.index ["book_id", "tag_id"], name: "index_books_tags_on_book_id_and_tag_id", unique: true
  end

  create_table "reviews", force: :cascade do |t|
    t.bigint "book_id", null: false
    t.integer "stars", limit: 2, null: false
    t.time "opened_at"
    t.bigint "likes", default: 0, null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["book_id"], name: "index_reviews_on_book_id", unique: true
  end

  create_table "shelves", force: :cascade do |t|
    t.string "label"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
  end

  create_table "tags", force: :cascade do |t|
    t.string "label", null: false
    t.index ["label"], name: "index_tags_on_label", unique: true
  end

  add_foreign_key "books", "authors"
  add_foreign_key "reviews", "books", on_delete: :restrict
end
