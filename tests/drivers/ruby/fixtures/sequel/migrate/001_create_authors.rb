Sequel.migration do
  change do
    create_table(:authors, comment: 'people who write books') do
      primary_key :id, serial: true
      String :name, size: 100, null: false, unique: true
      String :email
      String :bio, text: true
      Date :born
      Integer :rank
      BigDecimal :royalty, size: [8, 2], default: 0
      TrueClass :active, null: false, default: true
      DateTime :created_at, default: Sequel::CURRENT_TIMESTAMP
      constraint(:authors_rank_nonneg) { rank >= 0 }
    end
    add_index :authors, :born
    add_index :authors, :email, where: Sequel.~(email: nil), name: :authors_email_partial
  end
end
