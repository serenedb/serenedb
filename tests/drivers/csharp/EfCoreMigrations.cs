using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

namespace SerenedbDrivers;

[DbContext(typeof(LedgerContext))]
[Migration("20260101000000_Init")]
public class LedgerInit : Migration
{
    protected override void Up(MigrationBuilder mb)
    {
        mb.CreateSequence<long>("ledger_numbers", startValue: 100, incrementBy: 10);
        mb.CreateTable("Accounts", t => new
        {
            Id = t.Column<int>(type: "integer", nullable: false)
                .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
            Email = t.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
            Balance = t.Column<decimal>(type: "numeric(12,2)", nullable: false, defaultValue: 0m),
            CreatedAt = t.Column<DateTime>(type: "timestamp with time zone", nullable: false,
                defaultValueSql: "now()"),
        }, constraints: t => t.PrimaryKey("PK_Accounts", x => x.Id));
        mb.CreateIndex("IX_Accounts_Email", "Accounts", "Email", unique: true);
        mb.CreateTable("Entries", t => new
        {
            Id = t.Column<long>(type: "bigint", nullable: false)
                .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
            AccountId = t.Column<int>(type: "integer", nullable: false),
            Amount = t.Column<decimal>(type: "numeric(12,2)", nullable: false),
            Note = t.Column<string>(type: "text", nullable: true),
        }, constraints: t =>
        {
            t.PrimaryKey("PK_Entries", x => x.Id);
            t.ForeignKey("FK_Entries_Accounts_AccountId", x => x.AccountId, "Accounts", "Id");
        });
        mb.CreateIndex("IX_Entries_AccountId", "Entries", "AccountId");
    }

    protected override void Down(MigrationBuilder mb)
    {
        mb.DropTable("Entries");
        mb.DropTable("Accounts");
        mb.DropSequence("ledger_numbers");
    }
}

[DbContext(typeof(LedgerContext))]
[Migration("20260102000000_AddColumns")]
public class LedgerAddColumns : Migration
{
    protected override void Up(MigrationBuilder mb)
    {
        mb.AddColumn<string>("Nickname", "Accounts", type: "character varying(50)", maxLength: 50,
            nullable: true);
        mb.AddColumn<bool>("Booked", "Entries", type: "boolean", nullable: false, defaultValue: false);
        mb.Sql("INSERT INTO \"Accounts\" (\"Email\", \"Balance\") VALUES ('seed@ledger', 5);");
    }

    protected override void Down(MigrationBuilder mb)
    {
        mb.Sql("DELETE FROM \"Accounts\" WHERE \"Email\" = 'seed@ledger';");
        mb.DropColumn("Booked", "Entries");
        mb.DropColumn("Nickname", "Accounts");
    }
}

[DbContext(typeof(LedgerContext))]
[Migration("20260103000000_Rename")]
public class LedgerRename : Migration
{
    protected override void Up(MigrationBuilder mb)
    {
        mb.RenameColumn("Note", "Entries", "Memo");
        mb.RenameIndex("IX_Entries_AccountId", "IX_Entries_Account", "Entries");
    }

    protected override void Down(MigrationBuilder mb)
    {
        mb.RenameIndex("IX_Entries_Account", "IX_Entries_AccountId", "Entries");
        mb.RenameColumn("Memo", "Entries", "Note");
    }
}

[DbContext(typeof(LedgerContext))]
[Migration("20260104000000_Alter")]
public class LedgerAlter : Migration
{
    protected override void Up(MigrationBuilder mb)
    {
        mb.AlterColumn<string>("Nickname", "Accounts", type: "character varying(80)", maxLength: 80,
            nullable: true, oldClrType: typeof(string), oldType: "character varying(50)",
            oldMaxLength: 50, oldNullable: true);
        mb.AlterColumn<decimal>("Balance", "Accounts", type: "numeric(14,2)", nullable: false,
            defaultValue: 0m, oldClrType: typeof(decimal), oldType: "numeric(12,2)",
            oldDefaultValue: 0m);
        mb.DropColumn("Booked", "Entries");
        mb.AddCheckConstraint("CK_Entries_Amount", "Entries", "\"Amount\" <> 0");
        mb.CreateTable("Audits", t => new
        {
            Id = t.Column<int>(type: "integer", nullable: false)
                .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
            Action = t.Column<string>(type: "text", nullable: false),
        }, constraints: t => t.PrimaryKey("PK_Audits", x => x.Id));
    }

    protected override void Down(MigrationBuilder mb)
    {
        mb.DropTable("Audits");
        mb.DropCheckConstraint("CK_Entries_Amount", "Entries");
        mb.AddColumn<bool>("Booked", "Entries", type: "boolean", nullable: false, defaultValue: false);
        mb.AlterColumn<decimal>("Balance", "Accounts", type: "numeric(12,2)", nullable: false,
            defaultValue: 0m, oldClrType: typeof(decimal), oldType: "numeric(14,2)",
            oldDefaultValue: 0m);
        mb.AlterColumn<string>("Nickname", "Accounts", type: "character varying(50)", maxLength: 50,
            nullable: true, oldClrType: typeof(string), oldType: "character varying(80)",
            oldMaxLength: 80, oldNullable: true);
    }
}
