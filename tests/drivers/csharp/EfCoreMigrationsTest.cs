using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Migrations.Operations.Builders;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;
using Xunit;

namespace SerenedbDrivers;

public class EfCoreMigrationsTest
{
    private static readonly string[] AllMigrations =
        ["20260101000000_Init", "20260102000000_AddColumns", "20260103000000_Rename", "20260104000000_Alter"];

    private static string Golden(string name) => Path.Combine(Target.Fixtures(), "efcore", name);

    private static async Task RunOperations(string conn, Action<MigrationBuilder> build)
    {
        await using var ctx = new LedgerContext(conn);
        var builder = new MigrationBuilder(ctx.Database.ProviderName);
        build(builder);
        var commands = ctx.GetService<IMigrationsSqlGenerator>().Generate(builder.Operations, ctx.Model);
        await using var connection = new NpgsqlConnection(conn);
        await connection.OpenAsync();
        foreach (var command in commands)
        {
            await using var cmd = new NpgsqlCommand(command.CommandText, connection);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static OperationBuilder<AddColumnOperation> Serial<T>(ColumnsBuilder t, string type) =>
        t.Column<T>(type: type, nullable: false)
            .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn);

    [Fact]
    public async Task MigrateAppliesAndRevertsMigrations()
    {
        await Target.InFreshDatabase("migrate", conn => Target.XfailOnSereneDB(
            "SereneDB has no LOCK TABLE, Migrate() locks __EFMigrationsHistory (#724)",
            async () =>
            {
                await using var ctx = new LedgerContext(conn);
                await ctx.Database.MigrateAsync();
                Assert.Equal(AllMigrations, await ctx.Database.GetAppliedMigrationsAsync());
                Assert.Equal("seed@ledger", await ctx.Accounts.Select(a => a.Email).SingleAsync());
                await ctx.GetService<IMigrator>().MigrateAsync("0");
                Assert.Empty(await ctx.Database.GetAppliedMigrationsAsync());
            }));
    }

    private const string ReferencedTable =
        "SereneDB cannot change column types of, rename or drop columns of a table that a FOREIGN KEY references";

    [Fact]
    public async Task MigrationScriptsApply()
    {
        await Target.InFreshDatabase("script", async conn =>
        {
            await using var ctx = new LedgerContext(conn);
            await Target.Execute(conn, ctx.GetService<IMigrator>().GenerateScript(toMigration: "20260103000000_Rename"));
            Assert.Equal(AllMigrations[..3], await ctx.Database.GetAppliedMigrationsAsync());
            Assert.Equal(AllMigrations[3..], await ctx.Database.GetPendingMigrationsAsync());
            await EfDesign.CheckModel("ledger_model.txt", conn);
            ctx.Add(new Account { Email = "carry@ledger", Balance = 1234567890.25m, Nickname = new string('n', 50) });
            await ctx.SaveChangesAsync();
            Assert.Equal(2, await ctx.Accounts.CountAsync());
            await Target.Execute(conn,
                "INSERT INTO \"Entries\" (\"AccountId\", \"Amount\", \"Memo\") SELECT \"Id\", 10, 'first' FROM \"Accounts\"");
        });
    }

    [Fact]
    public async Task MigrationScriptAltersReferencedTable()
    {
        await Target.InFreshDatabase("scriptalter", async conn =>
        {
            await using var ctx = new LedgerContext(conn);
            var migrator = ctx.GetService<IMigrator>();
            await Target.Execute(conn, migrator.GenerateScript(toMigration: "20260103000000_Rename"));
            await Target.XfailOnSereneDB(ReferencedTable, async () =>
            {
                await Target.Execute(conn, migrator.GenerateScript("20260103000000_Rename", "20260104000000_Alter"));
                Assert.Equal(AllMigrations, await ctx.Database.GetAppliedMigrationsAsync());
                ctx.Add(new Account { Email = "carry@ledger", Balance = 123456789012.25m, Nickname = new string('n', 80) });
                await ctx.SaveChangesAsync();
            });
        });
    }

    [Fact]
    public async Task MigrationScriptsRevert()
    {
        await Target.InFreshDatabase("scriptrevert", async conn =>
        {
            await using var ctx = new LedgerContext(conn);
            var migrator = ctx.GetService<IMigrator>();
            await Target.Execute(conn, migrator.GenerateScript(toMigration: "20260103000000_Rename"));
            await Target.XfailOnSereneDB(ReferencedTable, async () =>
            {
                await Target.Execute(conn, migrator.GenerateScript("20260103000000_Rename", Migration.InitialDatabase));
                Assert.Empty(await ctx.Database.GetAppliedMigrationsAsync());
            });
        });
    }

    [Fact]
    public async Task IdempotentMigrationScript()
    {
        await Target.InFreshDatabase("idempotent", conn => Target.XfailOnSereneDB(
            "SereneDB has no DO blocks, idempotent scripts guard each migration with DO $EF$",
            async () =>
            {
                await using var ctx = new LedgerContext(conn);
                var script = ctx.GetService<IMigrator>()
                    .GenerateScript(options: MigrationsSqlGenerationOptions.Idempotent);
                await Target.Execute(conn, script);
                await Target.Execute(conn, script);
                Assert.Equal(AllMigrations, await ctx.Database.GetAppliedMigrationsAsync());
            }));
    }

    [Fact]
    public async Task TableColumnAndIndexOperations()
    {
        await Target.InFreshDatabase("ops", conn => RunOperations(conn, mb =>
        {
            mb.Sql("CREATE SCHEMA ops;");
            mb.CreateTable("Gadgets", t => new
            {
                Id = Serial<int>(t, "integer"),
                Name = t.Column<string>(type: "character varying(20)", maxLength: 20, nullable: false),
                Weight = t.Column<int>(type: "integer", nullable: false, defaultValue: 1),
            }, schema: "ops", constraints: t => t.PrimaryKey("PK_Gadgets", x => x.Id));
            mb.AddColumn<string>("Color", "Gadgets", schema: "ops", type: "text", nullable: false, defaultValue: "red");
            mb.AlterColumn<long>("Weight", "Gadgets", schema: "ops", type: "bigint", nullable: true,
                oldClrType: typeof(int), oldType: "integer", oldDefaultValue: 1);
            mb.AlterColumn<string>("Name", "Gadgets", schema: "ops", type: "character varying(60)", maxLength: 60,
                nullable: false, oldClrType: typeof(string), oldType: "character varying(20)", oldMaxLength: 20);
            mb.AlterColumn<string>("Color", "Gadgets", schema: "ops", type: "text", nullable: false,
                defaultValue: "blue", comment: "paint", oldClrType: typeof(string), oldType: "text",
                oldDefaultValue: "red");
            mb.RenameColumn("Color", "Gadgets", "Colour", "ops");
            mb.CreateIndex("IX_Gadgets_Name", "Gadgets", "Name", "ops");
            mb.CreateIndex("IX_Gadgets_Weight_Desc", "Gadgets", "Weight", "ops", descending: [true]);
            mb.AddCheckConstraint("CK_Gadgets_Weight", "Gadgets", "\"Weight\" >= 0", "ops");
            mb.DropCheckConstraint("CK_Gadgets_Weight", "Gadgets", "ops");
            mb.RenameIndex("IX_Gadgets_Name", "IX_Gadgets_Label", "Gadgets", "ops");
            mb.DropIndex("IX_Gadgets_Label", "Gadgets", "ops");
            mb.DropIndex("IX_Gadgets_Weight_Desc", "Gadgets", "ops");
            mb.DropColumn("Colour", "Gadgets", "ops");
            mb.RenameTable("Gadgets", "ops", "Widgets", "ops");
            mb.AlterTable("Widgets", "ops", comment: "renamed");
            mb.DropTable("Widgets", "ops");
            mb.DropSchema("ops");
        }));
    }

    [Fact]
    public async Task EnsureSchema()
    {
        await Target.InFreshDatabase("schema", conn => Target.XfailOnSereneDB(
            "SereneDB has no DO blocks, Npgsql's EnsureSchema runs one",
            () => RunOperations(conn, mb =>
            {
                mb.EnsureSchema("ops");
                mb.EnsureSchema("ops");
                mb.DropSchema("ops");
            })));
    }

    private static async Task<long> NextVal(string conn, string sequence)
    {
        await using var connection = new NpgsqlConnection(conn);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand($"SELECT nextval('{sequence}')", connection);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private static Action<MigrationBuilder> CreateTickets => mb =>
        mb.CreateSequence<long>("tickets", startValue: 10, incrementBy: 3, minValue: 1, maxValue: 1000,
            cyclic: false);

    private const string AlterSequenceOptions =
        "SereneDB has no ALTER SEQUENCE ... INCREMENT BY, MINVALUE, MAXVALUE, CYCLE or START WITH";

    [Fact]
    public async Task SequenceOperations()
    {
        await Target.InFreshDatabase("sequences", async conn =>
        {
            await RunOperations(conn, CreateTickets);
            Assert.Equal(10L, await NextVal(conn, "tickets"));
            Assert.Equal(13L, await NextVal(conn, "tickets"));
            await RunOperations(conn, mb => mb.RenameSequence("tickets", newName: "passes"));
            Assert.Equal(16L, await NextVal(conn, "passes"));
            await RunOperations(conn, mb => mb.DropSequence("passes"));
        });
    }

    [Fact]
    public async Task AlterSequence()
    {
        await Target.InFreshDatabase("altersequence", async conn =>
        {
            await RunOperations(conn, CreateTickets);
            await Target.XfailOnSereneDB(AlterSequenceOptions, async () =>
            {
                await RunOperations(conn, mb => mb.AlterSequence("tickets", incrementBy: 7, minValue: 1,
                    maxValue: 5000, cyclic: true, oldIncrementBy: 3, oldMinValue: 1, oldMaxValue: 1000,
                    oldCyclic: false));
                Assert.Equal(10L, await NextVal(conn, "tickets"));
                Assert.Equal(17L, await NextVal(conn, "tickets"));
            });
        });
    }

    [Fact]
    public async Task RestartSequence()
    {
        await Target.InFreshDatabase("restartsequence", async conn =>
        {
            await RunOperations(conn, CreateTickets);
            await Target.XfailOnSereneDB(AlterSequenceOptions, async () =>
            {
                await RunOperations(conn, mb => mb.RestartSequence("tickets", startValue: 100));
                Assert.Equal(100L, await NextVal(conn, "tickets"));
            });
        });
    }

    [Fact]
    public async Task PrimaryKeyAndUniqueConstraintOperations()
    {
        await Target.InFreshDatabase("keys", conn => RunOperations(conn, mb =>
        {
            mb.CreateTable("Heroes", t => new
            {
                Id = t.Column<int>(type: "integer", nullable: false),
                Name = t.Column<string>(type: "text", nullable: false),
            });
            mb.AddPrimaryKey("PK_Heroes", "Heroes", "Id");
            mb.AddUniqueConstraint("AK_Heroes_Name", "Heroes", "Name");
            mb.DropUniqueConstraint("AK_Heroes_Name", "Heroes");
            mb.DropPrimaryKey("PK_Heroes", "Heroes");
        }));
    }

    [Fact]
    public async Task AddForeignKey()
    {
        await Target.InFreshDatabase("addfk", async conn =>
        {
            await RunOperations(conn, mb =>
            {
                mb.CreateTable("Kennels", t => new { Id = Serial<int>(t, "integer") },
                    constraints: t => t.PrimaryKey("PK_Kennels", x => x.Id));
                mb.CreateTable("Hounds", t => new
                {
                    Id = Serial<int>(t, "integer"),
                    KennelId = t.Column<int>(type: "integer", nullable: false),
                }, constraints: t => t.PrimaryKey("PK_Hounds", x => x.Id));
            });
            await Target.XfailOnSereneDB("SereneDB cannot add a FOREIGN KEY with ALTER TABLE (#1427)",
                () => RunOperations(conn, mb => mb.AddForeignKey("FK_Hounds_Kennels_KennelId", "Hounds",
                    "KennelId", "Kennels", principalColumn: "Id")));
        });
    }

    [Fact]
    public async Task DropColumnBeforeIndexedColumn()
    {
        await Target.InFreshDatabase("dropcol", async conn =>
        {
            await RunOperations(conn, mb =>
            {
                mb.CreateTable("Labels", t => new
                {
                    Id = Serial<int>(t, "integer"),
                    Legacy = t.Column<string>(type: "text", nullable: true),
                    App = t.Column<string>(type: "text", nullable: false),
                    Model = t.Column<string>(type: "text", nullable: false),
                }, constraints: t => t.PrimaryKey("PK_Labels", x => x.Id));
                mb.CreateIndex("IX_Labels_App_Model", "Labels", ["App", "Model"], unique: true);
            });
            await Target.XfailOnSereneDB(
                "SereneDB cannot drop a column when an index covers a later column (#1428)",
                () => RunOperations(conn, mb => mb.DropColumn("Legacy", "Labels")));
        });
    }

    [Fact]
    public async Task AlterTypeOfUniqueColumn()
    {
        await Target.InFreshDatabase("altertype", async conn =>
        {
            await RunOperations(conn, mb =>
            {
                mb.CreateTable("Handles", t => new
                {
                    Id = Serial<int>(t, "integer"),
                    Name = t.Column<string>(type: "character varying(30)", maxLength: 30, nullable: false),
                }, constraints: t => t.PrimaryKey("PK_Handles", x => x.Id));
                mb.CreateIndex("IX_Handles_Name", "Handles", "Name", unique: true);
            });
            await Target.XfailOnSereneDB("SereneDB cannot change the type of a UNIQUE column (#1429)",
                () => RunOperations(conn, mb => mb.AlterColumn<string>("Name", "Handles",
                    type: "character varying(150)", maxLength: 150, nullable: false,
                    oldClrType: typeof(string), oldType: "character varying(30)", oldMaxLength: 30)));
        });
    }
}
