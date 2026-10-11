using System.Data;
using System.Text;
using Npgsql;
using Xunit;

namespace SerenedbDrivers;

public sealed class NpgsqlSchemaFixture : IAsyncLifetime
{
    public string Database { get; } = Target.DatabaseName("getschema");
    public string ConnString => Target.ConnString(Database);

    public async Task InitializeAsync()
    {
        await Target.RecreateDatabase(Database);
        await Target.Execute(ConnString, """
            CREATE SCHEMA drv;
            CREATE TABLE drv.teams (id serial PRIMARY KEY, name varchar(40) NOT NULL UNIQUE, founded date, budget numeric(12,2) DEFAULT 0);
            CREATE TABLE drv.players (id bigserial PRIMARY KEY, team_id integer NOT NULL REFERENCES drv.teams (id), nick text NOT NULL, mmr integer CHECK (mmr >= 0), active boolean DEFAULT true, UNIQUE (team_id, nick));
            CREATE INDEX players_mmr_idx ON drv.players (mmr);
            CREATE VIEW drv.active_players AS SELECT nick, mmr FROM drv.players WHERE active;
            COMMENT ON TABLE drv.teams IS 'pro teams';
            """);
    }

    public async Task DisposeAsync() => await Target.DropDatabase(Database);
}

public class NpgsqlSchemaTest(NpgsqlSchemaFixture fx) : IClassFixture<NpgsqlSchemaFixture>
{
    private static readonly HashSet<string> Volatile =
        ["DataSourceProductVersion", "DataSourceProductVersionNormalized", "user_sysid"];

    private static string Render(DataTable table)
    {
        var columns = table.Columns.Cast<DataColumn>().Where(c => !Volatile.Contains(c.ColumnName)).ToList();
        var sb = new StringBuilder();
        sb.AppendLine(string.Join(" | ", columns.Select(c => c.ColumnName)));
        var rows = table.Rows.Cast<DataRow>()
            .Select(r => string.Join(" | ", columns.Select(c => r[c] is DBNull ? "NULL" : Convert.ToString(r[c], System.Globalization.CultureInfo.InvariantCulture))))
            .Order(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            sb.AppendLine(row);
        }
        return sb.ToString();
    }

    public static TheoryData<string, string?[]> Collections() => new()
    {
        { "MetaDataCollections", [] },
        { "Restrictions", [] },
        { "DataSourceInformation", [] },
        { "ReservedWords", [] },
        { "DataTypes", [] },
        { "Databases", ["<database>"] },
        { "Schemata", [] },
        { "Tables", [null, "drv"] },
        { "Columns", [null, "drv"] },
        { "Views", [null, "drv"] },
        { "Users", ["postgres"] },
        { "Indexes", [null, "drv"] },
        { "IndexColumns", [null, "drv"] },
        { "Constraints", [null, "drv"] },
        { "PrimaryKey", [null, "drv"] },
        { "UniqueKeys", [null, "drv"] },
        { "ForeignKeys", [null, "drv"] },
        { "ConstraintColumns", [null, "drv"] },
    };

    [Theory]
    [MemberData(nameof(Collections))]
    public async Task GetSchemaMatchesPostgreSQL(string collection, string?[] restrictions)
    {
        await using var conn = new NpgsqlConnection(fx.ConnString);
        await conn.OpenAsync();
        var bound = restrictions.Select(r => r == "<database>" ? fx.Database : r).ToArray();
        var table = bound.Length == 0
            ? await conn.GetSchemaAsync(collection)
            : await conn.GetSchemaAsync(collection, bound);
        var text = Render(table).Replace(fx.Database, "<database>");
        Target.CheckGolden(Path.Combine(Target.Fixtures(), "npgsql", $"get_schema_{collection}.txt"), text);
    }

    [Fact]
    public async Task ReloadTypesSeesNewEnum()
    {
        await using var conn = new NpgsqlConnection(fx.ConnString);
        await conn.OpenAsync();
        await using (var cmd = new NpgsqlCommand("CREATE TYPE drv.lane AS ENUM ('safe', 'mid', 'off')", conn))
        {
            await cmd.ExecuteNonQueryAsync();
        }
        await conn.ReloadTypesAsync();
        await using (var cmd = new NpgsqlCommand("SELECT 'mid'::drv.lane, ARRAY['safe', 'off']::drv.lane[]", conn))
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            Assert.True(await reader.ReadAsync());
            Assert.Equal("drv.lane", reader.GetDataTypeName(0));
            Assert.Equal("mid", reader.GetString(0));
            Assert.Equal(["safe", "off"], reader.GetFieldValue<string[]>(1));
        }
        await using (var cmd = new NpgsqlCommand("DROP TYPE drv.lane", conn))
        {
            await cmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task ResultSetSchemaReportsTableColumns()
    {
        await using var conn = new NpgsqlConnection(fx.ConnString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT p.id, p.nick, t.name, t.budget, p.mmr + 1 AS next_mmr FROM drv.players p JOIN drv.teams t ON t.id = p.team_id",
            conn);
        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.KeyInfo);
        var schema = reader.GetColumnSchema();
        var text = new StringBuilder();
        foreach (var c in schema)
        {
            text.AppendLine($"{c.ColumnName} base {c.BaseSchemaName}.{c.BaseTableName}.{c.BaseColumnName} " +
                $"type {c.DataTypeName} size {c.ColumnSize} precision {c.NumericPrecision} scale {c.NumericScale} " +
                $"nullable {c.AllowDBNull} key {c.IsKey} unique {c.IsUnique} autoincrement {c.IsAutoIncrement} " +
                $"identity {c.IsIdentity} readonly {c.IsReadOnly} default {c.DefaultValue} " +
                $"attnum {c.ColumnAttributeNumber} oid {c.TypeOID} postgres type {c.PostgresType.DisplayName}");
        }
        Target.CheckGolden(Path.Combine(Target.Fixtures(), "npgsql", "column_schema.txt"), text.ToString());
    }
}
