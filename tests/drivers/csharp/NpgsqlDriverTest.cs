using System.Globalization;
using Npgsql;
using Xunit;

namespace SerenedbDrivers;

public class NpgsqlDriverTest : IAsyncLifetime
{
    private const string DriverKey = "csharp_npgsql";
    private NpgsqlConnection? _conn;
    private string _schema = "";

    public async Task InitializeAsync()
    {
        _conn = new NpgsqlConnection(Spec.ConnString());
        await _conn.OpenAsync();
        _schema = Spec.SchemaName(DriverKey);
        await using var cmd1 = new NpgsqlCommand(
            $"CREATE SCHEMA IF NOT EXISTS \"{_schema}\"", _conn);
        await cmd1.ExecuteNonQueryAsync();
        await using var cmd2 = new NpgsqlCommand(
            $"SET search_path TO \"{_schema}\", public, pg_catalog", _conn);
        await cmd2.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        if (_conn is not null)
        {
            try
            {
                await using var cmd = new NpgsqlCommand(
                    $"DROP SCHEMA IF EXISTS \"{_schema}\" CASCADE", _conn);
                await cmd.ExecuteNonQueryAsync();
            }
            catch { }
            await _conn.DisposeAsync();
        }
    }

    [Fact]
    public async Task SmokeSelectOne()
    {
        await using var cmd = new NpgsqlCommand("SELECT 1", _conn);
        var v = (int?)await cmd.ExecuteScalarAsync();
        Assert.Equal(1, v);
    }

    [Fact]
    public async Task SmokeServerVersion()
    {
        await using var cmd = new NpgsqlCommand("SHOW server_version", _conn);
        var s = (string?)await cmd.ExecuteScalarAsync();
        Assert.False(string.IsNullOrEmpty(s));
    }

    public static IEnumerable<object[]> Cases() =>
        Spec.CasesFor(DriverKey).Select(c => new object[] { c });

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Roundtrip(Case c)
    {
        var expected = await ServerText(c.Type.PgTypname, c.Sample);
        string? actual;
        switch (c.Protocol)
        {
            case "simple":
            case "extended-noparam":
                actual = await Inline(c);
                break;
            case "extended-text":
            case "extended-binary":
                actual = await Bound(c);
                break;
            default:
                throw new InvalidOperationException(c.Protocol);
        }
        Assert.Equal(expected, actual);
    }

    private async Task<string?> ServerText(string pgType, object? sample)
    {
        if (sample is null) return null;
        var s = Convert.ToString(sample, CultureInfo.InvariantCulture);
        await using var cmd = new NpgsqlCommand(
            $"SELECT (($1::text)::{pgType})::text", _conn);
        cmd.Parameters.AddWithValue(s ?? "");
        return AsNullableString(await cmd.ExecuteScalarAsync());
    }

    private async Task<string?> Inline(Case c)
    {
        string sql;
        if (c.Sample is null)
        {
            sql = $"SELECT NULL::{c.Type.PgTypname}::text";
        }
        else
        {
            var s = Convert.ToString(c.Sample, CultureInfo.InvariantCulture)!;
            var lit = s.Replace("'", "''");
            sql = $"SELECT '{lit}'::{c.Type.PgTypname}::text";
        }
        await using var cmd = new NpgsqlCommand(sql, _conn);
        return AsNullableString(await cmd.ExecuteScalarAsync());
    }

    // ExecuteScalarAsync returns DBNull.Value (not null) for SQL NULL columns.
    // A direct (string?) cast on DBNull throws InvalidCastException.
    private static string? AsNullableString(object? v) =>
        v is null || v is DBNull ? null : (string)v;

    private async Task<string?> Bound(Case c)
    {
        // Inject ::text in front of the per-OID cast so we can always
        // bind Npgsql's NpgsqlDbType.Text and let the server cast
        // server-side. Sidesteps Npgsql's per-type strict binder.
        var cast = c.Type.EffectiveCastSql.Replace("$1", "$1::text");
        await using var cmd = new NpgsqlCommand($"SELECT ({cast})::text", _conn);
        if (c.Sample is null)
        {
            cmd.Parameters.Add(new NpgsqlParameter
            {
                Value = DBNull.Value,
                NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text,
            });
        }
        else
        {
            cmd.Parameters.AddWithValue(
                Convert.ToString(c.Sample, CultureInfo.InvariantCulture)!);
        }
        return AsNullableString(await cmd.ExecuteScalarAsync());
    }
}
