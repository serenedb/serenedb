using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace SerenedbDrivers;

public class NpgsqlFeaturesTest
{
    private const string Schema = """
        CREATE TABLE heroes (id bigserial PRIMARY KEY, name text NOT NULL, mmr integer, born date, tags text[]);
        CREATE TYPE lane AS ENUM ('safe', 'mid', 'off');
        CREATE TABLE picks (hero_id bigint, lane lane);
        """;

    [Fact]
    public async Task BinaryAndTextCopy()
    {
        await Target.InFreshDatabase("copy", async conn =>
        {
            await Target.Execute(conn, Schema);
            await using var connection = new NpgsqlConnection(conn);
            await connection.OpenAsync();
            await using (var writer = await connection.BeginBinaryImportAsync(
                             "COPY heroes (name, mmr, born, tags) FROM STDIN (FORMAT BINARY)"))
            {
                await writer.WriteRowAsync(default, "Pudge", 4200, new DateOnly(2004, 2, 1), new[] { "hook", "rot" });
                await writer.WriteRowAsync(default, "Axe", DBNull.Value, DBNull.Value, Array.Empty<string>());
                Assert.Equal(2ul, await writer.CompleteAsync());
            }
            await using (var writer = await connection.BeginTextImportAsync("COPY heroes (name, mmr) FROM STDIN"))
            {
                await writer.WriteAsync("Lina\t5100\nLion\t\\N\n");
            }
            using var text = await connection.BeginTextExportAsync("COPY (SELECT name FROM heroes ORDER BY name) TO STDOUT");
            Assert.Equal("Axe\nLina\nLion\nPudge\n", await text.ReadToEndAsync());
        });
    }

    [Fact]
    public async Task BinaryCopyExport()
    {
        await Target.InFreshDatabase("copyout", async conn =>
        {
            await Target.Execute(conn, Schema + "INSERT INTO heroes (name, mmr) VALUES ('Axe', NULL), ('Lina', 5100);");
            await Target.XfailOnSereneDB(
                "SereneDB's COPY TO STDOUT (FORMAT BINARY) does not send one CopyData message per row, Npgsql's exporter hangs",
                async () =>
                {
                    var builder = new NpgsqlConnectionStringBuilder(conn) { CommandTimeout = 5 };
                    await using var connection = new NpgsqlConnection(builder.ConnectionString);
                    await connection.OpenAsync();
                    var names = new List<string>();
                    await using (var reader = await connection.BeginBinaryExportAsync(
                                     "COPY (SELECT name, mmr FROM heroes ORDER BY name) TO STDOUT (FORMAT BINARY)"))
                    {
                        while (await reader.StartRowAsync() != -1)
                        {
                            var name = await reader.ReadAsync<string>();
                            var mmr = "null";
                            if (reader.IsNull)
                            {
                                await reader.SkipAsync();
                            }
                            else
                            {
                                mmr = (await reader.ReadAsync<int>()).ToString();
                            }
                            names.Add($"{name}:{mmr}");
                        }
                    }
                    Assert.Equal(["Axe:null", "Lina:5100"], names);
                });
        });
    }

    [Fact]
    public async Task BatchesPreparedStatementsAndResultSets()
    {
        await Target.InFreshDatabase("batch", async conn =>
        {
            await Target.Execute(conn, Schema);
            await using var connection = new NpgsqlConnection(conn);
            await connection.OpenAsync();
            await using (var batch = new NpgsqlBatch(connection)
                         {
                             BatchCommands =
                             {
                                 new NpgsqlBatchCommand("INSERT INTO heroes (name, mmr) VALUES ($1, $2)")
                                     { Parameters = { new() { Value = "Pudge" }, new() { Value = 4200 } } },
                                 new NpgsqlBatchCommand("INSERT INTO heroes (name, mmr) VALUES ($1, $2)")
                                     { Parameters = { new() { Value = "Axe" }, new() { Value = 3000 } } },
                                 new NpgsqlBatchCommand("SELECT count(*) FROM heroes"),
                             },
                         })
            {
                Assert.Equal(2L, await batch.ExecuteScalarAsync());
                Assert.Equal(1, batch.BatchCommands[0].RecordsAffected);
            }
            await using (var cmd = new NpgsqlCommand("SELECT name FROM heroes WHERE mmr > $1 ORDER BY name", connection))
            {
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });
                await cmd.PrepareAsync();
                Assert.True(cmd.IsPrepared);
                foreach (var (threshold, expected) in new[] { (1000, "Axe"), (3500, "Pudge") })
                {
                    cmd.Parameters[0].Value = threshold;
                    Assert.Equal(expected, await cmd.ExecuteScalarAsync());
                }
            }
            await using (var cmd = new NpgsqlCommand("SELECT 1; SELECT name FROM heroes ORDER BY name; SELECT 'x'", connection))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                var sets = new List<string>();
                do
                {
                    var values = new List<string>();
                    while (await reader.ReadAsync())
                    {
                        values.Add(reader.GetValue(0).ToString()!);
                    }
                    sets.Add(string.Join(",", values));
                } while (await reader.NextResultAsync());
                Assert.Equal(["1", "Axe,Pudge", "x"], sets);
            }
        });
    }

    private static NpgsqlDataSource EnumDataSource(string conn)
    {
        var builder = new NpgsqlDataSourceBuilder(conn);
        builder.MapEnum<Lane>("lane");
        return builder.Build();
    }

    [Fact]
    public async Task EnumsThroughDataSource()
    {
        await Target.InFreshDatabase("enums", async conn =>
        {
            await Target.Execute(conn, Schema + "INSERT INTO picks VALUES (1, 'mid'), (2, 'off');");
            await using var dataSource = EnumDataSource(conn);
            await using (var cmd = dataSource.CreateCommand("SELECT array_agg(lane ORDER BY hero_id) FROM picks"))
            {
                Assert.Equal([Lane.Mid, Lane.Off], (Lane[])(await cmd.ExecuteScalarAsync())!);
            }
            await using (var cmd = dataSource.CreateCommand("SELECT lane FROM picks WHERE hero_id = 1"))
            {
                Assert.Equal(Lane.Mid, (Lane)(await cmd.ExecuteScalarAsync())!);
            }
        });
    }

    [Fact]
    public async Task EnumParameterOutsideTransaction()
    {
        await Target.InFreshDatabase("enumparam", async conn =>
        {
            await Target.Execute(conn, Schema);
            await Target.XfailOnSereneDB(
                "SereneDB fails to bind a parameter typed with a user-defined enum outside a transaction block (XX000)",
                async () =>
                {
                    await using var dataSource = EnumDataSource(conn);
                    await using var cmd = dataSource.CreateCommand("INSERT INTO picks VALUES ($1, $2)");
                    cmd.Parameters.Add(new NpgsqlParameter { Value = 1L });
                    cmd.Parameters.Add(new NpgsqlParameter { Value = Lane.Safe });
                    Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
                });
        });
    }

    [Fact]
    public async Task ListenNotify()
    {
        await Target.InFreshDatabase("listen", conn => Target.XfailOnSereneDB("SereneDB has no LISTEN/NOTIFY (#723)",
            async () =>
            {
                await using var listener = new NpgsqlConnection(conn);
                await listener.OpenAsync();
                string? payload = null;
                listener.Notification += (_, e) => payload = e.Payload;
                await using (var listen = new NpgsqlCommand("LISTEN drv_channel", listener))
                {
                    await listen.ExecuteNonQueryAsync();
                }
                await Target.Execute(conn, "SELECT pg_notify('drv_channel', 'hook')");
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await listener.WaitAsync(timeout.Token);
                Assert.Equal("hook", payload);
            }));
    }

    public enum Lane
    {
        Safe,
        Mid,
        Off,
    }
}
