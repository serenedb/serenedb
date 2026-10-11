using System.Runtime.CompilerServices;
using Npgsql;
using Xunit;

namespace SerenedbDrivers;

public static class Target
{
    private static readonly Lazy<bool> SereneDB = new(() =>
    {
        using var conn = new NpgsqlConnection(Spec.ConnString());
        conn.Open();
        using var cmd = new NpgsqlCommand("SELECT version()", conn);
        return ((string)cmd.ExecuteScalar()!).Contains("SereneDB");
    });

    public static bool IsSereneDB => SereneDB.Value;

    public static string Fixtures([CallerFilePath] string source = "") =>
        Path.Combine(Path.GetDirectoryName(source)!, "fixtures");

    public static void CheckGolden(string path, string text)
    {
        var twin = Path.Combine(Path.GetDirectoryName(path)!,
            $"{Path.GetFileNameWithoutExtension(path)}.serenedb{Path.GetExtension(path)}");
        if (Environment.GetEnvironmentVariable("SDB_DRV_UPDATE_GOLDENS") == "1")
        {
            if (!IsSereneDB)
            {
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                File.WriteAllText(path, text);
            }
            else if (text == File.ReadAllText(path))
            {
                File.Delete(twin);
            }
            else
            {
                File.WriteAllText(twin, text);
            }
            return;
        }
        var expected = IsSereneDB && File.Exists(twin) ? twin : path;
        Assert.Equal(File.ReadAllText(expected), text);
    }

    public static async Task XfailOnSereneDB(string reason, Func<Task> body)
    {
        if (!IsSereneDB)
        {
            await body();
            return;
        }
        try
        {
            await body();
        }
        catch (Exception e)
        {
            if (Environment.GetEnvironmentVariable("SDB_DRV_DEBUG") == "true")
            {
                var inner = e.GetBaseException();
                Console.Error.WriteLine($"xfail on SereneDB ({reason}): {inner.GetType().Name}: {inner.Message}");
            }
            return;
        }
        Assert.Fail($"SereneDB passes now, drop the expected failure: {reason}");
    }

    public static string DatabaseName(string prefix) =>
        $"cs_{prefix}_{Environment.GetEnvironmentVariable("SDB_DRV_RUN_ID") ?? "0"}";

    public static string ConnString(string database) =>
        new NpgsqlConnectionStringBuilder(Spec.ConnString()) { Database = database, Pooling = false }.ConnectionString;

    public static async Task RecreateDatabase(string database)
    {
        await using var conn = new NpgsqlConnection(Spec.ConnString());
        await conn.OpenAsync();
        await using (var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{database}\"", conn))
            await drop.ExecuteNonQueryAsync();
        await using var create = new NpgsqlCommand($"CREATE DATABASE \"{database}\"", conn);
        await create.ExecuteNonQueryAsync();
    }

    public static async Task DropDatabase(string database)
    {
        NpgsqlConnection.ClearAllPools();
        await using var conn = new NpgsqlConnection(Spec.ConnString());
        await conn.OpenAsync();
        await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{database}\"", conn);
        await drop.ExecuteNonQueryAsync();
    }

    public static async Task InFreshDatabase(string prefix, Func<string, Task> body)
    {
        var database = DatabaseName(prefix);
        await RecreateDatabase(database);
        try
        {
            await body(ConnString(database));
        }
        finally
        {
            await DropDatabase(database);
        }
    }

    public static async Task Execute(string connString, string sql)
    {
        await using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }
}
