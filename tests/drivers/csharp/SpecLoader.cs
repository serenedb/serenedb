using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace SerenedbDrivers;

public sealed class TypeSpec
{
    public int Oid { get; set; }
    public string Name { get; set; } = "";
    public string PgTypname { get; set; } = "";
    public string? CastSql { get; set; }
    public bool Text { get; set; }
    public bool Binary { get; set; }
    public List<object?> Samples { get; set; } = new();
    public Dictionary<string, List<string>> Skip { get; set; } = new();

    public string EffectiveCastSql => CastSql ?? $"$1::{PgTypname}";
    public string EffectiveName => string.IsNullOrEmpty(Name) ? PgTypname : Name;

    public bool Supports(string proto) => proto == "extended-binary" ? Binary : Text;
    public bool IsSkipped(string driverKey, string proto)
    {
        if (!Skip.TryGetValue(driverKey, out var modes)) return false;
        return modes.Contains("all") || modes.Contains(proto);
    }
}

public sealed record Case(TypeSpec Type, string Protocol, int Idx, object? Sample);

public static class Spec
{
    public static readonly string[] Protocols =
        ["simple", "extended-noparam", "extended-text", "extended-binary"];

    private static string SpecDir() =>
        Environment.GetEnvironmentVariable("SDB_DRV_SPEC")
        ?? Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "spec");

    public static List<TypeSpec> LoadTypes()
    {
        var path = Path.Combine(SpecDir(), "types.yaml");
        var deserializer = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .IgnoreUnmatchedProperties()
            .Build();
        using var reader = new StreamReader(path);
        return deserializer.Deserialize<List<TypeSpec>>(reader);
    }

    public static IEnumerable<Case> CasesFor(string driverKey)
    {
        var pat = Environment.GetEnvironmentVariable("SDB_DRV_TYPES") ?? ".*";
        var re = new Regex(pat);
        var protos = (Environment.GetEnvironmentVariable("SDB_DRV_PROTOCOLS")
            ?? string.Join(",", Protocols)).Split(",");
        foreach (var t in LoadTypes())
        {
            if (!re.IsMatch(t.EffectiveName)) continue;
            if (!t.Text && !t.Binary) continue;
            foreach (var p in protos)
            {
                if (!Protocols.Contains(p)) continue;
                if (!t.Supports(p)) continue;
                if (t.IsSkipped(driverKey, p)) continue;
                for (var i = 0; i < t.Samples.Count; i++)
                    yield return new Case(t, p, i, t.Samples[i]);
            }
        }
    }

    public static string ConnString(bool simpleProtocol = false)
    {
        var host = Environment.GetEnvironmentVariable("SDB_DRV_HOST") ?? "localhost";
        var port = Environment.GetEnvironmentVariable("SDB_DRV_PORT") ?? "5432";
        var db = Environment.GetEnvironmentVariable("SDB_DRV_DATABASE") ?? "postgres";
        var user = Environment.GetEnvironmentVariable("SDB_DRV_USER") ?? "postgres";
        // Npgsql refuses to even attempt SASL/SCRAM without a password set.
        // SereneDB runs in trust mode so any value works.
        // SSL Mode=Disable: avoid SCRAM-SHA-256-PLUS channel-binding probe.
        // Channel Binding=Disable: belt-and-suspenders; some Npgsql 8.x
        // versions still attempt PLUS validation otherwise.
        var s = $"Host={host};Port={port};Database={db};Username={user};Password=postgres;SSL Mode=Disable;Channel Binding=Disable";
        return simpleProtocol ? s + ";Multiplexing=false" : s;
    }

    public static string SchemaName(string driverKey) =>
        $"drv_{driverKey}_{Environment.GetEnvironmentVariable("SDB_DRV_RUN_ID") ?? "0"}";
}
