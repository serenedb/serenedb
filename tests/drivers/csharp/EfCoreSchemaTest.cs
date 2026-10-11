using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Npgsql.EntityFrameworkCore.PostgreSQL.Design.Internal;
using Xunit;

namespace SerenedbDrivers;

public static class EfDesign
{
    public static Task CheckModel(string golden, string conn)
    {
        Target.CheckGolden(Path.Combine(Target.Fixtures(), "efcore", golden), Describe(ReverseEngineer(conn)));
        return Task.CompletedTask;
    }

    public static IServiceProvider Services()
    {
        var services = new ServiceCollection().AddEntityFrameworkDesignTimeServices();
        new NpgsqlDesignTimeServices().ConfigureDesignTimeServices(services);
        return services.BuildServiceProvider();
    }

    public static DatabaseModel ReverseEngineer(string connString) =>
        Services().GetRequiredService<IDatabaseModelFactory>()
            .Create(connString, new DatabaseModelFactoryOptions());

    public static string Describe(DatabaseModel model)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"default schema {model.DefaultSchema}");
        sb.AppendLine($"collation {model.Collation}");
        Annotations(sb, "", model);
        foreach (var s in model.Sequences.OrderBy(s => s.Schema).ThenBy(s => s.Name))
        {
            sb.AppendLine($"sequence {s.Schema}.{s.Name} {s.StoreType} start {s.StartValue} " +
                $"increment {s.IncrementBy} min {s.MinValue} max {s.MaxValue} cyclic {s.IsCyclic}");
            Annotations(sb, "  ", s);
        }
        foreach (var t in model.Tables.OrderBy(t => t.Schema).ThenBy(t => t.Name))
        {
            sb.AppendLine($"{(t is DatabaseView ? "view" : "table")} {t.Schema}.{t.Name} comment {t.Comment}");
            Annotations(sb, "  ", t);
            foreach (var c in t.Columns)
            {
                sb.AppendLine($"  column {c.Name} {c.StoreType} nullable {c.IsNullable} " +
                    $"default {c.DefaultValueSql} computed {c.ComputedColumnSql} stored {c.IsStored} " +
                    $"generated {c.ValueGenerated} comment {c.Comment} collation {c.Collation}");
                Annotations(sb, "    ", c);
            }
            if (t.PrimaryKey is { } pk)
            {
                sb.AppendLine($"  primary key {pk.Name} ({Names(pk.Columns)})");
                Annotations(sb, "    ", pk);
            }
            foreach (var u in t.UniqueConstraints.OrderBy(u => u.Name))
            {
                sb.AppendLine($"  unique {u.Name} ({Names(u.Columns)})");
                Annotations(sb, "    ", u);
            }
            foreach (var i in t.Indexes.OrderBy(i => i.Name))
            {
                sb.AppendLine($"  index {i.Name} unique {i.IsUnique} ({Names(i.Columns)}) " +
                    $"descending [{string.Join(",", i.IsDescending)}] filter {i.Filter}");
                Annotations(sb, "    ", i);
            }
            foreach (var f in t.ForeignKeys.OrderBy(f => f.Name))
            {
                sb.AppendLine($"  foreign key {f.Name} ({Names(f.Columns)}) references " +
                    $"{f.PrincipalTable.Schema}.{f.PrincipalTable.Name} ({Names(f.PrincipalColumns)}) " +
                    $"on delete {f.OnDelete}");
                Annotations(sb, "    ", f);
            }
        }
        return sb.ToString();
    }

    private static string Names(IEnumerable<DatabaseColumn> columns) =>
        string.Join(", ", columns.Select(c => c.Name));

    private static void Annotations(StringBuilder sb, string indent, Microsoft.EntityFrameworkCore.Infrastructure.Annotatable item)
    {
        foreach (var a in item.GetAnnotations().OrderBy(a => a.Name))
        {
            sb.AppendLine($"{indent}annotation {a.Name} = {Value(a.Value)}");
        }
    }

    private static string Value(object? value) => value switch
    {
        null => "null",
        string s => s,
        System.Collections.IEnumerable e => $"[{string.Join(", ", e.Cast<object?>().Select(Value))}]",
        _ => value.ToString() ?? "",
    };

    public static string Scaffold(string connString)
    {
        var scaffolder = Services().GetRequiredService<IReverseEngineerScaffolder>();
        var model = scaffolder.ScaffoldModel(connString, new DatabaseModelFactoryOptions(),
            new ModelReverseEngineerOptions(),
            new ModelCodeGenerationOptions
            {
                ModelNamespace = "Scaffolded",
                ContextNamespace = "Scaffolded",
                ContextName = "ScaffoldedContext",
                SuppressConnectionStringWarning = true,
                SuppressOnConfiguring = true,
                UseNullableReferenceTypes = true,
            });
        var sb = new StringBuilder();
        foreach (var file in new[] { model.ContextFile }.Concat(model.AdditionalFiles.OrderBy(f => f.Path)))
        {
            sb.AppendLine($"// {file.Path}");
            sb.AppendLine(file.Code);
        }
        return sb.ToString();
    }
}

[Collection("efcore")]
public class EfCoreSchemaTest(EfCoreFixture fx)
{
    private static string Golden(string name) => Path.Combine(Target.Fixtures(), "efcore", name);

    [Fact]
    public async Task ReverseEngineeredModelMatchesPostgreSQL()
    {
        await EfDesign.CheckModel("database_model.txt", fx.ConnString);
    }

    [Fact]
    public void ScaffoldedCodeMatchesPostgreSQL()
    {
        Target.CheckGolden(Golden("scaffold.txt"), EfDesign.Scaffold(fx.ConnString));
    }

    [Fact]
    public async Task CreateScriptMatchesExecutedSchema()
    {
        await using var ctx = fx.NewContext();
        var script = ctx.Database.GenerateCreateScript();
        Assert.Contains("CREATE TABLE \"Blogs\"", script);
        Assert.Contains("CREATE SEQUENCE blog_numbers AS integer START WITH 1000 INCREMENT BY 5 NO CYCLE;", script);
        Assert.False(await ctx.Database.EnsureCreatedAsync());
        Assert.True(await ctx.Database.CanConnectAsync());
    }

    [Fact]
    public async Task EnsureCreatedCreatesDatabase()
    {
        var database = Target.DatabaseName("ensure");
        await Target.DropDatabase(database);
        try
        {
            await using var ctx = new BlogContext(Target.ConnString(database));
            Assert.False(await ctx.Database.CanConnectAsync());
            Assert.True(await ctx.Database.EnsureCreatedAsync());
            Assert.False(await ctx.Database.EnsureCreatedAsync());
            Assert.True(await ctx.Database.CanConnectAsync());
            ctx.Add(new Tag { Label = "ensure" });
            Assert.Equal(1, await ctx.SaveChangesAsync());
        }
        finally
        {
            await Target.DropDatabase(database);
        }
    }

    [Fact]
    public async Task EnsureDeletedDropsDatabase()
    {
        await Target.InFreshDatabase("ensuredel", conn => Target.XfailOnSereneDB(
            "SereneDB has no DROP DATABASE ... WITH (FORCE), Npgsql's EnsureDeleted runs it",
            async () =>
            {
                await using var ctx = new BlogContext(conn);
                Assert.True(await ctx.Database.EnsureDeletedAsync());
                Assert.False(await ctx.Database.CanConnectAsync());
            }));
    }

    [Fact]
    public async Task EnumsArraysAndStoredComputedColumns()
    {
        await Target.InFreshDatabase("diary", async conn =>
        {
            await using (var ctx = new DiaryContext(conn))
            {
                await ctx.Database.EnsureCreatedAsync();
                ctx.AddRange(
                    new Diary { Mood = Mood.Happy, Steps = [5, 10, 15], Words = ["hook", "rot"], Price = 2.5m },
                    new Diary { Mood = Mood.Meh, Steps = [], Words = ["dismember"], Price = 1m });
                await ctx.SaveChangesAsync();
            }
            await using (var ctx = new DiaryContext(conn))
            {
                var happy = await ctx.Diaries.SingleAsync(d => d.Mood == Mood.Happy);
                Assert.Equal([5, 10, 15], happy.Steps);
                Assert.Equal(["hook", "rot"], happy.Words);
                Assert.Equal(5m, happy.DoublePrice);
                Assert.Equal(1, await ctx.Diaries.CountAsync(d => d.Steps.Contains(10)));
                Assert.Equal(1, await ctx.Diaries.CountAsync(d => d.Steps.Length == 0));
                Assert.Equal(1, await ctx.Diaries.CountAsync(d => d.Steps[1] == 10));
                Assert.Equal(1, await ctx.Diaries.CountAsync(d => d.Words.Any(w => w.StartsWith("dis"))));
                Assert.Equal(2, await ctx.Diaries.CountAsync(d => d.Mood != Mood.Sad));
            }
            await EfDesign.CheckModel("diary_model.txt", conn);
        });
    }

    [Fact]
    public async Task IdentityColumnsByDefault()
    {
        await Target.InFreshDatabase("identity", conn => Target.XfailOnSereneDB(
            "SereneDB has no identity columns, Npgsql creates keys as GENERATED BY DEFAULT AS IDENTITY (#1426)",
            async () =>
            {
                await using var ctx = new IdentityContext(conn);
                await ctx.Database.EnsureCreatedAsync();
                ctx.Add(new Gadget { Name = "blink" });
                await ctx.SaveChangesAsync();
                Assert.Equal(1, await ctx.Gadgets.Select(g => g.Id).SingleAsync());
            }));
    }

    [Fact]
    public async Task CascadingForeignKeys()
    {
        await Target.InFreshDatabase("cascade", conn => Target.XfailOnSereneDB(
            "SereneDB rejects ON DELETE CASCADE, the default for required relationships",
            async () =>
            {
                await using var ctx = new CascadeContext(conn);
                await ctx.Database.EnsureCreatedAsync();
                var owner = new Owner();
                owner.Pets.Add(new Pet());
                ctx.Add(owner);
                await ctx.SaveChangesAsync();
                await ctx.Owners.Where(o => o.Id == owner.Id).ExecuteDeleteAsync();
                Assert.Equal(0, await ctx.Pets.CountAsync());
            }));
    }

    [Fact]
    public async Task JsonOwnedTypes()
    {
        await Target.InFreshDatabase("json", conn => Target.XfailOnSereneDB(
            "SereneDB has no jsonb columns (#613)",
            async () =>
            {
                await using var ctx = new JsonContext(conn);
                await ctx.Database.EnsureCreatedAsync();
                ctx.Add(new Customer { Address = new Address { City = "Radiant", Street = "Mid" } });
                await ctx.SaveChangesAsync();
                Assert.Equal(1, await ctx.Customers.CountAsync(c => c.Address.City == "Radiant"));
            }));
    }

    [Fact]
    public async Task XminRowVersion()
    {
        await Target.InFreshDatabase("xmin", conn => Target.XfailOnSereneDB(
            "SereneDB has no xmin system column, Npgsql maps uint row versions to it",
            async () =>
            {
                await using var ctx = new XminContext(conn);
                await ctx.Database.EnsureCreatedAsync();
                var ticket = new Ticket { Title = "rosh" };
                ctx.Add(ticket);
                await ctx.SaveChangesAsync();
                Assert.NotEqual(0u, ticket.Version);
                ticket.Title = "aegis";
                await ctx.SaveChangesAsync();
            }));
    }
}
