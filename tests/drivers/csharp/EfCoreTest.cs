using System.Data;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Xunit;

namespace SerenedbDrivers;

public sealed class EfCoreFixture : IAsyncLifetime
{
    public string Database { get; } = Target.DatabaseName("ef");
    public string ConnString => Target.ConnString(Database);
    public BlogContext NewContext() => new(ConnString);

    public async Task InitializeAsync()
    {
        await Target.RecreateDatabase(Database);
        await using var ctx = NewContext();
        await ctx.Database.EnsureCreatedAsync();
    }

    public async Task DisposeAsync() => await Target.DropDatabase(Database);
}

[CollectionDefinition("efcore")]
public class EfCoreCollection : ICollectionFixture<EfCoreFixture>;

[Collection("efcore")]
public class EfCoreTest(EfCoreFixture fx)
{
    private async Task<Blog> SeedBlog(string name, int posts)
    {
        await using var ctx = fx.NewContext();
        var blog = new Blog { Name = name, Url = $"https://{name}" };
        for (var i = 0; i < posts; i++)
        {
            blog.Posts.Add(new Post
            {
                Title = $"{(i % 2 == 0 ? "Fresh" : "Stale")} Meat {i}",
                Content = i == 0 ? null : $"chapter {i}",
                Rating = 1.25m * i,
                Published = i % 2 == 0,
                PublishedOn = new DateOnly(2024, 1 + i, 10 + i),
                PublishedAt = new TimeOnly(i, 30, 15),
                ReadingTime = TimeSpan.FromMinutes(15 * (i + 1)),
                ExternalId = Guid.NewGuid(),
                Score = i == 1 ? null : i * 1.5,
                Priority = (short)(10 - i),
            });
        }
        ctx.Add(blog);
        await ctx.SaveChangesAsync();
        return blog;
    }

    [Fact]
    public async Task CrudRoundTripOfEveryColumnType()
    {
        var external = Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff");
        long postId;
        await using (var ctx = fx.NewContext())
        {
            var blog = new Blog { Name = "crud", Url = "https://dota" };
            var post = new Post
            {
                Blog = blog, Title = "Fresh Meat", Content = "hook", Rating = 12.5m,
                Published = true, PublishedOn = new DateOnly(2024, 5, 6),
                PublishedAt = new TimeOnly(7, 8, 9, 123), ReadingTime = TimeSpan.FromMinutes(95.5),
                ExternalId = external, Thumbnail = [0, 1, 2, 255], Score = 4.25, Priority = 3,
            };
            post.Tags.Add(new Tag { Label = "crud-horror" });
            post.Tags.Add(new Tag { Label = "crud-comedy" });
            post.Comments.Add(new Comment
            {
                Body = "first", At = new DateTime(2024, 1, 2, 3, 4, 5, 678, DateTimeKind.Utc),
            });
            ctx.Add(post);
            Assert.Equal(7, await ctx.SaveChangesAsync());
            Assert.True(blog.Id > 0);
            Assert.True(post.Id > 0);
            Assert.True(blog.OrderNumber >= 1000);
            Assert.Equal(0, (blog.OrderNumber - 1000) % 5);
            Assert.True(blog.CreatedAt > DateTime.UtcNow.AddMinutes(-10));
            postId = post.Id;
        }
        await using (var ctx = fx.NewContext())
        {
            var post = await ctx.Posts.Include(p => p.Blog).Include(p => p.Tags)
                .Include(p => p.Comments).SingleAsync(p => p.Id == postId);
            Assert.Equal("crud", post.Blog.Name);
            Assert.Equal("hook", post.Content);
            Assert.Equal(12.5m, post.Rating);
            Assert.True(post.Published);
            Assert.Equal(new DateOnly(2024, 5, 6), post.PublishedOn);
            Assert.Equal(new TimeOnly(7, 8, 9, 123), post.PublishedAt);
            Assert.Equal(TimeSpan.FromMinutes(95.5), post.ReadingTime);
            Assert.Equal(external, post.ExternalId);
            Assert.Equal(new byte[] { 0, 1, 2, 255 }, post.Thumbnail);
            Assert.Equal(4.25, post.Score);
            Assert.Equal((short)3, post.Priority);
            Assert.Equal(["crud-comedy", "crud-horror"], post.Tags.Select(t => t.Label).Order());
            Assert.Equal(new DateTime(2024, 1, 2, 3, 4, 5, 678, DateTimeKind.Utc),
                Assert.Single(post.Comments).At);
            post.Content = "updated";
            post.Tags.RemoveAll(t => t.Label == "crud-comedy");
            Assert.Equal(2, await ctx.SaveChangesAsync());
        }
        await using (var ctx = fx.NewContext())
        {
            var blog = await ctx.Blogs.Include(b => b.Posts).ThenInclude(p => p.Comments)
                .Include(b => b.Posts).ThenInclude(p => p.Tags)
                .AsSplitQuery().SingleAsync(b => b.Name == "crud");
            var post = Assert.Single(blog.Posts);
            Assert.Equal("updated", post.Content);
            Assert.Equal("crud-horror", Assert.Single(post.Tags).Label);
            ctx.RemoveRange(post.Comments);
            post.Tags.Clear();
            await ctx.SaveChangesAsync();
            ctx.Remove(post);
            await ctx.SaveChangesAsync();
            Assert.Null(await ctx.Posts.FindAsync(postId));
        }
    }

    [Fact]
    public async Task StringTranslations()
    {
        var blog = await SeedBlog("strings", 4);
        await using var ctx = fx.NewContext();
        var posts = ctx.Posts.Where(p => p.BlogId == blog.Id);
        Assert.Equal(4, await posts.CountAsync(p => p.Title.Contains("Meat")));
        Assert.Equal(2, await posts.CountAsync(p => p.Title.StartsWith("Fresh")));
        Assert.Equal(1, await posts.CountAsync(p => p.Title.EndsWith(" 3")));
        Assert.Equal(2, await posts.CountAsync(p => p.Title.ToUpper().StartsWith("STALE")));
        Assert.Equal(4, await posts.CountAsync(p => p.Title.ToLower().Contains("meat")));
        Assert.Equal(4, await posts.CountAsync(p => p.Title.Length == 12));
        Assert.Equal(2, await posts.CountAsync(p => p.Title.Substring(0, 5) == "Fresh"));
        Assert.Equal(2, await posts.CountAsync(p => p.Title.Replace("Fresh", "Old").StartsWith("Old")));
        Assert.Equal(4, await posts.CountAsync(p => p.Title.IndexOf("Meat") == 6));
        Assert.Equal(2, await posts.CountAsync(p => EF.Functions.ILike(p.Title, "%fresh%")));
        Assert.Equal(2, await posts.CountAsync(p => EF.Functions.Like(p.Title, "Stale%")));
        Assert.Equal(1, await posts.CountAsync(p => string.IsNullOrEmpty(p.Content)));
        var concatenated = await posts.OrderBy(p => p.Id)
            .Select(p => p.Title + "/" + p.Priority).FirstAsync();
        Assert.Equal("Fresh Meat 0/10", concatenated);
    }

    [Fact]
    public async Task RegexIsMatch()
    {
        var blog = await SeedBlog("regex", 4);
        await Target.XfailOnSereneDB(
            "SereneDB rejects the (?p) embedded option, Npgsql prefixes every Regex.IsMatch pattern with it",
            async () =>
            {
                await using var ctx = fx.NewContext();
                Assert.Equal(1, await ctx.Posts.CountAsync(p => p.BlogId == blog.Id && Regex.IsMatch(p.Title, "^S.* 3$")));
            });
    }

    [Fact]
    public async Task StringTrim()
    {
        var blog = await SeedBlog("trim", 2);
        await Target.XfailOnSereneDB("SereneDB has no btrim(), Npgsql translates string.Trim() to it", async () =>
        {
            await using var ctx = fx.NewContext();
            Assert.Equal(2, await ctx.Posts.CountAsync(p => p.BlogId == blog.Id && (" " + p.Title + " ").Trim() == p.Title));
        });
    }

    [Fact]
    public async Task DateAndTimeTranslations()
    {
        var blog = await SeedBlog("dates", 4);
        await using var ctx = fx.NewContext();
        var posts = ctx.Posts.Where(p => p.BlogId == blog.Id);
        Assert.Equal(1, await posts.CountAsync(p => p.PublishedOn!.Value.Month == 3));
        Assert.Equal(4, await posts.CountAsync(p => p.PublishedOn!.Value.Year == 2024));
        Assert.Equal(1, await posts.CountAsync(p => p.PublishedOn!.Value.Day == 12));
        Assert.Equal(2, await posts.CountAsync(p => p.PublishedOn!.Value.AddDays(30) > new DateOnly(2024, 4, 1)));
        Assert.Equal(1, await posts.CountAsync(p => p.PublishedAt!.Value.Hour == 2));
        Assert.Equal(2, await posts.CountAsync(p => p.ReadingTime > TimeSpan.FromMinutes(30)));
        Assert.Equal(1, await posts.CountAsync(p => p.ReadingTime.TotalMinutes == 45));
        Assert.Equal(4, await ctx.Blogs.Where(b => b.Id == blog.Id)
            .SelectMany(b => b.Posts).CountAsync(p => p.Blog.CreatedAt <= DateTime.UtcNow));
    }

    [Fact]
    public async Task DateOfTimestampWithTimeZone()
    {
        var blog = await SeedBlog("day", 0);
        await Target.XfailOnSereneDB(
            "SereneDB has no date_trunc(text, timestamptz, text), Npgsql translates DateTime.Date to it",
            async () =>
            {
                await using var ctx = fx.NewContext();
                var day = await ctx.Blogs.Where(b => b.Id == blog.Id).Select(b => b.CreatedAt.Date).SingleAsync();
                Assert.Equal(DateTime.UtcNow.Date, day.Date);
            });
    }

    [Fact]
    public async Task MathAndConditionalTranslations()
    {
        var blog = await SeedBlog("math", 4);
        await using var ctx = fx.NewContext();
        var posts = ctx.Posts.Where(p => p.BlogId == blog.Id);
        Assert.Equal(1, await posts.CountAsync(p => Math.Round(p.Rating, 1) == 1.3m));
        Assert.Equal(2, await posts.CountAsync(p => Math.Floor(p.Rating) == 2m || Math.Ceiling(p.Rating) == 4m));
        Assert.Equal(4, await posts.CountAsync(p => Math.Abs(-p.Priority) == p.Priority));
        Assert.Equal(1, await posts.CountAsync(p => Math.Pow(p.Priority, 2) == 64));
        Assert.Equal(1, await posts.CountAsync(p => Math.Sqrt(p.Priority) == 3));
        Assert.Equal(2, await posts.CountAsync(p => p.Priority % 2 == 0));
        Assert.Equal(4, await posts.CountAsync(p => Math.Max(p.Priority, (short)8) >= 8));
        var scores = await posts.OrderBy(p => p.Id).Select(p => p.Score ?? -1).ToListAsync();
        Assert.Equal([0, -1, 3, 4.5], scores);
        var labels = await posts.OrderBy(p => p.Id)
            .Select(p => p.Published ? "on" : p.Content == null ? "none" : "off").ToListAsync();
        Assert.Equal(["on", "off", "on", "off"], labels);
    }

    [Fact]
    public async Task GroupByAndAggregates()
    {
        var blog = await SeedBlog("groups", 5);
        await using var ctx = fx.NewContext();
        var groups = await ctx.Posts.Where(p => p.BlogId == blog.Id)
            .GroupBy(p => p.Published)
            .Select(g => new
            {
                g.Key,
                Count = g.Count(),
                Priority = g.Sum(p => p.Priority),
                Best = g.Max(p => p.Score),
                Worst = g.Min(p => p.Rating),
                Titles = string.Join(",", g.OrderBy(p => p.Title).Select(p => p.Title)),
            })
            .OrderBy(g => g.Key).ToListAsync();
        Assert.Equal(2, groups.Count);
        Assert.Equal((false, 2, 16, 4.5, 1.25m, "Stale Meat 1,Stale Meat 3"),
            (groups[0].Key, groups[0].Count, groups[0].Priority, groups[0].Best, groups[0].Worst,
                groups[0].Titles));
        Assert.Equal((true, 3, 24, 6.0, 0m, "Fresh Meat 0,Fresh Meat 2,Fresh Meat 4"),
            (groups[1].Key, groups[1].Count, groups[1].Priority, groups[1].Best, groups[1].Worst,
                groups[1].Titles));
        var summary = await ctx.Blogs.Where(b => b.Id == blog.Id)
            .Select(b => new
            {
                b.Name,
                Count = b.Posts.Count(),
                Top = b.Posts.OrderByDescending(p => p.Rating).Select(p => p.Title).FirstOrDefault(),
                AnyNull = b.Posts.Any(p => p.Score == null),
                AllRated = b.Posts.All(p => p.Rating >= 0),
            }).SingleAsync();
        Assert.Equal(("groups", 5, "Fresh Meat 4", true, true),
            (summary.Name, summary.Count, summary.Top, summary.AnyNull, summary.AllRated));
    }

    [Fact]
    public async Task AverageOfNumericIsNumeric()
    {
        var blog = await SeedBlog("average", 5);
        await Target.XfailOnSereneDB("SereneDB's avg(numeric) returns double precision (#1435)", async () =>
        {
            await using var ctx = fx.NewContext();
            Assert.Equal(2.5m, await ctx.Posts.Where(p => p.BlogId == blog.Id).AverageAsync(p => p.Rating));
        });
    }

    [Fact]
    public async Task JoinsSetOperationsAndPaging()
    {
        var blog = await SeedBlog("joins", 4);
        var empty = await SeedBlog("joins-empty", 0);
        await using var ctx = fx.NewContext();
        var ids = new[] { blog.Id, empty.Id };
        var left = await (from b in ctx.Blogs
            where ids.Contains(b.Id)
            join p in ctx.Posts on b.Id equals p.BlogId into ps
            from p in ps.DefaultIfEmpty()
            orderby b.Name, p.Title
            select new { b.Name, Title = p == null ? null : p.Title }).ToListAsync();
        Assert.Equal(5, left.Count);
        Assert.Equal(("joins-empty", null), (left[4].Name, left[4].Title));
        var names = new List<string> { "joins", "nobody" };
        Assert.Equal(1, await ctx.Blogs.CountAsync(b => names.Contains(b.Name)));
        var posts = ctx.Posts.Where(p => p.BlogId == blog.Id);
        var page = await posts.OrderBy(p => p.Title).ThenByDescending(p => p.Id).Skip(1).Take(2)
            .Select(p => p.Title).ToListAsync();
        Assert.Equal(["Fresh Meat 2", "Stale Meat 1"], page);
        var fresh = posts.Where(p => p.Published).Select(p => p.Priority);
        var stale = posts.Where(p => !p.Published).Select(p => p.Priority);
        Assert.Equal(new short[] { 7, 8, 9, 10 }, (await fresh.Union(stale).ToListAsync()).Order());
        Assert.Equal(4, await fresh.Concat(stale).CountAsync());
        Assert.Empty(await fresh.Intersect(stale).ToListAsync());
        Assert.Equal(new short[] { 8, 10 }, (await fresh.Except(stale).ToListAsync()).Order());
        Assert.Equal(2, await posts.Select(p => p.Published).Distinct().CountAsync());
        var newest = await ctx.Blogs.Where(b => ids.Contains(b.Id))
            .Where(b => b.Posts.Any(p => p.Rating > ctx.Posts.Where(q => q.BlogId == b.Id)
                .Average(q => q.Rating)))
            .Select(b => b.Name).ToListAsync();
        Assert.Equal(["joins"], newest);
    }

    [Fact]
    public async Task PrimitiveCollectionInSubquery()
    {
        var first = await SeedBlog("unnest-a", 0);
        var second = await SeedBlog("unnest-b", 0);
        await Target.XfailOnSereneDB(
            "SereneDB describes a statement with a parameter inside unnest() as one 'unknown' column",
            async () =>
            {
                await using var ctx = fx.NewContext();
                var ids = new[] { first.Id, second.Id };
                Assert.Equal(1, await ctx.Blogs.CountAsync(b => ids.Where(i => i > first.Id).Contains(b.Id)));
            });
    }

    [Fact]
    public async Task BulkUpdateDeleteAndRawSql()
    {
        var blog = await SeedBlog("bulk", 4);
        await using var ctx = fx.NewContext();
        var posts = ctx.Posts.Where(p => p.BlogId == blog.Id);
        Assert.Equal(2, await posts.Where(p => p.Published)
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Priority, p => p.Priority + 100)
                .SetProperty(p => p.Content, "bulk")));
        Assert.Equal(2, await posts.CountAsync(p => p.Priority > 100 && p.Content == "bulk"));
        var name = "bulk";
        var raw = await ctx.Blogs.FromSqlInterpolated(
            $"SELECT * FROM \"Blogs\" WHERE \"Name\" = {name}").SingleAsync();
        Assert.Equal(blog.Id, raw.Id);
        var priorities = await ctx.Database.SqlQuery<short>(
                $"SELECT \"Priority\" AS \"Value\" FROM \"Posts\" WHERE \"BlogId\" = {blog.Id}")
            .OrderBy(x => x).ToListAsync();
        Assert.Equal(new short[] { 7, 9, 108, 110 }, priorities);
        Assert.Equal(4, await ctx.Database.ExecuteSqlInterpolatedAsync(
            $"UPDATE \"Posts\" SET \"Score\" = 1 WHERE \"BlogId\" = {blog.Id}"));
        await using (var conn = new NpgsqlConnection(fx.ConnString))
        {
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "INSERT INTO \"Comments\" (\"PostId\", \"Body\", \"At\") SELECT \"Id\", 'bulk', now() FROM \"Posts\" WHERE \"BlogId\" = $1",
                conn);
            cmd.Parameters.AddWithValue(blog.Id);
            Assert.Equal(4, await cmd.ExecuteNonQueryAsync());
        }
        Assert.Equal(4, await ctx.Comments.Where(c => c.Post.BlogId == blog.Id).ExecuteDeleteAsync());
        Assert.Equal(4, await posts.ExecuteDeleteAsync());
    }

    [Fact]
    public async Task TrackingFindAttachAndConcurrencyToken()
    {
        var blog = await SeedBlog("tracking", 1);
        await using (var ctx = fx.NewContext())
        {
            var found = await ctx.Blogs.FindAsync(blog.Id);
            Assert.Equal("tracking", found!.Name);
            Assert.Same(found, await ctx.Blogs.FindAsync(blog.Id));
            Assert.Equal(1, await ctx.Blogs.AsNoTracking().CountAsync(b => b.Id == blog.Id));
        }
        await using (var ctx = fx.NewContext())
        {
            var detached = new Blog { Id = blog.Id, Name = "tracking", Revision = 0, Url = "attached" };
            ctx.Attach(detached).Property(b => b.Url).IsModified = true;
            Assert.Equal(1, await ctx.SaveChangesAsync());
        }
        await using var first = fx.NewContext();
        await using var second = fx.NewContext();
        var a = await first.Blogs.SingleAsync(b => b.Id == blog.Id);
        var b = await second.Blogs.SingleAsync(x => x.Id == blog.Id);
        Assert.Equal("attached", a.Url);
        a.Revision++;
        await first.SaveChangesAsync();
        b.Revision++;
        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => second.SaveChangesAsync());
        await second.Entry(b).ReloadAsync();
        Assert.Equal(1, b.Revision);
    }

    [Fact]
    public async Task ExplicitTransactionsCommitAndRollback()
    {
        await using var ctx = fx.NewContext();
        ctx.Database.AutoSavepointsEnabled = false;
        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            ctx.Add(new Tag { Label = "tx-commit" });
            await ctx.SaveChangesAsync();
            await tx.CommitAsync();
        }
        await using (var tx = await ctx.Database.BeginTransactionAsync(IsolationLevel.RepeatableRead))
        {
            ctx.Add(new Tag { Label = "tx-rollback" });
            await ctx.SaveChangesAsync();
            await tx.RollbackAsync();
        }
        ctx.ChangeTracker.Clear();
        Assert.Equal(1, await ctx.Tags.CountAsync(t => t.Label.StartsWith("tx-")));
    }

    [Fact]
    public async Task SerializableTransaction()
    {
        await Target.XfailOnSereneDB("SereneDB rejects ISOLATION LEVEL SERIALIZABLE", async () =>
        {
            await using var ctx = fx.NewContext();
            await using var tx = await ctx.Database.BeginTransactionAsync(IsolationLevel.Serializable);
            Assert.True(await ctx.Tags.CountAsync() >= 0);
            await tx.CommitAsync();
        });
    }

    [Fact]
    public async Task SaveChangesInsideTransactionCreatesSavepoint()
    {
        await Target.XfailOnSereneDB("SereneDB has no SAVEPOINT (#722)", async () =>
        {
            await using var ctx = fx.NewContext();
            await using var tx = await ctx.Database.BeginTransactionAsync();
            ctx.Add(new Tag { Label = "savepoint-auto" });
            await ctx.SaveChangesAsync();
            await tx.CommitAsync();
        });
    }

    [Fact]
    public async Task ManualSavepoints()
    {
        await Target.XfailOnSereneDB("SereneDB has no SAVEPOINT (#722)", async () =>
        {
            await using var ctx = fx.NewContext();
            ctx.Database.AutoSavepointsEnabled = false;
            await using var tx = await ctx.Database.BeginTransactionAsync();
            ctx.Add(new Tag { Label = "savepoint-kept" });
            await ctx.SaveChangesAsync();
            await tx.CreateSavepointAsync("before");
            ctx.Add(new Tag { Label = "savepoint-undone" });
            await ctx.SaveChangesAsync();
            await tx.RollbackToSavepointAsync("before");
            await tx.ReleaseSavepointAsync("before");
            await tx.CommitAsync();
            ctx.ChangeTracker.Clear();
            Assert.Equal(["savepoint-kept"],
                await ctx.Tags.Where(t => t.Label.StartsWith("savepoint-k") || t.Label.StartsWith("savepoint-u"))
                    .Select(t => t.Label).ToListAsync());
        });
    }

    [Fact]
    public async Task ClientCascadeDeletesChildrenAndParentInOneSaveChanges()
    {
        var blog = await SeedBlog("cascade", 2);
        await Target.XfailOnSereneDB(
            "SereneDB rejects deleting a parent row after its children in one transaction (#1433)",
            async () =>
            {
                await using var ctx = fx.NewContext();
                var loaded = await ctx.Blogs.Include(b => b.Posts).SingleAsync(b => b.Id == blog.Id);
                ctx.Remove(loaded);
                Assert.Equal(3, await ctx.SaveChangesAsync());
            });
    }

    [Fact]
    public async Task SelectForUpdate()
    {
        var blog = await SeedBlog("locking", 0);
        await Target.XfailOnSereneDB("SereneDB has no SELECT ... FOR UPDATE (#1431)", async () =>
        {
            await using var ctx = fx.NewContext();
            await using var tx = await ctx.Database.BeginTransactionAsync();
            var locked = await ctx.Blogs.FromSql($"SELECT * FROM \"Blogs\" WHERE \"Id\" = {blog.Id} FOR UPDATE")
                .SingleAsync();
            locked.Url = "locked";
            await ctx.SaveChangesAsync();
            await tx.CommitAsync();
        });
    }
}
