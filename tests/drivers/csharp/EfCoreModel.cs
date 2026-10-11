using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace SerenedbDrivers;

public class Blog
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public string? Url { get; set; }
    public DateTime CreatedAt { get; set; }
    public bool Active { get; set; }
    public int OrderNumber { get; set; }
    public int Revision { get; set; }
    public List<Post> Posts { get; } = new();
}

public class Post
{
    public long Id { get; set; }
    public int BlogId { get; set; }
    public Blog Blog { get; set; } = null!;
    public string Title { get; set; } = "";
    public string? Content { get; set; }
    public decimal Rating { get; set; }
    public bool Published { get; set; }
    public DateOnly? PublishedOn { get; set; }
    public TimeOnly? PublishedAt { get; set; }
    public TimeSpan ReadingTime { get; set; }
    public Guid ExternalId { get; set; }
    public byte[]? Thumbnail { get; set; }
    public double? Score { get; set; }
    public short Priority { get; set; }
    public List<Tag> Tags { get; } = new();
    public List<Comment> Comments { get; } = new();
}

public class Tag
{
    public int Id { get; set; }
    public string Label { get; set; } = "";
    public List<Post> Posts { get; } = new();
}

public class Comment
{
    public long Id { get; set; }
    public long PostId { get; set; }
    public Post Post { get; set; } = null!;
    public string Body { get; set; } = "";
    public DateTime At { get; set; }
}

public class BlogContext(string connString) : DbContext
{
    public DbSet<Blog> Blogs => Set<Blog>();
    public DbSet<Post> Posts => Set<Post>();
    public DbSet<Tag> Tags => Set<Tag>();
    public DbSet<Comment> Comments => Set<Comment>();

    protected override void OnConfiguring(DbContextOptionsBuilder options) =>
        options.UseNpgsql(connString);

    protected override void OnModelCreating(ModelBuilder model)
    {
        model.UseSerialColumns();
        model.HasSequence<int>("blog_numbers").StartsAt(1000).IncrementsBy(5);
        model.Entity<Blog>(b =>
        {
            b.ToTable(t => t.HasComment("blogs we follow"));
            b.Property(x => x.Name).HasMaxLength(100);
            b.HasIndex(x => x.Name).IsUnique();
            b.Property(x => x.Url).HasComment("home page");
            b.Property(x => x.CreatedAt).HasDefaultValueSql("now()");
            b.Property(x => x.Active).HasDefaultValue(true);
            b.Property(x => x.OrderNumber).HasDefaultValueSql("nextval('blog_numbers')");
            b.Property(x => x.Revision).IsConcurrencyToken();
        });
        model.Entity<Post>(p =>
        {
            p.ToTable(t => t.HasCheckConstraint("CK_Posts_Rating", "\"Rating\" >= 0"));
            p.Property(x => x.Title).HasMaxLength(200);
            p.Property(x => x.Rating).HasPrecision(5, 2);
            p.HasIndex(x => new { x.BlogId, x.Published });
            p.HasIndex(x => x.ExternalId).IsUnique();
            p.HasIndex(x => x.Score).IsDescending();
            p.HasOne(x => x.Blog).WithMany(x => x.Posts).OnDelete(DeleteBehavior.ClientCascade);
            p.HasMany(x => x.Tags).WithMany(x => x.Posts).UsingEntity<Dictionary<string, object>>(
                "PostTag",
                r => r.HasOne<Tag>().WithMany().HasForeignKey("TagsId")
                    .OnDelete(DeleteBehavior.ClientCascade),
                l => l.HasOne<Post>().WithMany().HasForeignKey("PostsId")
                    .OnDelete(DeleteBehavior.ClientCascade));
        });
        model.Entity<Tag>(t =>
        {
            t.Property(x => x.Label).HasMaxLength(50);
            t.HasIndex(x => x.Label).IsUnique();
        });
        model.Entity<Comment>(c =>
        {
            c.HasIndex(x => new { x.PostId, x.At });
            c.HasOne(x => x.Post).WithMany(x => x.Comments).OnDelete(DeleteBehavior.ClientCascade);
        });
    }
}

public class Gadget
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}

public class IdentityContext(string connString) : DbContext
{
    public DbSet<Gadget> Gadgets => Set<Gadget>();

    protected override void OnConfiguring(DbContextOptionsBuilder options) =>
        options.UseNpgsql(connString);
}

public class Owner
{
    public int Id { get; set; }
    public List<Pet> Pets { get; } = new();
}

public class Pet
{
    public int Id { get; set; }
    public int OwnerId { get; set; }
    public Owner Owner { get; set; } = null!;
}

public class CascadeContext(string connString) : DbContext
{
    public DbSet<Owner> Owners => Set<Owner>();
    public DbSet<Pet> Pets => Set<Pet>();

    protected override void OnConfiguring(DbContextOptionsBuilder options) =>
        options.UseNpgsql(connString);

    protected override void OnModelCreating(ModelBuilder model) => model.UseSerialColumns();
}

public enum Mood
{
    Happy,
    Sad,
    Meh,
}

public class Diary
{
    public int Id { get; set; }
    public Mood Mood { get; set; }
    public int[] Steps { get; set; } = [];
    public List<string> Words { get; set; } = new();
    public decimal Price { get; set; }
    public decimal DoublePrice { get; set; }
}

public class DiaryContext(string connString) : DbContext
{
    public DbSet<Diary> Diaries => Set<Diary>();

    protected override void OnConfiguring(DbContextOptionsBuilder options) =>
        options.UseNpgsql(connString, o => o.MapEnum<Mood>("mood"));

    protected override void OnModelCreating(ModelBuilder model)
    {
        model.UseSerialColumns();
        model.Entity<Diary>().Property(x => x.DoublePrice)
            .HasComputedColumnSql("\"Price\" * 2", stored: true);
    }
}

public class Address
{
    public string City { get; set; } = "";
    public string Street { get; set; } = "";
}

public class Customer
{
    public int Id { get; set; }
    public Address Address { get; set; } = new();
}

public class JsonContext(string connString) : DbContext
{
    public DbSet<Customer> Customers => Set<Customer>();

    protected override void OnConfiguring(DbContextOptionsBuilder options) =>
        options.UseNpgsql(connString);

    protected override void OnModelCreating(ModelBuilder model)
    {
        model.UseSerialColumns();
        model.Entity<Customer>().OwnsOne(x => x.Address, a => a.ToJson());
    }
}

public class Ticket
{
    public int Id { get; set; }
    public string Title { get; set; } = "";
    public uint Version { get; set; }
}

public class XminContext(string connString) : DbContext
{
    public DbSet<Ticket> Tickets => Set<Ticket>();

    protected override void OnConfiguring(DbContextOptionsBuilder options) =>
        options.UseNpgsql(connString);

    protected override void OnModelCreating(ModelBuilder model)
    {
        model.UseSerialColumns();
        model.Entity<Ticket>().Property(x => x.Version).IsRowVersion();
    }
}

public class Account
{
    public int Id { get; set; }
    public string Email { get; set; } = "";
    public decimal Balance { get; set; }
    public string? Nickname { get; set; }
}

public class LedgerContext(string connString) : DbContext
{
    public DbSet<Account> Accounts => Set<Account>();

    protected override void OnConfiguring(DbContextOptionsBuilder options) =>
        options.UseNpgsql(connString)
            .ConfigureWarnings(w => w.Ignore(RelationalEventId.PendingModelChangesWarning));

    protected override void OnModelCreating(ModelBuilder model) => model.UseSerialColumns();
}
