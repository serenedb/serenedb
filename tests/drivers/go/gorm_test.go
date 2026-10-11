package drivers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type Company struct {
	ID        uint
	Name      string `gorm:"size:100;uniqueIndex"`
	Founded   time.Time
	Budget    float64 `gorm:"type:numeric(12,2);default:0"`
	Employees []Employee
}

type Employee struct {
	ID        uint64 `gorm:"primaryKey"`
	CompanyID uint   `gorm:"index"`
	Company   Company
	Name      string     `gorm:"size:80;not null"`
	Email     *string    `gorm:"size:120;uniqueIndex:idx_employee_email"`
	Salary    float64    `gorm:"type:numeric(10,2)"`
	Active    bool       `gorm:"default:true"`
	BirthDate *time.Time `gorm:"type:date"`
	Photo     []byte
	Level     int16 `gorm:"check:level_non_negative,level >= 0"`
	Profile   *Profile
	Languages []Language `gorm:"many2many:employee_languages"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

type Language struct {
	ID   uint
	Code string `gorm:"size:8;uniqueIndex"`
}

type Profile struct {
	ID         uint
	EmployeeID uint64 `gorm:"uniqueIndex"`
	Bio        string `gorm:"type:text"`
	Settings   string `gorm:"comment:user settings"`
}

var gormModels = []any{&Company{}, &Employee{}, &Language{}, &Profile{}}

type statementLog struct {
	mu         sync.Mutex
	statements []string
}

func (l *statementLog) LogMode(logger.LogLevel) logger.Interface { return l }
func (l *statementLog) Info(context.Context, string, ...any)     {}
func (l *statementLog) Warn(context.Context, string, ...any)     {}
func (l *statementLog) Error(context.Context, string, ...any)    {}
func (l *statementLog) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.statements = append(l.statements, sql)
}

func (l *statementLog) ddl() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, s := range l.statements {
		upper := strings.ToUpper(strings.TrimSpace(s))
		for _, verb := range []string{"CREATE ", "ALTER ", "DROP ", "COMMENT "} {
			if strings.HasPrefix(upper, verb) {
				out = append(out, s)
			}
		}
	}
	return out
}

func openGorm(t *testing.T, url string, config *gorm.Config) *gorm.DB {
	t.Helper()
	if config.Logger == nil {
		config.Logger = logger.Default.LogMode(logger.Silent)
	}
	db, err := gorm.Open(postgres.Open(url), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			sqlDB.Close()
		}
	})
	return db
}

func migratedGorm(t *testing.T, prefix string) *gorm.DB {
	t.Helper()
	db := openGorm(t, freshDatabase(t, prefix), &gorm.Config{})
	if err := db.AutoMigrate(gormModels...); err != nil {
		t.Fatal(err)
	}
	return db
}

func describeGormSchema(db *gorm.DB) (string, error) {
	var sb strings.Builder
	m := db.Migrator()
	tables, err := m.GetTables()
	if err != nil {
		return "", err
	}
	sort.Strings(tables)
	fmt.Fprintf(&sb, "tables %v\n", tables)
	for _, table := range []string{"companies", "employees", "languages", "profiles", "employee_languages"} {
		fmt.Fprintf(&sb, "table %s exists %v\n", table, m.HasTable(table))
		columns, err := m.ColumnTypes(table)
		if err != nil {
			return "", err
		}
		for _, c := range columns {
			columnType, _ := c.ColumnType()
			pk, _ := c.PrimaryKey()
			auto, _ := c.AutoIncrement()
			length, hasLength := c.Length()
			precision, scale, hasDecimal := c.DecimalSize()
			nullable, _ := c.Nullable()
			unique, _ := c.Unique()
			def, hasDefault := c.DefaultValue()
			comment, _ := c.Comment()
			fmt.Fprintf(&sb, "  column %s %s (%s) pk %v auto %v length %d/%v decimal %d,%d/%v nullable %v unique %v default %q/%v comment %q scan %v\n",
				c.Name(), c.DatabaseTypeName(), columnType, pk, auto, length, hasLength, precision, scale,
				hasDecimal, nullable, unique, def, hasDefault, comment, c.ScanType())
		}
		indexes, err := m.GetIndexes(table)
		if err != nil {
			return "", err
		}
		sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name() < indexes[j].Name() })
		for _, idx := range indexes {
			pk, _ := idx.PrimaryKey()
			unique, _ := idx.Unique()
			fmt.Fprintf(&sb, "  index %s %v pk %v unique %v option %q\n", idx.Name(), idx.Columns(), pk, unique, idx.Option())
		}
	}
	for _, check := range []struct {
		model any
		name  string
	}{
		{&Employee{}, "fk_companies_employees"},
		{&Employee{}, "level_non_negative"},
		{&Employee{}, "Company"},
		{&Profile{}, "fk_employees_profile"},
		{&Language{}, "idx_languages_code"},
	} {
		fmt.Fprintf(&sb, "constraint %T %s %v\n", check.model, check.name, m.HasConstraint(check.model, check.name))
	}
	fmt.Fprintf(&sb, "index idx_employee_email %v\n", m.HasIndex(&Employee{}, "idx_employee_email"))
	fmt.Fprintf(&sb, "column employees.email %v\n", m.HasColumn(&Employee{}, "Email"))
	fmt.Fprintf(&sb, "type aliases %v\n", m.GetTypeAliases("int4"))
	return sb.String(), nil
}

func TestGormAutoMigrateIntrospection(t *testing.T) {
	db := migratedGorm(t, "gorm_schema")
	text, err := describeGormSchema(db)
	if err != nil {
		t.Fatal(err)
	}
	checkGolden(t, fixture("gorm/introspection.txt"), text)
}

func TestGormAutoMigrateIsIdempotent(t *testing.T) {
	db := migratedGorm(t, "gorm_idempotent")
	log := &statementLog{}
	xfailOnSereneDB(t, "SereneDB reports varchar(n) as text (#1434), so AutoMigrate alters every sized column again", func() error {
		if err := db.Session(&gorm.Session{Logger: log}).AutoMigrate(gormModels...); err != nil {
			return err
		}
		if ddl := log.ddl(); len(ddl) != 0 {
			return fmt.Errorf("second AutoMigrate issued DDL: %q", ddl)
		}
		return nil
	})
}

func TestGormNestedCreateMapsReturnedIds(t *testing.T) {
	db := migratedGorm(t, "gorm_nested_create")
	company := Company{Name: "Radiant", Employees: []Employee{
		{Name: "Pudge", Profile: &Profile{Bio: "fresh meat"}, Languages: []Language{{Code: "en"}, {Code: "ru"}}},
		{Name: "Axe", Languages: []Language{{Code: "de"}}},
	}}
	if err := db.Create(&company).Error; err != nil {
		t.Fatal(err)
	}
	xfailOnSereneDB(t, "SereneDB returns INSERT ... ON CONFLICT ... RETURNING rows in reverse VALUES order, "+
		"so GORM attaches upserted associations to the wrong rows", func() error {
		var pudge Employee
		if err := db.Preload("Profile").Preload("Languages", func(db *gorm.DB) *gorm.DB { return db.Order("code") }).
			First(&pudge, "name = ?", "Pudge").Error; err != nil {
			return err
		}
		if pudge.Profile == nil || len(pudge.Languages) != 2 || pudge.Languages[0].Code != "en" {
			return fmt.Errorf("associations of Pudge: %+v", pudge)
		}
		return nil
	})
}

func TestGormCrudAndAssociations(t *testing.T) {
	db := migratedGorm(t, "gorm_crud")
	email := "pudge@dota"
	birth := time.Date(1990, 5, 6, 0, 0, 0, 0, time.UTC)
	company := Company{Name: "Radiant", Founded: time.Date(2005, 1, 2, 3, 4, 5, 0, time.UTC), Budget: 1234.5}
	if err := db.Create(&company).Error; err != nil {
		t.Fatal(err)
	}
	created := Employee{CompanyID: company.ID, Name: "Pudge", Email: &email, Salary: 99.95, BirthDate: &birth,
		Photo: []byte{0, 1, 255}, Level: 25, Profile: &Profile{Bio: "fresh meat", Settings: "hook"},
		Languages: []Language{{Code: "en"}}}
	if err := db.Create(&created).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&created).Association("Languages").Append(&Language{Code: "ru"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&Employee{CompanyID: company.ID, Name: "Axe", Salary: 50, Level: 3,
		Languages: []Language{{Code: "de"}}}).Error; err != nil {
		t.Fatal(err)
	}
	if company.ID == 0 || created.ID == 0 || created.Profile.ID == 0 {
		t.Fatalf("ids not returned: %+v %+v", company, created)
	}
	var loaded Company
	if err := db.Preload("Employees", func(db *gorm.DB) *gorm.DB { return db.Order("name") }).
		Preload("Employees.Languages", func(db *gorm.DB) *gorm.DB { return db.Order("code") }).
		Preload("Employees.Profile").First(&loaded, company.ID).Error; err != nil {
		t.Fatal(err)
	}
	if len(loaded.Employees) != 2 {
		t.Fatalf("preloaded employees: %+v", loaded)
	}
	pudge := loaded.Employees[1]
	if pudge.Email == nil || pudge.BirthDate == nil || pudge.Profile == nil {
		t.Fatalf("preloaded pointers: %+v", pudge)
	}
	if loaded.Name != "Radiant" || !loaded.Founded.Equal(company.Founded) || loaded.Budget != 1234.5 ||
		pudge.Name != "Pudge" || *pudge.Email != email || pudge.Salary != 99.95 ||
		!pudge.Active || !pudge.BirthDate.Equal(birth) || string(pudge.Photo) != "\x00\x01\xff" ||
		pudge.Profile.Bio != "fresh meat" || len(pudge.Languages) != 2 || pudge.Languages[1].Code != "ru" {
		t.Fatalf("round trip mismatch: %+v", loaded)
	}
	var joined []struct {
		Name    string
		Company string
	}
	if err := db.Model(&Employee{}).Select("employees.name, companies.name AS company").
		Joins("JOIN companies ON companies.id = employees.company_id").Order("employees.name").
		Scan(&joined).Error; err != nil || len(joined) != 2 || joined[0].Company != "Radiant" {
		t.Fatalf("joins: %v %+v", err, joined)
	}
	var withCompany []Employee
	if err := db.Joins("Company").Where("employees.level > ?", 10).Find(&withCompany).Error; err != nil ||
		len(withCompany) != 1 || withCompany[0].Company.Name != "Radiant" {
		t.Fatalf("joins preload: %v %+v", err, withCompany)
	}
	languages := func() *gorm.Association { return db.Model(&pudge).Association("Languages") }
	if n := languages().Count(); n != 2 {
		t.Fatalf("association count %d", n)
	}
	if err := languages().Append(&Language{Code: "fr"}); err != nil {
		t.Fatal(err)
	}
	if err := languages().Delete(&pudge.Languages[0]); err != nil {
		t.Fatal(err)
	}
	var kept []Language
	if err := languages().Find(&kept); err != nil || len(kept) != 2 {
		t.Fatalf("association after edit: %v %+v", err, kept)
	}
	if err := db.Model(&pudge).Updates(Employee{Salary: 120, Level: 30}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&pudge).Update("active", false).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&Employee{}).Where("company_id = ?", company.ID).
		UpdateColumn("salary", gorm.Expr("salary * 2")).Error; err != nil {
		t.Fatal(err)
	}
	var salaries []float64
	if err := db.Model(&Employee{}).Order("salary").Pluck("salary", &salaries).Error; err != nil ||
		fmt.Sprint(salaries) != "[100 240]" {
		t.Fatalf("pluck: %v %v", err, salaries)
	}
	var stats []struct {
		Active bool
		N      int64
		Total  float64
	}
	if err := db.Model(&Employee{}).Select("active, count(*) AS n, sum(salary) AS total").
		Group("active").Having("count(*) > ?", 0).Order("active").Scan(&stats).Error; err != nil ||
		len(stats) != 2 || stats[0].Total != 240 {
		t.Fatalf("group: %v %+v", err, stats)
	}
	var page []Employee
	if err := db.Order("name DESC").Limit(1).Offset(1).Find(&page).Error; err != nil || len(page) != 1 ||
		page[0].Name != "Axe" {
		t.Fatalf("paging: %v %+v", err, page)
	}
	highLevel := func(db *gorm.DB) *gorm.DB { return db.Where("level >= ?", 20) }
	var count int64
	if err := db.Model(&Employee{}).Scopes(highLevel).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("scopes: %v %d", err, count)
	}
	var batches int
	if err := db.Model(&Language{}).FindInBatches(&[]Language{}, 2, func(tx *gorm.DB, batch int) error {
		batches = batch
		return nil
	}).Error; err != nil || batches != 2 {
		t.Fatalf("batches: %v %d", err, batches)
	}
	if err := db.Create(&Employee{CompanyID: company.ID, Name: "Lina"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(&Employee{}, "name = ?", "Lina").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&Employee{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("soft delete: %v %d", err, count)
	}
	if err := db.Unscoped().Model(&Employee{}).Count(&count).Error; err != nil || count != 3 {
		t.Fatalf("unscoped: %v %d", err, count)
	}
	var raw []struct{ Code string }
	if err := db.Raw("SELECT code FROM languages WHERE code IN ? ORDER BY code", []string{"de", "fr"}).
		Scan(&raw).Error; err != nil || len(raw) != 2 {
		t.Fatalf("raw: %v %+v", err, raw)
	}
}

func TestGormSoftDeleteReferencedRow(t *testing.T) {
	db := migratedGorm(t, "gorm_softdelete")
	company := Company{Name: "Dire"}
	if err := db.Create(&company).Error; err != nil {
		t.Fatal(err)
	}
	axe := Employee{CompanyID: company.ID, Name: "Axe", Profile: &Profile{Bio: "berserker"}}
	if err := db.Create(&axe).Error; err != nil {
		t.Fatal(err)
	}
	xfailOnSereneDB(t, "SereneDB fails UPDATE of an indexed column (deleted_at) of a row a FOREIGN KEY references",
		func() error {
			if err := db.Delete(&axe).Error; err != nil {
				return err
			}
			var count int64
			if err := db.Unscoped().Model(&Employee{}).Where("deleted_at IS NOT NULL").Count(&count).Error; err != nil {
				return err
			}
			if count != 1 {
				return fmt.Errorf("soft deleted rows %d", count)
			}
			return nil
		})
}

func TestGormExpressionWithParameter(t *testing.T) {
	db := migratedGorm(t, "gorm_expr")
	company := Company{Name: "Dire"}
	if err := db.Create(&company).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&Employee{CompanyID: company.ID, Name: "Lion", Salary: 50}).Error; err != nil {
		t.Fatal(err)
	}
	xfailOnSereneDB(t, "SereneDB describes salary * $1 with an untyped parameter as an 'unknown' column and "+
		"aborts the transaction", func() error {
		if err := db.Model(&Employee{}).Where("company_id = ?", company.ID).
			UpdateColumn("salary", gorm.Expr("salary * ?", 3)).Error; err != nil {
			return err
		}
		var salary float64
		if err := db.Model(&Employee{}).Select("salary").Where("name = ?", "Lion").Scan(&salary).Error; err != nil {
			return err
		}
		if salary != 150 {
			return fmt.Errorf("salary %v", salary)
		}
		return nil
	})
}

func TestGormUpsertReturningAndBatches(t *testing.T) {
	db := migratedGorm(t, "gorm_upsert")
	languages := []Language{{Code: "en"}, {Code: "es"}, {Code: "it"}, {Code: "pt"}, {Code: "nl"}}
	if err := db.CreateInBatches(&languages, 2).Error; err != nil {
		t.Fatal(err)
	}
	for _, l := range languages {
		if l.ID == 0 {
			t.Fatalf("batch ids: %+v", languages)
		}
	}
	if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&Language{Code: "en"}).Error; err != nil {
		t.Fatal(err)
	}
	updated := Language{Code: "es"}
	if err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "code"}},
		DoUpdates: clause.Assignments(map[string]any{"code": "es-ES"})}).Create(&updated).Error; err != nil {
		t.Fatal(err)
	}
	var codes []string
	if err := db.Model(&Language{}).Order("code").Pluck("code", &codes).Error; err != nil ||
		strings.Join(codes, ",") != "en,es-ES,it,nl,pt" {
		t.Fatalf("upsert: %v %v", err, codes)
	}
	var returned []Language
	if err := db.Model(&returned).Clauses(clause.Returning{}).Where("code LIKE ?", "%t").
		Update("code", gorm.Expr("upper(code)")).Error; err != nil || len(returned) != 2 {
		t.Fatalf("returning: %v %+v", err, returned)
	}
	prepared := db.Session(&gorm.Session{PrepareStmt: true})
	for i := 0; i < 3; i++ {
		var n int64
		if err := prepared.Model(&Language{}).Where("code <> ?", fmt.Sprint(i)).Count(&n).Error; err != nil || n != 5 {
			t.Fatalf("prepared: %v %d", err, n)
		}
	}
}

func TestGormTransactions(t *testing.T) {
	db := migratedGorm(t, "gorm_tx")
	if err := db.Transaction(func(tx *gorm.DB) error {
		return tx.Create(&Language{Code: "kept"}).Error
	}); err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("rollback")
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&Language{Code: "gone"}).Error; err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("expected rollback, got %v", err)
	}
	tx := db.Begin()
	if err := tx.Create(&Language{Code: "manual"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	var codes []string
	if err := db.Model(&Language{}).Pluck("code", &codes).Error; err != nil || fmt.Sprint(codes) != "[kept]" {
		t.Fatalf("transactions: %v %v", err, codes)
	}
}

func TestGormNestedTransaction(t *testing.T) {
	db := migratedGorm(t, "gorm_nested")
	xfailOnSereneDB(t, "SereneDB has no SAVEPOINT (#722), GORM nests transactions with savepoints", func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Create(&Language{Code: "outer"}).Error; err != nil {
				return err
			}
			inner := tx.Transaction(func(tx2 *gorm.DB) error {
				if err := tx2.Create(&Language{Code: "inner"}).Error; err != nil {
					return err
				}
				return errors.New("undo inner")
			})
			if inner == nil {
				return errors.New("inner transaction did not fail")
			}
			var codes []string
			if err := tx.Model(&Language{}).Pluck("code", &codes).Error; err != nil {
				return err
			}
			if fmt.Sprint(codes) != "[outer]" {
				return fmt.Errorf("after inner rollback: %v", codes)
			}
			return nil
		})
	})
}

func TestGormSavePoint(t *testing.T) {
	db := migratedGorm(t, "gorm_savepoint")
	xfailOnSereneDB(t, "SereneDB has no SAVEPOINT (#722)", func() error {
		tx := db.Begin()
		defer tx.Rollback()
		if err := tx.Create(&Language{Code: "a"}).Error; err != nil {
			return err
		}
		if err := tx.SavePoint("sp1").Error; err != nil {
			return err
		}
		if err := tx.Create(&Language{Code: "b"}).Error; err != nil {
			return err
		}
		if err := tx.RollbackTo("sp1").Error; err != nil {
			return err
		}
		return tx.Commit().Error
	})
}

func TestGormRowLocking(t *testing.T) {
	db := migratedGorm(t, "gorm_locking")
	if err := db.Create(&Language{Code: "lock"}).Error; err != nil {
		t.Fatal(err)
	}
	xfailOnSereneDB(t, "SereneDB has no SELECT ... FOR UPDATE (#1431)", func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			var l Language
			return tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&l, "code = ?", "lock").Error
		})
	})
}

func TestGormCascadeDeleteAssociations(t *testing.T) {
	db := migratedGorm(t, "gorm_cascade")
	company := Company{Name: "Dire", Employees: []Employee{{Name: "Lich"}}}
	if err := db.Create(&company).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Unscoped().Where("company_id = ?", company.ID).Delete(&Employee{}).Error; err != nil {
		t.Fatal(err)
	}
	other := Company{Name: "Neutral", Employees: []Employee{{Name: "Roshan"}}}
	if err := db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	xfailOnSereneDB(t, "SereneDB rejects deleting a parent row after its children in one transaction (#1433)", func() error {
		return db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Unscoped().Where("company_id = ?", other.ID).Delete(&Employee{}).Error; err != nil {
				return err
			}
			return tx.Delete(&other).Error
		})
	})
	if err := db.Delete(&company).Error; err != nil {
		t.Fatal(err)
	}
}

type Gadget struct {
	ID     uint
	Name   string `gorm:"size:20"`
	Weight int32
}

type GadgetV2 struct {
	ID     uint
	Name   string `gorm:"size:60;index:idx_gadgets_name"`
	Weight int64
	Colour string `gorm:"default:red"`
}

func (GadgetV2) TableName() string { return "gadgets" }

type Pet struct {
	ID      uint
	OwnerID uint
	Owner   Owner
}

type Owner struct {
	ID uint
}

type Leash struct {
	ID    uint
	DogID uint
	Dog   Dog `gorm:"constraint:OnDelete:CASCADE"`
}

type Dog struct {
	ID uint
}

func TestGormMigratorOperations(t *testing.T) {
	db := openGorm(t, freshDatabase(t, "gorm_ops"), &gorm.Config{})
	m := db.Migrator()
	steps := []struct {
		name string
		run  func() error
	}{
		{"create table", func() error { return m.CreateTable(&Gadget{}) }},
		{"add column", func() error { return m.AddColumn(&GadgetV2{}, "Colour") }},
		{"alter column type", func() error { return m.AlterColumn(&GadgetV2{}, "Weight") }},
		{"alter column size", func() error { return m.AlterColumn(&GadgetV2{}, "Name") }},
		{"rename column", func() error { return m.RenameColumn(&GadgetV2{}, "colour", "color") }},
		{"create index", func() error { return m.CreateIndex(&GadgetV2{}, "idx_gadgets_name") }},
		{"rename index", func() error { return m.RenameIndex(&GadgetV2{}, "idx_gadgets_name", "idx_gadgets_label") }},
		{"drop index", func() error { return m.DropIndex(&GadgetV2{}, "idx_gadgets_label") }},
		{"create view", func() error {
			return m.CreateView("heavy_gadgets", gorm.ViewOption{Query: db.Model(&Gadget{}).Where("weight > ?", 10)})
		}},
		{"drop view", func() error { return m.DropView("heavy_gadgets") }},
		{"drop column", func() error { return m.DropColumn(&GadgetV2{}, "color") }},
		{"rename table", func() error { return m.RenameTable("gadgets", "widgets") }},
		{"drop table", func() error { return m.DropTable("widgets") }},
	}
	for _, step := range steps {
		if err := step.run(); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
	}
	if m.HasTable("widgets") || m.HasTable("gadgets") {
		t.Fatal("tables survived the drop")
	}
}

type Indexed struct {
	ID   uint
	Name string `gorm:"index:idx_indexed_name"`
}

func TestGormIndexHelpers(t *testing.T) {
	db := openGorm(t, freshDatabase(t, "gorm_indexes"), &gorm.Config{})
	m := db.Migrator()
	if err := m.AutoMigrate(&Indexed{}); err != nil {
		t.Fatal(err)
	}
	if !m.HasIndex(&Indexed{}, "idx_indexed_name") || !m.HasColumn(&Indexed{}, "Name") {
		t.Fatal("index or column not found")
	}
	if !m.HasIndex(&Indexed{}, "Name") {
		t.Fatal("index not found by field name")
	}
}

func TestGormCheckConstraintOperations(t *testing.T) {
	db := openGorm(t, freshDatabase(t, "gorm_checks"), &gorm.Config{})
	m := db.Migrator()
	if err := m.AutoMigrate(&Employee{}, &Company{}, &Language{}, &Profile{}); err != nil {
		t.Fatal(err)
	}
	if !m.HasConstraint(&Employee{}, "level_non_negative") {
		t.Fatal("check constraint not reported")
	}
	if err := m.DropConstraint(&Employee{}, "level_non_negative"); err != nil {
		t.Fatal(err)
	}
	if m.HasConstraint(&Employee{}, "level_non_negative") {
		t.Fatal("check constraint survived")
	}
	if err := m.CreateConstraint(&Employee{}, "level_non_negative"); err != nil {
		t.Fatal(err)
	}
	if !m.HasConstraint(&Employee{}, "level_non_negative") {
		t.Fatal("check constraint not recreated")
	}
}

func TestGormCreateForeignKeyConstraint(t *testing.T) {
	db := openGorm(t, freshDatabase(t, "gorm_addfk"), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	m := db.Migrator()
	if err := m.AutoMigrate(&Owner{}, &Pet{}); err != nil {
		t.Fatal(err)
	}
	xfailOnSereneDB(t, "SereneDB cannot add a FOREIGN KEY with ALTER TABLE (#1427)", func() error {
		if err := m.CreateConstraint(&Pet{}, "Owner"); err != nil {
			return err
		}
		if !m.HasConstraint(&Pet{}, "Owner") {
			return errors.New("foreign key not reported")
		}
		return nil
	})
}

func TestGormOnDeleteCascade(t *testing.T) {
	db := openGorm(t, freshDatabase(t, "gorm_ondelete"), &gorm.Config{})
	xfailOnSereneDB(t, "SereneDB rejects ON DELETE CASCADE", func() error {
		if err := db.AutoMigrate(&Dog{}, &Leash{}); err != nil {
			return err
		}
		dog := Dog{}
		if err := db.Create(&dog).Error; err != nil {
			return err
		}
		if err := db.Create(&Leash{DogID: dog.ID}).Error; err != nil {
			return err
		}
		if err := db.Delete(&dog).Error; err != nil {
			return err
		}
		var leashes int64
		if err := db.Model(&Leash{}).Count(&leashes).Error; err != nil {
			return err
		}
		if leashes != 0 {
			return fmt.Errorf("%d leashes survived the cascade", leashes)
		}
		return nil
	})
}
