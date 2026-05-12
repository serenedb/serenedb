# Ruby / pg (ruby-pg) round-trip matrix harness. Same shape as every
# other-language harness: load tests/drivers/spec/types.yaml, iterate
# (type x protocol x sample), round-trip via PG::Connection.

require 'pg'
require 'yaml'

DRIVER_KEY = 'ruby_pg'
PROTOCOLS = %w[simple extended-noparam extended-text extended-binary].freeze

spec_dir = ENV['SDB_DRV_SPEC'] || File.expand_path('../spec', __dir__)
host = ENV['SDB_DRV_HOST'] || 'localhost'
port = (ENV['SDB_DRV_PORT'] || '5432').to_i
db = ENV['SDB_DRV_DATABASE'] || 'postgres'
user = ENV['SDB_DRV_USER'] || 'postgres'
run_id = ENV['SDB_DRV_RUN_ID'] || '0'
schema = "drv_#{DRIVER_KEY}_#{run_id}"

types = YAML.safe_load(File.read(File.join(spec_dir, 'types.yaml')),
                       permitted_classes: [Symbol], aliases: true)

type_re = Regexp.new(ENV['SDB_DRV_TYPES'] || '.*')
proto_filter = (ENV['SDB_DRV_PROTOCOLS'] || PROTOCOLS.join(',')).split(',')

cases = []
types.each do |t|
  name = t['name'] || t['pg_typname']
  next unless type_re.match?(name)
  text_ok = t['text']
  bin_ok = t['binary']
  next unless text_ok || bin_ok
  PROTOCOLS.each do |proto|
    next unless proto_filter.include?(proto)
    supports = proto == 'extended-binary' ? bin_ok : text_ok
    next unless supports
    skip = (t['skip'] || {})[DRIVER_KEY] || []
    next if skip.include?('all') || skip.include?(proto)
    samples = t['samples'] || []
    samples.each_with_index do |sample, idx|
      cases << {
        type: t, name: name,
        pg_typname: t['pg_typname'] || name,
        cast_sql: t['cast_sql'] || "$1::#{t['pg_typname'] || name}",
        oid: t['oid'], proto: proto, idx: idx, sample: sample,
      }
    end
  end
end

conn = PG::Connection.new(host: host, port: port, dbname: db, user: user)
conn.exec("CREATE SCHEMA IF NOT EXISTS \"#{schema}\"")
conn.exec("SET search_path TO \"#{schema}\", public, pg_catalog")

passed = 0
failed = 0
junit_lines = []

# smoke
v = conn.exec('SELECT 1').getvalue(0, 0).to_i
if v == 1
  passed += 1
  junit_lines << '  <testcase classname="ruby_pg" name="smoke_select_one"/>'
else
  failed += 1
  junit_lines << '  <testcase classname="ruby_pg" name="smoke_select_one"><failure/></testcase>'
end

cases.each do |cs|
  id = "#{cs[:oid]}-#{cs[:name]}-#{cs[:proto]}-#{cs[:idx]}"
  begin
    expected = nil
    unless cs[:sample].nil?
      r = conn.exec_params(
        "SELECT (($1::text)::#{cs[:pg_typname]})::text",
        [cs[:sample].to_s])
      expected = r.getvalue(0, 0)
    end

    actual = nil
    if cs[:proto] == 'simple' || cs[:proto] == 'extended-noparam'
      sql =
        if cs[:sample].nil?
          "SELECT NULL::#{cs[:pg_typname]}::text AS v"
        else
          lit = cs[:sample].to_s.gsub("'", "''")
          "SELECT '#{lit}'::#{cs[:pg_typname]}::text AS v"
        end
      r = conn.exec(sql)
      actual = r.getvalue(0, 0)
    else
      cast = cs[:cast_sql].sub('$1', '$1::text')
      sql = "SELECT (#{cast})::text AS v"
      params = [cs[:sample].nil? ? nil : cs[:sample].to_s]
      r = conn.exec_params(sql, params)
      actual = r.getvalue(0, 0)
    end

    if expected == actual
      passed += 1
      junit_lines << "  <testcase classname=\"ruby_pg\" name=\"#{id}\"/>"
    else
      failed += 1
      junit_lines << "  <testcase classname=\"ruby_pg\" name=\"#{id}\">" \
                     "<failure>expected=#{expected.inspect} " \
                     "actual=#{actual.inspect}</failure></testcase>"
    end
  rescue => e
    failed += 1
    msg = e.message.gsub('<', '&lt;').gsub('>', '&gt;').gsub('&', '&amp;')
    junit_lines << "  <testcase classname=\"ruby_pg\" name=\"#{id}\">" \
                   "<error>#{msg}</error></testcase>"
  end
end

if ARGV[0]
  File.write(ARGV[0],
    "<?xml version=\"1.0\"?>\n<testsuite name=\"ruby_pg\">\n" +
    junit_lines.join("\n") +
    "\n</testsuite>\n")
end

warn "[ruby_pg] #{passed} passed, #{failed} failed"
exit(failed.zero? ? 0 : 1)
