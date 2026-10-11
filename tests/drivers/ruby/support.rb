require 'json'
require 'minitest'
require 'pathname'
require 'pg'

module Drivers
  FIXTURES = Pathname.new(__dir__).join('fixtures')

  module_function

  def conn_args
    {
      host: ENV['SDB_DRV_HOST'] || 'localhost',
      port: (ENV['SDB_DRV_PORT'] || '5432').to_i,
      user: ENV['SDB_DRV_USER'] || 'postgres',
      dbname: ENV['SDB_DRV_DATABASE'] || 'postgres',
    }
  end

  def database_name(suite)
    "rb_#{suite}_#{ENV['SDB_DRV_RUN_ID'] || '0'}"
  end

  def admin(&block)
    conn = PG::Connection.new(**conn_args)
    conn.set_notice_processor { |_| }
    block.call(conn)
  ensure
    conn&.close
  end

  def recreate_database(name)
    admin do |conn|
      conn.exec("DROP DATABASE IF EXISTS #{name}")
      conn.exec("CREATE DATABASE #{name}")
    end
  end

  def drop_database(name)
    admin { |conn| conn.exec("DROP DATABASE IF EXISTS #{name}") }
  end

  def serenedb?
    return @serenedb unless @serenedb.nil?

    @serenedb = admin { |conn| conn.exec('SELECT version()').getvalue(0, 0).include?('SereneDB') }
  end

  def report_junit(suite)
    dir = ENV['SDB_DRV_JUNIT'] or return
    Minitest::JunitReporter.path = File.join(dir, "tests-drivers-ruby-#{suite}-junit.xml")
    Minitest::JunitReporter.suite = "ruby_#{suite}"
    Minitest.extensions << 'sdb_junit'
  end

  def outcome(&block)
    block.call
  rescue StandardError => e
    { 'error' => "#{e.class}: #{e.message.lines.first.to_s.strip}" }
  end
end

module Minitest
  def self.plugin_sdb_junit_init(_options)
    reporter << JunitReporter.new
  end

  class JunitReporter < AbstractReporter
    class << self
      attr_accessor :path, :suite
    end

    def initialize
      super
      @results = []
    end

    def record(result)
      @results << result
    end

    def report
      cases = @results.map do |r|
        attrs = %(classname="#{escape(r.klass)}" name="#{escape(r.name)}" time="#{format('%.3f', r.time)}")
        if r.skipped?
          %(  <testcase #{attrs}><skipped message="#{escape(r.failure.message)}"/></testcase>)
        elsif r.error?
          %(  <testcase #{attrs}><error message="#{escape(r.failure.message)}"/></testcase>)
        elsif !r.passed?
          %(  <testcase #{attrs}><failure message="#{escape(r.failure.message)}"/></testcase>)
        else
          %(  <testcase #{attrs}/>)
        end
      end
      counts = %(tests="#{@results.size}" failures="#{@results.count { |r| !r.passed? && !r.skipped? && !r.error? }}" ) +
               %(errors="#{@results.count(&:error?)}" skipped="#{@results.count(&:skipped?)}")
      File.write(self.class.path,
                 %(<?xml version="1.0"?>\n<testsuite name="#{self.class.suite}" #{counts}>\n#{cases.join("\n")}\n</testsuite>\n))
    end

    def escape(text)
      text.to_s.gsub('&', '&amp;').gsub('<', '&lt;').gsub('>', '&gt;').gsub('"', '&quot;')
    end
  end
end

module DriverAssertions
  def xfail_on_serenedb(reason)
    return yield unless Drivers.serenedb?

    begin
      yield
    rescue StandardError, Minitest::Assertion => e
      skip "xfail: #{reason} (#{e.class}: #{e.message.lines.first.to_s.strip})"
    end
    flunk "XPASS(strict): #{reason}"
  end

  def assert_golden(path, text)
    twin = path.sub_ext(".serenedb#{path.extname}")
    if ENV['SDB_DRV_UPDATE_GOLDENS'] == '1'
      if !Drivers.serenedb?
        path.dirname.mkpath
        path.write(text)
      elsif text == path.read
        twin.delete if twin.exist?
      else
        twin.write(text)
      end
      return
    end
    expected = Drivers.serenedb? && twin.exist? ? twin : path
    assert_equal expected.read, text
  end
end
