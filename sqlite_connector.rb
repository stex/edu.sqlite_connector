#!/usr/bin/env ruby
#ENCODING: UTF-8

#
# @version 1.0
# @author Stefan Exner (ste@informatik.uni-kiel.de)
#
# This class handles connections to SQLite3 databases.
# It includes requirement installers for windows, osx and linux,
# so using it should be fairly easy for the students.
#
# Everything is in one big file to avoid having to put multiple
# files into the students' working directories.
#
# Please take a look at the examples to get an idea of
# how the connector works.
#

require 'rubygems'

#
# Extend the String class to support colorized output
#
class String
  def colorize(color_code)
    ; "\e[#{color_code}m#{self}\e[0m"
  end

  def red;
    colorize(31)
  end

  def green;
    colorize(32)
  end

  def yellow;
    colorize(33)
  end
end

#
# Tries to execute the given command without sudo.
# If it fails, it re-tries with sudo.
#
def try_sudo(command)
  unless system(command)
    system("sudo #{command}")
  end
end

#
# Displays a red text in the console with a given prefix
# and exits the problem with code 1
#
def error_and_exit(reason, prefix = nil)
  prefix ||= 'Das sqlite3-Gem konnte nicht installiert werden. Bitte wenden Sie '\
             'sich an Ihren Übungsgruppenleiter. Sagen Sie ihm, es lag an '
  puts "#{prefix} #{reason}".red
  exit(1)
end

#
# Displays a green text in the console
#
def info(msg)
  puts msg.green
end

#-----------------------------------------------------
#       sqlite3 Gem Detection and Installation
#-----------------------------------------------------

WINDOWS_SOURCES = ['https://rubygems.org', 'https://rubygems.org/']

begin
  require 'sqlite3'
rescue LoadError
  info 'Das sqlite3 gem ist nicht korrekt installiert.'.red
  if Gem.win_platform?
    info 'Das Programm versucht nun, das Gem selbstständig zu installieren...'
    #Update rubygems version, necessary to communicate over ssl with rubygems.org
    #To achieve this, we have to disable the https source first (...) and re-active it later.
    WINDOWS_SOURCES.each { |s| system('gem sources --remove #{s}') }
    system('gem sources --add http://rubygems.org/')

    if system('gem update --system')
      if system('gem install sqlite3')
        system('gem sources --add https://rubygems.org/')
        info 'Das sqlite3-Gem wurde erfolgreich installiert!'
      else
        error_and_exit 'der Gem-Installation unter Windows.'
      end
    else
      error_and_exit 'dem Rubygems-Update unter Windows.'
    end
  elsif Gem.platforms.last.os =~ /linux/
    info 'Zunächst müssen die Abhängigkeiten für sqlite3 installiert werden.'
    info 'Dies geschieht über apt-get, es kann also sein, dass Sie während der Installation
          Ihr Passwort eingeben müssen.'

    if system('sudo apt-get install libsqlite3-dev ruby-dev')
      if try_sudo('gem install sqlite3')
        info 'Das Gem wurde erfolgreich installiert.'
      else
        error_and_exit 'der Gem-Installation unter Linux.'
      end
    else
      error_and_exit 'der Installation der Abhängigkeiten unter Linux.'
    end
  elsif Gem.platforms.last.os =~ /darwin/
    puts 'Das sqlite3-Gem sollte in OSX eigentlich von Haus aus installiert sein.
          Bitte wenden Sie sich an Ihren Übungsgruppenleiter.'.red
    exit(1)
  else
    puts 'Ihr Betriebssystem konnte nicht korrekt erkannt werden. Bitte wenden Sie
          sich an ihren Übungsgruppenleiter.'.red
    exit(1)
  end

  info 'Bitte starten Sie Ihr Programm erneut, damit die neuen Komponenten geladen werden.'

  exit(1)
end

#-----------------------------------------------------
#                 The actual connector
#-----------------------------------------------------

#
# Creates and/or opens a database in the current (= the script's)
# directory and returns its handler
#
# @param [String, Symbol] database_name
#   database name without extension
#
# @param [Hash] options
#   Custom behaviour settings for this database session
#
# @option options ['array', 'hash'] :return_format (:array)
#   Determines the format query results are returned in (either 2D-arrays or Array<Hash>)
#
# @yield [SQLiteWrapper::Connector] The database handler to work on.
#   The database connection is automatically closed once the block
#   is left.
#
# @return [Object] the value returned by the the yielded block, e.g.
#    users = use_database('test') { |db| db.execute('SELECT * FROM users;') }
#
def use_database(database_name, options = {}, &proc)
  db = SQLiteWrapper::Connector.new(database_name, options, proc)
  db.send :open
  yield(db).tap do
    db.send :close
  end
end

def drop_database(database_name, &proc)
  SQLiteWrapper::Connector.drop_database(database_name, &proc)
end

module SQLiteWrapper
  class DatabaseNotLoadedError      < StandardError; end
  class DatabaseExistsError         < StandardError; end
  class DatabaseNotFoundError       < StandardError; end
  class SQLError                    < StandardError; end
  class ColumnNotFoundError         < StandardError; end
  class TableNotFoundError          < StandardError; end
  class TableAndColumnNotFoundError < StandardError; end
  class BadCodingStyleError         < StandardError; end
  class AmbiguousColumnNameError    < StandardError; end

  class Connector
    def self.drop_database(database_name, &proc)
      if File.exists?(self.database_path(database_name, proc))
        File.unlink(self.database_path(database_name, proc))
        true
      else
        false
      end
    end

    def initialize(database_name, options, proc)
      @options       = options
      @database_path = Connector.database_path(database_name, proc)
    end

    #
    # Executes the given query
    #
    # @param [String] query
    #   The SQL query to be executed
    #
    # @return [Array<Array<*>>]
    #   The result set as 2D-Array.
    #   For SELECT queries, the values are already type casted
    #     to ruby types (if possible)
    #
    def execute(query)
      raise_not_loaded
      ResultSet.new(query, @database, @options).to_a
    end

    private

    def self.database_path(database_name, proc)
      database_filename = "#{database_name}.sqlite3"
      File.join(File.dirname(eval('__FILE__', proc.binding)), database_filename)
    end

    def open
      @database = SQLite3::Database.new(@database_path)
    end

    def close
      @database.close if @database
      @database = nil
    end

    def raise_not_loaded
      if !@database || @database && !File.exists?(@database_path)
        fail DatabaseNotLoadedError.new("The database could not be loaded correctly.".red)
      end
    end
  end

  #
  # This class handles type conversions and validity checks
  # on the query to be executed
  #
  class ResultSet

    GLOBAL_SELECT_FUNCTIONS = %w(last_insert_rowid).freeze
    R_GLOBAL_FUNCTION = "(#{GLOBAL_SELECT_FUNCTIONS.map { |f| f + '\(\)'}.join('|') })"
    R_FROM_OR_GLOBAL  = "(from|#{GLOBAL_SELECT_FUNCTIONS.join('|')})"
    R_SELECT_QUERY    = "^select.*#{R_FROM_OR_GLOBAL}"
    R_COUNT           = 'count\((.+)\)'

    def initialize(query, database, options)
      @options  = options
      @query    = query.strip
      @database = database
    end

    def to_a
      execute
    end

    private

    #
    # @return [Regexp] a regular expression based on the given constant
    #
    def regexp(name)
      Regexp.new(ResultSet.const_get("r_#{name}".upcase.to_sym), 'i')
    end

    #
    # Removes newlines from queries
    #
    def sanitize_query(query)
      query.gsub(/[\r\n]/, ' ').gsub(/[ ]{2,}/, ' ').strip
    end

    #
    # Tries to execute the given +@query+
    #
    # SELECT queries: Tries to perform type casts to
    #                 the correct ruby types based on the
    #                 original SQL types and not the basic SQLite types
    #
    def execute
      #Raise an exception if the query does not end with a semicolon
      unless @query.strip =~ /;$/
        raise BadCodingStyleError.new('Missing semicolon at end of line in query '.red + @query.yellow)
      end

      unless @result
        select_information if select_query?
        #insert_information if insert_query?

        begin
          @result = @database.execute2(@query)
        rescue SQLite3::SQLException => e
          msg = 'Error in SQL Query '.red + @query.yellow + ": #{e.message}".red
          raise SQLError.new(msg)
        end

        @columns = @result.delete_at(0)

        if select_query?
          @result = perform_ruby_type_casts(@result)
          @result = format_select_results(@result)
        end
      end

      @result
    end

    def result
      @result
    end

    #
    # @return [TrueClass, FalseClass]
    #   +true+ if the query is a select query
    #
    def select_query?
      !!(@query =~ regexp(:select_query))
    end

    #
    # @return [TrueClass, FalseClass]
    #   +true+ if the query is an insert query
    #
    def insert_query?
      !!(@query.downcase =~ /^insert into/)
    end

    #
    # Gathers all necessary information from insert queries
    #
    # @return [Hash]
    #   The used table, column names and corresponding values
    #
    def insert_information
      if m = @query.match(/^insert into ([a-zA-Z]+) \((([a-zA-Z]+,? ?)+)\) values \((([a-zA-Z'"0-9]+,? ?)+)\)/i)
        {:table_name   => m[1],
         :column_names => split_n_strip(m[2]),
         :values       => split_n_strip(m[4])}
      else
        {}
      end
    end

    #
    # Gathers all necessary information from select queries
    #
    # @return [Hash]
    #   The used table names, columns and possible aliases in the used query.
    #
    #   Please note that for * and table.* all of the table's columns
    #   are returned as used columns. This is necessary for type conversions
    #
    #   Key mapping:
    #     :tables  => All used tables as Array<String>
    #     :columns => All used columns as Array<Array<String, String>> for tables and columns
    #     :aliases => Used aliases as Hashes
    #
    def select_information
      sanitized_query = sanitize_query(@query)

      global_function_query = Regexp.new("select\s+#{regexp(:global_function)}(.*);", 'i')
      simple_query          = /select\s+(.*)\s+from\s+(.*);/i
      complex_query         = /select\s+(.*)\s+from\s+(.*)\s+(where|order)\s+(.*);/i
      simple_join_query     = /select\s+(.*)\s+from\s+(.*)\s+inner join\s+(.*)\s+on\s+.*;/i
      complex_join_query    = /select\s+(.*)\s+from\s+(.*)\s+inner join\s+(.*)\s+on\s+.*(where|order)\s+(.*);/i

      m1 = complex_join_query.match(sanitized_query)
      m2 = simple_join_query.match(sanitized_query)
      mg = global_function_query.match(sanitized_query)
      m  = m1 || m2 || complex_query.match(sanitized_query) || simple_query.match(sanitized_query)

      if mg
        fail 'Global functions are currently not supported.'
      end

      if m
        aliases     = {}
        table_names = split_n_strip(m[2])

        if (m1 || m2)
          table_names = table_names + split_n_strip(m[3])
        end

        table_names.each do |t|
          unless table_exists?(t)
            raise_table_not_found(t)
          end
        end

        column_names = []
        split_n_strip(m[1]).each do |cn|
          #Format: "table_name.column_name"
          if cm = cn.match(/(.*) as (.*)/i)
            column_names << infer_table_and_column(cm[1], table_names)
            aliases[cm[2]] = cm[1]
            #Format: "*"
          elsif cn == '*'
            table_names.each do |table|
              table_column_names(table).map do |c|
                column_names << [table, c]
              end
            end
            #Format: "table_name.*"
          elsif cm = cn.match(/^([a-zA-Z_]+)\.\*/)
            table_column_names(cm[1]).map do |c|
              column_names << [cm[1], c]
            end
            #Format: "column_name" (hopefully)
          else
            column_names << infer_table_and_column(cn, table_names)
          end
        end

        {:tables => table_names, :columns => column_names, :aliases => aliases}
      else
        {}
      end
    end

    #
    # Tries to extract the table and column name from a query string.
    # Allowed formats here are:
    #
    #   - table_name.column_name
    #   - column_name
    #
    # If no table name is given, the function will try all tables which were
    # part of the query and use the first matching one (= the one which actually
    # has a column with the given name)
    #
    # @param [String] column_string
    #   The column string in one of the above formats
    #
    # @param [Array<String>] tables
    #   All tables used in the query
    #
    # @raise [SQLiteWrapper::ColumnNotFoundError]
    #   Raised if
    #     - +column_string+ does not match any of the above formats OR
    #     - a table was given and the table does not contain a column with the given name
    #     - no column with the given name could be found in the entire database
    #
    # @raise [SQLiteWrapper::TableNotFoundError]
    #   Raised if the given table does not exist in the database
    #
    def infer_table_and_column(column_string, tables)
      #format: table.column
      if m = column_string.match(/^([a-zA-Z_]+)\.([a-zA-Z_]+)$/i)
        table, column = m[1], m[2]
        if table_has_column?(table, column)
          [table, column]
        else
          if table_exists?(table)
            raise ColumnNotFoundError.new("The table '#{table}' does not have a column named '#{column}'".red)
          else
            raise_table_not_found(table)
          end
        end
      # format: count(...)
      elsif m = column_string.match(regexp(:count))
        [infer_table_and_column(m[1], tables).first, column_string]
      # format: column
      elsif t = tables.select { |t| table_has_column?(t, column_string) }.first
        [t, column_string]
      else
        fail TableAndColumnNotFoundError.new("Could not find a table and column for '#{column_string}'".red)
      end
    end

    def split_n_strip(str)
      str.split(',').map(&:strip)
    end

    #
    # @return [String]
    #   The SQL type of the given table column
    #
    # @example Retrieve the sql type for a table column
    #    execute("CREATE TABLE users (id INT, name VARCHAR(255))")
    #    sql_type("users", "name") #=> "VARCHAR(255)"
    #
    def sql_type(table, column)
      case column
        when regexp(:count)
          'INTEGER'
        else
          column_info(table, column)['type']
      end
    end

    #
    # @param [String] table
    #   The table name to be searched in
    #
    # @param [String] column
    #   The column name to be searched for
    #
    # @return [Hash]
    #   Information about the column with the given name
    #
    def column_info(table, column)
      table_info(table).select { |h| h['name'] == column }.first
    end

    #
    # @return [TrueClass, FalseClass]
    #   +true+ if the given table has a column with the given name
    #
    def table_has_column?(table, column)
      !column_info(table, column).nil?
    end

    #
    # @return [Array<String>]
    #   The names of all columns in the given table
    #
    def table_column_names(table)
      table_info(table).map { |h| h['name'] }
    end

    #
    # @return [Array<Hash>]
    #   Information about all columns in the given table.
    #   Each column is returned as a hash containing its properties.
    #
    def table_info(table)
      @database.table_info(table)
    end

    #
    # @param [String, Symbol] table_name
    #   The table name to be checked
    #
    # @return [TrueClass, FalseClass] +true+ if a table with the given
    #   name exists in the database
    #
    def table_exists?(table_name)
      table_info(table_name).any?
    end

    #
    # Tries to cast the returned values from the database to their
    # corresponding ruby types. This will only work for select queries.
    #
    # @param [Array<Array<*>>] results
    #   a result array returned from the database
    #
    def perform_ruby_type_casts(results)
      si = select_information
      results.map do |row|
        row.map.each_with_index do |v, i|
          table, column = si[:columns][i]
          c_type        = sql_type(table, column)
          @database.translator.translate(c_type, v.to_s)
        end
      end
    end

    #
    # Formats the SELECT results based on the chosen return_format.
    # Current formats are 2D arrays and an array of hash
    #
    # @param [Array<Array<*>>] results
    #   a result array returned from the database
    #
    # @return [Array<Array<*>>, Array<Hash>]
    #
    def format_select_results(results)
      case @options[:return_format]
        when 'hash'
          results.map do |row|
            row.each_with_object({}).with_index do |(v, hash), idx|
              if hash.has_key?(@columns[idx])
                fail AmbiguousColumnNameError, "Multiple columns named '#{@columns[idx]}' are part of the " \
                                               "results which is not permitted when using `:return_format => 'hash'`"
              end
              hash[@columns[idx]] = v
            end
          end
        else
          results
      end
    end

    def raise_table_not_found(table)
      fail TableNotFoundError.new("A table with the name '#{table}' could not be found in the database".red)
    end
  end
end
