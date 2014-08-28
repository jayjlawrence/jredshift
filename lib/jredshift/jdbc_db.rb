require 'java'
require 'jdbc/postgres'
require 'jredshift/array_of_hashes_result'
require 'jredshift/logger'


module Jredshift
  class JdbcDb
    attr_reader :conn

    # Java uses an int for the updateCount field in ResultHandler. If the
    # update count overflows an int, you'll get this error message (but the
    # records will be updated/inserted correctly).
    ERROR_UPDATE_COUNT_OVERFLOW_REGEX =
      /Unable to interpret the update count in command completion tag: /i


    def initialize(jdbc_url, user, password, options={})
      @abort_on_error = options.fetch(:abort_on_error, true)
      @quiet = options.fetch(:quiet, false)
      @jdbc_url = jdbc_url
      @user = user
      @password = password
      @error_occurred = false
      @retry_count = 0

      secrets = options.fetch(:secrets, [])
      @logger = Logger.new(:secrets => secrets)

      init_connection
    end

    def execute(sql, options={})
      errors_to_ignore = options.fetch(:errors_to_ignore, [])
      quiet = options.fetch(:quiet, @quiet)

      log "\n#{sql}\n" unless quiet

      affected_rows = with_error_handling(:errors_to_ignore => errors_to_ignore) do
        @statement.execute_update(sql)
      end

      unless transaction_statement?(sql) || quiet || affected_rows.nil?
        log "Affected #{affected_rows} row(s)."
      end

      affected_rows
    end

    # Returns an empty array for no results, or nil if an error occurred and
    # @abort_on_error = false
    def query(sql, options={})
      quiet = options.fetch(:quiet, @quiet)

      log "\n#{sql}\n" unless quiet
      result_set = with_error_handling { @statement.execute_query(sql) }
      result_set ? ArrayOfHashesResult.new(result_set).result : nil
    end

    # Returns a ResultSet (instead of an Array of Hashes). Requires caller to
    # close ResultSet and set autocommit back to true
    def big_query(sql, options={})
      quiet = options.fetch(:quiet, @quiet)
      fetch_size = options.fetch(:fetch_size, 100000)

      @conn.set_auto_commit(false);
      stmt = @conn.create_statement
      stmt.set_fetch_size(fetch_size)
      stmt.execute_query(sql)

      log "\n#{sql}\n" unless quiet
      with_error_handling { stmt.execute_query(sql) }
    end

    def execute_script(filename, options={})
      File.open(filename) do |fh|
        sql = fh.read
        sql = remove_comments(sql)
        sql = substitute_variables(sql)
        execute_each_statement(sql, options)
      end
    end

    def error_occurred?
      @error_occurred
    end

    def clear_error_state
      @error_occurred = false
    end

    private

    def init_connection
      Jdbc::Postgres.load_driver
      with_error_handling do
        @conn = java.sql::DriverManager.get_connection(@jdbc_url, @user, @password)
      end
      @statement = @conn.create_statement
    end

    # errors_to_ignore should only be used where no return value is expected
    def with_error_handling(options={})
      errors_to_ignore = options.fetch(:errors_to_ignore, [])
      return_value = yield
      @retry_count = 0
      return_value

    rescue Exception => e
      if errors_to_ignore.include?(e.message)
        @retry_count = 0

      elsif interpretable_error?(e.message)
        @retry_count = 0
        interpret_error(e.message)

      elsif recoverable_error?(e.message) && @retry_count < 3
        @retry_count += 1
        sleep_time = sleep_time_for_error(e.message)
        log "Failed with recoverable error: #{e.message}"
        log "Retry attempt #{@retry_count} will occur after #{sleep_time} seconds"
        sleep sleep_time
        retry

      else
        @retry_count = 0
        @error_occurred = true
        log e.class.to_s
        log e.message
        log e.backtrace.join("\n") unless e.class == sql_exception_class

        raise e if @abort_on_error
      end
    end

    def interpretable_error?(err_msg)
      err_msg =~ ERROR_UPDATE_COUNT_OVERFLOW_REGEX
    end

    def interpret_error(err_msg)
      if err_msg =~ ERROR_UPDATE_COUNT_OVERFLOW_REGEX
        get_update_count_from_error(err_msg)
      else
        fail ArgumentError, 'Unexpected interpretable_error'
      end
    end

    def get_update_count_from_error(err_msg)
      err_msg.split(' ').last.split('.').first
    end

    def recoverable_error?(err_msg)
      false
    end

    def sleep_time_for_error(err_msg)
      60
    end

    def sql_exception_class
      fail NotImplementedError, 'sql_exception_class'
    end

    # TODO add other transaction keywords
    def transaction_statement?(sql)
      sql =~ /begin\s*;?/i || sql =~ /end\s*;?/i
    end

    def remove_comments(sql)
      lines = sql.split("\n")
      stripped_lines = lines.reject {|line| starts_with_double_dash?(line) }
      stripped_lines.join("\n")
    end

    def starts_with_double_dash?(line)
      line =~ /\A\s*--/
    end

    def substitute_variables(sql)
      sql
    end

    def execute_each_statement(sql, options={})
      sql.split(/;\s*$/).each do |statement|
        execute("#{statement};", options)
      end
    end

    def log(msg)
      @logger.log(msg)
    end
  end
end
