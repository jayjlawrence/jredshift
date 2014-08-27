require 'jredshift/jdbc_db'


module Jredshift
  class Redshift < JdbcDb
    attr_reader :query_group, :query_slot_count

    # I've encountered a Redshift bug where the DB breaks all connections and
    # can't be connected to again for a short time.
    ERROR_CONNECTION_REFUSED = 'Connection refused. Check that the hostname and ' +
      'port are correct and that the postmaster is accepting TCP/IP connections.'
    # This is seen when the connection is broken.
    ERROR_IO = 'An I/O error occurred while sending to the backend'


    def initialize(jdbc_url, user, password, options={})
      super
      @query_group = options.fetch(:query_group, nil)
      @query_slot_count = options.fetch(:query_slot_count, nil)

      set_query_group(@query_group) if @query_group
      set_query_slot_count(@query_slot_count) if @query_slot_count
    end

    def set_query_group(query_group)
      execute("SET query_group TO #{query_group};")
    end

    def set_query_slot_count(count)
      execute("SET wlm_query_slot_count TO #{count};")
    end

    def drop_table_if_exists(table, options={})
      cascade = options.fetch(:cascade, false) ? ' CASCADE' : ''
      err_msg = "ERROR: table \"#{remove_schema(table)}\" does not exist"

      execute("DROP TABLE #{table}#{cascade};", :quiet => true, :errors_to_ignore => [err_msg])
    end

    def drop_view_if_exists(view, options={})
      cascade = options.fetch(:cascade, false) ? ' CASCADE' : ''
      err_msg = "ERROR: view \"#{remove_schema(view)}\" does not exist"

      execute("DROP VIEW #{view}#{cascade};", :quiet => true, :errors_to_ignore => [err_msg])
    end

    def table_exists?(schema, table)
      sql = <<-SQL
        SELECT count(*) FROM pg_tables
        WHERE schemaname = '#{schema}' AND tablename = '#{table}'
        ;
      SQL
      query(sql, :quiet => true).first['count'] == 1
    end

    private

    def recoverable_error?(err_msg)
      err_msg =~ /S3ServiceException:speed limit exceeded/i ||
      err_msg =~ /Communications link failure/i ||
      err_msg =~ /VACUUM is running/i ||
      err_msg =~ /system is in maintenance mode/i ||
      err_msg == ERROR_CONNECTION_REFUSED ||
      super
    end

    def sleep_time_for_error(err_msg)
      if err_msg =~ /system is in maintenance mode/i
        900
      elsif err_msg == ERROR_CONNECTION_REFUSED
        600
      else
        super
      end
    end

    def sql_exception_class
      Java::OrgPostgresqlUtil::PSQLException
    end

    def remove_schema(table)
      table.split('.').last
    end
  end
end
