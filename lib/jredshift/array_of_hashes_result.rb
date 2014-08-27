require 'bigdecimal'


module Jredshift
  class ArrayOfHashesResult
    attr_reader :result

    # Defined at http://docs.oracle.com/javase/6/docs/api/java/sql/Types.html
    COL_TYPE_BIGINT = -5
    COL_TYPE_BIT = -7
    COL_TYPE_CHAR = 1
    COL_TYPE_DATE = 41
    COL_TYPE_DECIMAL = 3
    COL_TYPE_DOUBLE = 8
    COL_TYPE_FLOAT = 6
    COL_TYPE_INTEGER = 4
    COL_TYPE_NCHAR = -15
    COL_TYPE_NUMERIC = 2
    COL_TYPE_NVARCHAR = -9
    COL_TYPE_SMALLINT = 5
    COL_TYPE_TIME = 92
    COL_TYPE_TIMESTAMP = 93
    COL_TYPE_TINYINT = -6
    COL_TYPE_VARCHAR = 12


    def initialize(java_result_set)
      @java_result_set = java_result_set
      @result = result_set_to_array_of_hashes
    end

    private

    def result_set_to_array_of_hashes
      meta = @java_result_set.meta_data
      rows = []

      while @java_result_set.next
        row = {}

        (1..meta.column_count).each do |i|
          name = meta.column_name(i)
          row[name] = get_field_by_type(meta, i)
        end

        rows << row
      end
      rows
    end

    def get_field_by_type(meta, i)
      return nil if @java_result_set.get_string(i).nil?

      case meta.column_type(i)

      when COL_TYPE_TINYINT, COL_TYPE_SMALLINT, COL_TYPE_INTEGER
        @java_result_set.get_int(i).to_i
      when COL_TYPE_BIGINT
        @java_result_set.get_long(i).to_i
      when COL_TYPE_DATE
        @java_result_set.get_date(i)
      when COL_TYPE_TIME
        @java_result_set.get_time(i).to_i
      when COL_TYPE_TIMESTAMP
        datetime_str = @java_result_set.get_timestamp(i).to_s
        DateTime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
      when COL_TYPE_NUMERIC, COL_TYPE_DECIMAL, COL_TYPE_FLOAT, COL_TYPE_DOUBLE
        BigDecimal.new(@java_result_set.get_string(i).to_s)
      when COL_TYPE_CHAR, COL_TYPE_NCHAR, COL_TYPE_NVARCHAR, COL_TYPE_VARCHAR
        @java_result_set.get_string(i).to_s
      when COL_TYPE_BIT
        str = @java_result_set.get_string(i)
        if str == 't'
          true
        elsif str == 'f'
          false
        else
          str
        end
      else
        @java_result_set.get_string(i).to_s
      end
    end
  end
end
