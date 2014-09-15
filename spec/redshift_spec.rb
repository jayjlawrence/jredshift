require_relative 'dev_redshift'


module Jredshift
  describe 'Redshift' do

    before(:all) do
      @db_conn = DevRedshift.new
    end

    after(:all) do
      @db_conn.drop_table_if_exists('test.tmp_drop_if_exists')
      @db_conn.drop_table_if_exists('test.tmp_test_table')
      @db_conn.drop_view_if_exists('test.tmp_test_table_vw')
    end

    describe '#drop_table_if_exists' do

      it 'ignores a missing table' do
        @db_conn.drop_table_if_exists('test.i_dont_exist')
        expect(@db_conn.error_occurred?).to be false
      end

      it 'drops an existing table' do
        @db_conn.drop_table_if_exists('test.tmp_drop_if_exists')
        @db_conn.execute('CREATE TABLE test.tmp_drop_if_exists (tmp boolean)')
        @db_conn.drop_table_if_exists('test.tmp_drop_if_exists')
        expect(@db_conn.error_occurred?).to be false
      end

      it 'cascades when :cascade option is true' do
        @db_conn.execute('CREATE TABLE test.tmp_test_table (tmp boolean)')
        @db_conn.execute('CREATE VIEW test.tmp_test_table_vw AS SELECT * ' +
                         'FROM test.tmp_test_table')
        @db_conn.drop_table_if_exists('test.tmp_test_table', :cascade => true)
        sql = "SELECT count(*) FROM pg_views WHERE viewname = 'tmp_test_table_vw'"

        count = @db_conn.query(sql).first['count']
        expect(count).to eq(0)
      end
    end

    describe '#drop_view_if_exists' do

      it 'ignores a missing view' do
        @db_conn.drop_view_if_exists('test.i_dont_exist_vw')
        expect(@db_conn.error_occurred?).to be false
      end

      it 'drops an existing view' do
        @db_conn.drop_table_if_exists('test.tmp_test_table', :cascade => true)
        @db_conn.execute('CREATE TABLE test.tmp_test_table (tmp boolean)')
        @db_conn.execute('CREATE VIEW test.tmp_drop_if_exists_vw AS SELECT * ' +
                         'FROM test.tmp_test_table')
        @db_conn.drop_view_if_exists('test.tmp_drop_if_exists_vw')
        @db_conn.execute('DROP TABLE test.tmp_test_table')
        expect(@db_conn.error_occurred?).to be false
      end
    end

    describe '#get_field_by_type' do

      it 'converts booleans correctly' do
        @db_conn.execute('CREATE TABLE test.tmp_test_table (tmp boolean)')
        @db_conn.execute('INSERT INTO test.tmp_test_table VALUES (true), (false)')
        rows = @db_conn.query('SELECT * FROM test.tmp_test_table ORDER BY tmp')
        @db_conn.drop_table_if_exists('test.tmp_test_table')

        expect(rows[0]['tmp']).to be false
        expect(rows[1]['tmp']).to be true
      end
    end
  end
end
