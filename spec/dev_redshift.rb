require 'jredshift'


module Jredshift
  class DevRedshift < Redshift

    def initialize(options={})
      super(
        ENV['JREDSHIFT_JDBC_URL'],
        ENV['JREDSHIFT_USER'],
        ENV['JREDSHIFT_PASSWORD'],
        options
      )
    end
  end
end
