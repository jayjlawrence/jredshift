require 'singleton'
require 'time'


module Jredshift
  class Logger

    def initialize(options={})
      @secrets = options.fetch(:secrets, [])
      @secrets << ENV['AWS_SECRET_ACCESS_KEY'] if ENV['AWS_SECRET_ACCESS_KEY']
    end

    def log(msg)
      puts "#{now_utc} | #{hide_secrets(msg)}"
    end

    private

    def hide_secrets(msg)
      @secrets.inject(msg) {|message, secret| hide_secret(message, secret) }
    end

    def hide_secret(msg, secret)
      msg.sub(secret, '*' * secret.length)
    end

    def now_utc
      DateTime.now.new_offset(0).strftime('%Y-%m-%d %H:%M:%S')
    end
  end
end
