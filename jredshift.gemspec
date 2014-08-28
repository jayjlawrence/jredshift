# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'jredshift/version'


Gem::Specification.new do |spec|
  spec.name          = 'jredshift'
  spec.version       = Jredshift::VERSION
  spec.authors       = ['Dean Morin']
  spec.email         = ['morin.dean@gmail.com']
  spec.summary       = 'Redshift wrapper for JRuby.'
  spec.description = <<-EOS
    Convience wrapper for using Redshift with JRuby.
  EOS
  spec.homepage      = 'http://github.com/deanmorin/jredshift'
  spec.license       = 'The Unlicense'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_dependency 'jdbc-postgres', '~> 9.3'
  spec.add_development_dependency 'bundler', '~> 1.6'
  spec.add_development_dependency 'rake', '~> 10'
end
