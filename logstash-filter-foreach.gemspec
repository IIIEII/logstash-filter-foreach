Gem::Specification.new do |s|
  s.name          = 'logstash-filter-foreach'
  s.version       = '0.1.1'
  s.licenses      = ['Apache-2.0']
  s.summary       = 'Process filters for every array item'
  s.description   = 'Plugin splits event for every item in array, then you could process other filters for every item and then join event back'
  s.homepage      = 'https://github.com/IIIEII/logstash-filter-foreach'
  s.authors       = ['IIIEII']
  s.email         = 'al.iiieii@gmail.com'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "filter" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", "~> 2.0"
  s.add_development_dependency 'logstash-filter-mutate'
  s.add_development_dependency 'logstash-filter-drop'
  s.add_development_dependency 'logstash-devutils', "~> 1.3", ">= 1.3.1"
end
