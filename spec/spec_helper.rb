require 'redis'

RSpec.configure do |config|
  config.color = true
end

Dir["./spec/support/**/*.rb"].sort.each { |f| require f }
