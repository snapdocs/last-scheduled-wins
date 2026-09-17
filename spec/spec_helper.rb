# frozen_string_literal: true

require 'active_support/all'
require 'connection_pool'
require 'logger'
require 'redis'
require 'sidekiq'
require 'timecop'

require_relative '../lib/sidekiq_helper/last_scheduled_wins'

# The module logs through `Rails.logger`. Outside Rails, stand in a plain
# Logger so the examples that assert on log level have something to stub.
unless defined?(Rails)
  module Rails
    def self.logger
      @logger ||= Logger.new(File::NULL)
    end
  end
end

# Specs exercise real Redis round trips — TTL arithmetic, SET XX KEEPTTL and
# the store_jid Lua script have no faithful in-memory substitute.
REDIS_URL = ENV.fetch('REDIS_URL', 'redis://localhost:6379/15')

Sidekiq.configure_client do |config|
  config.redis = { url: REDIS_URL }
  config.logger = Logger.new(File::NULL)
end

RSpec.configure do |config|
  config.expect_with(:rspec) { |c| c.syntax = :expect }
  config.mock_with(:rspec) { |c| c.verify_partial_doubles = true }
  config.disable_monkey_patching!
  config.order = :random
  Kernel.srand config.seed

  config.before(:suite) do
    Sidekiq.redis { |redis| redis.call('PING') }
  rescue StandardError => e
    abort "Redis is required at #{REDIS_URL} (#{e.class}: #{e.message})"
  end
end
