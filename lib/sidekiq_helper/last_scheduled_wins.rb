require 'digest'
require 'securerandom'

module SidekiqHelper
  # Schedule debounced Sidekiq jobs without scanning the schedule sorted set.
  #
  # Each enqueue records the Sidekiq jid in a Redis string keyed by `(worker class, args)`.
  # When a job fires, it compares its own jid to the value in Redis; if a newer enqueue has
  # overwritten the key, the older job short-circuits.
  #
  # Usage from the enqueuing side:
  #
  #   SidekiqHelper::LastScheduledWins.perform_at(
  #     5.minutes.from_now,
  #     MyWorker,
  #     param1, param2, param3
  #   )
  #
  # Usage from the worker:
  #
  #   class MyWorker < BaseWorker
  #     def perform(*args)
  #       return if SidekiqHelper::LastScheduledWins.superseded?(self.class, jid, *args)
  #       # ...
  #     end
  #
  #     # Override Retry Window (optional)
  #     # Default tracking key has a TTL that outlives Sidekiq's default retry envelope (~22 days)
  #     # Workers with longer retries should declare:
  #     def self.last_scheduled_wins_retry_window_seconds
  #       3.weeks.to_i # or whatever your worst-case retry envelope is
  #     end
  #
  #     # Override Tracking Key (optional)
  #     # Customize the args used to compute the tracking key. Default: full args list.
  #     def self.last_scheduled_wins_key_args(args)
  #       [args.first]
  #     end
  #   end
  #
  # Argument types:
  # - Only pass primitives (Integer, String, Boolean, Array, Hash) as `*args`.
  # - The tracking-key digest runs on the Ruby objects you pass, NOT on Sidekiq's
  #   post-serialization form, so two callers must agree on the in-Ruby type
  #   (e.g. `Time.current` vs `Time.current.to_s` produce different digests).
  # - Symbols are collapsed to strings, so `{foo: 1}` and `{"foo" => 1}` digest equivalently.
  #
  # Concurrency:
  # - Exactly one job fires per `(worker_class, last_scheduled_wins_key_args(args))`.
  # - The worker must be idempotent on its args.
  # - Under concurrent enqueues, the winner is the caller whose tracking-key write
  #   landed last in Redis, which is usually, but not guaranteed to be, the most-recent `perform_at`.
  # - A job past the `superseded?` check runs to completion, and is not retroactively superseded by a later enqueue.
  # - A job that fails could be superseded before retrying.
  #
  # Notes:
  # - Sidekiq Redis is environment-scoped — tracking keys are global
  #   within an env (no pod/process identifier), so the most-recent
  #   enqueue is visible to every worker.
  # - Renaming an adopting worker class invalidates in-flight tracking
  #   keys (the key includes `worker_class.to_s`) — duplicates possible
  #   until the scheduled set drains. Drain before deploying renames.
  #
  module LastScheduledWins
    KEY_PREFIX = 'sidekiq_last_scheduled_wins'.freeze
    CANCELED_JID = '__canceled__'.freeze

    # Thread local flag checked before every tracking-key write. Set by `without_tracking`
    TRACKING_DISABLED_KEY = :last_scheduled_wins_tracking_disabled

    # Default tracking-key retry window: 22 days, sized to outlive Sidekiq's
    # default retry curve (~21 days at `max_retries: 25` worst case).
    # Workers with longer envelopes should override `last_scheduled_wins_retry_window_seconds`.
    DEFAULT_RETRY_WINDOW_SECONDS = 22 * 86_400

    # store_jid retry envelope: sized for typical sub-second Redis hiccups, not sustained outages.
    STORE_JID_MAX_ATTEMPTS = 3
    STORE_JID_RETRY_BACKOFF_MS = 50

    module_function

    # Enqueue a job and record its jid as the current winner for (worker, args).
    # Returns the jid.
    def perform_at(scheduled_at, worker_class, *args)
      # Pre-generate the jid (Sidekiq's default format) so the tracking key
      # can be written BEFORE the schedule entry. Without this ordering, a
      # worker could pick up the job and find a prior caller's jid (or no
      # key) instead of its own.
      jid = SecureRandom.hex(12)

      begin
        store_jid(redis_key(worker_class, args), jid, ttl_seconds: ttl_for(worker_class, scheduled_at))
      rescue Redis::BaseError, RedisClient::Error, ConnectionPool::TimeoutError => e
        # Retries exhausted on a known connection-flavored error.
        #   - RedisClient::Error: redis-client gem (Sidekiq 7+).
        #   - ConnectionPool::TimeoutError: pool exhausted before checkout.
        #   - Redis::BaseError: older `redis` gem; adjacent code paths still raise from it.
        # We proceed to enqueue anyway. Outcome depends on prior state:
        #   - No prior tracking key: worker fails-open at fire-time → our job fires.
        #   - Prior tracking key with someone else's jid: that key is untouched
        #     (the failing SET never landed), so our jid mismatches at fire-time.
        #     The prior job still fires and our job drops.
        Rails.logger.warn(
          message: "LastScheduledWins: store_jid failed after retries",
          worker_class: worker_class.to_s,
          error_class: e.class.to_s,
          error_message: e.message,
        )
      rescue StandardError => e
        # Anything else: log at error level for visibility, swallow rather than aborting the caller.
        Rails.logger.error(
          message: "LastScheduledWins: store_jid unexpected error",
          worker_class: worker_class.to_s,
          error_class: e.class.to_s,
          error_message: e.message,
        )
      end

      worker_class.set(jid: jid).perform_at(scheduled_at, *args)

      jid
    end

    # Same as perform_at but takes a duration from now.
    def perform_in(interval, worker_class, *args)
      perform_at(interval.from_now, worker_class, *args)
    end

    # Returns true when the running job's jid does NOT match the most-recently stored
    # jid for (worker_class, last_scheduled_wins_key_args(args)). Workers should `return` early in that case.
    #
    # Returns false (i.e. NOT superseded → fire) when:
    #   - The stored jid matches our jid.
    #   - No key exists — fail-open for backwards compatibility with
    #     pre-tracking jobs and direct `Worker.new.perform(...)` test
    #     invocations.
    #
    # `Sidekiq.redis` failures are NOT rescued — they propagate, which puts
    # the job back on the Sidekiq retry queue.
    def superseded?(worker_class, jid, *args)
      return false if jid.blank?

      stored = Sidekiq.redis { |redis| redis.get(redis_key(worker_class, args)) }
      return false if stored.blank?

      stored != jid
    end

    # Mark every in-flight job for `(worker_class, last_scheduled_wins_key_args(args))` as canceled.
    # Overwrites the tracking key with CANCELED_JID (no real jid can match),
    # keeping the existing TTL. A later real `perform_at` overwrites the
    # sentinel, so cancel doesn't permanently lock the key.
    #
    # No-op when no tracking key exists (uses SET XX).
    def cancel(worker_class, *args)
      return if tracking_disabled?

      Sidekiq.redis do |redis|
        # Sidekiq yields a redis-client adapter, which only coerces key/value
        # kwargs (`ex: 60`) and rejects flag-style boolean kwargs (`xx: true`).
        # Use the raw command form.
        redis.call('SET', redis_key(worker_class, args), CANCELED_JID, 'XX', 'KEEPTTL')
      end
    end

    # Drop the tracking key for (worker_class, last_scheduled_wins_key_args(args)). Use before enqueuing
    # through a non-tracked path (i.e. a flipper-off branch of a caller)
    # so a stale tracking key from a prior tracked enqueue doesn't cause
    # the new job to drop at fire-time.
    #
    # NOTE: `untrack` causes the supersede check to FAIL-OPEN (missing key → fire).
    # It is NOT a cancel primitive. To stop a scheduled job from firing, use `cancel` instead.
    def untrack(worker_class, *args)
      return if tracking_disabled?

      Sidekiq.redis { |redis| redis.del(redis_key(worker_class, args)) }
    end

    # Skip tracking-key writes (`cancel`, `untrack`, `store_jid`) for the duration of the block
    # Note that unless otherwise prevented, an untracked job will still be enqueued.
    def without_tracking
      prev = Thread.current[TRACKING_DISABLED_KEY]
      Thread.current[TRACKING_DISABLED_KEY] = true
      yield
    ensure
      Thread.current[TRACKING_DISABLED_KEY] = prev
    end

    def tracking_disabled?
      Thread.current[TRACKING_DISABLED_KEY] || false
    end

    def redis_key(worker_class, args)
      key_args = if worker_class.respond_to?(:last_scheduled_wins_key_args)
                   worker_class.last_scheduled_wins_key_args(args)
                 else
                   args
                 end
      args_key = Digest::SHA1.hexdigest(canonicalize(key_args).to_json)
      "#{KEY_PREFIX}:#{worker_class}:#{args_key}"
    end

    # Write the jid with TTL = max(new_ttl, existing_remaining_ttl, 1).
    # The max-with-existing protects against the re-enqueue-for-earlier-time
    # failure mode: a near-future enqueue overwriting a far-future one would
    # shrink the TTL, the key would expire before the older job's fire-time,
    # and the older job would fire erroneously under fail-open (e.g. enqueue
    # A for T=3d, enqueue B for T=1d with TTL=2d; B fires, key expires at
    # T=2d, A fires at T=3d with no tracking key).
    #
    # `TTL` returns -2 (no key) or -1 (no expiry); clamping to 1
    # handles both and also avoids `SET ... EX 0` (Redis rejects it).
    #
    # Retries transient Redis errors with linear backoff. The SET is
    # idempotent on retry (same jid + same/longer TTL).
    def store_jid(key, jid, ttl_seconds:)
      return if tracking_disabled?

      # Atomic TTL-read + SET to prevent concurrent writers from overwriting each other's TTLs
      lua = <<~LUA
        local existing_ttl = redis.call('TTL', KEYS[1])
        local ttl = math.max(tonumber(ARGV[2]), existing_ttl, 1)
        redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl)
      LUA

      attempts = 0
      begin
        attempts += 1
        Sidekiq.redis do |redis|
          redis.call('EVAL', lua, '1', key, jid, ttl_seconds.to_s)
        end
      rescue Redis::BaseError, RedisClient::Error, ConnectionPool::TimeoutError
        raise if attempts >= STORE_JID_MAX_ATTEMPTS
        Kernel.sleep((STORE_JID_RETRY_BACKOFF_MS * attempts) / 1000.0)
        retry
      end
    end

    # Compute TTL for this enqueue: time until the job fires + a retry window.
    # The retry window defaults to DEFAULT_RETRY_WINDOW_SECONDS but the worker
    # can override via class method `last_scheduled_wins_retry_window_seconds`.
    # Negative `seconds_until_fire` (caller passed an already-elapsed time) is
    # clamped to 0 so we still get at least the retry window.
    def ttl_for(worker_class, scheduled_at)
      seconds_until_fire = [scheduled_at.to_i - Time.current.to_i, 0].max
      retry_window =
        if worker_class.respond_to?(:last_scheduled_wins_retry_window_seconds)
          worker_class.last_scheduled_wins_retry_window_seconds.to_i
        else
          DEFAULT_RETRY_WINDOW_SECONDS
        end
      seconds_until_fire + retry_window
    end

    # Recursively sort hash keys so digests are stable regardless of insertion order.
    # Non-hash collections keep their order (positional args are semantically ordered).
    def canonicalize(value)
      case value
      when Hash
        value.keys.sort_by(&:to_s).each_with_object({}) do |key, acc|
          acc[key.to_s] = canonicalize(value[key])
        end
      when Array
        value.map { |element| canonicalize(element) }
      else
        value
      end
    end
  end
end
