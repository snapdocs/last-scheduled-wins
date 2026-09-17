require 'spec_helper'

RSpec.describe SidekiqHelper::LastScheduledWins do
  # Local stand-in for a Sidekiq worker. Anonymous class (rather than a
  # top-level constant) so this spec doesn't pollute the global namespace
  # or trigger "already initialized constant" warnings under parallel runs.
  # `to_s` is overridden so the Redis key prefix stays stable across examples
  # — the module's `redis_key` interpolates `worker_class.to_s`.
  #
  # `set(jid:)` mimics Sidekiq's class-level `set` (returns a Setter responding
  # to `perform_at`). Returns self so subsequent `.perform_at` lands on the
  # class. The dummy's `perform_at` return value is ignored by the module —
  # the jid is pre-generated and tracked by `LastScheduledWins` itself.
  let(:dummy_tracked_worker) do
    Class.new do
      def self.set(**)
        self
      end

      def self.perform_at(_at, *_args)
        nil
      end

      def self.to_s
        'DummyTrackedWorker'
      end

      def self.inspect
        to_s
      end
    end
  end

  # Variant that opts into the optional `last_scheduled_wins_key_args` override —
  # dedups on the first positional arg only, so callers can pass execution-flag
  # args (e.g. an options hash) without splitting the dedup chain.
  let(:dummy_dedup_worker) do
    Class.new do
      def self.set(**); self; end
      def self.perform_at(_at, *_args); nil; end
      def self.to_s; 'DummyDedupWorker'; end
      def self.inspect; to_s; end

      def self.last_scheduled_wins_key_args(args)
        [args.first]
      end
    end
  end

  # Clean up any tracking keys this spec writes so it doesn't leak state into
  # adjacent specs that share the same SIDEKIQ_REDIS_URL.
  #
  # Wrapped in `rescue StandardError` so individual examples that stub
  # `Sidekiq.redis` to raise don't surface the stub's exception during
  # teardown — which would mask the real assertion result.
  after do
    Sidekiq.redis do |redis|
      keys = redis.keys("#{SidekiqHelper::LastScheduledWins::KEY_PREFIX}:DummyTrackedWorker:*") +
             redis.keys("#{SidekiqHelper::LastScheduledWins::KEY_PREFIX}:DummyDedupWorker:*")
      redis.del(*keys) if keys.any?
    end
  rescue StandardError
    # Spec-level cleanup raised (likely because the example stubbed
    # `Sidekiq.redis` to raise and the stub hasn't been restored yet).
    # Cleanup is best-effort.
  end

  describe '.perform_at' do
    it 'returns a jid in Sidekiq\'s default format (24 hex chars)' do
      jid = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 1, 'a')
      expect(jid).to match(/\A[a-f0-9]{24}\z/)
    end

    it 'passes the returned jid to .set(jid:) before calling perform_at' do
      captured_jid = nil
      allow(dummy_tracked_worker).to receive(:set).and_wrap_original do |orig, **kwargs|
        captured_jid = kwargs[:jid]
        orig.call(**kwargs)
      end
      expect(dummy_tracked_worker).to receive(:perform_at).with(kind_of(Time), 1, 'a')

      returned_jid = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 1, 'a')
      expect(captured_jid).to eq(returned_jid)
    end

    it 'stores the returned jid in Redis keyed by (worker class, args)' do
      jid = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 42, ['time'], false)

      stored = Sidekiq.redis do |redis|
        redis.get(described_class.redis_key(dummy_tracked_worker, [42, ['time'], false]))
      end
      expect(stored).to eq(jid)
    end

    it 'overwrites the previously stored jid when called again with the same args' do
      first_jid = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 1)
      second_jid = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 1)

      expect(second_jid).not_to eq(first_jid)
      stored = Sidekiq.redis { |redis| redis.get(described_class.redis_key(dummy_tracked_worker, [1])) }
      expect(stored).to eq(second_jid)
    end

    it 'keeps separate keys for different argument sets' do
      jid_1 = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 1)
      jid_2 = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 2)

      Sidekiq.redis do |redis|
        expect(redis.get(described_class.redis_key(dummy_tracked_worker, [1]))).to eq(jid_1)
        expect(redis.get(described_class.redis_key(dummy_tracked_worker, [2]))).to eq(jid_2)
      end
    end

    # Store-first ordering is what closes the worker-side race. Lock it in
    # so a future refactor that reorders the calls is caught by the suite.
    it 'writes the tracking key BEFORE calling Sidekiq schedule (store-first ordering)' do
      order = []
      allow(Sidekiq).to receive(:redis).and_wrap_original do |orig, &block|
        order << :store_jid
        orig.call(&block)
      end
      allow(dummy_tracked_worker).to receive(:perform_at) { order << :schedule }

      described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 'order-check')
      expect(order.first(2)).to eq([:store_jid, :schedule])
    end

    # Workers can opt into a narrower dedup key via `self.last_scheduled_wins_key_args(args)`.
    # Used when perform takes execution flags (e.g. an `options` hash) that vary
    # per enqueue but should share one dedup chain. If a future refactor drops
    # the `respond_to?(:last_scheduled_wins_key_args)` lookup, these catch it.
    context 'when the worker defines last_scheduled_wins_key_args' do
      it 'digests the key-method result for the tracking key, not the full perform args' do
        jid = described_class.perform_at(5.minutes.from_now, dummy_dedup_worker, 42, { 'flag' => true })

        stored = Sidekiq.redis { |redis| redis.get(described_class.redis_key(dummy_dedup_worker, [42])) }
        expect(stored).to eq(jid)
      end

      it 'passes the original perform args (not the dedup key) to Sidekiq.perform_at' do
        expect(dummy_dedup_worker).to receive(:perform_at).with(kind_of(Time), 42, { 'flag' => true })

        described_class.perform_at(5.minutes.from_now, dummy_dedup_worker, 42, { 'flag' => true })
      end

      it 'collapses two enqueues with the same dedup key but different perform args onto one tracking chain' do
        first_jid = described_class.perform_at(5.minutes.from_now, dummy_dedup_worker, 42, { 'a' => 1 })
        second_jid = described_class.perform_at(5.minutes.from_now, dummy_dedup_worker, 42, { 'b' => 2 })

        stored = Sidekiq.redis { |redis| redis.get(described_class.redis_key(dummy_dedup_worker, [42])) }
        expect(stored).to eq(second_jid)
        expect(second_jid).not_to eq(first_jid)
      end
    end

    # When worker_class.perform_at raises (Sidekiq's ZADD failed), the error
    # propagates to the caller. No silent message loss. A future "defensive"
    # rescue here would swallow real scheduling failures and the caller would
    # think the job was queued when it wasn't.
    context 'when worker_class.perform_at raises (Sidekiq schedule write failed)' do
      it 'propagates the error to the caller (no silent loss)' do
        allow(dummy_tracked_worker).to receive(:perform_at).and_raise(RedisClient::CannotConnectError.new('sidekiq-redis down'))

        expect {
          described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 'schedule-fail')
        }.to raise_error(RedisClient::CannotConnectError, /sidekiq-redis down/)
      end

      # With store-first ordering, store_jid runs before perform_at. If the
      # schedule write then fails, the tracking key is left behind as an
      # orphan jid pointing at a job that was never scheduled. Bounded by
      # TTL; overwritten by the next LastScheduledWins.perform_at for these
      # args. Locking this in so a future refactor that re-adds cleanup
      # surfaces here.
      it 'leaves the tracking key as an orphan (bounded by TTL, no cleanup)' do
        allow(dummy_tracked_worker).to receive(:perform_at).and_raise(RedisClient::CannotConnectError.new('boom'))

        begin
          described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 'schedule-fail-2')
        rescue RedisClient::CannotConnectError
          # expected
        end

        stored = Sidekiq.redis { |r| r.get(described_class.redis_key(dummy_tracked_worker, ['schedule-fail-2'])) }
        expect(stored).to match(/\A[a-f0-9]{24}\z/)
      end
    end

    # store_jid's rescue must catch three independent class hierarchies:
    # Redis::BaseError (older `redis` gem), RedisClient::Error (Sidekiq 7+
    # redis-client), and ConnectionPool::TimeoutError (the pool layer above
    # the Redis client). Missing any of them lets the error bubble past the
    # rescue and kill the caller's after_save chain. The "Unexpected error"
    # branch is the catch-all (error log + swallow) for everything else.
    context 'when store_jid raises a connection-related error' do
      # Stub out the retry backoff so persistently-failing store_jid tests
      # don't actually sleep STORE_JID_MAX_ATTEMPTS * backoff_ms each.
      before { allow(Kernel).to receive(:sleep) }

      it 'still proceeds to the enqueue path after rescuing ConnectionPool::TimeoutError' do
        allow(Sidekiq).to receive(:redis).and_raise(ConnectionPool::TimeoutError.new('pool exhausted'))
        allow(Rails.logger).to receive(:warn)

        expect(dummy_tracked_worker).to receive(:perform_at).with(kind_of(Time), 'pool-test')

        expect {
          described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 'pool-test')
        }.not_to raise_error
      end

      it 'retries store_jid STORE_JID_MAX_ATTEMPTS times before giving up' do
        allow(Rails.logger).to receive(:warn)

        call_count = 0
        allow(Sidekiq).to receive(:redis) do
          call_count += 1
          raise Redis::CannotConnectError.new('legacy redis-gem path')
        end

        described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 'retry-test')
        expect(call_count).to eq(described_class::STORE_JID_MAX_ATTEMPTS)
      end

      it 'logs ConnectionPool::TimeoutError at warn level (expected connection-flavored error)' do
        allow(Sidekiq).to receive(:redis).and_raise(ConnectionPool::TimeoutError.new('pool exhausted'))

        expect(Rails.logger).to receive(:warn).with(
          hash_including(
            message: /LastScheduledWins.*store_jid failed after retries/,
            worker_class: 'DummyTrackedWorker',
            error_class: 'ConnectionPool::TimeoutError',
            error_message: /pool exhausted/,
          )
        )
        described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 'pool-test-2')
      end

      # Sidekiq 7+ uses redis-client directly; Sidekiq.redis raises
      # RedisClient::CannotConnectError (a RedisClient::Error) on Sidekiq-Redis
      # connection failures, NOT a Redis::BaseError. Without RedisClient::Error
      # in the named rescue, this expected failure mode falls through to the
      # `rescue StandardError` branch and is logged at error level on every
      # transient blip. Lock it in here.
      it 'treats RedisClient::CannotConnectError as expected (warn + swallow, not error level)' do
        allow(Sidekiq).to receive(:redis).and_raise(RedisClient::CannotConnectError.new('connection refused'))

        expect(Rails.logger).not_to receive(:error)
        expect(Rails.logger).to receive(:warn).with(
          hash_including(
            message: /LastScheduledWins.*store_jid failed after retries/,
            worker_class: 'DummyTrackedWorker',
            error_class: 'RedisClient::CannotConnectError',
            error_message: /connection refused/,
          )
        )

        expect {
          described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 'redis-client-test')
        }.not_to raise_error
      end

      it 'recovers without logging when an early attempt succeeds' do
        original_redis = Sidekiq.method(:redis)
        call_count = 0
        allow(Sidekiq).to receive(:redis) do |&block|
          call_count += 1
          raise ConnectionPool::TimeoutError.new('transient') if call_count == 1
          original_redis.call(&block)
        end

        expect(Rails.logger).not_to receive(:warn)
        described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 'recover-test')
        expect(call_count).to be >= 2
      end
    end

    context 'when store_jid raises an unexpected error (programming bug or unknown Redis wrapper)' do
      it 'logs at error level, then proceeds to the enqueue path after rescuing' do
        # Stub Sidekiq.redis to raise something outside the known
        # connection-error allowlist — e.g. a programming-style error.
        allow(Sidekiq).to receive(:redis).and_raise(NoMethodError.new('oops'))
        allow(Rails.logger).to receive(:warn)

        expect(Rails.logger).to receive(:error).with(
          hash_including(
            message: /LastScheduledWins.*store_jid unexpected error/,
            worker_class: 'DummyTrackedWorker',
            error_class: 'NoMethodError',
            error_message: /oops/,
          )
        )
        expect(dummy_tracked_worker).to receive(:perform_at).with(kind_of(Time), 'unexpected-err')

        expect {
          described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 'unexpected-err')
        }.not_to raise_error
      end
    end
  end

  describe '.perform_in' do
    it 'delegates to perform_at with interval.from_now' do
      freeze_time = Time.local(2026, 5, 14, 12, 0, 0)
      Timecop.freeze(freeze_time) do
        expect(dummy_tracked_worker).to receive(:perform_at).with(freeze_time + 5.minutes, 99)
        described_class.perform_in(5.minutes, dummy_tracked_worker, 99)
      end
    end
  end

  describe '.superseded?' do
    context 'when the worker defines last_scheduled_wins_key_args' do
      it 'reads the tracking key via the worker-defined key method' do
        described_class.perform_at(5.minutes.from_now, dummy_dedup_worker, 42, { 'flag' => true })

        # Different perform args, same dedup chain → stale jid is superseded by the live one.
        expect(described_class.superseded?(dummy_dedup_worker, 'stale-jid', 42, { 'flag' => false }))
          .to eq(true)
      end

      it 'returns false when the live jid matches' do
        jid = described_class.perform_at(5.minutes.from_now, dummy_dedup_worker, 42, { 'flag' => true })

        expect(described_class.superseded?(dummy_dedup_worker, jid, 42, { 'flag' => false }))
          .to eq(false)
      end
    end

    context 'when no key has been stored for these args' do
      it 'returns false (do not drop the job)' do
        expect(described_class.superseded?(dummy_tracked_worker, 'some-jid', 1)).to eq(false)
      end
    end

    context 'when the stored jid matches our jid' do
      it 'returns false (this job is still the current one)' do
        jid = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 1, 'a')
        expect(described_class.superseded?(dummy_tracked_worker, jid, 1, 'a')).to eq(false)
      end
    end

    context 'when the stored jid is different from our jid' do
      let(:our_jid) { 'our-jid' }
      let!(:newer_jid) { described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 1, 'a') }

      it 'returns true (a newer enqueue has won the slot)' do
        expect(described_class.superseded?(dummy_tracked_worker, our_jid, 1, 'a')).to eq(true)
      end

      it 'does not mutate the stored jid (read-only) — retries of a superseded job see the same state' do
        described_class.superseded?(dummy_tracked_worker, our_jid, 1, 'a')

        stored = Sidekiq.redis { |redis| redis.get(described_class.redis_key(dummy_tracked_worker, [1, 'a'])) }
        expect(stored).to eq(newer_jid)
      end
    end

    context 'when our jid is blank' do
      it 'returns false — legacy jobs without a jid in the Sidekiq middleware context should still fire' do
        expect(described_class.superseded?(dummy_tracked_worker, nil, 1)).to eq(false)
        expect(described_class.superseded?(dummy_tracked_worker, '', 1)).to eq(false)
      end
    end

    # Locks in the module-level Redis-error contract: superseded? must NOT
    # rescue exceptions from Sidekiq.redis. Propagating the error puts the
    # job back on Sidekiq's retry queue (where it will re-evaluate the
    # supersede check against current Redis state on retry). A future
    # "defensive" rescue here would silently turn the supersede check into
    # fail-open during a Redis blip, which can re-deliver superseded jobs.
    context 'when Sidekiq.redis raises' do
      it 'propagates the error rather than swallowing it' do
        allow(Sidekiq).to receive(:redis).and_raise(ConnectionPool::TimeoutError.new('pool exhausted'))

        expect {
          described_class.superseded?(dummy_tracked_worker, 'some-jid', 1)
        }.to raise_error(ConnectionPool::TimeoutError)
      end
    end

    context 'argument-shape sensitivity' do
      it 'does not treat a different args list as superseded — different key entirely' do
        described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 1)
        expect(described_class.superseded?(dummy_tracked_worker, 'any-jid', 2)).to eq(false)
      end
    end

    context 'hash-arg canonicalization' do
      it 'treats hashes with identical content but different key order as the same args' do
        jid = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, { a: 1, b: 2 })

        # Same jid, differently-ordered hash → not superseded.
        expect(described_class.superseded?(dummy_tracked_worker, jid, { b: 2, a: 1 })).to eq(false)
        # Different jid, differently-ordered hash → superseded.
        expect(described_class.superseded?(dummy_tracked_worker, 'other-jid', { b: 2, a: 1 })).to eq(true)
      end

      it 'treats HashWithIndifferentAccess the same as a plain Hash with equivalent content' do
        hwia = ActiveSupport::HashWithIndifferentAccess.new(a: 1, b: 2)
        plain = { a: 1, b: 2 }

        expect(described_class.redis_key(dummy_tracked_worker, [hwia])).to eq(
          described_class.redis_key(dummy_tracked_worker, [plain])
        )
      end
    end
  end

  describe '.redis_key' do
    it 'uses the worker class name and an args digest under a stable prefix' do
      key = described_class.redis_key(dummy_tracked_worker, [1, 'x'])
      expect(key).to start_with('sidekiq_last_scheduled_wins:DummyTrackedWorker:')
    end

    it 'produces the same digest for hashes regardless of key order' do
      key_a = described_class.redis_key(dummy_tracked_worker, [{ x: 1, y: 2 }])
      key_b = described_class.redis_key(dummy_tracked_worker, [{ y: 2, x: 1 }])
      expect(key_a).to eq(key_b)
    end

    it 'collapses symbols and strings to the same digest (keys and values)' do
      expect(described_class.redis_key(dummy_tracked_worker, [{ foo: :bar }]))
        .to eq(described_class.redis_key(dummy_tracked_worker, [{ 'foo' => 'bar' }]))
    end

    it 'canonicalizes hashes nested inside collections' do
      expect(described_class.redis_key(dummy_tracked_worker, [[{ a: 1, b: 2 }]]))
        .to eq(described_class.redis_key(dummy_tracked_worker, [[{ b: 2, a: 1 }]]))
    end
  end

  describe '.ttl_for' do
    it 'returns (scheduled_at - now) + DEFAULT_RETRY_WINDOW_SECONDS when the worker does not override' do
      Timecop.freeze(Time.local(2026, 5, 14, 12, 0, 0)) do
        scheduled_at = 3.days.from_now
        expected = 3.days.to_i + described_class::DEFAULT_RETRY_WINDOW_SECONDS
        expect(described_class.ttl_for(dummy_tracked_worker, scheduled_at)).to eq(expected)
      end
    end

    it 'uses the worker-declared retry window when last_scheduled_wins_retry_window_seconds is defined' do
      long_retry_worker = Class.new do
        def self.last_scheduled_wins_retry_window_seconds; 1.week.to_i; end
        def self.to_s; 'LongRetryWorker'; end
      end

      Timecop.freeze(Time.local(2026, 5, 14, 12, 0, 0)) do
        expect(described_class.ttl_for(long_retry_worker, 3.days.from_now))
          .to eq(3.days.to_i + 1.week.to_i)
      end
    end

    it 'clamps a past-dated scheduled_at to 0 — at least the retry window is granted' do
      Timecop.freeze(Time.local(2026, 5, 14, 12, 0, 0)) do
        expect(described_class.ttl_for(dummy_tracked_worker, 1.day.ago))
          .to eq(described_class::DEFAULT_RETRY_WINDOW_SECONDS)
      end
    end
  end

  describe '.perform_at TTL behavior' do
    # Regression: an earlier version derived TTL from scheduled_at without a
    # max-with-existing rule. A newer enqueue scheduled for a closer time
    # would shrink the TTL, the key would expire before the older job's
    # fire-time, and the older job would see a missing key + fire under
    # fail-open. The max-with-existing rule prevents this.
    it 'does not shrink the existing key TTL when a newer enqueue has a closer fire-time' do
      # Far-future enqueue writes TTL ~= 3d + retry window.
      described_class.perform_at(3.days.from_now, dummy_tracked_worker, 'shared-args')
      ttl_after_far = Sidekiq.redis { |r| r.ttl(described_class.redis_key(dummy_tracked_worker, ['shared-args'])) }

      # Near-future enqueue would compute a smaller TTL, but the max-with-existing
      # rule keeps the longer one that's already there.
      described_class.perform_at(1.day.from_now, dummy_tracked_worker, 'shared-args')
      ttl_after_near = Sidekiq.redis { |r| r.ttl(described_class.redis_key(dummy_tracked_worker, ['shared-args'])) }

      expect(ttl_after_near).to be >= ttl_after_far - 5  # tiny slack for elapsed time between the two calls
      expect(ttl_after_near).to be_within(5).of(3.days.to_i + described_class::DEFAULT_RETRY_WINDOW_SECONDS)
    end

    it 'grows the TTL when a newer enqueue has a farther fire-time than the existing key' do
      # Near-future enqueue writes TTL ~= 1d + retry window.
      described_class.perform_at(1.day.from_now, dummy_tracked_worker, 'shared-args')
      ttl_after_near = Sidekiq.redis { |r| r.ttl(described_class.redis_key(dummy_tracked_worker, ['shared-args'])) }

      # Farther-future enqueue computes a larger TTL; max-with-existing picks it.
      described_class.perform_at(5.days.from_now, dummy_tracked_worker, 'shared-args')
      ttl_after_far = Sidekiq.redis { |r| r.ttl(described_class.redis_key(dummy_tracked_worker, ['shared-args'])) }

      expect(ttl_after_far).to be > ttl_after_near
      expect(ttl_after_far).to be_within(5).of(5.days.to_i + described_class::DEFAULT_RETRY_WINDOW_SECONDS)
    end

    # Regression: Redis rejects `SET ... EX 0` with "invalid expire time in
    # set". A worker overriding last_scheduled_wins_retry_window_seconds to 0 combined
    # with a past-dated scheduled_at would otherwise compute TTL=0 and crash
    # the store. Clamping existing_ttl to 1 (in store_jid) ensures effective_ttl
    # is always positive.
    it 'does not raise when ttl_for would compute 0 (zero retry window + past-dated scheduled_at)' do
      zero_retry_worker = Class.new do
        def self.set(**); self; end
        def self.perform_at(_at, *_args); nil; end
        def self.last_scheduled_wins_retry_window_seconds; 0; end
        def self.to_s; 'ZeroRetryWorker'; end
      end

      expect {
        described_class.perform_at(1.day.ago, zero_retry_worker, 'past-dated')
      }.not_to raise_error

      stored = Sidekiq.redis { |r| r.get(described_class.redis_key(zero_retry_worker, ['past-dated'])) }
      expect(stored).to match(/\A[a-f0-9]{24}\z/)

      Sidekiq.redis { |r| r.del(described_class.redis_key(zero_retry_worker, ['past-dated'])) }
    end
  end

  describe '.cancel' do
    it 'overwrites the stored jid with CANCELED_JID' do
      described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 50)

      described_class.cancel(dummy_tracked_worker, 50)

      stored = Sidekiq.redis { |r| r.get(described_class.redis_key(dummy_tracked_worker, [50])) }
      expect(stored).to eq(described_class::CANCELED_JID)
    end

    it 'preserves the TTL of the existing key (so the sentinel outlives the in-flight job)' do
      described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 60)

      key = described_class.redis_key(dummy_tracked_worker, [60])
      ttl_before = Sidekiq.redis { |r| r.ttl(key) }

      described_class.cancel(dummy_tracked_worker, 60)

      ttl_after = Sidekiq.redis { |r| r.ttl(key) }
      expect(ttl_after).to be_within(2).of(ttl_before)
      expect(ttl_after).to be > 0
    end

    it 'causes superseded? to return true for the original job' do
      original_jid = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 70)

      described_class.cancel(dummy_tracked_worker, 70)

      expect(described_class.superseded?(dummy_tracked_worker, original_jid, 70)).to eq(true)
    end

    it 'is a no-op when no key exists (does not create one via fail-open)' do
      described_class.cancel(dummy_tracked_worker, 'nonexistent')

      stored = Sidekiq.redis { |r| r.get(described_class.redis_key(dummy_tracked_worker, ['nonexistent'])) }
      expect(stored).to be_nil
    end

    it 'is overwritten by a subsequent perform_at — cancel does not permanently lock the key' do
      described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 80)
      described_class.cancel(dummy_tracked_worker, 80)
      new_jid = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 80)

      stored = Sidekiq.redis { |r| r.get(described_class.redis_key(dummy_tracked_worker, [80])) }
      expect(stored).to eq(new_jid)
      expect(described_class.superseded?(dummy_tracked_worker, new_jid, 80)).to eq(false)
    end

    context 'when the worker defines last_scheduled_wins_key_args' do
      it 'cancels via the worker-defined dedup key, not the full perform args' do
        jid = described_class.perform_at(5.minutes.from_now, dummy_dedup_worker, 42, { 'flag' => true })

        described_class.cancel(dummy_dedup_worker, 42, { 'other' => true })

        stored = Sidekiq.redis { |r| r.get(described_class.redis_key(dummy_dedup_worker, [42])) }
        expect(stored).to eq(described_class::CANCELED_JID)
        expect(described_class.superseded?(dummy_dedup_worker, jid, 42)).to eq(true)
      end
    end

    # Locks in the design choice: cancel does NOT swallow Sidekiq.redis errors.
    # Same rationale as `superseded?` — if a future "defensive" rescue is added
    # here, a Sidekiq-Redis blip would silently turn cancel into a no-op,
    # leaving in-flight jobs to fire when they should have been suppressed.
    # Propagation lets the caller decide (retry, log, no-op) with full visibility.
    it 'propagates Sidekiq.redis errors to the caller (does not swallow)' do
      allow(Sidekiq).to receive(:redis).and_raise(ConnectionPool::TimeoutError.new('pool exhausted'))

      expect {
        described_class.cancel(dummy_tracked_worker, 100)
      }.to raise_error(ConnectionPool::TimeoutError)
    end
  end

  describe '.untrack' do
    it 'removes the stored tracking key for (worker, args)' do
      jid = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 99)

      key = described_class.redis_key(dummy_tracked_worker, [99])
      expect(Sidekiq.redis { |r| r.get(key) }).to eq(jid)

      described_class.untrack(dummy_tracked_worker, 99)

      expect(Sidekiq.redis { |r| r.get(key) }).to be_nil
    end

    it 'is a no-op when no key exists' do
      expect {
        described_class.untrack(dummy_tracked_worker, 'nonexistent')
      }.not_to raise_error
    end

    context 'when the worker defines last_scheduled_wins_key_args' do
      it 'removes the tracking key via the worker-defined dedup key, not the full perform args' do
        described_class.perform_at(5.minutes.from_now, dummy_dedup_worker, 42, { 'flag' => true })
        key = described_class.redis_key(dummy_dedup_worker, [42])
        expect(Sidekiq.redis { |r| r.get(key) }).to be_present

        described_class.untrack(dummy_dedup_worker, 42, { 'other' => true })

        expect(Sidekiq.redis { |r| r.get(key) }).to be_nil
      end
    end

    # Same rationale as `cancel`: untrack does NOT swallow Sidekiq.redis errors.
    # A future "defensive" rescue here would silently leave stale tracking keys
    # in place after a flipper-off rollback, causing legacy jobs to drop at
    # fire-time via the supersede check. Propagation surfaces the failure so
    # the caller knows the rollback affordance didn't take effect.
    it 'propagates Sidekiq.redis errors to the caller (does not swallow)' do
      allow(Sidekiq).to receive(:redis).and_raise(ConnectionPool::TimeoutError.new('pool exhausted'))

      expect {
        described_class.untrack(dummy_tracked_worker, 101)
      }.to raise_error(ConnectionPool::TimeoutError)
    end
  end

  # A caller whose work must not change scheduling state wraps it in
  # `without_tracking`: the tracking-key writes are skipped, enqueues still go out.
  describe '.without_tracking' do
    describe '.cancel' do
      let!(:live_jid) { described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 50) }

      it 'does not overwrite the live tracking key with the cancel sentinel' do
        described_class.without_tracking { described_class.cancel(dummy_tracked_worker, 50) }

        stored = Sidekiq.redis { |r| r.get(described_class.redis_key(dummy_tracked_worker, [50])) }
        expect(stored).to eq(live_jid)
      end
    end

    describe '.untrack' do
      let!(:live_jid) { described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 60) }

      it 'leaves the live tracking key in place' do
        described_class.without_tracking { described_class.untrack(dummy_tracked_worker, 60) }

        stored = Sidekiq.redis { |r| r.get(described_class.redis_key(dummy_tracked_worker, [60])) }
        expect(stored).to eq(live_jid)
      end
    end

    describe '.perform_at' do
      it 'does not write a tracking key' do
        described_class.without_tracking { described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 70) }

        stored = Sidekiq.redis { |r| r.get(described_class.redis_key(dummy_tracked_worker, [70])) }
        expect(stored).to be_nil
      end

      it 'still hands the enqueue to Sidekiq' do
        expect(dummy_tracked_worker).to receive(:perform_at).with(kind_of(Time), 70)

        described_class.without_tracking { described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 70) }
      end
    end

    it 'restores tracking after the block, including when it raises' do
      expect { described_class.without_tracking { raise 'boom' } }.to raise_error('boom')

      jid = described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 80)
      stored = Sidekiq.redis { |r| r.get(described_class.redis_key(dummy_tracked_worker, [80])) }
      expect(stored).to eq(jid)
    end

    it 'restores the outer suppression when nested, not tracking' do
      described_class.without_tracking do
        described_class.without_tracking { nil }

        described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 90)
      end

      stored = Sidekiq.redis { |r| r.get(described_class.redis_key(dummy_tracked_worker, [90])) }
      expect(stored).to be_nil
    end
  end

  # Outside the block every caller's tracking writes must be untouched.
  describe 'outside .without_tracking' do
    let!(:live_jid) { described_class.perform_at(5.minutes.from_now, dummy_tracked_worker, 100) }

    it 'overwrites the tracking key with the cancel sentinel' do
      described_class.cancel(dummy_tracked_worker, 100)

      stored = Sidekiq.redis { |r| r.get(described_class.redis_key(dummy_tracked_worker, [100])) }
      expect(stored).to eq(described_class::CANCELED_JID)
    end
  end
end
