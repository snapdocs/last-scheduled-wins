# Last Scheduled Wins

A strategy for "cancelling" scheduled Sidekiq jobs that does not scan the schedule sorted set.

This is a single file, meant to be copied into your application. It is not a gem and there is no gemspec.

The API, usage, concurrency guarantees, and failure behaviour are documented in [`lib/sidekiq_helper/last_scheduled_wins.rb`](lib/sidekiq_helper/last_scheduled_wins.rb).

## The problem

You want the last enqueue of a job to cancel previously scheduled executions.

## The approach

Every enqueue writes its job id to one Redis key derived from the worker class
and the arguments, then schedules the job as normal. Every job stays on the
schedule. When one fires, it compares its own job id against the key and
returns without working if the id no longer matches. Both sides cost one Redis
operation, whatever the size of the scheduled set.

## Running the specs

```bash
bundle install
redis-server & # or point REDIS_URL at an existing instance
bundle exec rspec
```

## License

MIT. See [LICENSE](LICENSE).
