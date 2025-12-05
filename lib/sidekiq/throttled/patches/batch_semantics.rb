# frozen_string_literal: true

require_relative "../strategy"

# When a job is throttled:
#   - The original JID is ACKed but never executed.
#   - Sidekiq Batch sees it as "pending forever".
#
# When Sidekiq Pro retries a job, it marks the original JID as finished
# and attaches the batch metadata to the retry job.
#
# We mimic that behavior here:
#   1. Mark the original job as completed in the batch.
#   2. Copy batch metadata (bid/bs) into the replacement job payload.

module Sidekiq
  module Throttled
    module Patches
      module BatchSemantics
        private

        # Reschedule the job to be executed later in the target queue.
        # The queue name should NOT include the "queue:" prefix, so we remove it if it's present.
        def reschedule_throttled(work, target_queue)
          target_queue = target_queue.delete_prefix("queue:")
          message      = JSON.parse(work.job)

          job_class = message.fetch("wrapped") { message["class"] }
          return false unless job_class

          job_args = message["args"]

          bid = message["bid"]
          bs  = message["bs"]
          original_jid = message["jid"]

          # 1. Mark ORIGINAL job as complete inside the batch
          if bid
            begin
              status = Sidekiq::Batch::Status.new(bid)
              status.poke(original_jid, :complete)
            rescue => e
              Sidekiq.logger.warn("Could not mark throttled batch job as complete: #{e.class} #{e.message}")
            end
          end

          #
          # 2. Build a new job payload with preserved batch context
          #
          payload = {
            "class" => job_class,
            "args"  => job_args,
          }

          payload["bid"] = bid if bid
          payload["bs"]  = bs  if bs

          # Respect Sidekiq default options (queue, retry, etc.)
          payload = Sidekiq::Client.default_worker_options.merge(payload)

          #
          # 3. Push replacement job into the same batch (same bid)
          #
          Sidekiq::Client.push(
            {
              "queue" => target_queue,
              "at"    => Time.now.to_f + retry_in(work)
            }.merge(payload)
          )

          #
          # 4. ACK original job so SuperFetch doesn't retry it
          #
          work.acknowledge
        end
      end
    end
  end
end

begin
  require "sidekiq/batch"
  Sidekiq::Throttled::Strategy.prepend(Sidekiq::Throttled::Patches::BatchSemantics)
rescue LoadError
  # Sidekiq Batch is not available
end

