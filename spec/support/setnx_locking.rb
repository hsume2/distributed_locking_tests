module SETNX
  # Internal: Locking algorithm implementation backed by Redis SETNX.
  # See http://redis.io/commands/setnx.
  #
  # Examples:
  #
  #   class ProtectedOperation
  #     include SETNX::Locking
  #
  #     def run
  #       with_lock(client, 'document.lock', 15) do
  #         value = client.get('document')
  #         value += "\nTyping..."
  #         client.set('document', value)
  #         clear_lock(client, 'document.lock')
  #       end
  #     end
  #
  #     def client
  #       Redis.new
  #     end
  #   end
  #
  #   5.times do
  #     fork { ProtectedOperation.new.run }
  #   end
  #
  #   Process.waitall
  #
  # As of this writing, I have been able to verify correctness of this algorithm
  # at 100 concurrent clients.
  module Locking
    # Internal: Guards against multiple processes from running the supplied block
    # at the same time.
    #
    # client       - The Redis client to connect to the Redis server with.
    #                See https://github.com/redis/redis-rb.
    # lock_key     - The String to use as the locking primitive.
    # lock_timeout - The Fixnum to define how long until a lock hold is
    #                considered expired.
    # block        - The block that contains code that will be safely run once a lock
    #                is held.
    #
    # Examples:
    #
    #   with_lock(client, 'document.lock', 15) do
    #     value = client.get('document')
    #     value += "\nTyping..."
    #     client.set('document', value)
    #     clear_lock(client, 'document.lock')
    #   end
    def with_lock(client, lock_key, lock_timeout, &block)
      SETNX::Locking::Lock.new(client, lock_key, lock_timeout).execute(&block)
    end

    # Internal: Clears lock held by a process. This should be performed
    # once a process is done executing its code. This is specifically left up
    # to users of this API to determine when a lock should be given up.
    #
    # For example. In cases of guarding Resque enqueues, we do not want to release a
    # lock until a Resque worker has finished processing a job.
    def clear_lock(client, lock_key)
      client.del(lock_key)
    end

    def check_lock(client, lock_key, lock_timeout)
      SETNX::Locking::Lock.new(client, lock_key, lock_timeout).check
    end

    # Private: Abstraction of the locking algorithm
    class Lock
      attr_reader :client, :lock_key, :lock_timeout

      def initialize(client, lock_key, lock_timeout)
        @client, @lock_key, @lock_timeout, @block = client, lock_key, lock_timeout
      end

      def execute(&block)
        reply = client.setnx(lock_key, next_timeout)
        result = if reply
          yield if block_given?
        else
          value = client.get(lock_key)
          if value.nil?
            reply = client.setnx(lock_key, next_timeout)
            yield if reply && block_given?
          elsif timeout_expired?(value)
            original_timeout = client.getset(lock_key, next_timeout)
            if original_timeout.nil? || timeout_expired?(original_timeout)
              yield if block_given?
            end
          end
        end
        client.expire(lock_key, lock_timeout) if reply
        result
      end

      def check
        value = client.get(lock_key)
        value && !timeout_expired?(value)
      end

      private

      def next_timeout
        Time.now.to_f + lock_timeout + 1
      end

      def timeout_expired?(value)
        value.to_f < Time.now.to_f
      end
    end
  end
end
