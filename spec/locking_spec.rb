require 'spec_helper'

PROCESSES = [1, 2, 10, 50]

shared_examples 'a proper locking implementation' do
  before(:each) do
    redis_client.flushdb
  end

  it 'acquires lock' do
    locked = false
    invoke_lock.call do |lock, redis|
      locked = true
    end
    expect(locked).to be_truthy
  end

  PROCESSES.each do |n|
    describe "for #{n} threads" do
      it 'increments safely' do
        threads = []

        n.times do
          threads << Thread.new do
            invoke_lock.call do |lock, redis|
              count = redis.get('testing:count').to_i
              redis.set('testing:count', count + 1)
            end
          end
        end

        threads.each(&:join)

        expect(redis_client.get('testing:count').to_i).to eq(n)
      end

      it 'increments with a slow worker' do
        threads = []

        n.times do
          threads << Thread.new do
            acquisition_timeout = n*3*2 # Acquisition timeout of n processes * 3 seconds for sleeps (max) * 2 (just to be safe)

            invoke_lock.call(acquisition_timeout) do |lock, redis|
              sleep rand
              count = redis.get('testing:count').to_i
              sleep rand
              redis.set('testing:count', count + 1)
              sleep rand
            end
          end
        end

        threads.each(&:join)

        expect(redis_client.get('testing:count').to_i).to eq(n)
      end

      it 'locks and relocks' do
        threads = []

        n.times do
          threads << Thread.new do
            acquisition_timeout = n*3*2

            3.times do
              invoke_lock.call(acquisition_timeout) do |lock, redis|
                count = redis.get('testing:count').to_i
                redis.set('testing:count', count + 1)
                sleep rand
              end
            end
          end
        end

        threads.each(&:join)

        expect(redis_client.get('testing:count').to_i).to eq(n*3)
      end
    end
  end

  PROCESSES.each do |n|
    describe "for #{n} processes" do
      it 'increments safely' do
        pids = []

        n.times do
          pids << fork do
            invoke_lock.call do |lock, redis|
              count = redis.get('testing:count').to_i
              redis.set('testing:count', count + 1)
            end
          end
        end

        pids.each { |pid| Process.waitpid(pid) }

        expect(redis_client.get('testing:count').to_i).to eq(n)
      end

      it 'increments with a slow worker' do
        pids = []

        n.times do
          pids << fork do
            acquisition_timeout = n*3*2 # Acquisition timeout of n processes * 3 seconds for sleeps (max) * 2 (just to be safe)

            invoke_lock.call(acquisition_timeout) do |lock, redis|
              sleep rand
              count = redis.get('testing:count').to_i
              sleep rand
              redis.set('testing:count', count + 1)
              sleep rand
            end
          end
        end

        pids.each { |pid| Process.waitpid(pid) }

        expect(redis_client.get('testing:count').to_i).to eq(n)
      end

      it 'locks and relocks' do
        pids = []

        n.times do
          pids << fork do
            acquisition_timeout = n*3*2 # Acquisition timeout of n processes * 3 seconds for sleeps (max) * 2 (just to be safe)

            3.times do
              invoke_lock.call(acquisition_timeout) do |lock, redis|
                count = redis.get('testing:count').to_i
                redis.set('testing:count', count + 1)
              end
              sleep rand
            end
          end
        end

        pids.each { |pid| Process.waitpid(pid) }

        expect(redis_client.get('testing:count').to_i).to eq(n*3)
      end
    end
  end

  it 'expires lock' do
    pids = []

    pids << fork do
      invoke_lock.call do |lock, redis|
        puts "[#{Process.pid}] Acquired lock"
        sleep 10
      end
      puts "[#{Process.pid}] Unlocked (should never get here)"
    end

    3.times do
      if check_lock.call
        puts "[#{Process.pid}] Sees that lock is acquired"
        pids.each { |pid|
          puts "[#{Process.pid}] Killing #{pid}"
          Process.kill(9, pid)
        }
        break
      else
        puts "[#{Process.pid}] Waiting for lock to be acquired"
      end

      sleep 1
    end

    pids.each { |pid| Process.waitpid(pid) }

    expect(check_lock.call).to be_truthy

    10.times do
      if check_lock.call
        puts "[#{Process.pid}] Waiting for lock to expire"
      else
        puts "[#{Process.pid}] Lock expired"
        break
      end

      sleep 1
    end

    expect(check_lock.call).to be_falsy
  end
end

describe 'mlanett/redis-lock' do
  require 'redis-lock'

  Redis::Lock.class_eval do
    def check( now = Time.now.to_i )
      # read both in a transaction in a multi to ensure we have a consistent view
      result = redis.multi do |multi|
        multi.get( okey )
        multi.get( xkey )
      end
      result = result.compact
      result && result.size == 2 && !is_deleteable?( result[0], result[1], now )
    end
  end

  let(:redis_client) { Redis.new(:db => 2) }
  let(:invoke_lock) {
    lambda { |*args, &block|
      acquisition_timeout = args.first || 1
      redis = Redis.new(:db => 2)
      lock = Redis::Lock.new(redis, 'testing', :life => 5)
      lock.lock(acquisition_timeout) do
        block.call(lock, redis)
      end
    }
  }
  let(:check_lock) {
    lambda {
      redis = Redis.new(:db => 2)
      lock = Redis::Lock.new(redis, 'testing', :life => 5)
      lock.check
    }
  }

  it_behaves_like 'a proper locking implementation'
end

describe 'kenn/redis-mutex' do
  require 'redis-mutex'

  let(:redis_client) { Redis.new(:db => 2) }
  let(:invoke_lock) {
    lambda { |*args, &block|
      acquisition_timeout = args.first || 1
      redis = Redis.new(:db => 2)
      Redis::Classy.db = redis
      Redis::Mutex.db = redis
      mutex = Redis::Mutex.new('testing', :block => acquisition_timeout, :expire => 5)
      mutex.with_lock do
        block.call(mutex, redis)
      end
    }
  }
  let(:check_lock) {
    lambda {
      redis = Redis.new(:db => 2)
      Redis::Classy.db = redis
      Redis::Mutex.db = redis
      mutex = Redis::Mutex.new('testing', :expire => 5)
      mutex.locked?
    }
  }

  it_behaves_like 'a proper locking implementation'
end

describe 'SETNX locking' do # See http://redis.io/commands/setnx
  let(:redis_client) { Redis.new(:db => 2) }
  let(:setnx) {
    Class.new do
      include SETNX::Locking
    end
  }
  let(:invoke_lock) {
    lambda { |*args, &block|
      acquisition_timeout = args.first || 1
      redis = Redis.new(:db => 2)
      lock = setnx.new

      start_at = Time.now
      while Time.now - start_at < acquisition_timeout
        lock.with_lock(redis, 'testing', 5) do
          block.call(lock, redis)
          lock.clear_lock(redis, 'testing')
        end and break
        sleep 0.1
      end
    }
  }
  let(:check_lock) {
    lambda {
      redis = Redis.new(:db => 2)
      lock = setnx.new
      lock.check_lock(redis, 'testing', 5)
    }
  }

  it_behaves_like 'a proper locking implementation'
end
