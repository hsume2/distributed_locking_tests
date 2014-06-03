require 'spec_helper'

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

PROCESSES = [1, 2, 10, 50]

describe 'Locking' do
  let(:redis) { Redis.new(:db => 2) }
  let(:key) { 'testing'}
  let(:lock) { Redis::Lock.new(redis, key, :life => 5) }

  before(:each) do
    redis.flushdb
  end

  it 'acquires lock' do
    locked = false
    lock.lock do
      locked = true
    end
    expect(locked).to be_truthy
  end

  it 'acquires and releases lock' do
    expect(lock.locked?).to be_falsy
    lock.lock do
      expect(lock.locked?).to be_truthy
    end
    expect(lock.locked?).to be_falsy
  end

  PROCESSES.each do |n|
    describe "for #{n} threads" do
      it 'increments safely' do
        threads = []

        n.times do
          threads << Thread.new do
            redis = Redis.new(:db => 2)
            lock = Redis::Lock.new(redis, key)

            lock.lock do
              count = redis.get('testing:count').to_i
              redis.set('testing:count', count + 1)
            end
          end
        end

        threads.each(&:join)

        expect(redis.get('testing:count').to_i).to eq(n)
      end

      it 'increments with a slow worker' do
        threads = []

        n.times do
          threads << Thread.new do
            redis = Redis.new(:db => 2)
            lock = Redis::Lock.new(redis, key)

            lock.lock(n*3*2) do # Acquisition timeout of n processes * 3 seconds for sleeps (max) * 2 (just to be safe)
              sleep rand
              count = redis.get('testing:count').to_i
              sleep rand
              redis.set('testing:count', count + 1)
              sleep rand
            end
          end
        end

        threads.each(&:join)

        expect(redis.get('testing:count').to_i).to eq(n)
      end

      it 'locks and relocks' do
        threads = []

        n.times do
          threads << Thread.new do
            redis = Redis.new(:db => 2)
            lock = Redis::Lock.new(redis, key)

            3.times do
              lock.lock(n*3*2) do
                count = redis.get('testing:count').to_i
                redis.set('testing:count', count + 1)
              end
              sleep rand
            end
          end
        end

        threads.each(&:join)

        expect(redis.get('testing:count').to_i).to eq(n*3)
      end
    end
  end

  PROCESSES.each do |n|
    describe "for #{n} processes" do
      it 'increments safely' do
        pids = []

        n.times do
          pids << fork do
            redis = Redis.new(:db => 2)
            lock = Redis::Lock.new(redis, key)

            lock.lock do
              count = redis.get('testing:count').to_i
              redis.set('testing:count', count + 1)
            end
          end
        end

        pids.each { |pid| Process.waitpid(pid) }

        expect(redis.get('testing:count').to_i).to eq(n)
      end

      it 'increments with a slow worker' do
        pids = []

        n.times do
          pids << fork do
            redis = Redis.new(:db => 2)
            lock = Redis::Lock.new(redis, key)

            lock.lock(n*3*2) do # Acquisition timeout of n processes * 3 seconds for sleeps (max) * 2 (just to be safe)
              sleep rand
              count = redis.get('testing:count').to_i
              sleep rand
              redis.set('testing:count', count + 1)
              sleep rand
            end
          end
        end

        pids.each { |pid| Process.waitpid(pid) }

        expect(redis.get('testing:count').to_i).to eq(n)
      end

      it 'locks and relocks' do
        pids = []

        n.times do
          pids << fork do
            redis = Redis.new(:db => 2)
            lock = Redis::Lock.new(redis, key)

            3.times do
              lock.lock(n*3*2) do
                count = redis.get('testing:count').to_i
                redis.set('testing:count', count + 1)
              end
              sleep rand
            end
          end
        end

        pids.each { |pid| Process.waitpid(pid) }

        expect(redis.get('testing:count').to_i).to eq(n*3)
      end
    end
  end

  it 'expires lock' do
    pids = []

    pids << fork do
      redis = Redis.new(:db => 2)
      lock = Redis::Lock.new(redis, key, :life => 5)
      lock.lock do
        puts "[#{Process.pid}] Acquired lock"
        sleep 10
      end
      puts "[#{Process.pid}] Unlocked (should never get here)"
    end

    3.times do
      if lock.check
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

    expect(lock.check).to be_truthy

    10.times do
      if lock.check
        puts "[#{Process.pid}] Waiting for lock to expire"
      else
        puts "[#{Process.pid}] Lock expired"
        break
      end

      sleep 1
    end

    expect(lock.check).to be_falsy
  end
end
