require 'spec_helper'
require 'hyperll'

module Hyperll
  describe HyperLogLog do
    it 'computes cardinality' do
      hll = HyperLogLog.new(16)
      hll.offer(0)
      hll.offer(1)
      hll.offer(2)
      hll.offer(3)
      hll.offer(16)
      hll.offer(17)
      hll.offer(18)
      hll.offer(19)
      hll.offer(19)

      expect(hll.cardinality).to eq(8)
    end

    it 'is accurate within an expected amount for high cardinalities' do
      hll = HyperLogLog.new(10)

      size = 1_000_000
      size.times do
        hll.offer(rand(2**63))
      end

      expect(hll.cardinality).to be_within(10).percent_of(size)
    end

    it 'merges with other hyperloglog instances' do
      size = 100_000
      hlls = Array.new(5) do
        HyperLogLog.new(16).tap { |hll|
          size.times { hll.offer(rand(2**63)) }
        }
      end

      merged = HyperLogLog.new(16)
      merged.merge(*hlls)

      expect(merged.cardinality).to be_within(10).percent_of(size * hlls.length)
    end

    it 'serializes to a string' do
      hll = HyperLogLog.new(4)
      hll.offer(1)
      hll.offer(2)

      # h = Java::com::clearspring::analytics::stream::cardinality::HyperLogLog.new(4)
      # h.offer(1)
      # h.offer(2)
      # h.getBytes()
      expect(hll.serialize.unpack("C*")).to eq(
        [0, 0, 0, 4, 0, 0, 0, 12, 2, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0]
      )
    end

    it 'unserializes from a string' do
      serialized = [0, 0, 0, 4, 0, 0, 0, 12, 2, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0].pack("C*")
      hll = HyperLogLog.unserialize(serialized)

      expect(hll.cardinality).to eq(2)
      hll.offer(1)
      hll.offer(2)
      hll.offer(3)
      expect(hll.cardinality).to eq(3)
    end

    it 'supports Ractor' do
      return unless defined?(Ractor)
      shll = HyperLogLog.new(4)
      shll.offer(1)
      shll.offer(1)
      shll.offer(2)
      shll = HyperLogLog.unserialize(shll.serialize)
      orig = shll.cardinality
      expect(orig).to eq(2)

      serialized = shll.serialize
      1000.times do |n|
        begin
        a = {}
        b = Array.new 1000
        c = n
        end
      end

      shll = HyperLogLog.unserialize(serialized)
      expect(shll.cardinality).to eq(orig)
      # puts "L#{serialized.length}, #{serialized.unpack("N*").join('.')}"


      # via ractors:
      ractor2 = Ractor.new do
        hll_raw = Ractor.receive
        # puts "r2: L#{hll_raw.length}, #{hll_raw.unpack("N*").join('.')}"
        hll = HyperLogLog.unserialize(hll_raw)
        # puts "r3: #{hll.serialize.unpack("N*").join('.')}"
        Ractor.yield hll.cardinality
      end

      ractor = Ractor.new ractor2 do |ractor2|
        hll = HyperLogLog.new(4)
        hll.offer(1)
        hll.offer(1)
        hll.offer(2)
        card = hll.cardinality
        ser  = hll.serialize
        # puts "r1: L#{ser.length}, #{hll.serialize.unpack("N*").join('.')}"
        ractor2.send(hll.serialize.freeze, move: true) # move: true  => test fails. Ruby does not clean the memory? or HLL's bug of alignment/reading past pointer? #freeze solved the problem. Ractor bug probably in 3.3.0
        Ractor.yield card
      end

      expect(ractor.take).to eq(orig)
      expect(ractor2.take).to eq(orig)

    end
  end
end
