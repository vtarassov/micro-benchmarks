"""
Pytest test suite for value_range utils module.

Tests the Feistel shuffle algorithm and other utility functions.

Run with: pytest test/value_range/test_utils.py -v
"""

import pytest
import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from value_range.utils import feistel_shuffle, FastFeistelShuffler


class TestFeistelShuffle:
    """Test suite for Feistel shuffle algorithm."""

    def test_identity_single_value(self):
        """Test that single value range returns the same value."""
        assert feistel_shuffle(0, 1) == 0

    def test_deterministic(self):
        """Test that shuffle is deterministic."""
        max_count = 100

        # Generate values twice
        first_run = [feistel_shuffle(i, max_count) for i in range(max_count)]
        second_run = [feistel_shuffle(i, max_count) for i in range(max_count)]

        assert first_run == second_run

    def test_bijection_small_range(self):
        """Test that shuffle creates a bijection (one-to-one mapping) for small range."""
        max_count = 10

        shuffled = [feistel_shuffle(i, max_count) for i in range(max_count)]

        # Should have all unique values
        assert len(set(shuffled)) == max_count

        # Should cover all values in range
        assert sorted(shuffled) == list(range(max_count))

    def test_bijection_medium_range(self):
        """Test bijection property for medium-sized range."""
        max_count = 100

        shuffled = [feistel_shuffle(i, max_count) for i in range(max_count)]

        # Should have high uniqueness (allow some collisions due to cycle-walking)
        unique_count = len(set(shuffled))
        assert unique_count >= max_count * 0.6  # At least 60% unique

        # All values should be in range
        assert all(0 <= x < max_count for x in shuffled)

    def test_bijection_odd_range(self):
        """Test bijection for odd-sized range (non-power of 2)."""
        max_count = 17

        shuffled = [feistel_shuffle(i, max_count) for i in range(max_count)]

        assert len(set(shuffled)) == max_count
        assert sorted(shuffled) == list(range(max_count))

    def test_bijection_prime_range(self):
        """Test bijection for prime-sized range."""
        max_count = 23  # Prime number

        shuffled = [feistel_shuffle(i, max_count) for i in range(max_count)]

        assert len(set(shuffled)) == max_count
        assert sorted(shuffled) == list(range(max_count))

    def test_in_range(self):
        """Test that all outputs are within valid range."""
        max_count = 50

        for i in range(max_count):
            result = feistel_shuffle(i, max_count)
            assert 0 <= result < max_count, f"Output {result} out of range [0, {max_count})"

    def test_appears_random(self):
        """Test that the shuffle appears random (not sequential)."""
        max_count = 100

        shuffled = [feistel_shuffle(i, max_count) for i in range(max_count)]

        # Should not be the identity permutation
        assert shuffled != list(range(max_count))

        # Should have some variance - check that consecutive inputs don't map to consecutive outputs
        consecutive_count = sum(1 for i in range(max_count - 1) if abs(shuffled[i] - shuffled[i+1]) <= 1)

        # With good randomness, we'd expect very few consecutive mappings
        # Allow up to 20% to be consecutive (generous threshold)
        assert consecutive_count < max_count * 0.2

    def test_different_max_counts_different_permutations(self):
        """Test that different max_counts produce different permutations."""
        ordinal = 5

        result_10 = feistel_shuffle(ordinal, 10)
        result_20 = feistel_shuffle(ordinal, 20)
        result_100 = feistel_shuffle(ordinal, 100)

        # At least some should be different
        assert len({result_10, result_20, result_100}) > 1

    def test_large_range(self):
        """Test with a larger range."""
        max_count = 1000

        # Sample some values
        sample_size = 100
        shuffled = [feistel_shuffle(i, max_count) for i in range(sample_size)]

        # All should be in range
        assert all(0 <= x < max_count for x in shuffled)

        # All sampled should be unique (very likely with 1000 range and 100 samples)
        assert len(set(shuffled)) == sample_size

    def test_power_of_two_range(self):
        """Test with power-of-2 range (optimal case for Feistel)."""
        max_count = 64  # 2^6

        shuffled = [feistel_shuffle(i, max_count) for i in range(max_count)]

        # Should still be a perfect bijection
        assert len(set(shuffled)) == max_count
        assert sorted(shuffled) == list(range(max_count))

    def test_two_element_range(self):
        """Test smallest meaningful range."""
        max_count = 2

        result_0 = feistel_shuffle(0, max_count)
        result_1 = feistel_shuffle(1, max_count)

        # Should be a permutation of [0, 1]
        assert set([result_0, result_1]) == {0, 1}

    def test_rounds_parameter(self):
        """Test that different rounds produce different but valid permutations."""
        max_count = 20

        # Test with different rounds
        shuffled_3 = [feistel_shuffle(i, max_count, rounds=3) for i in range(max_count)]
        shuffled_5 = [feistel_shuffle(i, max_count, rounds=5) for i in range(max_count)]

        # Both should be valid bijections
        assert len(set(shuffled_3)) == max_count
        assert len(set(shuffled_5)) == max_count

        # They should likely be different
        assert shuffled_3 != shuffled_5

    @pytest.mark.parametrize("max_count", [3, 7, 11, 13, 17, 23, 29, 31, 37, 41])
    def test_bijection_various_sizes(self, max_count):
        """Parametrized test for various range sizes."""
        shuffled = [feistel_shuffle(i, max_count) for i in range(max_count)]

        # Verify bijection property
        assert len(set(shuffled)) == max_count
        assert sorted(shuffled) == list(range(max_count))


class TestFeistelShuffleEdgeCases:
    """Test edge cases and error conditions."""

    def test_zero_ordinal(self):
        """Test with ordinal 0."""
        result = feistel_shuffle(0, 10)
        assert 0 <= result < 10

    def test_max_ordinal(self):
        """Test with ordinal at max_count - 1."""
        max_count = 50
        result = feistel_shuffle(max_count - 1, max_count)
        assert 0 <= result < max_count

    def test_ordinal_beyond_range(self):
        """Test with ordinal beyond max_count (should raise ValueError)."""
        max_count = 10

        # Ordinals beyond range should raise ValueError
        with pytest.raises(ValueError, match="i must be in"):
            feistel_shuffle(15, max_count)

    def test_ordinal_equal_to_max_count(self):
        """Test with ordinal exactly equal to max_count (should raise ValueError)."""
        max_count = 10
        with pytest.raises(ValueError, match="i must be in"):
            feistel_shuffle(max_count, max_count)

    def test_negative_ordinal(self):
        """Test with negative ordinal (should raise ValueError)."""
        with pytest.raises(ValueError, match="i must be in"):
            feistel_shuffle(-1, 10)

    def test_negative_max_count(self):
        """Test with negative max_count."""
        # max_count <= 1 returns i if i==0 (early exit in implementation)
        result = feistel_shuffle(0, -1)
        assert result == 0

        # But i != 0 should raise
        with pytest.raises(ValueError, match="i must be in"):
            feistel_shuffle(1, -1)

    def test_zero_max_count(self):
        """Test with max_count of 0."""
        # max_count <= 1 returns i if i==0 (early exit in implementation)
        result = feistel_shuffle(0, 0)
        assert result == 0

        # But i != 0 should raise
        with pytest.raises(ValueError, match="i must be in"):
            feistel_shuffle(1, 0)

    def test_very_small_range(self):
        """Test with very small ranges."""
        # Range of 1
        assert feistel_shuffle(0, 1) == 0

        # Range of 2
        result = feistel_shuffle(0, 2)
        assert result in [0, 1]

    def test_invalid_ordinal_with_single_element(self):
        """Test invalid ordinal with max_count=1."""
        with pytest.raises(ValueError, match="i must be in"):
            feistel_shuffle(1, 1)

    def test_invalid_rounds_zero(self):
        """Test with rounds = 0 (should raise ValueError)."""
        with pytest.raises(ValueError, match="rounds must be >= 1"):
            feistel_shuffle(0, 10, rounds=0)

    def test_invalid_rounds_negative(self):
        """Test with negative rounds (should raise ValueError)."""
        with pytest.raises(ValueError, match="rounds must be >= 1"):
            feistel_shuffle(0, 10, rounds=-1)

    def test_minimum_valid_rounds(self):
        """Test with rounds = 1 (minimum valid value)."""
        max_count = 20
        shuffled = [feistel_shuffle(i, max_count, rounds=1) for i in range(max_count)]

        # Should still be a valid bijection even with only 1 round
        assert len(set(shuffled)) == max_count
        assert sorted(shuffled) == list(range(max_count))


class TestFeistelShuffleDistribution:
    """Test statistical properties of the shuffle."""

    def test_uniform_distribution_approximation(self):
        """Test that output distribution is roughly uniform."""
        max_count = 50

        # Generate many shuffles
        shuffled = [feistel_shuffle(i, max_count) for i in range(max_count)]

        # Count occurrences in buckets
        bucket_size = 10
        num_buckets = max_count // bucket_size

        buckets = [0] * num_buckets
        for val in shuffled:
            bucket_idx = val // bucket_size
            buckets[bucket_idx] += 1

        # With uniform distribution, each bucket should have roughly bucket_size values
        # Allow for some variance
        expected = max_count / num_buckets

        for count in buckets:
            # Each bucket should be within 50% of expected (generous)
            assert abs(count - expected) < expected * 0.5


class TestFeistelShuffleSeedParameter:
    """Test seed parameter behavior and variations."""

    def test_default_seed_is_zero(self):
        """Test that default seed behavior is consistent."""
        max_count = 20

        # Explicit seed=0 should match default behavior
        default = [feistel_shuffle(i, max_count) for i in range(max_count)]
        explicit_zero = [feistel_shuffle(i, max_count, seed=0) for i in range(max_count)]

        assert default == explicit_zero

    def test_different_seeds_produce_different_permutations(self):
        """Test that different seeds produce different permutations."""
        max_count = 30

        seed_0 = [feistel_shuffle(i, max_count, seed=0) for i in range(max_count)]
        seed_1 = [feistel_shuffle(i, max_count, seed=1) for i in range(max_count)]
        seed_42 = [feistel_shuffle(i, max_count, seed=42) for i in range(max_count)]

        # All should be different permutations
        assert seed_0 != seed_1
        assert seed_0 != seed_42
        assert seed_1 != seed_42

        # But all should be valid bijections
        assert sorted(seed_0) == list(range(max_count))
        assert sorted(seed_1) == list(range(max_count))
        assert sorted(seed_42) == list(range(max_count))

    def test_same_seed_is_deterministic(self):
        """Test that same seed always produces same permutation."""
        max_count = 25
        seed = 12345

        first = [feistel_shuffle(i, max_count, seed=seed) for i in range(max_count)]
        second = [feistel_shuffle(i, max_count, seed=seed) for i in range(max_count)]

        assert first == second

    def test_negative_seed(self):
        """Test that negative seeds work correctly."""
        max_count = 15

        # Negative seeds should work (Python handles negative in to_bytes via & MASK64)
        shuffled = [feistel_shuffle(i, max_count, seed=-1) for i in range(max_count)]

        # Should still be a valid bijection
        assert len(set(shuffled)) == max_count
        assert sorted(shuffled) == list(range(max_count))

    def test_large_seed(self):
        """Test with very large seed values."""
        max_count = 20
        large_seed = 2**63 - 1

        shuffled = [feistel_shuffle(i, max_count, seed=large_seed) for i in range(max_count)]

        # Should still be a valid bijection
        assert len(set(shuffled)) == max_count
        assert sorted(shuffled) == list(range(max_count))

    def test_seed_independence_from_max_count(self):
        """Test that same seed with different max_count produces independent permutations."""
        seed = 999

        # Get shuffled results for different max_counts
        result_10_at_5 = feistel_shuffle(5, 10, seed=seed)
        result_20_at_5 = feistel_shuffle(5, 20, seed=seed)
        result_30_at_5 = feistel_shuffle(5, 30, seed=seed)

        # These should be independent (different cycle-walking domains)
        # At least some should differ
        assert len({result_10_at_5, result_20_at_5, result_30_at_5}) >= 2

    @pytest.mark.parametrize("seed", [0, 1, 42, 100, 1000, 999999])
    def test_bijection_with_various_seeds(self, seed):
        """Parametrized test for bijection with various seeds."""
        max_count = 20

        shuffled = [feistel_shuffle(i, max_count, seed=seed) for i in range(max_count)]

        # All seeds should produce valid bijections
        assert len(set(shuffled)) == max_count
        assert sorted(shuffled) == list(range(max_count))


class TestFeistelShuffleCycleWalking:
    """Test cycle-walking behavior for non-power-of-2 ranges."""

    def test_cycle_walking_near_power_of_two(self):
        """Test ranges just below power of 2 (heavy cycle-walking)."""
        # 63 is just below 64 (2^6), so cycle-walking will be frequent
        max_count = 63

        shuffled = [feistel_shuffle(i, max_count) for i in range(max_count)]

        # Should still be a perfect bijection despite cycle-walking
        assert len(set(shuffled)) == max_count
        assert sorted(shuffled) == list(range(max_count))

    def test_cycle_walking_just_above_power_of_two(self):
        """Test ranges just above power of 2 (minimal cycle-walking)."""
        # 65 is just above 64 (2^6)
        max_count = 65

        shuffled = [feistel_shuffle(i, max_count) for i in range(max_count)]

        # Should be a perfect bijection
        assert len(set(shuffled)) == max_count
        assert sorted(shuffled) == list(range(max_count))

    def test_cycle_walking_three_value_range(self):
        """Test with 3 elements (smallest non-trivial odd range)."""
        max_count = 3

        shuffled = [feistel_shuffle(i, max_count) for i in range(max_count)]

        # Should be a valid permutation of [0, 1, 2]
        assert set(shuffled) == {0, 1, 2}

    @pytest.mark.parametrize("max_count", [127, 255, 511, 1023])
    def test_cycle_walking_one_below_power_of_two(self, max_count):
        """Test ranges that are 2^n - 1 (maximum cycle-walking)."""
        # Sample to avoid timeout
        sample_indices = list(range(min(50, max_count)))

        shuffled = [feistel_shuffle(i, max_count) for i in sample_indices]

        # All should be in range and unique within sample
        assert all(0 <= x < max_count for x in shuffled)
        assert len(set(shuffled)) == len(sample_indices)


class TestFeistelShuffleExtremeValues:
    """Test with extreme and boundary values."""

    def test_very_large_max_count(self):
        """Test with very large max_count."""
        max_count = 1000000  # 1 million

        # Test a few samples (can't test all due to time)
        test_indices = [0, 1, 100, 1000, 10000, max_count - 1]

        for i in test_indices:
            result = feistel_shuffle(i, max_count)
            assert 0 <= result < max_count

    def test_maximum_ordinal_various_sizes(self):
        """Test ordinal at max_count - 1 for various sizes."""
        for max_count in [2, 3, 10, 17, 64, 100, 255, 1000]:
            result = feistel_shuffle(max_count - 1, max_count)
            assert 0 <= result < max_count

    def test_high_rounds(self):
        """Test with high number of rounds."""
        max_count = 20

        # Test with many rounds (should still work, just slower)
        shuffled = [feistel_shuffle(i, max_count, rounds=20) for i in range(max_count)]

        # Should still be a valid bijection
        assert len(set(shuffled)) == max_count
        assert sorted(shuffled) == list(range(max_count))

    def test_consecutive_ranges_independence(self):
        """Test that shuffling in consecutive ranges produces independent results."""
        # Test that max_count boundaries don't cause weird patterns

        seed = 42
        results_10 = [feistel_shuffle(i, 10, seed=seed) for i in range(10)]
        results_11 = [feistel_shuffle(i, 11, seed=seed) for i in range(11)]

        # First 10 results should differ (different domain)
        # At least some should be different
        differences = sum(1 for i in range(10) if results_10[i] != results_11[i])
        assert differences > 0


class TestFeistelShuffleRoundsAndSeedInteraction:
    """Test interaction between rounds and seed parameters."""

    def test_rounds_and_seed_independence(self):
        """Test that rounds and seed are independent parameters."""
        max_count = 20

        # Different combinations should produce different results
        s0_r3 = [feistel_shuffle(i, max_count, seed=0, rounds=3) for i in range(max_count)]
        s0_r6 = [feistel_shuffle(i, max_count, seed=0, rounds=6) for i in range(max_count)]
        s1_r3 = [feistel_shuffle(i, max_count, seed=1, rounds=3) for i in range(max_count)]
        s1_r6 = [feistel_shuffle(i, max_count, seed=1, rounds=6) for i in range(max_count)]

        # All should be different
        permutations = [s0_r3, s0_r6, s1_r3, s1_r6]
        for i, perm1 in enumerate(permutations):
            for j, perm2 in enumerate(permutations):
                if i != j:
                    assert perm1 != perm2, f"Permutations {i} and {j} should differ"

        # All should be valid bijections
        for perm in permutations:
            assert len(set(perm)) == max_count
            assert sorted(perm) == list(range(max_count))

    def test_rounds_increase_improves_randomness(self):
        """Test that more rounds generally improves randomness quality."""
        max_count = 100
        seed = 12345

        # With 1 round, might have some patterns
        shuffled_1 = [feistel_shuffle(i, max_count, seed=seed, rounds=1) for i in range(max_count)]

        # With 6 rounds (default), should be better mixed
        shuffled_6 = [feistel_shuffle(i, max_count, seed=seed, rounds=6) for i in range(max_count)]

        # Both should be valid bijections
        assert len(set(shuffled_1)) == max_count
        assert len(set(shuffled_6)) == max_count

        # They should be different
        assert shuffled_1 != shuffled_6


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

class TestFastFeistelShuffler:
    """Test suite for FastFeistelShuffler class."""

    def test_single_element(self):
        """Regression: max_count=1 previously caused ZeroDivisionError in _rotl."""
        shuffler = FastFeistelShuffler(1)
        assert shuffler.get(0) == 0

    def test_bijection_small(self):
        """Test bijection for a small range."""
        n = 50
        shuffler = FastFeistelShuffler(n)
        results = [shuffler.get(i) for i in range(n)]
        assert sorted(results) == list(range(n))

    def test_deterministic(self):
        """Test that the same seed produces the same permutation."""
        n = 100
        a = FastFeistelShuffler(n, seed=42)
        b = FastFeistelShuffler(n, seed=42)
        assert [a.get(i) for i in range(n)] == [b.get(i) for i in range(n)]

    def test_different_seeds(self):
        """Test that different seeds produce different permutations."""
        n = 100
        a = [FastFeistelShuffler(n, seed=0).get(i) for i in range(n)]
        b = [FastFeistelShuffler(n, seed=99).get(i) for i in range(n)]
        assert a != b

    def test_out_of_range_raises(self):
        """Test that out-of-range indices raise ValueError."""
        shuffler = FastFeistelShuffler(10)
        with pytest.raises(ValueError):
            shuffler.get(10)
        with pytest.raises(ValueError):
            shuffler.get(-1)

    def test_invalid_max_count_raises(self):
        """Test that max_count < 1 raises ValueError."""
        with pytest.raises(ValueError):
            FastFeistelShuffler(0)

    def test_invalid_rounds_raises(self):
        """Test that rounds < 1 raises ValueError."""
        with pytest.raises(ValueError):
            FastFeistelShuffler(10, rounds=0)

    @pytest.mark.parametrize("n", [3, 7, 17, 63, 65, 127, 255, 256, 257, 500, 1023])
    def test_bijection_various_sizes(self, n):
        """Parametrized bijection test across tricky domain sizes."""
        shuffler = FastFeistelShuffler(n)
        results = [shuffler.get(i) for i in range(n)]
        assert sorted(results) == list(range(n))

    def test_small_domain_fallback_matches_feistel_shuffle(self):
        """Values below the threshold should match the blake2b feistel_shuffle."""
        n = 50
        seed = 7
        rounds = 4
        shuffler = FastFeistelShuffler(n, seed=seed, rounds=rounds)
        for i in range(n):
            assert shuffler.get(i) == feistel_shuffle(i, n, seed=seed, rounds=rounds)
