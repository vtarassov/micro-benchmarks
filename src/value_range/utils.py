"""
Utility functions for value range operations.

This module provides helper functions for deterministic data generation,
including the Feistel shuffle algorithm for pseudorandom permutations
and the FastFeistelShuffler class for high-throughput use cases.
"""

import hashlib
from functools import lru_cache

MASK64 = (1 << 64) - 1


def _u64_le(x: int) -> bytes:
    return (x & MASK64).to_bytes(8, "little")


def _next_pow2(n: int) -> int:
    return 1 << (n - 1).bit_length()


@lru_cache(maxsize=256)
def _derive_round_keys(seed: int, rounds: int) -> tuple[int, ...]:
    """
    Cached round key derivation.
    Returns a tuple for faster iteration and cacheability.
    """
    seed_key = _u64_le(seed)
    out: list[int] = []
    person = b"feistelrk"

    # Local bindings
    blake2b = hashlib.blake2b
    u64 = _u64_le
    from_bytes = int.from_bytes

    for r in range(rounds):
        h = blake2b(digest_size=8, key=seed_key, person=person)
        h.update(u64(r))
        out.append(from_bytes(h.digest(), "little") & MASK64)

    return tuple(out)


@lru_cache(maxsize=256)
def _make_round_hasher_bases(seed: int, rounds: int):
    """
    Build base hashers keyed by each round key (for the round function),
    so per-round computation becomes: base.copy(); update(msg); digest().
    """
    keys = _derive_round_keys(seed, rounds)
    person = b"feistelF"
    blake2b = hashlib.blake2b
    u64 = _u64_le

    bases = []
    for rk in keys:
        bases.append(blake2b(digest_size=8, key=u64(rk), person=person))
    return tuple(bases)


def feistel_shuffle(i: int, max_count: int, seed: int = 0, rounds: int = 6) -> int:
    """
    Deterministic permutation of [0, max_count) using Feistel + cycle-walking.

    - O(1) memory
    - Bijective on [0, max_count)
    - Uses blake2b for both round keys and the round function
    """
    if max_count <= 1:
        if i != 0:
            raise ValueError("i must be in [0, max_count)")
        return i

    if not (0 <= i < max_count):
        raise ValueError("i must be in [0, max_count)")

    if rounds < 1:
        raise ValueError("rounds must be >= 1")

    # Power-of-two Feistel domain
    M = _next_pow2(max_count)
    k = M.bit_length() - 1
    if k & 1:
        M <<= 1
        k += 1

    half_bits = k >> 1
    half_mask = (1 << half_bits) - 1
    left_shift = half_bits

    # Prebuilt keyed bases for the round function
    round_bases = _make_round_hasher_bases(seed, rounds)

    # Local bindings (avoid global lookups in inner loops)
    u64 = _u64_le
    from_bytes = int.from_bytes
    hm = half_mask
    ls = left_shift

    x = i
    while True:
        L = (x >> ls) & hm
        R = x & hm

        # Feistel rounds
        for base in round_bases:
            # F(R): hash of 64-bit R with per-round keyed base
            h = base.copy()
            h.update(u64(R))
            f = from_bytes(h.digest(), "little") & hm
            L, R = R, (L ^ f) & hm

        x = (L << ls) | R
        if x < max_count:
            return x


class FastFeistelShuffler:
    """
    Stable, fast, bijective shuffle of range [0, max_count).

    Designed for high-performance non-cryptographic use cases
    such as btree testing.

    - O(1) memory
    - Deterministic per seed
    - Even coverage (true permutation)
    """

    _SMALL_DOMAIN_THRESHOLD = 256

    def __init__(self, max_count: int, seed: int = 0, rounds: int = 4):
        if max_count < 1:
            raise ValueError("max_count must be >= 1")
        if rounds < 1:
            raise ValueError("rounds must be >= 1")

        self.max_count = max_count
        self.seed = seed
        self.rounds = rounds

        # For small domains the ARX round function degenerates,
        # so we fall back to the blake2b-based feistel_shuffle.
        self._use_fallback = max_count < self._SMALL_DOMAIN_THRESHOLD

        # Only build the fast-path state when not using the fallback
        if not self._use_fallback:
            # Build power-of-two Feistel domain
            M = 1 << (max_count - 1).bit_length()
            k = M.bit_length() - 1
            if k & 1:
                M <<= 1
                k += 1

            self._half_bits = k // 2
            self._half_mask = (1 << self._half_bits) - 1
            self._left_shift = self._half_bits

            # Precompute round keys
            self._round_keys = [
                self._splitmix64(seed ^ r) & self._half_mask
                for r in range(rounds)
            ]

    _MAX_CYCLE_WALK = 1000

    def get(self, i: int) -> int:
        """Return the shuffled value of i."""
        if not (0 <= i < self.max_count):
            raise ValueError("i must be in [0, max_count)")

        if self.max_count == 1:
            return 0

        if self._use_fallback:
            return feistel_shuffle(i, self.max_count, seed=self.seed, rounds=self.rounds)

        x = i
        for _ in range(self._MAX_CYCLE_WALK):
            L = (x >> self._left_shift) & self._half_mask
            R = x & self._half_mask

            for key in self._round_keys:
                L, R = R, (L ^ self._F(R, key)) & self._half_mask

            x = (L << self._left_shift) | R
            if x < self.max_count:
                return x

        raise RuntimeError(
            f"cycle-walking did not converge after {self._MAX_CYCLE_WALK} iterations"
        )

    @staticmethod
    def _splitmix64(x: int) -> int:
        x = (x + 0x9E3779B97F4A7C15) & MASK64
        x ^= x >> 30
        x = (x * 0xBF58476D1CE4E5B9) & MASK64
        x ^= x >> 27
        x = (x * 0x94D049BB133111EB) & MASK64
        x ^= x >> 31
        return x

    def _rotl(self, x: int, r: int) -> int:
        bits = self._half_bits
        if bits == 0:
            return 0
        r %= bits
        return ((x << r) | (x >> (bits - r))) & self._half_mask

    def _F(self, r: int, key: int) -> int:
        """
        Fast ARX-style round function.
        Maps half-domain -> half-domain.
        """
        x = (r + key) & self._half_mask
        x ^= self._rotl(x, 7)
        x = (x * ((0x9E3779B1 | 1) & self._half_mask)) & self._half_mask
        x ^= x >> (self._half_bits // 3 if self._half_bits >= 3 else 1)
        return x & self._half_mask
