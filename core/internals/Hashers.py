class Hashers:
    # --- MurmurHash64A constants (Redis's exact HLL variant) ---
    _MH64A_M    = 0xc6a4a7935bd1e995
    _MH64A_R    = 47
    _MH64A_MASK = 0xFFFFFFFFFFFFFFFF
    _MH64A_SEED = 0                       # fixed seed (matches Redis default)

     # ---------- 64-bit FNV-1a ----------
    _FNV1A_OFFSET = 0xCBF29CE484222325
    _FNV1A_PRIME  = 0x100000001B3
    _FNV1A_MASK   = 0xFFFFFFFFFFFFFFFF
    _MURMUR_HASH_FINALISERS = (
     0xff51afd7ed558ccd,  
     0xc4ceb9fe1a85ec53  
    )

    # Fmix Algorithm -- FNV1A with MurMurHash3 finaliser
    @staticmethod
    def fnv1a(value: str | bytes) -> int:
        if isinstance(value, str):
            value = value.encode("utf-8")

        h = Hashers._FNV1A_OFFSET

        for b in value:
            h ^= b
            h = (h * Hashers._FNV1A_PRIME) & Hashers._FNV1A_MASK

        # MurmurHash3 Finaliser
        h ^= h >> 33
        h = (h * Hashers._MURMUR_HASH_FINALISERS[0]) & Hashers._FNV1A_MASK
        h ^= h >> 33
        h = (h * Hashers._MURMUR_HASH_FINALISERS[1]) & Hashers._FNV1A_MASK
        h ^= h >> 33
        
        return h

    # MurmurHash64A —  Redis like  implementation for HyperLogLog.
    # Processes 8-byte blocks (little-endian), handles a 1-7 byte tail with the
    # same fall-through logic as the original C switch, then applies a two-round
    # finalizer (h ^= h>>r; h *= m; h ^= h>>r) for uniform bit distribution.
    @staticmethod
    def murmur64a(value: bytes) -> int:
        m    = Hashers._MH64A_M
        r    = Hashers._MH64A_R
        MASK = Hashers._MH64A_MASK
        length = len(value)

        h = (Hashers._MH64A_SEED ^ ((length * m) & MASK)) & MASK

        # --- body: process 8-byte blocks (little-endian, mirrors C *((uint64_t*)data)) ---
        nblocks = length >> 3
        for i in range(nblocks):
            o = i << 3
            k = int.from_bytes(value[o : o + 8], 'little')
            k  = (k * m) & MASK
            k ^= k >> r
            k  = (k * m) & MASK
            h ^= k
            h  = (h * m) & MASK

        # --- tail: remaining 1-7 bytes (mirrors C fall-through switch) ---
        tail = value[nblocks << 3:]
        tlen = length & 7
        if tlen >= 7: h ^= tail[6] << 48
        if tlen >= 6: h ^= tail[5] << 40
        if tlen >= 5: h ^= tail[4] << 32
        if tlen >= 4: h ^= tail[3] << 24
        if tlen >= 3: h ^= tail[2] << 16
        if tlen >= 2: h ^= tail[1] <<  8
        if tlen >= 1:
            h ^= tail[0]
            h  = (h * m) & MASK

        # --- finalizer ---
        h ^= h >> r
        h  = (h * m) & MASK
        h ^= h >> r
        return h
