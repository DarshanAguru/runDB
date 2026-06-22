import ctypes
import weakref
from .Malloc_internal import MallocInternal

class GeoHashRange(ctypes.Structure):
    _fields_ = [
        ("min", ctypes.c_double),
        ("max", ctypes.c_double)
    ]

class GeoHashBits(ctypes.Structure):
    _fields_ = [
        ("bits", ctypes.c_uint64),
        ("step", ctypes.c_uint8)
    ]

class GeoHashArea(ctypes.Structure):
    _fields_ = [
        ("hash", GeoHashBits),
        ("longitude", GeoHashRange),
        ("latitude", GeoHashRange)
    ]

class GeoHashNeighbors(ctypes.Structure):
    _fields_ = [
        ("north", GeoHashBits),
        ("east", GeoHashBits),
        ("west", GeoHashBits),
        ("south", GeoHashBits),
        ("north_east", GeoHashBits),
        ("south_east", GeoHashBits),
        ("north_west", GeoHashBits),
        ("south_west", GeoHashBits)
    ]

class GeoHashStruct(ctypes.Structure):
    _fields_ = [
        ("lat", ctypes.c_double),
        ("lon", ctypes.c_double),
        ("hash", ctypes.c_char * 12),
    ]

class GeoHashHelper:
    MAX_STEP = 26
    LAT_MAX = 85.05112878
    LAT_MIN = -85.05112878
    LON_MAX = 180.0
    LON_MIN = -180.0

    NORTH = 0
    EAST = 1
    WEST = 2
    SOUTH = 3
    SOUTH_WEST = 4
    SOUTH_EAST = 5
    NORTH_WEST = 6
    NORTH_EAST = 7

    @staticmethod
    def _unwrap(x):
        if hasattr(x, '_obj'):
            return x._obj
        if hasattr(x, 'contents'):
            return x.contents
        return x

    @staticmethod
    def _range_p_is_zero(r) -> bool:
        if r is None:
            return True
        r = GeoHashHelper._unwrap(r)
        return r.min == 0.0 and r.max == 0.0

    @staticmethod
    def _range_is_zero(r) -> bool:
        if r is None:
            return True
        r = GeoHashHelper._unwrap(r)
        return r.min == 0.0 and r.max == 0.0

    @staticmethod
    def _hash_is_zero(h) -> bool:
        if h is None:
            return True
        h = GeoHashHelper._unwrap(h)
        return h.bits == 0 and h.step == 0

    @staticmethod
    def interleave64(xlo: int, ylo: int) -> int:
        B = [0x5555555555555555, 0x3333333333333333,
             0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF,
             0x0000FFFF0000FFFF]
        S = [1, 2, 4, 8, 16]

        x = xlo & 0xFFFFFFFF
        y = ylo & 0xFFFFFFFF

        x = (x | (x << S[4])) & B[4]
        y = (y | (y << S[4])) & B[4]

        x = (x | (x << S[3])) & B[3]
        y = (y | (y << S[3])) & B[3]

        x = (x | (x << S[2])) & B[2]
        y = (y | (y << S[2])) & B[2]

        x = (x | (x << S[1])) & B[1]
        y = (y | (y << S[1])) & B[1]

        x = (x | (x << S[0])) & B[0]
        y = (y | (y << S[0])) & B[0]

        return (x | (y << 1)) & 0xFFFFFFFFFFFFFFFF

    @staticmethod
    def deinterleave64(interleaved: int) -> int:
        B = [0x5555555555555555, 0x3333333333333333,
             0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF,
             0x0000FFFF0000FFFF, 0x00000000FFFFFFFF]
        S = [0, 1, 2, 4, 8, 16]

        x = interleaved & 0xFFFFFFFFFFFFFFFF
        y = (interleaved >> 1) & 0xFFFFFFFFFFFFFFFF

        x = (x | (x >> S[0])) & B[0]
        y = (y | (y >> S[0])) & B[0]

        x = (x | (x >> S[1])) & B[1]
        y = (y | (y >> S[1])) & B[1]

        x = (x | (x >> S[2])) & B[2]
        y = (y | (y >> S[2])) & B[2]

        x = (x | (x >> S[3])) & B[3]
        y = (y | (y >> S[3])) & B[3]

        x = (x | (x >> S[4])) & B[4]
        y = (y | (y >> S[4])) & B[4]

        x = (x | (x >> S[5])) & B[5]
        y = (y | (y >> S[5])) & B[5]

        return (x | (y << 32)) & 0xFFFFFFFFFFFFFFFF

    @staticmethod
    def geohashGetCoordRange(long_range, lat_range) -> None:
        long_range = GeoHashHelper._unwrap(long_range)
        lat_range = GeoHashHelper._unwrap(lat_range)
        long_range.max = GeoHashHelper.LON_MAX
        long_range.min = GeoHashHelper.LON_MIN
        lat_range.max = GeoHashHelper.LAT_MAX
        lat_range.min = GeoHashHelper.LAT_MIN

    @staticmethod
    def geohashEncode(long_range, lat_range, longitude: float, latitude: float, step: int, hash_obj) -> int:
        if hash_obj is None or step > 32 or step == 0:
            return 0
        if GeoHashHelper._range_p_is_zero(lat_range) or GeoHashHelper._range_p_is_zero(long_range):
            return 0
        if longitude > GeoHashHelper.LON_MAX or longitude < GeoHashHelper.LON_MIN or \
           latitude > GeoHashHelper.LAT_MAX or latitude < GeoHashHelper.LAT_MIN:
            return 0

        hash_obj = GeoHashHelper._unwrap(hash_obj)
        long_range = GeoHashHelper._unwrap(long_range)
        lat_range = GeoHashHelper._unwrap(lat_range)

        hash_obj.bits = 0
        hash_obj.step = step

        if latitude < lat_range.min or latitude > lat_range.max or \
           longitude < long_range.min or longitude > long_range.max:
            return 0

        lat_offset = (latitude - lat_range.min) / (lat_range.max - lat_range.min)
        long_offset = (longitude - long_range.min) / (long_range.max - long_range.min)

        lat_offset *= (1 << step)
        long_offset *= (1 << step)

        xlo = int(long_offset) & 0xFFFFFFFF
        ylo = int(lat_offset) & 0xFFFFFFFF

        hash_obj.bits = GeoHashHelper.interleave64(xlo, ylo)
        return 1

    @staticmethod
    def geohashEncodeType(longitude: float, latitude: float, step: int, hash_obj) -> int:
        r_long = GeoHashRange()
        r_lat = GeoHashRange()
        GeoHashHelper.geohashGetCoordRange(ctypes.byref(r_long), ctypes.byref(r_lat))
        return GeoHashHelper.geohashEncode(ctypes.byref(r_long), ctypes.byref(r_lat), longitude, latitude, step, hash_obj)

    @staticmethod
    def geohashEncodeWGS84(longitude: float, latitude: float, step: int, hash_obj) -> int:
        return GeoHashHelper.geohashEncodeType(longitude, latitude, step, hash_obj)

    @staticmethod
    def geohashDecode(long_range, lat_range, hash_obj, area) -> int:
        long_range = GeoHashHelper._unwrap(long_range)
        lat_range = GeoHashHelper._unwrap(lat_range)
        hash_obj = GeoHashHelper._unwrap(hash_obj)
        area = GeoHashHelper._unwrap(area)

        if GeoHashHelper._hash_is_zero(hash_obj) or area is None or \
           GeoHashHelper._range_is_zero(lat_range) or GeoHashHelper._range_is_zero(long_range):
            return 0

        area.hash.bits = hash_obj.bits
        area.hash.step = hash_obj.step

        step = hash_obj.step
        hash_sep = GeoHashHelper.deinterleave64(hash_obj.bits)

        lat_scale = lat_range.max - lat_range.min
        long_scale = long_range.max - long_range.min

        ilono = hash_sep & 0xFFFFFFFF
        ilato = (hash_sep >> 32) & 0xFFFFFFFF

        divisor = 1 << step

        area.latitude.min = lat_range.min + (ilato / divisor) * lat_scale
        area.latitude.max = lat_range.min + ((ilato + 1) / divisor) * lat_scale
        area.longitude.min = long_range.min + (ilono / divisor) * long_scale
        area.longitude.max = long_range.min + ((ilono + 1) / divisor) * long_scale

        return 1

    @staticmethod
    def geohashDecodeType(hash_obj, area) -> int:
        r_long = GeoHashRange()
        r_lat = GeoHashRange()
        GeoHashHelper.geohashGetCoordRange(ctypes.byref(r_long), ctypes.byref(r_lat))
        return GeoHashHelper.geohashDecode(r_long, r_lat, hash_obj, area)

    @staticmethod
    def geohashDecodeWGS84(hash_obj, area) -> int:
        return GeoHashHelper.geohashDecodeType(hash_obj, area)

    @staticmethod
    def geohashDecodeAreaToLongLat(area, xy) -> int:
        area = GeoHashHelper._unwrap(area)
        if not xy:
            return 0

        mid_lon = (area.longitude.min + area.longitude.max) / 2.0
        if mid_lon > GeoHashHelper.LON_MAX:
            mid_lon = GeoHashHelper.LON_MAX
        if mid_lon < GeoHashHelper.LON_MIN:
            mid_lon = GeoHashHelper.LON_MIN

        mid_lat = (area.latitude.min + area.latitude.max) / 2.0
        if mid_lat > GeoHashHelper.LAT_MAX:
            mid_lat = GeoHashHelper.LAT_MAX
        if mid_lat < GeoHashHelper.LAT_MIN:
            mid_lat = GeoHashHelper.LAT_MIN

        xy[0] = mid_lon
        xy[1] = mid_lat
        return 1

    @staticmethod
    def geohashDecodeToLongLatType(hash_obj, xy) -> int:
        area = GeoHashArea()
        if not xy or not GeoHashHelper.geohashDecodeType(hash_obj, ctypes.byref(area)):
            return 0
        return GeoHashHelper.geohashDecodeAreaToLongLat(ctypes.byref(area), xy)

    @staticmethod
    def geohashDecodeToLongLatWGS84(hash_obj, xy) -> int:
        return GeoHashHelper.geohashDecodeToLongLatType(hash_obj, xy)

    @staticmethod
    def _geohash_move_x(hash_obj, d: int) -> None:
        hash_obj = GeoHashHelper._unwrap(hash_obj)
        if d == 0:
            return

        x = hash_obj.bits & 0xAAAAAAAAAAAAAAAA
        y = hash_obj.bits & 0x5555555555555555

        zz = 0xAAAAAAAAAAAAAAAA >> (64 - hash_obj.step * 2)

        if d > 0:
            y = (y + (zz + 1)) & 0xFFFFFFFFFFFFFFFF
        else:
            y = y | zz
            y = (y - (zz + 1)) & 0xFFFFFFFFFFFFFFFF

        y &= (0x5555555555555555 >> (64 - hash_obj.step * 2))
        hash_obj.bits = x | y

    @staticmethod
    def _geohash_move_y(hash_obj, d: int) -> None:
        hash_obj = GeoHashHelper._unwrap(hash_obj)
        if d == 0:
            return

        x = hash_obj.bits & 0xAAAAAAAAAAAAAAAA
        y = hash_obj.bits & 0x5555555555555555

        zz = 0x5555555555555555 >> (64 - hash_obj.step * 2)

        if d > 0:
            x = (x + (zz + 1)) & 0xFFFFFFFFFFFFFFFF
        else:
            x = x | zz
            x = (x - (zz + 1)) & 0xFFFFFFFFFFFFFFFF

        x &= (0xAAAAAAAAAAAAAAAA >> (64 - hash_obj.step * 2))
        hash_obj.bits = x | y

    @staticmethod
    def geohashNeighbors(hash_obj, neighbors) -> None:
        hash_obj = GeoHashHelper._unwrap(hash_obj)
        neighbors = GeoHashHelper._unwrap(neighbors)

        for field_name in ["east", "west", "north", "south", "south_east", "south_west", "north_east", "north_west"]:
            field = getattr(neighbors, field_name)
            field.bits = hash_obj.bits
            field.step = hash_obj.step

        GeoHashHelper._geohash_move_x(ctypes.byref(neighbors.east), 1)
        GeoHashHelper._geohash_move_y(ctypes.byref(neighbors.east), 0)

        GeoHashHelper._geohash_move_x(ctypes.byref(neighbors.west), -1)
        GeoHashHelper._geohash_move_y(ctypes.byref(neighbors.west), 0)

        GeoHashHelper._geohash_move_x(ctypes.byref(neighbors.south), 0)
        GeoHashHelper._geohash_move_y(ctypes.byref(neighbors.south), -1)

        GeoHashHelper._geohash_move_x(ctypes.byref(neighbors.north), 0)
        GeoHashHelper._geohash_move_y(ctypes.byref(neighbors.north), 1)

        GeoHashHelper._geohash_move_x(ctypes.byref(neighbors.north_west), -1)
        GeoHashHelper._geohash_move_y(ctypes.byref(neighbors.north_west), 1)

        GeoHashHelper._geohash_move_x(ctypes.byref(neighbors.north_east), 1)
        GeoHashHelper._geohash_move_y(ctypes.byref(neighbors.north_east), 1)

        GeoHashHelper._geohash_move_x(ctypes.byref(neighbors.south_east), 1)
        GeoHashHelper._geohash_move_y(ctypes.byref(neighbors.south_east), -1)

        GeoHashHelper._geohash_move_x(ctypes.byref(neighbors.south_west), -1)
        GeoHashHelper._geohash_move_y(ctypes.byref(neighbors.south_west), -1)

    @staticmethod
    def geohashGetDistance(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
        import math
        EARTH_RADIUS = 6372797.560856
        
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        dlon = lon2_rad - lon1_rad
        dlat = lat2_rad - lat1_rad
        
        a = math.sin(dlat / 2.0) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2.0) ** 2
        c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
        return EARTH_RADIUS * c

    @staticmethod
    def toBase32Geohash(lon: float, lat: float) -> str:
        step = 26
        long_offset = (lon - (-180.0)) / (180.0 - (-180.0)) * (1 << step)
        lat_offset = (lat - (-90.0)) / (90.0 - (-90.0)) * (1 << step)
        
        xlo = int(lat_offset) & 0xFFFFFFFF
        ylo = int(long_offset) & 0xFFFFFFFF
        bits = GeoHashHelper.interleave64(xlo, ylo)
        
        geoalphabet = "0123456789bcdefghjkmnpqrstuvwxyz"
        res = []
        for i in range(11):
            if i == 10:
                idx = 0
            else:
                idx = (bits >> (52 - (i + 1) * 5)) & 0x1f
            res.append(geoalphabet[idx])
        return "".join(res)

class GeoHash:
    __slots__ = ("ptr", "has_ownership", "_finalizer", "__weakref__")

    def __init__(self, lat: float = 0.0, lon: float = 0.0, ptr = None):
        if ptr is not None:
            self.ptr = ptr
            self.has_ownership = False
        else:
            self.ptr = MallocInternal.zcalloc(ctypes.sizeof(GeoHashStruct))
            self.has_ownership = True
            struct_obj = ctypes.cast(self.ptr, ctypes.POINTER(GeoHashStruct)).contents
            struct_obj.lat = lat
            struct_obj.lon = lon
            struct_obj.hash = b""

        self._finalizer = weakref.finalize(
            self,
            self._cleanup,
            self.ptr
        )

    @staticmethod
    def _cleanup(ptr):
        if ptr:
            MallocInternal.zfree(ptr)

    def free(self):
        if hasattr(self, "_finalizer") and self._finalizer.alive:
            self._finalizer()

    def release(self) -> int:
        self.has_ownership = False
        if hasattr(self, "_finalizer") and self._finalizer.alive:
            self._finalizer.detach()
        return self.ptr

    @property
    def lat(self) -> float:
        return ctypes.cast(self.ptr, ctypes.POINTER(GeoHashStruct)).contents.lat

    @lat.setter
    def lat(self, val: float):
        ctypes.cast(self.ptr, ctypes.POINTER(GeoHashStruct)).contents.lat = val

    @property
    def lon(self) -> float:
        return ctypes.cast(self.ptr, ctypes.POINTER(GeoHashStruct)).contents.lon

    @lon.setter
    def lon(self, val: float):
        ctypes.cast(self.ptr, ctypes.POINTER(GeoHashStruct)).contents.lon = val