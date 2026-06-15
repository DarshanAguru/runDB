import ctypes
from .Malloc_internal import MallocInternal, MemTracker


# Malloc provides an abstraction layer over raw memory allocations for dynamic data types:
# - Ctypes Integration: Translates Python objects (ints, floats, strings, lists) to native C equivalents.
# - Contiguous Memory Layouts: Facilitates allocation of arrays, nested arrays, and structures on the C-heap.
# - Memory Lifecycles: Integrates with MallocInternal and weakref finalize tracking to prevent memory leaks.
class DATATYPES:

    # Returns the size and ctypes type for the specified integer bit width
    @staticmethod
    def INT(bits = 32):
        mapp = {
            8: ctypes.c_int8,
            16: ctypes.c_int16,
            32: ctypes.c_int32,
            64: ctypes.c_int64
        }
        if bits not in mapp:
            raise ValueError(f"Unsupported integer bit-width: {bits}. Supported sizes are 8, 16, 32, and 64.")
        ctype = mapp[bits]
        return (
            ctypes.sizeof(ctype),
            ctype
        )
    
    # Returns the size and ctypes type for a double precision float
    @staticmethod
    def DOUBLE():
        return (
            ctypes.sizeof(ctypes.c_double),
            ctypes.c_double
        )
        
    # Infers size and ctypes array type from Python list elements
    @staticmethod
    def ARRAY(data):

        if not data:
            raise ValueError("Array initialization data cannot be empty.")

        first = data[0]

        if isinstance(first, int):
            base_type = ctypes.c_int64
            length = len(data)
            array_type = base_type * length
            return (
                ctypes.sizeof(array_type),
                array_type
            )

        elif isinstance(first, float):
            base_type = ctypes.c_double
            length = len(data)
            array_type = base_type * length
            return (
                ctypes.sizeof(array_type),
                array_type
            )

        elif isinstance(first, bytes):
            base_type = ctypes.c_char
            length = len(data)
            array_type = base_type * length
            return (
                ctypes.sizeof(array_type),
                array_type
            )

        elif isinstance(first, list):
            inner_size, inner_type = (DATATYPES.ARRAY(first))
            length = len(data)
            array_type = inner_type * length
            return (
                ctypes.sizeof(array_type),
                array_type
            )

        raise TypeError("Unsupported array element type. Must be int, float, bytes, or a list of these.")
    
    # Returns the size and ctypes structure type if valid
    @staticmethod
    def STRUCT(struct):
        if not issubclass(struct, ctypes.Structure):
            raise TypeError("The provided argument must be a valid ctypes.Structure subclass.")
        
        return (
            ctypes.sizeof(struct),
            struct
        )

class Malloc:

    # Allocates native C memory and writes a null-terminated string
    @staticmethod
    def alloc_string(data):
        if isinstance(data, str):
            data = data.encode()
        if not isinstance(data, bytes):
            raise TypeError("String allocation input must be either a Python str or bytes instance.")
        size = len(data) + 1
        ptr = MallocInternal(size)
        ptr.write(data + b"\0")
        return ptr
    
    # Allocates native C memory and writes an integer value
    @staticmethod
    def alloc_int(data, length = 32):
        size, ctype = DATATYPES.INT(length)
        ptr = MallocInternal(size)
        ptr.as_type(ctype)[0] = data
        return ptr
    
    # Allocates native C memory and writes a double float value
    @staticmethod
    def alloc_double(data):
        size, ctype = DATATYPES.DOUBLE()
        ptr = MallocInternal(size)
        ptr.as_type(ctype)[0] = data
        return ptr
    
    # Recursively builds and populates a ctypes array from nested Python lists
    @staticmethod
    def _build_ctypes_array(data, ctype):
        arr = ctype()

        for i, item in enumerate(data):
            if isinstance(item, list):
                nested = Malloc._build_ctypes_array(
                    item,
                    ctype._type_
                )
                arr[i] = nested
            else:
                arr[i] = item

        return arr

    # Allocates C memory and copies a flat or nested Python list into a ctypes array
    @staticmethod
    def alloc_array(data):
        size, ctype = DATATYPES.ARRAY(data)
        ptr = MallocInternal(size)
        source = Malloc._build_ctypes_array(data, ctype)
        ctypes.memmove(ptr.ptr, ctypes.addressof(source), size)
        return ptr
    
    # Allocates C memory for a structure and populates its fields
    @staticmethod
    def alloc_struct(struct, **kwargs):
        size, ctype = DATATYPES.STRUCT(struct)
        ptr = MallocInternal(size)
        obj = ctype()

        for field, value in kwargs.items():
            setattr(
                obj,
                field,
                value
            )

        ctypes.memmove(
            ptr.ptr,
            ctypes.addressof(obj),
            size
        )
        return ptr
    
    
    # Allocates C memory for an array of ctypes structures and populates them
    @staticmethod
    def alloc_struct_arr(struct, items):
        if not issubclass(struct, ctypes.Structure):
            raise TypeError("The provided argument must be a valid ctypes.Structure subclass.")
        if not items:
            raise ValueError("Structure array initialization items list cannot be empty.")
        
        array_type = struct * len(items)
        size = ctypes.sizeof(array_type)
        ptr = MallocInternal(size)

        arr = ctypes.cast(
            ptr.ptr,
            ctypes.POINTER(array_type)
        ).contents

        for i, item in enumerate(items):
            if isinstance(item, struct):
                arr[i] = item
                continue
            obj = struct()
            for field, value in item.items():
                setattr(
                    obj,
                    field,
                    value
                )
            arr[i] = obj
        
        return ptr
