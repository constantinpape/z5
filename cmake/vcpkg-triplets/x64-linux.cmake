# Overlay of vcpkg's stock x64-linux triplet (registered via the
# "overlay-triplets" entry in vcpkg.json), identical to upstream except for the
# snappy override below.
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Linux)

# snappy 1.2.2 uses AVX intrinsics in MemCopy64 when __AVX__ is defined, but
# only includes <immintrin.h> when its BMI2 / CRC32 checks pass. With the
# compiler on the ubuntu-26.04 runner image this combination fails with
# "'__m256i' was not declared in this scope". Build snappy without AVX (which
# also disables AVX2) so it falls back to its portable memmove path; this also
# keeps the static snappy linked into the wheels free of AVX instructions.
if(PORT STREQUAL "snappy")
    set(VCPKG_C_FLAGS "-mno-avx")
    set(VCPKG_CXX_FLAGS "-mno-avx")
endif()
