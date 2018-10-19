rem apply xtensor patches
git apply conda-recipe/patches/xtensor-copysign.patch --verbose --unsafe-paths
git apply conda-recipe/patches/xtensor-sizet.patch --verbose --unsafe-paths

mkdir build
cd build

set CONFIGURATION=Release

cmake .. -G "%CMAKE_GENERATOR%" -DCMAKE_PREFIX_PATH=%PREFIX% ^
    -DCMAKE_INSTALL_PREFIX="%LIBRARY_PREFIX" ^
    -DBOOST_ROOT=%LIBRARY% ^
    -DWITH_BLOSC=ON ^
    -DWITH_ZLIB=ON ^
    -DWITH_BZIP2=OFF ^
    -DWITH_XZ=OFF ^
    -DPYTHON_EXECUTABLE=%PYTHON%


cmake --build . --config %CONFIGURATION%
REM xcopy ${SRC_DIR}/build/python/z5py %LIBRARY%/lib/python${PY_VER}/site-packages/ /E
