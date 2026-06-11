cmake -S . -B bld -G "NMake Makefiles" -DCMAKE_PREFIX_PATH="%CONDA_PREFIX%" -DWITH_ZLIB=ON -DWITH_BZIP2=ON -DWITH_XZ=ON -DWITH_BLOSC=ON -DWITHIN_TRAVIS=ON -DBUILD_TESTS=OFF -DBUILD_Z5PY=ON -DWITH_LZ4=ON -DWITH_ZSTD=ON -DCMAKE_INSTALL_PREFIX="%CONDA_PREFIX%"
if errorlevel 1 exit /b 1
cmake --build bld --target INSTALL --config Release
if errorlevel 1 exit /b 1
