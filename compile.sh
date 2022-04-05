cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang++-13 -DCMAKE_C_COMPILER=clang-13 -DWITH_TRICACHE=OFF
cmake -S . -B build-cache -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang++-13 -DCMAKE_C_COMPILER=clang-13 -DWITH_TRICACHE=ON
cmake --build build -j
cmake --build build-cache -j
