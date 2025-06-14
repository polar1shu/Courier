set(CMAKE_CXX_STANDARD 20)

message(STATUS "CPP Compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "CPP Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")

if(CMAKE_BUILD_TYPE AND (CMAKE_BUILD_TYPE STREQUAL  "Debug"))

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_DEBUG} -Wall -O0 -g -fsanitize=address -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mfma -msse4.2 -mrtm -mcx16 -mbmi2 -mlzcnt -mclwb")

    message("Compile Flags[Debug]: ${CMAKE_CXX_FLAGS}")

elseif(CMAKE_BUILD_TYPE AND (CMAKE_BUILD_TYPE STREQUAL "Release"))

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_RELEASE} -Wall -O3 -flto")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mfma -msse4.2 -mno-avx512f -mrtm -mcx16 -mbmi2 -mlzcnt -mclwb")

    message("Compile Flags[Release]: ${CMAKE_CXX_FLAGS}")

elseif(CMAKE_BUILD_TYPE AND (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo"))

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_RELEASE} -Wall -O3 -g")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mfma -msse4.2 -mrtm -mcx16 -mbmi2 -mlzcnt -mclwb")

    message("Compile Flags[Release]: ${CMAKE_CXX_FLAGS}")

else()
    message(STATUS "Build as Debug by default")

    set(CMAKE_BUILD_TYPE "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_DEBUG} -Wall -O0 -g")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mfma -msse4.2 -mrtm -mcx16 -mbmi2 -mlzcnt -mclwb")

    message("Compile Flags[Default]: ${CMAKE_CXX_FLAGS}")
endif()