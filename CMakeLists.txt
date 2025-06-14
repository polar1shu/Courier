cmake_minimum_required(VERSION 3.20)

include(compiler_base.cmake)

# ------------- Project -----------------
#--------------

project(PTM)

# ------------- Sub module
#--------------

# Other library
find_package(TBB                                CONFIG REQUIRED)
find_package(magic_enum                         CONFIG REQUIRED)
find_package(spdlog                             CONFIG REQUIRED)
find_package(unofficial-concurrentqueue         CONFIG REQUIRED)

# My library
add_subdirectory(workload)
add_subdirectory(concurrent_control)
add_subdirectory(storage_manager)
add_subdirectory(transaction_manager)
add_subdirectory(util)

include(auto_test.cmake)

# ------------- Main module
#--------------

FILE(GLOB_RECURSE header_files CONFIGURE_DEPENDS include/*.hpp include/*.h)
FILE(GLOB_RECURSE source_files CONFIGURE_DEPENDS src/*.cpp)

# Main Function
add_executable(${PROJECT_NAME} ${header_files} ${source_files})

target_include_directories(${PROJECT_NAME} PUBLIC ${PROJECT_SOURCE_DIR}/include)

# ------------- Library linkage
#--------------

target_link_libraries(${PROJECT_NAME}
        # Outer library
        PUBLIC pthread
        PUBLIC atomic
        PUBLIC jemalloc
        PUBLIC numa
        PUBLIC TBB::tbb
        PUBLIC spdlog::spdlog_header_only
        PUBLIC magic_enum::magic_enum
        PUBLIC unofficial::concurrentqueue::concurrentqueue

        # Profiler
        PUBLIC profiler
        PUBLIC papi

        # User library
        PUBLIC workload
        PUBLIC concurrent_control
        PUBLIC storage_manager
        PUBLIC transaction_manager
        PUBLIC util)