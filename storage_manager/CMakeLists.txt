project(storage_manager)

FILE(GLOB_RECURSE header_files CONFIGURE_DEPENDS include/*.hpp include/*.h)

add_library(${PROJECT_NAME} INTERFACE ${header_files})

target_include_directories(${PROJECT_NAME} INTERFACE include)