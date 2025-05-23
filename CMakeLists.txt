cmake_minimum_required(VERSION 3.30)

project(pubsub)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED True)

option(PUBSUB_USE_QUILL "Use Quill for logging" ON)

if (PUBSUB_USE_QUILL)
    message(STATUS "Using Quill for logging")
    add_compile_definitions(PUBSUB_USE_QUILL)
    include(FetchContent)
    FetchContent_Declare(
        quill
        GIT_REPOSITORY https://github.com/odygrd/quill.git
        GIT_TAG        v9.0.3
    )
    FetchContent_MakeAvailable(quill)
else()
    message(STATUS "Not using Quill for logging")
endif()


find_package(Boost COMPONENTS system REQUIRED)

add_library(${PROJECT_NAME} INTERFACE)
target_include_directories(${PROJECT_NAME} INTERFACE include)
target_link_libraries(${PROJECT_NAME} INTERFACE ${Boost_LIBRARIES})
if (PUBSUB_USE_QUILL)
    target_link_libraries(${PROJECT_NAME} INTERFACE quill::quill)
endif()

if (${PROJECT_IS_TOP_LEVEL})
file( GLOB DRIVERS_SOURCES examples/*.cpp )
foreach( sourcefile ${DRIVERS_SOURCES} )
    get_filename_component( name ${sourcefile} NAME_WE )
    add_executable( ${name} ${sourcefile} )
    target_link_libraries( ${name} ${PROJECT_NAME} )
endforeach( sourcefile ${DRIVERS_SOURCES} )
endif()