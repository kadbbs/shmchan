cmake_minimum_required(VERSION 3.20)

project(
    shmchan
    VERSION 0.2.0
    DESCRIPTION "A low-latency C++20 shared-memory channel for Linux"
    LANGUAGES CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "shmchan currently supports Linux only")
endif()

include(GNUInstallDirs)

add_library(shmchan INTERFACE)
add_library(shmchan::shmchan ALIAS shmchan)

target_compile_features(shmchan INTERFACE cxx_std_20)
target_include_directories(
    shmchan
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(shmchan INTERFACE rt)

option(SHMCHAN_BUILD_EXAMPLES "Build shmchan examples" ${PROJECT_IS_TOP_LEVEL})

if(SHMCHAN_BUILD_EXAMPLES)
    add_executable(shmchan_demo examples/demo.cpp)
    target_link_libraries(shmchan_demo PRIVATE shmchan::shmchan)

    add_executable(shmchan_variable_demo examples/variable_demo.cpp)
    target_link_libraries(shmchan_variable_demo PRIVATE shmchan::shmchan)
endif()

include(CTest)
if(BUILD_TESTING)
    find_package(Threads REQUIRED)
    add_executable(shmchan_tests tests/channel_tests.cpp)
    target_link_libraries(shmchan_tests PRIVATE shmchan::shmchan Threads::Threads)
    target_compile_options(shmchan_tests PRIVATE -Wall -Wextra -Wpedantic -Wconversion)
    add_test(NAME shmchan_tests COMMAND shmchan_tests)
    set_tests_properties(shmchan_tests PROPERTIES TIMEOUT 30)

    add_executable(shmchan_variable_tests tests/variable_channel_tests.cpp)
    target_link_libraries(shmchan_variable_tests PRIVATE shmchan::shmchan Threads::Threads)
    target_compile_options(
        shmchan_variable_tests PRIVATE -Wall -Wextra -Wpedantic -Wconversion)
    add_test(NAME shmchan_variable_tests COMMAND shmchan_variable_tests)
    set_tests_properties(shmchan_variable_tests PROPERTIES TIMEOUT 30)
endif()

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(
    TARGETS shmchan
    EXPORT shmchanTargets
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(
    EXPORT shmchanTargets
    FILE shmchanTargets.cmake
    NAMESPACE shmchan::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/shmchan)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/shmchanConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
configure_package_config_file(
    cmake/shmchanConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/shmchanConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/shmchan)
install(
    FILES
        "${CMAKE_CURRENT_BINARY_DIR}/shmchanConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/shmchanConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/shmchan)
