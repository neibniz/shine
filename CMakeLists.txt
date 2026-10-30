cmake_minimum_required(VERSION 3.22)
project(shine CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(SHINE_WITH_TESTS "Build unit tests" ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(
    -Wall -Wextra -Wpedantic
    -Wno-unused-parameter
    -fno-omit-frame-pointer
)

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O3)
endif()

# Boost: enable std::coroutine support for asio
add_compile_definitions(
    BOOST_ASIO_HAS_CO_AWAIT
    BOOST_ASIO_HAS_STD_COROUTINE
    BOOST_ASIO_NO_DEPRECATED
    SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE
)

find_package(Boost REQUIRED COMPONENTS system)
find_package(absl REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(prometheus-cpp REQUIRED)
find_package(Protobuf REQUIRED)
find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)
find_package(ZLIB REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_BINARY_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_BINARY_DIR})

# ---------- libraries ----------

add_library(shine_core STATIC
    src/core/scheduler.cpp
    src/core/logging.cpp
    src/core/signal.cpp
)
target_link_libraries(shine_core PUBLIC
    Boost::system
    absl::strings
    absl::status
    absl::statusor
    absl::time
    absl::flat_hash_map
    absl::flat_hash_set
    absl::inlined_vector
    spdlog::spdlog
    fmt::fmt
)

add_library(shine_proto STATIC
    src/proto/resp2_parser.cpp
    src/proto/resp2_writer.cpp
    src/proto/shine_frame.cpp
)
target_link_libraries(shine_proto PUBLIC shine_core)

add_library(shine_transport STATIC
    src/transport/address.cpp
    src/transport/link.cpp
    src/transport/session.cpp
    src/transport/pipe.cpp
    src/transport/tcp_stream.cpp
    src/redis/resp.cpp
    src/redis/stream.cpp
    src/redis/fake.cpp
    src/redis/acceptor.cpp
)
target_link_libraries(shine_transport PUBLIC shine_proto ZLIB::ZLIB)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS src/proto/v2ray.proto)

add_library(shine_route STATIC
    src/route/router.cpp
    src/route/geo_matcher.cpp
    ${PROTO_SRCS}
    ${PROTO_HDRS}
)
target_link_libraries(shine_route PUBLIC shine_transport protobuf::libprotobuf)

add_library(shine_config STATIC
    src/config/yaml_loader.cpp
)
target_link_libraries(shine_config PUBLIC shine_core yaml-cpp::yaml-cpp)

add_library(shine_metrics STATIC
    src/metrics/registry.cpp
    src/metrics/exporter.cpp
)
target_link_libraries(shine_metrics PUBLIC
    shine_core
    prometheus-cpp::core
    prometheus-cpp::pull
)

add_library(shine_inbound STATIC
    src/inbound/socks5_inbound.cpp
    src/inbound/http_connect_inbound.cpp
    src/inbound/shine_inbound.cpp
    src/inbound/factory.cpp
)
target_link_libraries(shine_inbound PUBLIC shine_transport shine_metrics)

add_library(shine_outbound STATIC
    src/outbound/direct_outbound.cpp
    src/outbound/block_outbound.cpp
    src/outbound/shine_outbound.cpp
    src/outbound/socks5_outbound.cpp
    src/outbound/http_connect_outbound.cpp
    src/outbound/factory.cpp
)
target_link_libraries(shine_outbound PUBLIC shine_transport shine_metrics)

# ---------- executable ----------

add_executable(shine src/main.cpp)
target_link_libraries(shine PRIVATE
    shine_inbound
    shine_outbound
    shine_route
    shine_config
    shine_metrics
)

# ---------- tests ----------

if(SHINE_WITH_TESTS)
    enable_testing()
    find_package(GTest REQUIRED)
    include(GoogleTest)

    add_executable(shine_tests
        tests/resp2_test.cpp
        tests/shine_frame_test.cpp
        tests/address_test.cpp
        tests/route_test.cpp
        tests/flow_control_test.cpp
        tests/redis_test.cpp
    )
    target_link_libraries(shine_tests PRIVATE
        shine_proto
        shine_transport
        shine_route
        shine_config
        GTest::gtest
        GTest::gtest_main
    )
    gtest_discover_tests(shine_tests)
endif()
