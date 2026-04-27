cmake_minimum_required(VERSION 3.20)
project(dkv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(spdlog CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(xxHash CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

get_target_property(grpc_cpp_plugin gRPC::grpc_cpp_plugin LOCATION)

set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/proto/kv_service.proto)
set(PROTO_OUT  ${CMAKE_CURRENT_BINARY_DIR})

add_custom_command(
    OUTPUT
        ${PROTO_OUT}/kv_service.pb.cc
        ${PROTO_OUT}/kv_service.pb.h
        ${PROTO_OUT}/kv_service.grpc.pb.cc
        ${PROTO_OUT}/kv_service.grpc.pb.h
    COMMAND protobuf::protoc
    ARGS
        --cpp_out=${PROTO_OUT}
        --grpc_out=${PROTO_OUT}
        --plugin=protoc-gen-grpc=${grpc_cpp_plugin}
        -I${CMAKE_CURRENT_SOURCE_DIR}/proto
        ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
)

add_library(dkv-proto
    ${PROTO_OUT}/kv_service.pb.cc
    ${PROTO_OUT}/kv_service.grpc.pb.cc
)
target_include_directories(dkv-proto PUBLIC ${PROTO_OUT})
target_link_libraries(dkv-proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

add_library(dkv-config
    src/config/cluster_config.cc
)
target_include_directories(dkv-config PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)

add_library(dkv-storage
    src/storage/shard_map.cc
    src/storage/wal.cc
    src/storage/store.cc
)
target_include_directories(dkv-storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(dkv-storage PUBLIC spdlog::spdlog xxHash::xxhash dkv-proto)

add_executable(dkv-server
    src/server/main.cc
    src/server/kv_service_impl.cc
)
target_link_libraries(dkv-server PRIVATE dkv-proto dkv-storage spdlog::spdlog)

add_executable(dkv-cli
    src/client/cli.cc
)
target_link_libraries(dkv-cli PRIVATE dkv-proto)
target_include_directories(dkv-cli PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

enable_testing()

add_executable(test-shard-map
    test/test_shard_map.cc
)
target_link_libraries(test-shard-map PRIVATE dkv-storage GTest::gtest_main)
add_test(NAME ShardMap COMMAND test-shard-map)

add_executable(test-wal
    test/test_wal.cc
)
target_link_libraries(test-wal PRIVATE dkv-storage GTest::gtest_main)
add_test(NAME WAL COMMAND test-wal)

