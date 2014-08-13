project(turnip)
cmake_minimum_required(VERSION 2.8)

aux_source_directory(src SRC_LIST)
SET( CMAKE_CXX_FLAGS  "-O2 -Wall -fmessage-length=0 -std=c++0x" )
SET( CMAKE_SHARED_LINKER_FLAGS "--no-as-needed" )
add_executable(${PROJECT_NAME} ${SRC_LIST}  ${CMAKE_CURRENT_LIST_DIR}/debian/rules ${CMAKE_CURRENT_LIST_DIR}/debian/control
    ${CMAKE_CURRENT_LIST_DIR}/pyturnip/setup.py ${CMAKE_CURRENT_LIST_DIR}/pyturnip/turnip.py ${CMAKE_CURRENT_LIST_DIR}/settings.conf)
TARGET_LINK_LIBRARIES(${PROJECT_NAME} zmq msgpack leveldb config++ snappy)

install(TARGETS ${PROJECT_NAME} RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
