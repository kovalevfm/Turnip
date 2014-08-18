project(turnip)
cmake_minimum_required(VERSION 2.8)

aux_source_directory(src SRC_LIST)
SET( CMAKE_CXX_FLAGS  "-O2 -Wall -fmessage-length=0 -std=c++0x -pthread" )
SET( CMAKE_SHARED_LINKER_FLAGS "--no-as-needed" )
FILE(GLOB ADD_HEADERS ${CMAKE_CURRENT_LIST_DIR}/src/*.h)
FILE(GLOB DEBIAN ${CMAKE_CURRENT_LIST_DIR}/debian/*)
FILE(GLOB PYTHON ${CMAKE_CURRENT_LIST_DIR}/pyturnip/*)
FILE(GLOB CFG ${CMAKE_CURRENT_LIST_DIR}/*.conf ${CMAKE_CURRENT_LIST_DIR}/*.md ${CMAKE_CURRENT_LIST_DIR}/LICENSE)
add_executable(${PROJECT_NAME} ${SRC_LIST} ${ADD_HEADERS} ${DEBIAN} ${PYTHON} ${CFG})
TARGET_LINK_LIBRARIES(${PROJECT_NAME} zmq msgpack leveldb config++ snappy)

install(TARGETS ${PROJECT_NAME} RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
