cmake_minimum_required(VERSION 3.8)
project({{cookiecutter.bringup_name}})

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# find dependencies
find_package(ament_cmake REQUIRED)

# Register this package as a discoverable triplestar bringup package.
# CONTENT lets you stash the relative launch file path so the CLI
# doesn't have to guess a filename convention.
ament_index_register_resource("triplestar_bringup"
  CONTENT "launch/bringup.launch.xml"
)

install(DIRECTORY launch DESTINATION share/${PROJECT_NAME})
install(DIRECTORY config DESTINATION share/${PROJECT_NAME})
install(DIRECTORY preload DESTINATION share/${PROJECT_NAME})
install(DIRECTORY queries DESTINATION share/${PROJECT_NAME})
install(DIRECTORY templates DESTINATION share/${PROJECT_NAME})
install(DIRECTORY functions DESTINATION share/${PROJECT_NAME})

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  # the following line skips the linter which checks for copyrights
  # comment the line when a copyright and license is added to all source files
  set(ament_cmake_copyright_FOUND TRUE)
  # the following line skips cpplint (only works in a git repo)
  # comment the line when this package is in a git repo and when
  # a copyright and license is added to all source files
  set(ament_cmake_cpplint_FOUND TRUE)
  ament_lint_auto_find_test_dependencies()
endif()

ament_package()
