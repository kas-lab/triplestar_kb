from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.conditions import IfCondition
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import LifecycleNode
from launch_ros.actions import Node
from triplestar_bringup.lifecycle import configure_and_activate


def generate_launch_description():
    log_level_arg = DeclareLaunchArgument(
        'log-level',
        default_value='info',
        description='Logging level',
    )
    log_level = LaunchConfiguration('log-level', default='info')

    bringup_package_arg = DeclareLaunchArgument(
        'bringup-package',
        default_value='',
        description='Name of custom TriplestarKB bringup package',
    )
    bringup_package = LaunchConfiguration('bringup-package')

    triplestar_core_node = LifecycleNode(
        package='triplestar_core',
        executable='triplestar_kb_node',
        name='triplestar_core',
        namespace='',
        output='screen',
        parameters=[{'bringup_package': bringup_package, 'log_level': log_level}],
        arguments=['--ros-args', '--log-level', ['triplestar_core:=', log_level]],
        emulate_tty=True,
    )

    lifecycle_events = configure_and_activate(triplestar_core_node)

    enable_geometry_visualizer_arg = DeclareLaunchArgument(
        'enable-geometry-visualizer',
        default_value='false',
        description='Enable geometry visualizer',
    )
    enable_geometry_visualizer = LaunchConfiguration('enable-geometry-visualizer')

    triplestar_geometry_visualizer = Node(
        package='triplestar_viz',
        executable='kb_geometry_visualizer',
        name='triplestar_geometry_visualizer',
        condition=IfCondition(enable_geometry_visualizer),
    )

    return LaunchDescription(
        [
            log_level_arg,
            bringup_package_arg,
            enable_geometry_visualizer_arg,
            triplestar_core_node,
            *lifecycle_events,
            triplestar_geometry_visualizer,
        ]
    )
