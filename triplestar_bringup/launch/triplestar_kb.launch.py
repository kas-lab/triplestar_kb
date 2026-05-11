import lifecycle_msgs
from launch import LaunchDescription
from launch.actions import (
    DeclareLaunchArgument,
    EmitEvent,
)
from launch.conditions import IfCondition
from launch.events import matches_action
from launch.substitutions import (
    LaunchConfiguration,
)
from launch_ros.actions import LifecycleNode, Node
from launch_ros.events.lifecycle import ChangeState


def generate_launch_description():
    log_level_arg = DeclareLaunchArgument(
        'log-level',
        default_value='info',
        description='Logging level',
    )
    log_level = LaunchConfiguration('log-level', default='info')

    bringup_package_arg = DeclareLaunchArgument(
        'bringup-package',
        default_value='triplestar_bringup',
        description='Name of custom TriplestarKB bringup package',
    )
    bringup_package = LaunchConfiguration('bringup-package')

    triplestar_core_node = LifecycleNode(
        package='triplestar_core',
        executable='kb_node',
        name='triplestar_core',
        namespace='',
        output='screen',
        parameters=[{'bringup_package': bringup_package, 'log_level': log_level}],
        arguments=['--ros-args', '--log-level', ['triplestar_core:=', log_level]],
        emulate_tty=True,
    )

    triplestar_core_node_config_event = EmitEvent(
        event=ChangeState(
            lifecycle_node_matcher=matches_action(triplestar_core_node),
            transition_id=lifecycle_msgs.msg.Transition.TRANSITION_CONFIGURE,
        )
    )

    triplestar_core_node_activate_event = EmitEvent(
        event=ChangeState(
            lifecycle_node_matcher=matches_action(triplestar_core_node),
            transition_id=lifecycle_msgs.msg.Transition.TRANSITION_ACTIVATE,  # type: ignore
        )
    )

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
            triplestar_core_node_config_event,
            triplestar_core_node_activate_event,
            triplestar_geometry_visualizer,
        ]
    )
