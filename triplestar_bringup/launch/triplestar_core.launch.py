import os

import lifecycle_msgs
from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import (
    DeclareLaunchArgument,
    EmitEvent,
    RegisterEventHandler,
)
from launch.events import matches_action
from launch.substitutions import (
    LaunchConfiguration,
)
from launch_ros.actions import LifecycleNode, Node
from launch_ros.event_handlers import OnStateTransition
from launch_ros.events.lifecycle import ChangeState


def generate_launch_description():
    log_level_arg = DeclareLaunchArgument(
        'log-level',
        default_value='info',
        description='Logging level',
    )
    log_level = LaunchConfiguration('log-level', default='info')

    config = os.path.join(
        get_package_share_directory('triplestar_bringup'),
        'config',
        'kb_params.yaml',
    )

    triplestar_core_node = LifecycleNode(
        package='triplestar_core',
        executable='kb_node',
        name='triplestar_core',
        namespace='',
        output='screen',
        parameters=[config],
        arguments=['--ros-args', '--log-level', ['triplestar_core:=', log_level]],
        emulate_tty=True,
    )

    triplestar_core_node_config_event = EmitEvent(
        event=ChangeState(
            lifecycle_node_matcher=matches_action(triplestar_core_node),
            transition_id=lifecycle_msgs.msg.Transition.TRANSITION_CONFIGURE,  # type: ignore
        )
    )

    triplestar_core_node_activate_event = EmitEvent(
        event=ChangeState(
            lifecycle_node_matcher=matches_action(triplestar_core_node),
            transition_id=lifecycle_msgs.msg.Transition.TRANSITION_ACTIVATE,  # type: ignore
        )
    )

    marker_publisher_node = Node(
        package='triplestar_core',
        executable='kb_marker_publisher',
        name='marker_publisher',
        namespace='',
        output='screen',
    )

    viz_node = Node(
        package='triplestar_viz',
        executable='kb_visualizer_node',
        name='visualizer_node',
        namespace='triplestar_core',
        parameters=[
            {
                'store_path': '/tmp/triplestar_core'  # Add this line
            }
        ],
        output='screen',
    )

    _start_marker_publisher = RegisterEventHandler(
        OnStateTransition(
            target_lifecycle_node=triplestar_core_node,
            goal_state='active',
            entities=[marker_publisher_node],
        )
    )

    return LaunchDescription(
        [
            log_level_arg,
            triplestar_core_node,
            triplestar_core_node_config_event,
            triplestar_core_node_activate_event,
            # start_marker_publisher,
            viz_node,
        ]
    )
