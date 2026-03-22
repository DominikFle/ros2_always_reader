"""Test cases for coercing dynamic MCAP messages to ROS2 messages."""

from typing import Any

import pytest
from mcap.records import Schema
from mcap_ros2.decoder import DecoderFactory
from rclpy.serialization import serialize_message
from ros2_always_reader.mcap_schema.build_ros2_schema_text import build_ros2_schema_text
from ros2_always_reader.mcap_schema.coerce_dynamic_mcap_msg_to_ros2_msg import (
    TypeSpec,
    coerce_dynamic_mcap_msg_to_ros2_msg,
)
from sensor_msgs.msg import Imu


def imu_mcap_msg() -> Any:
    """Test coercing a dynamic MCAP message to a ROS2 message."""
    imu_message = Imu()
    imu_message.header.frame_id = "test_frame"
    imu_message.linear_acceleration.x = 1.0
    imu_message.linear_acceleration.y = 2.0
    imu_message.linear_acceleration.z = 3.0
    imu_message.angular_velocity.x = 0.1
    imu_message.angular_velocity.y = 0.2
    imu_message.angular_velocity.z = 0.3
    imu_message.orientation.x = 0.0
    imu_message.orientation.y = 0.0
    imu_message.orientation.z = 0.0
    imu_message.orientation.w = 1.0
    schema_name, schema_text = build_ros2_schema_text(Imu)

    # Serialize the ROS2 message
    serialized_data = serialize_message(imu_message)

    # Create a dynamic MCAP message using the DecoderFactory
    decoder_factory = DecoderFactory()
    schema = Schema(id=1, name=schema_name, encoding="ros2msg", data=schema_text.encode())
    assert schema is not None, "Schema should not be None for valid schema text"
    decoder = decoder_factory.decoder_for("cdr", schema)
    assert decoder is not None, "Decoder should not be None for valid schema"
    dynamic_mcap_msg = decoder(serialized_data)
    assert dynamic_mcap_msg is not None, "Decoded dynamic MCAP message should not be None"
    return dynamic_mcap_msg


def test_coerce_dynamic_mcap_msg_to_ros2_msg() -> None:
    """
    Test coercing a dynamic MCAP message to a ROS2 message.

    This is a conversion only since no fields are missing or too much.
    """
    dynamic_mcap_msg = imu_mcap_msg()
    ros2_msg, report = coerce_dynamic_mcap_msg_to_ros2_msg(dynamic_mcap_msg, Imu)
    assert isinstance(ros2_msg, Imu), f"Expected ROS2 message of type Imu, but got {type(ros2_msg)}"
    assert (
        ros2_msg.header.frame_id == "test_frame"
    ), f"Expected frame_id 'test_frame', but got '{ros2_msg.header.frame_id}'"
    assert (
        ros2_msg.linear_acceleration.x == 1.0
    ), f"Expected linear_acceleration.x to be 1.0, but got {ros2_msg.linear_acceleration.x}"
    assert (
        ros2_msg.linear_acceleration.y == 2.0
    ), f"Expected linear_acceleration.y to be 2.0, but got {ros2_msg.linear_acceleration.y}"
    assert (
        ros2_msg.linear_acceleration.z == 3.0
    ), f"Expected linear_acceleration.z to be 3.0, but got {ros2_msg.linear_acceleration.z}"
    assert (
        ros2_msg.angular_velocity.x == 0.1
    ), f"Expected angular_velocity.x to be 0.1, but got {ros2_msg.angular_velocity.x}"
    assert (
        ros2_msg.angular_velocity.y == 0.2
    ), f"Expected angular_velocity.y to be 0.2, but got {ros2_msg.angular_velocity.y}"
    assert (
        ros2_msg.angular_velocity.z == 0.3
    ), f"Expected angular_velocity.z to be 0.3, but got {ros2_msg.angular_velocity.z}"
    assert ros2_msg.orientation.x == 0.0, f"Expected orientation.x to be 0.0, but got {ros2_msg.orientation.x}"
    assert ros2_msg.orientation.y == 0.0, f"Expected orientation.y to be 0.0, but got {ros2_msg.orientation.y}"
    assert ros2_msg.orientation.z == 0.0, f"Expected orientation.z to be 0.0, but got {ros2_msg.orientation.z}"
    assert ros2_msg.orientation.w == 1.0, f"Expected orientation.w to be 1.0, but got {ros2_msg.orientation.w}"


def test_coerce_dynamic_mcap_msg_to_ros2_msg_with_missing_primitive_field() -> None:
    """
    Test coercing a dynamic MCAP message to a ROS2 message when a primitive field is missing.

    This tests that missing primitive fields are set to their default values.
    """
    dynamic_mcap_msg = imu_mcap_msg()
    del dynamic_mcap_msg.linear_acceleration.y  # Remove a primitive field
    ros2_msg, report = coerce_dynamic_mcap_msg_to_ros2_msg(dynamic_mcap_msg, Imu)
    assert isinstance(ros2_msg, Imu), f"Expected ROS2 message of type Imu, but got {type(ros2_msg)}"
    assert (
        ros2_msg.linear_acceleration.y == 0.0
    ), f"Expected default value 0.0, but got {ros2_msg.linear_acceleration.y}"


def test_coerce_dynamic_mcap_msg_to_ros2_msg_with_extra_field() -> None:
    """
    Test coercing a dynamic MCAP message to a ROS2 message when an extra field is present.

    This tests that extra fields in the dynamic MCAP message are ignored.
    """
    dynamic_mcap_msg = imu_mcap_msg()
    dynamic_mcap_msg.extra_field = "extra_value"  # Add an extra field
    ros2_msg, report = coerce_dynamic_mcap_msg_to_ros2_msg(dynamic_mcap_msg, Imu)
    assert isinstance(ros2_msg, Imu), f"Expected ROS2 message of type Imu, but got {type(ros2_msg)}"
    assert not hasattr(ros2_msg, "extra_field"), "ROS2 message should not have the extra field"


def test_coerce_dynamic_mcap_msg_to_ros2_msg_with_missing_nested_field() -> None:
    """
    Test coercing a dynamic MCAP message to a ROS2 message when a nested field is missing.

    This tests that missing nested fields are set to their default values.
    """
    dynamic_mcap_msg = imu_mcap_msg()
    del dynamic_mcap_msg.orientation.x  # Remove a nested primitive field
    ros2_msg, report = coerce_dynamic_mcap_msg_to_ros2_msg(dynamic_mcap_msg, Imu)
    assert isinstance(ros2_msg, Imu), f"Expected ROS2 message of type Imu, but got {type(ros2_msg)}"
    assert ros2_msg.orientation.x == 0.0, f"Expected default value 0.0, but got {ros2_msg.orientation.x}"


def test_coerce_dynamic_mcap_msg_to_ros2_msg_with_missing_header() -> None:
    """
    Test coercing a dynamic MCAP message to a ROS2 message when the header is missing.

    This tests that a missing header field is set to its default values.
    """
    dynamic_mcap_msg = imu_mcap_msg()
    del dynamic_mcap_msg.header  # Remove the header
    ros2_msg, report = coerce_dynamic_mcap_msg_to_ros2_msg(dynamic_mcap_msg, Imu)
    assert isinstance(ros2_msg, Imu), f"Expected ROS2 message of type Imu, but got {type(ros2_msg)}"
    assert ros2_msg.header.frame_id == "", f"Expected default frame_id '', but got '{ros2_msg.header.frame_id}'"
    assert (
        ros2_msg.header.stamp.sec == 0
    ), f"Expected default header.stamp.sec to be 0, but got {ros2_msg.header.stamp.sec}"
    assert (
        ros2_msg.header.stamp.nanosec == 0
    ), f"Expected default header.stamp.nanosec to be 0, but got {ros2_msg.header.stamp.nanosec}"


def test_coerce_dynamic_mcap_msg_to_ros2_msg_with_incorrect_type() -> None:
    """
    Test coercing a dynamic MCAP message to a ROS2 message when a field has an incorrect type.

    This tests that fields with incorrect types are set to their default values.
    """
    dynamic_mcap_msg = imu_mcap_msg()
    dynamic_mcap_msg.linear_acceleration.x = "not_a_float"  # Set a field to an incorrect type
    ros2_msg, report = coerce_dynamic_mcap_msg_to_ros2_msg(dynamic_mcap_msg, Imu)
    assert isinstance(ros2_msg, Imu), f"Expected ROS2 message of type Imu, but got {type(ros2_msg)}"
    assert (
        ros2_msg.linear_acceleration.x == 0.0
    ), f"Expected default value 0.0 for incorrect type, but got {ros2_msg.linear_acceleration.x}"


def test_coerce_dynamic_mcap_msg_to_ros2_msg_with_field_converter() -> None:
    """
    Test coercing a dynamic MCAP message to a ROS2 message when a field converter is provided.

    This tests that the field converter is used to convert the field value.
    """
    dynamic_mcap_msg = imu_mcap_msg()
    dynamic_mcap_msg.linear_acceleration.x = "1.0"  # Set a field to a string that can be converted to float

    def string_to_float_converter(value: Any) -> float:
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return 0.0
        return 0.0

    ros2_msg, report = coerce_dynamic_mcap_msg_to_ros2_msg(
        dynamic_mcap_msg,
        Imu,
        field_converters={"linear_acceleration.x": string_to_float_converter},
    )
    assert isinstance(ros2_msg, Imu), f"Expected ROS2 message of type Imu, but got {type(ros2_msg)}"
    assert (
        ros2_msg.linear_acceleration.x == 1.0
    ), f"Expected converted value 1.0, but got {ros2_msg.linear_acceleration.x}"


def test_coerce_dynamic_mcap_msg_to_ros2_msg_with_path_converter() -> None:
    """
    Test coercing a dynamic MCAP message to a ROS2 message when a path converter is provided.

    This tests that the path converter is used to convert the field value conditioned on the path.
    """
    dynamic_mcap_msg = imu_mcap_msg()
    dynamic_mcap_msg.linear_acceleration.x = "1.0"  # Set a field to a string that can be converted to float

    def string_to_float_converter(old_value: Any, path: str, target_spec: TypeSpec) -> float:
        if path == "linear_acceleration.x" and isinstance(old_value, str):
            try:
                return float(old_value)
            except ValueError:
                return 0.0
        return 0.0

    ros2_msg, report = coerce_dynamic_mcap_msg_to_ros2_msg(
        dynamic_mcap_msg,
        Imu,
        path_converters={"linear_acceleration.x": string_to_float_converter},
    )
    assert isinstance(ros2_msg, Imu), f"Expected ROS2 message of type Imu, but got {type(ros2_msg)}"
    assert (
        ros2_msg.linear_acceleration.x == 1.0
    ), f"Expected converted value 1.0, but got {ros2_msg.linear_acceleration.x}"


def test_coerce_dynamic_mcap_msg_to_ros2_msg_with_wrong_converter_type() -> None:
    """
    Test coercing a dynamic MCAP message to a ROS2 message when a field converter is provided.

    This tests that the field converter is used to convert the field value.
    But the converter returns the wrong type, so the final value should be the default.
    """
    dynamic_mcap_msg = imu_mcap_msg()
    dynamic_mcap_msg.linear_acceleration.x = "1.0"  # Set a field to a string that can be converted to float

    def string_to_float_converter(value: Any) -> int:
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return 0
        return 0

    ros2_msg, report = coerce_dynamic_mcap_msg_to_ros2_msg(
        dynamic_mcap_msg,
        Imu,
        field_converters={"linear_acceleration.x": string_to_float_converter},
    )
    assert isinstance(ros2_msg, Imu), f"Expected ROS2 message of type Imu, but got {type(ros2_msg)}"
    assert (
        ros2_msg.linear_acceleration.x == 0.0
    ), f"Expected default value 0.0 for wrong converter type, but got {ros2_msg.linear_acceleration.x}"
    with pytest.raises(Exception):  # noqa: B017
        coerce_dynamic_mcap_msg_to_ros2_msg(
            dynamic_mcap_msg,
            Imu,
            field_converters={"linear_acceleration.x": string_to_float_converter},
            strict=True,
        )
