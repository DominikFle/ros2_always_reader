"""Get the ROS 2 type string from a message class."""

from typing import Type


def get_type_string_from_class(cls: Type) -> str:
    """
    Convert a ROS 2 message class to its type string.

    Example:
        >>> from sensor_msgs.msg import Image
        >>> get_type_string_from_class(Image)
        'sensor_msgs/msg/Image'

    Args:
        cls: The message class (e.g., sensor_msgs.msg.Image)

    Returns:
        The corresponding ROS 2 type string (e.g., 'sensor_msgs/msg/Image')

    Raises:
        ValueError: If the module path is too short or malformed.
    """
    if not hasattr(cls, "__module__") or not hasattr(cls, "__name__"):
        raise TypeError("Expected a class with __module__ and __name__ attributes")

    module_parts: list[str] = cls.__module__.split(".")

    if len(module_parts) < 2:
        raise ValueError(f"Cannot determine package from module path: {cls.__module__}")

    package: str = module_parts[0]  # e.g., 'sensor_msgs'
    class_name: str = cls.__name__  # e.g., 'Image'

    return f"{package}/msg/{class_name}"
