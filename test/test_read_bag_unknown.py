"""Test to read bag file with unknown message defintion."""

from pathlib import Path

import pytest
from ros2_always_reader.read_bag import read_bag
from rosidl_generator_py.import_type_support_impl import UnsupportedTypeSupport


def test_read_unknown() -> None:
    """Test reading a broken MCAP bag file using the native reader."""
    bag_path = Path("/home/anavs/ros_ws/libpy/vision/ros2_always_reader/test/bag_with_unknown_msgs")
    assert len(list(read_bag(str(bag_path), drop_unknown_msgs=True))) == 0

    with pytest.raises(UnsupportedTypeSupport):
        list(read_bag(str(bag_path), drop_unknown_msgs=False))
