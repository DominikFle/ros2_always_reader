"""Round-trip test using rosbag2_py writer and read_bag."""

from pathlib import Path

import rosbag2_py
from rclpy.serialization import serialize_message
from ros2_always_reader.read_bag import read_bag
from std_msgs.msg import String


def test_read_round_trip(tmp_path: Path) -> None:
    """Write a small bag with rosbag2_py and verify read_bag output."""
    bag_path = tmp_path / "round_trip_bag"

    writer = rosbag2_py.SequentialWriter()
    writer.open(
        rosbag2_py.StorageOptions(uri=str(bag_path), storage_id="sqlite3"),
        rosbag2_py.ConverterOptions(
            input_serialization_format="cdr",
            output_serialization_format="cdr",
        ),
    )

    topic_name = "/chatter"
    msg_type = "std_msgs/msg/String"
    writer.create_topic(
        rosbag2_py.TopicMetadata(
            name=topic_name,
            type=msg_type,
            serialization_format="cdr",
        )
    )

    payloads = ["hello", "world", "ros2"]
    timestamps = [10, 20, 30]
    for stamp, text in zip(timestamps, payloads, strict=True):
        msg = String()
        msg.data = text
        writer.write(topic_name, serialize_message(msg), stamp)

    del writer

    read_messages = list(read_bag(str(bag_path)))
    assert len(read_messages) == len(payloads)
    for idx, bag_msg in enumerate(read_messages):
        assert bag_msg.topic == topic_name
        assert bag_msg.msg.data == payloads[idx]
        assert bag_msg.writer_timestamp == timestamps[idx]
