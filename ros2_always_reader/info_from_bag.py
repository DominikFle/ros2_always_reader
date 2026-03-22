"""Information extraction from bag files."""

import glob
import os
from dataclasses import dataclass
from typing import Any

import rosbag2_py


@dataclass
class BagMetadata:
    """Class for keeping track of bag metadata."""

    message_count: int
    topics_count: int
    start_time_ns: int
    end_time_ns: int
    duration_ns: int
    topics: list[str]
    types: list[str]
    topics_to_type_and_count: dict[str, dict[str, Any]]  # topic_name: {type: str, count: int}


def info_from_bag(
    input_bag_path: str,
    verbose: bool = False,
) -> BagMetadata:
    """
    Obtains the bag info / metadata from a bag.

    Args:
        input_bag_path: The path to the input bag
        verbose: If verbose, the info is printed to the console

    Returns:
        A dictionary with bag info / metadata. It contains the following keys:
        - message_count: Total number of messages in the bag
        - topics_count: Number of topics in the bag
        - start_time_ns: Start time of the bag since epoch in nanoseconds
        - end_time_ns: End time of the bag since epoch in nanoseconds
        - duration_ns: Duration of the bag in nanoseconds
        - topics: List of topics in the bag len(topics) == len(types) == len(msg_counts)==topics_count
        - types: List of types in the bag
        - topics_to_type_and_count: Dictionary mapping topic name to its type and message count, such as
            {
                "topic_name_1": {"type": "std_msgs/msg/String", "count": 42},
                "topic_name_2": {"type": "sensor_msgs/msg/Image", "count": 100},
                ...
            }
    """
    storage_id = get_storage_id_from_bag_folder(input_bag_path)
    info = rosbag2_py.Info()
    bag_info = info.read_metadata(input_bag_path, storage_id)
    info_dict: dict[str, Any] = {}
    info_dict["message_count"] = bag_info.message_count
    info_dict["start_time_ns"] = bag_info.starting_time.nanoseconds
    info_dict["end_time_ns"] = bag_info.starting_time.nanoseconds + bag_info.duration.nanoseconds
    info_dict["duration_ns"] = bag_info.duration.nanoseconds
    info_dict["version"] = bag_info.version
    topic_to_type_and_count = {}
    topics = set()
    types = set()
    for topic in bag_info.topics_with_message_count:
        topic_name = topic.topic_metadata.name
        type_name = topic.topic_metadata.type
        msg_count_per_topic = topic.message_count
        topics.add(topic_name)
        types.add(type_name)
        topic_to_type_and_count[topic_name] = {"type": type_name, "count": msg_count_per_topic}
    info_dict["topic_to_type_and_count"] = topic_to_type_and_count
    info_dict["topics_count"] = len(topic_to_type_and_count)
    info_dict["topics"] = list(topics)
    info_dict["types"] = list(types)

    bag_metadata = BagMetadata(
        message_count=info_dict["message_count"],
        topics_count=info_dict["topics_count"],
        start_time_ns=info_dict["start_time_ns"],
        end_time_ns=info_dict["end_time_ns"],
        duration_ns=info_dict["duration_ns"],
        topics=info_dict["topics"],
        types=info_dict["types"],
        topics_to_type_and_count=info_dict["topic_to_type_and_count"],
    )
    if verbose:
        print("Bag info:")
        for key, value in info_dict.items():
            print(f"  {key}: {value}")
    return bag_metadata


def get_topic_to_type_and_count(bag_folder_paths: list[str] | str) -> dict[str, dict[str, Any]]:
    """
    Get a mapping from topic name to its type and message count across multiple bag folders.

    Args:
        bag_folder_paths (list[str]|str): A list of paths to bag folders.

    Returns:
        A dictionary mapping topic names to their type and message count, such as
        {
            <bag_name_1>: {
                "topic_name_1": {"type": "std_msgs/msg/String", "count": 42},
                "topic_name_2": {"type": "sensor_msgs/msg/Image", "count": 100},
                ...
            }
            <bag_name_2>: {
                "topic_name_1": {"type": "std_msgs/msg/String", "count": 42},
                "topic_name_2": {"type": "sensor_msgs/msg/Image", "count": 100},
                ...
            }
        }
    """
    if isinstance(bag_folder_paths, str):
        bag_folder_paths = [bag_folder_paths]
    bag_to_topic_to_type_and_count = {}
    for bag_folder in bag_folder_paths:
        topic_to_type_and_count = {}
        bag_metadata = info_from_bag(bag_folder)
        for topic in bag_metadata.topics:
            topic_info = bag_metadata.topics_to_type_and_count[topic]
            topic_to_type_and_count[topic] = {"type": topic_info["type"], "count": topic_info["count"]}
        bag_to_topic_to_type_and_count[bag_folder] = topic_to_type_and_count
    return bag_to_topic_to_type_and_count


def get_storage_id_from_bag_folder(bag_folder_path: str) -> str:
    """
    Get the storage id from a bag folder path.

    Args:
        bag_folder_path: The path to the bag folder, a path to the mcap or db3 file is also possible.

    Returns:
        The storage id of the bag folder (either 'mcap' or 'sqlite3')
    """
    if os.path.isdir(bag_folder_path):
        mcap_files = glob.glob(os.path.join(bag_folder_path, "*.mcap"))
        db3_files = glob.glob(os.path.join(bag_folder_path, "*.db3"))
        if mcap_files and db3_files:
            raise ValueError(f"Both .mcap and .db3 files found in {bag_folder_path}, unable to determine storage id.")
        if mcap_files:
            return "mcap"
        if db3_files:
            return "sqlite3"
        raise FileNotFoundError(f"No .mcap or .db3 files found in {bag_folder_path}")
    else:
        # single file path
        _, ext = os.path.splitext(bag_folder_path)
        if ext == ".mcap":
            return "mcap"
        elif ext == ".db3":
            return "sqlite3"
        else:
            raise ValueError(f"Unsupported file extension: {ext}. Expected .mcap or .db3 in {bag_folder_path}")


def get_topics_and_types_from_bag(
    input_bag_path: str,
    storage_id: str,
) -> list[rosbag2_py.TopicMetadata]:
    """
    Obtains the topics and types from a bag.

    Usage:

    Args:
        input_bag_path: The path to the input bag
        storage_id: The storage id of the bag, currently supports mcap and sqlite3

    Returns:
        A list of TopicMetadata for each topic with at least the fields .type and .name. Similar to
        https://docs.ros.org/en/rolling/p/rosbag2_storage/generated/structrosbag2__storage_1_1TopicMetadata.html#_CPPv4N15rosbag2_storage13TopicMetadataE
    """

    if not (storage_id == "mcap" or storage_id == "sqlite3"):
        raise AttributeError(f"Storage_id not supported, received {storage_id}")
    bag_files = []
    if os.path.isdir(input_bag_path):
        if storage_id == "mcap":
            ext = "mcap"
        elif storage_id == "sqlite3":
            ext = "db3"
        else:
            raise ValueError("Storage ID not available.")
        pattern = os.path.join(input_bag_path, f"*.{ext}")
        bag_files = sorted(glob.glob(pattern))
        if not bag_files:
            raise FileNotFoundError(f"No .{ext} files found in {input_bag_path}")
    else:
        bag_files = [input_bag_path]
    by_name = {}
    for bag_file in bag_files:
        reader = rosbag2_py.SequentialReader()
        reader.open(
            rosbag2_py.StorageOptions(uri=bag_file, storage_id=storage_id),
            rosbag2_py.ConverterOptions(
                input_serialization_format="cdr",
                output_serialization_format="cdr",
            ),
        )

        for meta in reader.get_all_topics_and_types():
            if meta.name in by_name:
                if by_name[meta.name].type != meta.type:
                    raise ValueError(f"Type conflict for {meta.name}: " f"{by_name[meta.name].type} vs {meta.type}")
                # else: same type → ignore duplicate
            else:
                by_name[meta.name] = meta

    return list(by_name.values())
