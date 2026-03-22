"""Read Messages from a ROS2 bag file using Generator syntax."""

import glob
import os
import warnings
from dataclasses import dataclass
from typing import Any, Generator, Type

import rosbag2_py
from mcap.reader import make_reader
from mcap.records import Channel, Message, Schema
from mcap_ros2.decoder import DecoderFactory
from rclpy.serialization import deserialize_message
from rosidl_generator_py.import_type_support_impl import UnsupportedTypeSupport
from rosidl_runtime_py.utilities import get_message

from ros2_always_reader.info_from_bag import get_storage_id_from_bag_folder
from ros2_always_reader.mcap_schema.coerce_dynamic_mcap_msg_to_ros2_msg import (
    CoercionOptions,
    coerce_dynamic_mcap_msg_to_ros2_msg,
)


@dataclass
class BagReadMessage:
    """Represents a deserialized bag message from a reader."""

    topic: str
    msg_type: Type
    msg: Any
    writer_timestamp: int
    bag_file_path: str
    schema: Schema | None = None


_warning_counter: dict[str, int] = {}


def _deserialize_msg(
    data: bytes,
    msg_type: Type,
    schema: Schema | None,
    coercion_options: CoercionOptions | None,
) -> Any:
    try:
        return deserialize_message(data, msg_type)
    except Exception as e:  # noqa: BLE001
        if "failed to deserialize" not in str(e):
            raise
        if schema is None:
            raise ValueError(f"Failed to deserialize message and no schema available for coercion: {e}") from e

        decoder = DecoderFactory().decoder_for("cdr", schema)
        assert decoder is not None, "Decoder should not be None when schema is available"
        dynamic_msg = decoder(data)
        if _warning_counter.get(schema.name, 0) < 1:
            _warning_counter[schema.name] = _warning_counter.get(schema.name, 0) + 1
            warnings.warn(
                f"rosbag2_py failed to deserialize message {msg_type}, but schema is available. "
                "Attempting coercion to ROS2 message using schema. "
                "It is likely that the msg definition has changed since the bag was recorded, "
                f"compare the installed msg of {schema.name} with the bag contained msg schema:\n"
                f"{schema.data.decode()}",
                stacklevel=1,
            )
        if coercion_options is None:
            msg, _ = coerce_dynamic_mcap_msg_to_ros2_msg(dynamic_msg, msg_type, strict=True)
            return msg
        msg, _ = coerce_dynamic_mcap_msg_to_ros2_msg(
            dynamic_mcap_msg=dynamic_msg,
            ros2_message_cls=msg_type,
            rename_map=coercion_options.rename_map,
            field_converters=coercion_options.field_converters,
            path_converters=coercion_options.path_converters,
            strict=coercion_options.strict,
        )
        return msg


def _coercion_options_for_topic(
    topic: str,
    coercion_options_by_topic: dict[str, CoercionOptions | dict[str, Any]] | None,
) -> CoercionOptions | None:
    if not coercion_options_by_topic:
        return None
    raw_options = coercion_options_by_topic.get(topic)
    if raw_options is None:
        return None
    if isinstance(raw_options, CoercionOptions):
        return raw_options
    if not isinstance(raw_options, dict):
        raise TypeError(
            "Coercion options must be CoercionOptions or a dict with keys: "
            "rename_map, field_converters, path_converters, strict."
        )
    allowed_keys = {"rename_map", "field_converters", "path_converters", "strict"}
    extra_keys = set(raw_options.keys()) - allowed_keys
    if extra_keys:
        raise ValueError(f"Unsupported coercion option keys for {topic}: {sorted(extra_keys)}")
    return CoercionOptions(
        rename_map=raw_options.get("rename_map"),
        field_converters=raw_options.get("field_converters"),
        path_converters=raw_options.get("path_converters"),
        strict=raw_options.get("strict", True),
    )


# inspired from https://mcap.dev/guides/python/ros2
def read_bag(
    input_bag_path: str,
    drop_unknown_msgs: bool = False,
    coercion_options_by_topic: dict[str, CoercionOptions | dict[str, Any]] | None = None,
) -> Generator[BagReadMessage, None, None]:
    """
    Creates a generator from a bag file.

    Usage:
    Read in the messages of the bag file using generator syntax:

    for bag_msg in read_bag(path):
        print(bag_msg.topic, bag_msg.writer_timestamp, bag_msg.msg)

    Args:
        input_bag_path: The path to the input bag
        drop_unknown_msgs: Drop Messages, when their message definition is not installed.
            When False, unknown messages will raise an error.
        coercion_options_by_topic: Optional per-topic coercion settings used only when
            deserialization fails for MCAP messages. Keys are topic names.
            Values can be CoercionOptions or dicts with keys: rename_map, field_converters,
            path_converters, strict. Not supported for db3 bags.

    Returns:
        Generator, that can be used to iterate through the bag file messages

    """
    storage_id = get_storage_id_from_bag_folder(input_bag_path)
    if storage_id == "sqlite3" and coercion_options_by_topic:
        raise ValueError("Coercion options are only supported for MCAP bags, not db3.")

    # figure out which files to read
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

    # We'll capture topic-to-type mapping from the first segment
    topic_types = None

    for bag_file in bag_files:
        reader_rosbag_py = rosbag2_py.SequentialReader()
        reader_rosbag_py.open(
            rosbag2_py.StorageOptions(uri=bag_file, storage_id=storage_id),
            rosbag2_py.ConverterOptions(
                input_serialization_format="cdr",
                output_serialization_format="cdr",
            ),
        )

        def iter_reader(reader: rosbag2_py.SequentialReader) -> Generator[tuple[str, bytes, int], None, None]:
            while reader.has_next():
                yield reader.read_next()

        rosbag2_py_iterator = iter_reader(reader_rosbag_py)

        # only once: grab topic<->type info
        if topic_types is None:
            topic_types = {t.name: t.type for t in reader_rosbag_py.get_all_topics_and_types()}
        mcap_reader = None
        if storage_id == "mcap":
            # for mcap, we use the mcap reader to also get the schema info, which is not available from rosbag2_py
            mcap_reader = make_reader(
                open(bag_file, "rb"),  # noqa: SIM115
            )
            mcap_iterator = mcap_reader.iter_messages()

            bag_iterator = mcap_iterator
        else:
            bag_iterator = rosbag2_py_iterator

        for bag_msg_tuple in bag_iterator:
            if (
                isinstance(bag_msg_tuple, tuple)
                and len(bag_msg_tuple) == 3
                and isinstance(bag_msg_tuple[0], str)
                and isinstance(bag_msg_tuple[1], bytes)
                and isinstance(bag_msg_tuple[2], int)
            ):
                topic, data, timestamp = bag_msg_tuple
                assert isinstance(topic, str), "Expected topic to be a string"
                assert isinstance(data, bytes), "Expected data to be bytes"
                assert isinstance(timestamp, int), "Expected timestamp to be an integer"
                schema = None
            elif (
                isinstance(bag_msg_tuple, tuple)
                and len(bag_msg_tuple) == 3
                and isinstance(bag_msg_tuple[0], Schema)
                and isinstance(bag_msg_tuple[1], Channel)
                and isinstance(bag_msg_tuple[2], Message)
            ):
                schema, channel, message = bag_msg_tuple
                assert isinstance(
                    channel, Channel
                ), f"Expected channel to be a Channel instance, but got {type(channel)}"
                assert isinstance(
                    message, Message
                ), f"Expected message to be a Message instance, but got {type(message)}"
                assert isinstance(schema, Schema), f"Expected schema to be a Schema instance, but got {type(schema)}"
                topic = channel.topic
                data = message.data
                timestamp = message.log_time
            else:
                raise ValueError(f"Unexpected bag message tuple format: {bag_msg_tuple}")
            try:
                type_name = topic_types[topic]
                msg_type = get_message(type_name)
            except KeyError:
                raise ValueError(f"Topic {topic!r} not found in bag metadata")  # noqa: B904
            # Catch Unsupported type, when msgs package is not there at all,
            # AttributeError when msgs package does not contain msg
            except (UnsupportedTypeSupport, AttributeError) as e:
                if drop_unknown_msgs:
                    # we drop messages that are not installed
                    continue
                else:
                    raise UnsupportedTypeSupport(
                        f"The msg definitions for {topic} is not installed, install it or set drop_unknown_msgs, to drop the msg."  # noqa: E501
                    ) from e
            assert isinstance(topic, str), "Expected topic to be a string"
            coercion_options = _coercion_options_for_topic(topic, coercion_options_by_topic)
            msg = _deserialize_msg(data, msg_type, schema, coercion_options)
            yield BagReadMessage(
                topic=topic,
                msg_type=msg_type,
                msg=msg,
                writer_timestamp=timestamp,
                bag_file_path=bag_file,
                schema=schema,
            )
        # clean up before the next file
        del reader_rosbag_py
        if mcap_reader is not None:
            del mcap_reader
