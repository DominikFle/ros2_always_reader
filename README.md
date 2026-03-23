# ros2_always_reader

Read ROS2 bag files with a tiny, generator-based API. Works with MCAP and sqlite3 bags, supports multiple bag segments, and can optionally drop messages whose types are not installed. Can read messages even though the installed message definition is newer than the one in the bag by coercing using the self-contained schema stored in the bag (MCAP only).

## Why this package

- Read all bag segments in a folder (db3 or MCAP).
- Keep reading even when the installed ROS2 message definition is newer than the one recorded in the bag by coercing MCAP messages using the stored schema.
- Drop messages for unknown types with a single flag.
##Installation
Use rosdep to install pcakage.xml deps.

Additionally install `mcap` and `mcap-ros2-support` using e.g. `pip`.
## Usage

```python
from ros2_always_reader.read_bag import read_bag

for bag_msg in read_bag(
    "/path/to/bag",
    drop_unknown_msgs=True,
):
    print(bag_msg.topic, bag_msg.writer_timestamp, type(bag_msg.msg))
```
### Coercion for newer message definitions (MCAP only)

If a MCAP bag was recorded with an older message definition, deserialization can fail. This package can fall back to the schema embedded in the bag and coerce the message into the currently installed ROS2 type.

```python
from ros2_always_reader.read_bag import read_bag

# If deserialization fails, the MCAP schema is used to coerce into the installed type.
for bag_msg in read_bag("/path/to/bag", drop_unknown_msgs=False):
    ...
```

You can also pass per-topic coercion options when needed:

```python
from ros2_always_reader.read_bag import read_bag
from ros2_always_reader.mcap_schema.coerce_dynamic_mcap_msg_to_ros2_msg import CoercionOptions

options_by_topic = {
    "/my_topic": CoercionOptions(
        rename_map={"old_field": "new_field"},
        strict=False,
    ),
}

for bag_msg in read_bag(
    "/path/to/bag",
    coercion_options_by_topic=options_by_topic,
):
    ...
```

