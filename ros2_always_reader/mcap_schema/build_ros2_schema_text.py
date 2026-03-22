"""Build MCAP-compatible concatenated ROS schema text from installed .msg files."""

import re
from pathlib import Path

from ament_index_python.packages import get_package_share_path

from ros2_always_reader.mcap_schema.get_type_string_from_class import get_type_string_from_class

_SEPARATOR = "=" * 80

_FIELD_RE = re.compile(
    r"""
    ^
    (?P<type>[A-Za-z][A-Za-z0-9_]*(?:/[A-Za-z][A-Za-z0-9_]*){0,2})
    (?P<array>\[[^\]]*\])?
    \s+
    (?P<name>[A-Za-z][A-Za-z0-9_]*)
    (?:\s+.*)?
    $
    """,
    re.VERBOSE,
)

_BUILTIN_TYPES = {
    "bool",
    "byte",
    "char",
    "float32",
    "float64",
    "int8",
    "uint8",
    "int16",
    "uint16",
    "int32",
    "uint32",
    "int64",
    "uint64",
    "string",
    "wstring",
    "time",
    "duration",
}


def _schema_to_msg_path(schema_name: str) -> Path:
    pkg, _, type_name = schema_name.split("/")
    return get_package_share_path(pkg) / "msg" / f"{type_name}.msg"


def _read_msg_text(schema_name: str) -> str:
    path = _schema_to_msg_path(schema_name)
    if not path.exists():
        raise FileNotFoundError(f"Message definition file not found: {path}")
    return path.read_text(encoding="utf-8").strip()


def _strip_comments(line: str) -> str:
    return line.split("#", 1)[0].strip()


def _parse_dependency_type_tokens(msg_text: str) -> list[str]:
    deps: list[str] = []

    for raw_line in msg_text.splitlines():
        line = _strip_comments(raw_line)
        if not line:
            continue

        # skip constants
        if "=" in line:
            continue

        m = _FIELD_RE.match(line)
        if not m:
            continue

        type_token = m.group("type")
        if type_token in _BUILTIN_TYPES:
            continue

        deps.append(type_token)

    return deps


def _resolve_dependency_type(type_token: str, current_pkg: str) -> str | None:
    if type_token in _BUILTIN_TYPES:
        return None

    if "/" not in type_token:
        if type_token == "Header":
            return "std_msgs/msg/Header"
        return f"{current_pkg}/msg/{type_token}"

    parts = type_token.split("/")
    if len(parts) == 2:
        return f"{parts[0]}/msg/{parts[1]}"
    if len(parts) == 3 and parts[1] == "msg":
        return type_token

    raise ValueError(f"Unsupported dependency type token: {type_token!r}")


def _append_dependencies(schema_name: str, seen: set[str], out: list[str]) -> None:
    msg_text = _read_msg_text(schema_name)
    current_pkg = schema_name.split("/")[0]

    ordered_deps: list[str] = []
    local_seen: set[str] = set()
    header_schema: str | None = None
    time_schema: str | None = None

    for dep_token in _parse_dependency_type_tokens(msg_text):
        dep_schema = _resolve_dependency_type(dep_token, current_pkg)
        if dep_schema is None or dep_schema in seen:
            continue

        if dep_schema in local_seen:
            continue
        local_seen.add(dep_schema)

        if dep_schema == "std_msgs/msg/Header":
            header_schema = dep_schema
            continue

        if dep_schema == "builtin_interfaces/msg/Time":
            time_schema = dep_schema
            continue

        ordered_deps.append(dep_schema)

    ordered_deps.sort()

    if header_schema is not None:
        ordered_deps.append(header_schema)
    if time_schema is not None:
        ordered_deps.append(time_schema)

    for dep_schema in ordered_deps:
        seen.add(dep_schema)

        dep_text = _read_msg_text(dep_schema)
        display_schema = dep_schema.replace("/msg/", "/")
        out.append(f"\n{_SEPARATOR}\nMSG: {display_schema}\n{dep_text}")

        _append_dependencies(dep_schema, seen, out)


def build_ros2_schema_text(ros2_msg_class: type) -> tuple[str, str]:
    """
    Build concatenated ROS schema text for the given message type, suitable for MCAP.

    Args:
        ros2_msg_class: The ROS message class to build the schema for.

            or as a class (e.g. std_msgs.msg.String).

    Returns:
        A tuple of (schema_name, schema_text),
        where schema_name is the normalized ROS message type (e.g. "std_msgs/msg/String")
        and schema_text is the concatenated text of the message definition and all its dependencies,
        separated by lines of "=".
    """
    schema_name = get_type_string_from_class(ros2_msg_class)
    top_text = _read_msg_text(schema_name)

    parts = [top_text]
    seen = {schema_name}
    _append_dependencies(schema_name, seen, parts)

    return schema_name, "\n".join(parts) + "\n"
