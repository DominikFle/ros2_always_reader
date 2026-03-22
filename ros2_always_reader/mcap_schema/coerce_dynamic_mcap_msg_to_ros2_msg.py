"""Coercion of old dynamic to current ROS messages, with renames, nested messages, sequences, and primitive coercion."""

import array
import math
import re
from dataclasses import dataclass
from typing import Any, Callable

from rosidl_runtime_py.utilities import get_message

# ----------------------------
# Type parsing
# ----------------------------

_PRIMITIVE_TYPES = {
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
}
_TYPE_ALIASES = {
    "float": "float32",
    "double": "float64",
}

_INT_TYPES = {
    "byte",
    "char",
    "int8",
    "uint8",
    "int16",
    "uint16",
    "int32",
    "uint32",
    "int64",
    "uint64",
}

_FLOAT_TYPES = {"float32", "float64"}
_STRING_TYPES = {"string", "wstring"}
_BYTE_LIKE_TYPES = {"byte", "char", "uint8", "int8"}

_SEQUENCE_RE = re.compile(r"^sequence<(.+)>$")
_BOUNDED_SEQUENCE_RE = re.compile(r"^bounded_sequence<(.+),\s*(\d+)>$")
_ARRAY_RE = re.compile(r"^(.+)\[(\d*)\]$")
_BOUNDED_STRING_RE = re.compile(r"^(w?string)<=\d+$")


@dataclass(frozen=True)
class TypeSpec:
    """
    Structured representation of a ROS message field type specification.

    kind: One of "primitive", "nested", "sequence", "bounded_sequence", "array", "sequence_legacy".
    raw: The original raw type string, e.g. "sequence<geometry_msgs/TransformStamped>"
    base: For sequences/arrays, the inner type spec raw string.
        For nested, the normalized ROS type name. None for primitives.
        E.g. "geometry_msgs/TransformStamped" for sequence<geometry_msgs/TransformStamped>,
        or "std_msgs/msg/Header" for nested std_msgs/msg/Header.
    size: For fixed-size arrays, the specified size. None for non-arrays or unbounded sequences.
    bound: For bounded sequences, the maximum number of elements. None for non-bounded sequences.
    is_primitive: True if this is a primitive type.
    is_nested: True if this is a nested ROS message type.
    """

    kind: str
    raw: str
    base: str | None = None
    size: int | None = None
    bound: int | None = None
    is_primitive: bool = False
    is_nested: bool = False


def _normalize_scalar_type_name(s: str) -> str:
    return _TYPE_ALIASES.get(s, s)


def parse_type_spec(type_str: str) -> TypeSpec:
    """
    Parse a ROS message field type string into a structured TypeSpec.

    E.g. "sequence<geometry_msgs/TransformStamped>" -
    -> TypeSpec(kind="sequence", raw="sequence<geometry_msgs/TransformStamped>", base="geometry_msgs/TransformStamped")
    """
    s = type_str.strip()

    m = _BOUNDED_SEQUENCE_RE.match(s)
    if m:
        inner = _normalize_scalar_type_name(m.group(1).strip())
        bound = int(m.group(2))
        inner_spec = parse_type_spec(inner)
        return TypeSpec(
            kind="bounded_sequence",
            raw=s,
            base=inner_spec.raw,
            bound=bound,
        )

    m = _SEQUENCE_RE.match(s)
    if m:
        inner = _normalize_scalar_type_name(m.group(1).strip())
        inner_spec = parse_type_spec(inner)
        return TypeSpec(
            kind="sequence",
            raw=s,
            base=inner_spec.raw,
        )

    m = _ARRAY_RE.match(s)
    if m:
        inner = _normalize_scalar_type_name(m.group(1).strip())
        size_txt = m.group(2)
        size = int(size_txt) if size_txt else None
        inner_spec = parse_type_spec(inner)
        return TypeSpec(
            kind="array" if size is not None else "sequence_legacy",
            raw=s,
            base=inner_spec.raw,
            size=size,
        )
    s = _normalize_scalar_type_name(s)

    if _BOUNDED_STRING_RE.match(s):
        # Treat bounded string exactly like string for coercion.
        base = "wstring" if s.startswith("wstring") else "string"
        return TypeSpec(
            kind="primitive",
            raw=s,
            base=base,
            is_primitive=True,
        )

    if s in _PRIMITIVE_TYPES:
        return TypeSpec(
            kind="primitive",
            raw=s,
            base=s,
            is_primitive=True,
        )

    # Otherwise assume nested ROS message type.
    return TypeSpec(
        kind="nested",
        raw=s,
        base=normalize_ros_type_name(s),
        is_nested=True,
    )


# ----------------------------
# ROS message helpers
# ----------------------------


def is_ros_message_instance(obj: Any) -> bool:
    """Return True if obj is an instance of a ROS 2 message class."""
    return hasattr(obj, "get_fields_and_field_types") and callable(obj.get_fields_and_field_types)


def normalize_ros_type_name(type_name: str) -> str:
    """
    Normalize a ROS type name to the form "pkg/msg/Type".

    Normalize:
      pkg/msg/Type -> pkg/msg/Type
      pkg/Type     -> pkg/msg/Type
    """
    parts = type_name.split("/")
    if len(parts) == 3 and parts[1] == "msg":
        return type_name
    if len(parts) == 2:
        return f"{parts[0]}/msg/{parts[1]}"
    raise ValueError(f"Unsupported ROS type name: {type_name!r}")


def get_field_type_map(msg: Any) -> dict[str, str]:
    """
    Return a mapping of field name to field type string for the given ROS message instance.

    E.g. for geometry_msgs/TransformStamped, return:
    {
    'header': 'std_msgs/Header',
    'child_frame_id': 'string',
    'transform': 'geometry_msgs/Transform',
    }
    """
    return dict(msg.get_fields_and_field_types())


def default_value_for_spec(spec: TypeSpec) -> Any:
    """Return a default value for the given TypeSpec."""
    if spec.kind == "primitive":
        if spec.base in _STRING_TYPES:
            return ""
        if spec.base in _FLOAT_TYPES:
            return 0.0
        if spec.base == "bool":
            return False
        if spec.base in _INT_TYPES:
            return 0
        raise NotImplementedError(f"Unsupported primitive type: {spec.base}")

    if spec.kind in {"sequence", "bounded_sequence", "sequence_legacy"}:
        return []

    if spec.kind == "array":
        assert spec.base is not None, "Array spec must have a base type"
        inner = parse_type_spec(spec.base)
        return [default_value_for_spec(inner) for _ in range(spec.size or 0)]

    if spec.kind == "nested":
        assert spec.base is not None, "Nested spec must have a base type"
        cls = get_message(spec.base)
        return cls()

    raise NotImplementedError(f"Unsupported spec kind: {spec.kind}")


# ----------------------------
# Primitive coercion
# ----------------------------


def coerce_primitive_value(value: Any, spec: TypeSpec) -> Any:
    """Coerce a value to the specified primitive type."""
    t = spec.base
    if t is None:
        raise ValueError(f"Primitive spec missing base: {spec!r}")

    if value is None:
        return default_value_for_spec(spec)

    if t in _STRING_TYPES:
        return str(value)

    if t in _FLOAT_TYPES:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return f
        return f

    if t == "bool":
        return bool(value)

    if t in _INT_TYPES:
        return int(value)

    raise NotImplementedError(f"Unsupported primitive type: {t}")


def make_array_array(typecode: str, values: list[Any]) -> array.array:
    """Create an array.array of the specified typecode from the given values."""
    arr = array.array(typecode)
    arr.extend(values)
    return arr


# ----------------------------
# Mapping / coercion
# ----------------------------

FieldConverter = Callable[[Any], Any]
FieldPathConverter = Callable[[Any, str, TypeSpec], Any]


@dataclass
class CoercionOptions:
    """
    Options for coercing old dynamic messages into current ROS messages.

    rename_map: Optional mapping of old field names to new field names. Keys are old field names,
        values are new field names. Only applied at the top level (not nested fields).
    field_converters: Optional mapping of field paths to converter functions. Keys are field paths
        (e.g. "header.stamp", "objects[0].id"), values are functions that take the old value and return
        the converted value.
    path_converters: Optional mapping of field paths to converter functions. Keys are field paths,
        values are functions that take (old_value, path, target_spec) and return the converted value.
    drop_extra_sequence_items: If True, extra items in sequences/arrays beyond the target
        size/bound will be dropped with a warning. If False, they will be kept with a warning.
    strict: If True, any coercion error will raise an exception.
        If False, coercion errors will be collected as warnings in the report.
    collect_errors: If True, coercion errors will be collected in the report.
        If False, they will be ignored (only warnings will be collected).
        This is useful when strict=False but you want to ignore certain expected
        coercion issues without even warning about them.
    """

    rename_map: dict[str, str] | None = None
    field_converters: dict[str, FieldConverter] | None = None
    path_converters: dict[str, FieldPathConverter] | None = None
    drop_extra_sequence_items: bool = True
    strict: bool = False
    collect_errors: bool = True


class CoercionError(Exception):
    """."""

    pass


class CoercionReport:
    """Report of coercion results, including warnings and errors."""

    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def warn(self, msg: str) -> None:
        """Add a warning message to the report."""
        self.warnings.append(msg)

    def error(self, msg: str) -> None:
        """Add an error message to the report."""
        self.errors.append(msg)

    def raise_if_errors(self) -> None:
        """If there are any errors in the report, raise a CoercionError with the collected error messages."""
        if self.errors:
            raise CoercionError("\n".join(self.errors))

    def __repr__(self) -> str:
        """Return a string representation of the coercion report, including warnings and errors."""
        return f"CoercionReport(warnings={self.warnings}, errors={self.errors})"

    def __str__(self) -> str:
        """Return a string representation of the coercion report, including warnings and errors."""
        return self.__repr__()


def reverse_rename_map(rename_map: dict[str, str] | None) -> dict[str, str]:
    """Reverse the rename map from old->new to new->old."""
    if not rename_map:
        return {}
    return {new: old for old, new in rename_map.items()}


def find_old_field_name(old_obj: Any, new_field: str, rev_rename: dict[str, str]) -> str | None:
    """Find the corresponding old field name for a given new field name, considering renames."""
    if new_field in rev_rename and hasattr(old_obj, rev_rename[new_field]):
        return rev_rename[new_field]
    if hasattr(old_obj, new_field):
        return new_field
    return None


def get_attr_or_key(obj: Any, name: str) -> Any:
    """Get an attribute or dict key from an object, returning None if neither exists."""
    if hasattr(obj, name):
        return getattr(obj, name)
    try:
        return obj[name]
    except Exception:
        return None


def is_sequence_like(value: Any) -> bool:
    """Return True if the value is sequence-like (list, tuple, array, bytes, or bytearray)."""
    return isinstance(value, (list, tuple, array.array, bytes, bytearray))


def coerce_value(
    old_value: Any,
    target_spec: TypeSpec,
    current_value: Any,
    path: str,
    options: CoercionOptions,
    report: CoercionReport,
) -> Any:
    """
    Coerce an old value to match the target type specification, using the provided options and reporting any issues.

    Args:
        old_value: The old value to be coerced, such as a dynamic message field value.
        target_spec: The TypeSpec describing the target type to coerce to.
        current_value: The current value of the target field, used for context in coercion (e.g. preserving byte array).
        path: The path to the field being coerced.
        options: The coercion options.
        report: The coercion report.

    Returns:
        The coerced value matching the target type specification.
    """
    if path in (options.path_converters or {}):
        assert options.path_converters is not None, "path_converters must be provided if using path-based conversion"
        return options.path_converters[path](old_value, path, target_spec)

    if target_spec.kind == "primitive":
        return coerce_primitive_value(old_value, target_spec)

    if target_spec.kind == "nested":
        if is_ros_message_instance(current_value):
            target_cls = type(current_value)
        else:
            assert target_spec.base is not None, "Nested spec must have a base type"
            target_cls = get_message(target_spec.base)
        target_obj = current_value if is_ros_message_instance(current_value) else target_cls()
        if old_value is None:
            # If the old value is None, just return the default-initialized target object (which may have defaults set).
            return target_obj
        if is_ros_message_instance(old_value) and isinstance(old_value, target_cls):
            return old_value
        if (
            not hasattr(old_value, "__slots__")
            and not hasattr(old_value, "__dict__")
            and not isinstance(old_value, dict)
        ):
            msg = f"{path}: cannot coerce scalar {type(old_value)!r} into nested {target_spec.base}"
            if options.strict:
                raise CoercionError(msg)
            report.warn(msg)
            return target_obj
        # Recursively copy fields from old to new, applying renames and conversions as needed.
        return copy_dynamic_obj_into_ros2_obj(
            old_obj=old_value,
            new_obj=target_obj,
            options=options,
            report=report,
            path_prefix=path,
        )

    if target_spec.kind in {"sequence", "bounded_sequence", "sequence_legacy", "array"}:
        assert target_spec.base is not None, "Sequence/array spec must have a base type"
        inner_spec = parse_type_spec(target_spec.base)
        if old_value is None:
            items = []
        elif (
            isinstance(old_value, (bytes, bytearray))
            and inner_spec.kind == "primitive"
            and inner_spec.base in {"uint8", "byte"}
        ) or isinstance(old_value, (array.array, list, tuple)):
            items = list(old_value)
        else:
            msg = f"{path}: expected sequence-like value for {target_spec.raw}, got {type(old_value)!r}"
            if options.strict:
                raise CoercionError(msg)
            report.warn(msg)
            items = []

        coerced_items = []
        for i, item in enumerate(items):
            item_path = f"{path}[{i}]"

            current_item = None
            if target_spec.kind == "array" and isinstance(current_value, (list, tuple)) and i < len(current_value):
                current_item = current_value[i]

            if inner_spec.kind == "nested" and current_item is None:
                assert inner_spec.base is not None, "Nested spec must have a base type"
                current_item = get_message(inner_spec.base)()

            coerced_items.append(
                coerce_value(
                    old_value=item,
                    target_spec=inner_spec,
                    current_value=current_item,
                    path=item_path,
                    options=options,
                    report=report,
                )
            )

        if target_spec.kind == "bounded_sequence" and target_spec.bound is not None:  # noqa: SIM102
            if len(coerced_items) > target_spec.bound:
                report.warn(f"{path}: truncating bounded sequence from {len(coerced_items)} to {target_spec.bound}")
                coerced_items = coerced_items[: target_spec.bound]

        if target_spec.kind == "array" and target_spec.size is not None:
            if len(coerced_items) > target_spec.size:
                if options.drop_extra_sequence_items:
                    report.warn(f"{path}: truncating fixed array from {len(coerced_items)} to {target_spec.size}")
                    coerced_items = coerced_items[: target_spec.size]
                else:
                    msg = f"{path}: fixed array too long ({len(coerced_items)} > {target_spec.size})"
                    if options.strict:
                        raise CoercionError(msg)
                    report.warn(msg)
                    coerced_items = coerced_items[: target_spec.size]

            while len(coerced_items) < target_spec.size:
                coerced_items.append(default_value_for_spec(inner_spec))

        # Preserve byte-oriented containers when possible.
        if inner_spec.kind == "primitive" and inner_spec.base in {"uint8", "byte"}:
            if isinstance(current_value, bytes):
                return bytes(int(x) & 0xFF for x in coerced_items)
            if isinstance(current_value, bytearray):
                return bytearray(int(x) & 0xFF for x in coerced_items)

        if inner_spec.kind == "primitive" and inner_spec.base == "int8":  # noqa: SIM102
            if isinstance(current_value, array.array) and current_value.typecode == "b":
                return make_array_array("b", [int(x) for x in coerced_items])

        if inner_spec.kind == "primitive" and inner_spec.base in {"uint8", "byte"}:  # noqa: SIM102
            if isinstance(current_value, array.array) and current_value.typecode == "B":
                return make_array_array("B", [int(x) & 0xFF for x in coerced_items])

        if isinstance(current_value, array.array):
            # Best-effort preservation of other array-backed primitive sequences.
            return make_array_array(current_value.typecode, coerced_items)

        return coerced_items

    raise NotImplementedError(f"{path}: unsupported target spec kind {target_spec.kind!r}")


def copy_dynamic_obj_into_ros2_obj(
    old_obj: Any,
    new_obj: Any,
    options: CoercionOptions | None = None,
    report: CoercionReport | None = None,
    path_prefix: str = "",
) -> Any:
    """
    Recursively copy/coerce fields from an dynamic object into a real ROS 2 message.

    Supports:
      - renamed fields
      - nested messages
      - sequences / fixed arrays / bounded sequences
      - primitive coercion
      - byte arrays
      - custom per-field converters

    `field_converters` keys are destination field paths, e.g.:
      "header.stamp"
      "objects[0].id"   # typically prefer path_converters for arrays
      "velocity"

    `path_converters` are called first and receive:
      (old_value, path, target_spec) -> converted_value
    """
    options = options or CoercionOptions()
    report = report or CoercionReport()

    if not is_ros_message_instance(new_obj):
        raise TypeError(f"new_obj must be a ROS 2 message instance, got {type(new_obj)!r}")

    rev_rename = reverse_rename_map(options.rename_map)
    field_types = get_field_type_map(new_obj)

    for new_field, type_str in field_types.items():
        path = f"{path_prefix}.{new_field}" if path_prefix else new_field
        target_spec = parse_type_spec(type_str)

        old_field = find_old_field_name(old_obj, new_field, rev_rename)
        if old_field is None:
            # Field only exists in new schema -> keep default.
            continue

        old_value = get_attr_or_key(old_obj, old_field)
        current_value = getattr(new_obj, new_field)

        if path in (options.field_converters or {}):
            try:
                assert (
                    options.field_converters is not None
                ), "field_converters must be provided if using field-based conversion"
                converted = options.field_converters[path](old_value)
                setattr(new_obj, new_field, converted)
            except Exception as e:
                msg = f"{path}: field converter failed: {e}"
                if options.strict:
                    raise CoercionError(msg) from e
                report.error(msg)
            continue

        try:
            converted = coerce_value(
                old_value=old_value,
                target_spec=target_spec,
                current_value=current_value,
                path=path,
                options=options,
                report=report,
            )
            setattr(new_obj, new_field, converted)
        except Exception as e:
            msg = f"{path}: coercion failed ({type(e).__name__}: {e})"
            if options.strict:
                raise CoercionError(msg) from e
            if options.collect_errors:
                report.error(msg)

    return new_obj


def coerce_dynamic_mcap_msg_to_ros2_msg(
    dynamic_mcap_msg: Any,
    ros2_message_cls: type,
    rename_map: dict[str, str] | None = None,
    field_converters: dict[str, FieldConverter] | None = None,
    path_converters: dict[str, FieldPathConverter] | None = None,
    strict: bool = False,
) -> tuple[Any, CoercionReport]:
    """
    Migrate an dynamic_mcap_msg (e.g. from MCAP) into a  ROS2 message instance, with coercion and reporting.

    Args:
        dynamic_mcap_msg: The old dynamic message object to migrate
            Obtained from decoding MCAP bytes with the original mcap schema.
        ros2_message_cls: The target ROS2 message class to migrate into (e.g. std_msgs.msg.String).
        rename_map: Optional mapping of old field names to new field names for top-level fields.
        field_converters: Optional mapping of field paths to converter functions for specific fields.
        path_converters: Optional mapping of field paths to converter functions that receive
            (old_value, path, target_spec).
        strict: If True, any coercion error will raise an exception.
            If False, coercion errors will be collected as warnings in the report.

    Returns:
        A tuple of (current_msg, report)
            where current_msg is the migrated ROS message instance
            and report is a CoercionReport containing any warnings or errors that occurred during coercion.
    """
    report = CoercionReport()
    options = CoercionOptions(
        rename_map=rename_map,
        field_converters=field_converters,
        path_converters=path_converters,
        strict=strict,
        collect_errors=True,
    )
    current_msg = ros2_message_cls()
    current_msg = copy_dynamic_obj_into_ros2_obj(
        old_obj=dynamic_mcap_msg,
        new_obj=current_msg,
        options=options,
        report=report,
    )
    if strict:
        report.raise_if_errors()
    return current_msg, report


if __name__ == "__main__":
    pass
    # def decode_old_with_mcap_dynamic(schema_name: str, schema_text: str, raw_bytes: bytes):
    #     from mcap_ros2._dynamic import generate_dynamic

    #     decoders = generate_dynamic(schema_name, schema_text)
    #     decoder = decoders[schema_name]
    #     return decoder(raw_bytes)

    # def migrate_old_bytes_to_current_ros_bytes(
    #     *,
    #     old_schema_name: str,
    #     old_schema_text: str,
    #     old_raw_bytes: bytes,
    #     current_msg_type,
    #     rename_map: dict[str, str] | None = None,
    #     field_converters: dict[str, FieldConverter] | None = None,
    #     path_converters: dict[str, FieldPathConverter] | None = None,
    #     strict: bool = False,
    # ):
    #     from rclpy.serialization import serialize_message, deserialize_message

    #     old_dynamic = decode_old_with_mcap_dynamic(
    #         schema_name=old_schema_name,
    #         schema_text=old_schema_text,
    #         raw_bytes=old_raw_bytes,
    #     )

    #     current_msg, report = migrate_dynamic_to_current_ros(
    #         old_dynamic_msg=old_dynamic,
    #         current_msg_type=current_msg_type,
    #         rename_map=rename_map,
    #         field_converters=field_converters,
    #         path_converters=path_converters,
    #         strict=strict,
    #     )

    #     current_raw = serialize_message(current_msg)

    #     # Optional validation roundtrip.
    #     validated = deserialize_message(current_raw, current_msg_type)

    #     return {
    #         "old_dynamic": old_dynamic,
    #         "current_msg": current_msg,
    #         "current_raw": current_raw,
    #         "validated": validated,
    #         "report": report,
    #     }
