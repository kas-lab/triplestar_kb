import base64
from collections.abc import Callable

from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import StrictUndefined
from jinja2 import Template
from rclpy.serialization import serialize_message
from triplestar_core.conversions import to_rdf_literal


def _serialize_filter(value) -> str:
    return base64.b64encode(serialize_message(value)).decode('utf-8')


def _rdf_filter(value) -> str:
    literal = to_rdf_literal(value)
    return str(literal) if literal is not None else repr(value)


def make_insertion_environment(templates_dir) -> Environment:
    """Build the Jinja environment shared by every insertion source."""
    env = Environment(
        loader=FileSystemLoader(templates_dir),
        autoescape=False,
        undefined=StrictUndefined,
    )
    env.filters['rdf'] = _rdf_filter
    env.filters['serialize'] = _serialize_filter
    return env


def apply_insertion_template(
    template: Template,
    update_fn: Callable[[str], None],
    **context,
) -> bool:
    """Render and apply an insertion template, returning whether it emitted an update."""
    query = template.render(**context)
    if not query:
        return False
    update_fn(query)
    return True
