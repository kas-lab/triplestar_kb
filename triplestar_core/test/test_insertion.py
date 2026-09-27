from jinja2 import Template
from triplestar_core.insertion import apply_insertion_template


def test_apply_insertion_template_maps_context_to_update():
    updates = []
    template = Template('response={{ response.value }} request={{ request.name }}')

    emitted = apply_insertion_template(
        template,
        updates.append,
        request=type('Request', (), {'name': 'robot'})(),
        response=type('Response', (), {'value': 42})(),
    )

    assert emitted is True
    assert updates == ['response=42 request=robot']


def test_apply_insertion_template_skips_empty_output():
    updates = []

    emitted = apply_insertion_template(Template(''), updates.append, response=object())

    assert emitted is False
    assert updates == []
