# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function, unicode_literals

from collections import Mapping, namedtuple

from marshmallow import Schema, fields, post_dump, post_load, pre_dump

from prospyr.fields import Email


class TrimSchema(Schema):
    """
    A schema which does not dump "empty" fields.

    ...where empty is defined as None for value fields, and an empty list for
    collection fields.

    This is in line with the behaviour ProsperWorks expects.
    """

    @post_dump
    def clean_empty(self, data):
        to_clean = []
        for key, value in data.items():
            collection = getattr(self.fields[key], 'many', False)
            if collection and value == []:
                to_clean.append(key)
            elif value is None:
                to_clean.append(key)

        for key in to_clean:
            data.pop(key)

        return data


class NamedTupleSchema(Schema):
    """
    (De)serialise to namedtuple instead of dict
    """

    def __init__(self, *args, **kwargs):
        super(NamedTupleSchema, self).__init__(*args, **kwargs)
        name = type(self).__name__.replace('Schema', '')
        fields = self.declared_fields.keys()
        self.namedtuple_class = namedtuple(name, fields)

    @post_load
    def to_namedtuple(self, data):
        return self.namedtuple_class(**data)

    @pre_dump
    def from_namedtuple(self, obj):
        if not isinstance(obj, Mapping):
            return obj._asdict()
        return obj


class EmailSchema(NamedTupleSchema):
    email = Email()
    category = fields.String()


class WebsiteSchema(NamedTupleSchema):
    url = fields.String()  # PW does not validate URLs so neither do we
    category = fields.String(allow_none=True)


class SocialSchema(NamedTupleSchema):
    url = fields.String()  # PW does not validate URLs so neither do we
    category = fields.String(allow_none=True)


class PhoneNumberSchema(NamedTupleSchema):
    number = fields.String()
    category = fields.String()


class CustomFieldSchema(NamedTupleSchema):
    custom_field_definition_id = fields.Integer(attribute="id")
    value = fields.Raw(allow_none=True)  # TODO base this on field definition


class CustomFieldOptionSchema(Schema):
    id = fields.Number()
    rank = fields.Number()
    name = fields.String()


class AddressSchema(Schema):
    street = fields.String(allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    postal_code = fields.String(allow_none=True)
    country = fields.String(allow_none=True)


class PipelineStageSchema(NamedTupleSchema):
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    win_probability = fields.Integer()


class RelatedResourceSchema(Schema):
    id = fields.Integer(required=True)
    type = fields.String(required=True)
