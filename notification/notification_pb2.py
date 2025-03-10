# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: notification.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='notification.proto',
  package='notification',
  syntax='proto3',
  serialized_options=b'\n\035io.grpc.examples.notificationB\014notificationP\001\242\002\003HLW',
  serialized_pb=b'\n\x12notification.proto\x12\x0cnotification\"(\n\x13NotificationRequest\x12\x11\n\tbody_data\x18\x01 \x01(\t\"$\n\x11NotificationReply\x12\x0f\n\x07message\x18\x01 \x01(\t\",\n\x17NotificationLogsRequest\x12\x11\n\tjson_data\x18\x01 \x01(\t\"(\n\x15NotificationLogsReply\x12\x0f\n\x07message\x18\x01 \x01(\t2\xc5\x01\n\x07Greeter\x12X\n\x10SendNotification\x12!.notification.NotificationRequest\x1a\x1f.notification.NotificationReply\"\x00\x12`\n\x10NotificationLogs\x12%.notification.NotificationLogsRequest\x1a#.notification.NotificationLogsReply\"\x00\x42\x35\n\x1dio.grpc.examples.notificationB\x0cnotificationP\x01\xa2\x02\x03HLWb\x06proto3'
)




_NOTIFICATIONREQUEST = _descriptor.Descriptor(
  name='NotificationRequest',
  full_name='notification.NotificationRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='body_data', full_name='notification.NotificationRequest.body_data', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=36,
  serialized_end=76,
)


_NOTIFICATIONREPLY = _descriptor.Descriptor(
  name='NotificationReply',
  full_name='notification.NotificationReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='message', full_name='notification.NotificationReply.message', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=78,
  serialized_end=114,
)


_NOTIFICATIONLOGSREQUEST = _descriptor.Descriptor(
  name='NotificationLogsRequest',
  full_name='notification.NotificationLogsRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='json_data', full_name='notification.NotificationLogsRequest.json_data', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=116,
  serialized_end=160,
)


_NOTIFICATIONLOGSREPLY = _descriptor.Descriptor(
  name='NotificationLogsReply',
  full_name='notification.NotificationLogsReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='message', full_name='notification.NotificationLogsReply.message', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=162,
  serialized_end=202,
)

DESCRIPTOR.message_types_by_name['NotificationRequest'] = _NOTIFICATIONREQUEST
DESCRIPTOR.message_types_by_name['NotificationReply'] = _NOTIFICATIONREPLY
DESCRIPTOR.message_types_by_name['NotificationLogsRequest'] = _NOTIFICATIONLOGSREQUEST
DESCRIPTOR.message_types_by_name['NotificationLogsReply'] = _NOTIFICATIONLOGSREPLY
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

NotificationRequest = _reflection.GeneratedProtocolMessageType('NotificationRequest', (_message.Message,), {
  'DESCRIPTOR' : _NOTIFICATIONREQUEST,
  '__module__' : 'notification_pb2'
  # @@protoc_insertion_point(class_scope:notification.NotificationRequest)
  })
_sym_db.RegisterMessage(NotificationRequest)

NotificationReply = _reflection.GeneratedProtocolMessageType('NotificationReply', (_message.Message,), {
  'DESCRIPTOR' : _NOTIFICATIONREPLY,
  '__module__' : 'notification_pb2'
  # @@protoc_insertion_point(class_scope:notification.NotificationReply)
  })
_sym_db.RegisterMessage(NotificationReply)

NotificationLogsRequest = _reflection.GeneratedProtocolMessageType('NotificationLogsRequest', (_message.Message,), {
  'DESCRIPTOR' : _NOTIFICATIONLOGSREQUEST,
  '__module__' : 'notification_pb2'
  # @@protoc_insertion_point(class_scope:notification.NotificationLogsRequest)
  })
_sym_db.RegisterMessage(NotificationLogsRequest)

NotificationLogsReply = _reflection.GeneratedProtocolMessageType('NotificationLogsReply', (_message.Message,), {
  'DESCRIPTOR' : _NOTIFICATIONLOGSREPLY,
  '__module__' : 'notification_pb2'
  # @@protoc_insertion_point(class_scope:notification.NotificationLogsReply)
  })
_sym_db.RegisterMessage(NotificationLogsReply)


DESCRIPTOR._options = None

_GREETER = _descriptor.ServiceDescriptor(
  name='Greeter',
  full_name='notification.Greeter',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  serialized_start=205,
  serialized_end=402,
  methods=[
  _descriptor.MethodDescriptor(
    name='SendNotification',
    full_name='notification.Greeter.SendNotification',
    index=0,
    containing_service=None,
    input_type=_NOTIFICATIONREQUEST,
    output_type=_NOTIFICATIONREPLY,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='NotificationLogs',
    full_name='notification.Greeter.NotificationLogs',
    index=1,
    containing_service=None,
    input_type=_NOTIFICATIONLOGSREQUEST,
    output_type=_NOTIFICATIONLOGSREPLY,
    serialized_options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_GREETER)

DESCRIPTOR.services_by_name['Greeter'] = _GREETER

# @@protoc_insertion_point(module_scope)
